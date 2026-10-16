package memory

import (
	"context"
	"errors"
	"sync"
	"time"

	"telesrv/internal/domain"
)

// defaultChatStreakListLimit mirrors the PostgreSQL path's behaviour of
// resolving an unset limit to a finite page.
const defaultChatStreakListLimit = 50

// ChatStreakStore is the in-memory implementation of store.ChatStreakStore.
//
// It reproduces the invariants migration 20261014020000 encodes:
//
//   - chat_streaks is keyed by (user_a, user_b) with user_a < user_b, so one
//     row per pair regardless of who wrote first.
//   - last_day is nullable and NULL means "never counted", distinct from day
//     zero of a broken streak.
//   - days and frozen_days stay inside their CHECK ranges.
//
// A single mutex replaces the SELECT ... FOR UPDATE the SQL store takes: the
// advance reads and writes under one lock, which is the property that makes two
// simultaneous messages count once instead of twice.
type ChatStreakStore struct {
	mu      sync.Mutex
	streaks map[chatStreakKey]domain.ChatStreak
}

type chatStreakKey struct {
	userA int64
	userB int64
}

// NewChatStreakStore builds an empty in-memory streak store.
func NewChatStreakStore() *ChatStreakStore {
	return &ChatStreakStore{streaks: map[chatStreakKey]domain.ChatStreak{}}
}

func chatStreakKeyOf(a, b int64) chatStreakKey {
	return chatStreakKey{userA: a, userB: b}
}

func chatStreakPage(limit int) int {
	if limit <= 0 || limit > domain.MaxChatStreakListLimit {
		if limit <= 0 {
			return defaultChatStreakListLimit
		}
		return domain.MaxChatStreakListLimit
	}
	return limit
}

// AdvanceChatStreak applies one private message between the pair.
func (s *ChatStreakStore) AdvanceChatStreak(_ context.Context, userA, userB int64, now time.Time) (domain.ChatStreak, domain.ChatStreakOutcome, error) {
	if s == nil {
		return domain.ChatStreak{}, domain.ChatStreakUnchanged, errors.New("chat streak store is not configured")
	}
	a, b, err := domain.NormalizeChatStreakPeers(userA, userB)
	if err != nil {
		return domain.ChatStreak{}, domain.ChatStreakUnchanged, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := chatStreakKeyOf(a, b)
	prev, found := s.streaks[key]
	if !found {
		prev = domain.ChatStreak{UserA: a, UserB: b}
	}
	next, outcome := domain.AdvanceChatStreak(prev, now)
	if outcome == domain.ChatStreakUnchanged {
		// Same rule as the SQL path: an already-counted day must not bump
		// updated_at or version, or a chatty pair would keep every recency
		// column on the dashboard warm forever.
		return prev, domain.ChatStreakUnchanged, nil
	}
	next.Version = prev.Version + 1
	if next.Version <= 0 {
		next.Version = 1
	}
	s.streaks[key] = next
	return next, outcome, nil
}

// ChatStreak returns the stored state for a pair.
func (s *ChatStreakStore) ChatStreak(_ context.Context, userA, userB int64) (domain.ChatStreak, error) {
	if s == nil {
		return domain.ChatStreak{}, errors.New("chat streak store is not configured")
	}
	a, b, err := domain.NormalizeChatStreakPeers(userA, userB)
	if err != nil {
		return domain.ChatStreak{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	streak, found := s.streaks[chatStreakKeyOf(a, b)]
	if !found {
		return domain.ChatStreak{}, domain.ErrChatStreakNotFound
	}
	return streak, nil
}

// StrongestChatStreak returns the pair's streak, in any order.
func (s *ChatStreakStore) StrongestChatStreak(_ context.Context, viewerUserID, targetUserID int64) (domain.ChatStreak, error) {
	if s == nil {
		return domain.ChatStreak{}, errors.New("chat streak store is not configured")
	}
	a, b, err := domain.NormalizeChatStreakPeers(viewerUserID, targetUserID)
	if err != nil {
		return domain.ChatStreak{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	streak, found := s.streaks[chatStreakKeyOf(a, b)]
	if !found || streak.Days <= 0 || streak.LastDay.IsZero() {
		return domain.ChatStreak{}, domain.ErrChatStreakNotFound
	}
	return streak, nil
}

// ListUserChatStreaks returns every streak one account is part of, longest
// first. Both sides are walked because the row does not record who started it.
func (s *ChatStreakStore) ListUserChatStreaks(_ context.Context, userID int64, limit int) ([]domain.ChatStreak, error) {
	if s == nil {
		return nil, errors.New("chat streak store is not configured")
	}
	if userID <= 0 || domain.IsSystemUserID(userID) {
		return nil, domain.ErrChatStreakInvalid
	}
	s.mu.Lock()
	out := []domain.ChatStreak{}
	for _, streak := range s.streaks {
		if streak.UserA == userID || streak.UserB == userID {
			out = append(out, streak)
		}
	}
	s.mu.Unlock()
	domain.SortChatStreaksForDisplay(out)
	return chatStreakTruncate(out, chatStreakPage(limit)), nil
}

// TopChatStreaks is the live leaderboard, longest first.
func (s *ChatStreakStore) TopChatStreaks(_ context.Context, limit int) ([]domain.ChatStreak, error) {
	if s == nil {
		return nil, errors.New("chat streak store is not configured")
	}
	s.mu.Lock()
	out := []domain.ChatStreak{}
	for _, streak := range s.streaks {
		if streak.LastDay.IsZero() || streak.Days <= 0 {
			continue
		}
		out = append(out, streak)
	}
	s.mu.Unlock()
	domain.SortChatStreaksForDisplay(out)
	return chatStreakTruncate(out, chatStreakPage(limit)), nil
}

// GrantChatStreakFreeze tops one pair's skip budget up, creating the row when
// the pair has never talked so the grant is waiting for its first message.
func (s *ChatStreakStore) GrantChatStreakFreeze(_ context.Context, userA, userB int64, days int, actor, reason string) (domain.ChatStreak, error) {
	if s == nil {
		return domain.ChatStreak{}, errors.New("chat streak store is not configured")
	}
	a, b, err := domain.NormalizeChatStreakPeers(userA, userB)
	if err != nil {
		return domain.ChatStreak{}, err
	}
	if days <= 0 || days > domain.MaxChatStreakFreeze {
		return domain.ChatStreak{}, domain.ErrChatStreakFreezeInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := chatStreakKeyOf(a, b)
	prev, found := s.streaks[key]
	if !found {
		next, err := domain.NewChatStreak(a, b, days, time.Now())
		if err != nil {
			return domain.ChatStreak{}, err
		}
		next.Version = 1
		next.UpdatedAt = time.Now().UTC()
		s.streaks[key] = next
		return next, nil
	}
	next, err := domain.GrantChatStreakFreeze(prev, days)
	if err != nil {
		return domain.ChatStreak{}, err
	}
	next.Version = prev.Version + 1
	if next.Version <= 0 {
		next.Version = 1
	}
	next.UpdatedAt = time.Now().UTC()
	s.streaks[key] = next
	return next, nil
}

// GrantUserChatStreakFreeze applies the grant to every live streak of one
// account. Zero touched rows is a normal answer.
func (s *ChatStreakStore) GrantUserChatStreakFreeze(_ context.Context, userID int64, days int, actor, reason string) (int, error) {
	if s == nil {
		return 0, errors.New("chat streak store is not configured")
	}
	if userID <= 0 || domain.IsSystemUserID(userID) {
		return 0, domain.ErrChatStreakInvalid
	}
	if days <= 0 || days > domain.MaxChatStreakFreeze {
		return 0, domain.ErrChatStreakFreezeInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	granted := 0
	for key, streak := range s.streaks {
		if streak.LastDay.IsZero() {
			continue
		}
		if streak.UserA != userID && streak.UserB != userID {
			continue
		}
		next, err := domain.GrantChatStreakFreeze(streak, days)
		if err != nil {
			continue
		}
		next.Version = streak.Version + 1
		next.UpdatedAt = time.Now().UTC()
		s.streaks[key] = next
		granted++
	}
	return granted, nil
}

// chatStreakTruncate keeps the memory store honest about the SQL LIMIT without
// letting a negative or absent limit through as "everything".
func chatStreakTruncate(rows []domain.ChatStreak, limit int) []domain.ChatStreak {
	if limit > 0 && len(rows) > limit {
		return rows[:limit]
	}
	return rows
}
