package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

// ChatStreakStore is the Postgres half of the "огонёк" read model.
//
// The whole mechanic is a single row per pair, and the only concurrency hazard
// is the obvious one: two people writing to each other in the same second both
// try to advance the same row. Every read here is therefore SELECT ... FOR
// UPDATE inside a transaction, the decision is taken by
// domain.AdvanceChatStreak -- the one place the rules live -- and the write
// follows it. No branch of the ladder is re-expressed in SQL, because two
// copies of "is the gap covered by the budget" is two copies that eventually
// disagree.
type ChatStreakStore struct {
	db sqlcgen.DBTX
}

// NewChatStreakStore builds the store over an already-migrated pool.
func NewChatStreakStore(db sqlcgen.DBTX) *ChatStreakStore {
	return &ChatStreakStore{db: db}
}

// chatStreakColumns is the projection every read shares, in the order
// scanChatStreak expects.
const chatStreakColumns = `user_a, user_b, days, last_day, frozen_days, updated_at, version`

// scanChatStreak maps one row onto the domain type.
//
// last_day arrives as a date and is handed back as Go's zero time when it is
// NULL, so domain.AdvanceChatStreak sees exactly the "never counted" state it
// branches on. A date column cannot represent Go's zero instant, which is why
// the mapping cannot be left to the driver.
func scanChatStreak(row pgx.Row) (domain.ChatStreak, error) {
	var out domain.ChatStreak
	var lastDay *time.Time
	if err := row.Scan(&out.UserA, &out.UserB, &out.Days, &lastDay, &out.FrozenDays, &out.UpdatedAt, &out.Version); err != nil {
		return domain.ChatStreak{}, err
	}
	if lastDay != nil {
		out.LastDay = lastDay.UTC()
	}
	return out, nil
}

func scanChatStreakRows(rows pgx.Rows) ([]domain.ChatStreak, error) {
	defer rows.Close()
	out := []domain.ChatStreak{}
	for rows.Next() {
		streak, err := scanChatStreak(rows)
		if err != nil {
			return nil, fmt.Errorf("scan chat streak: %w", err)
		}
		out = append(out, streak)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan chat streaks: %w", err)
	}
	return out, nil
}

func chatStreakLastDayArg(streak domain.ChatStreak) any {
	if streak.LastDay.IsZero() {
		return nil
	}
	return streak.LastDay
}

func chatStreakLimit(limit int) int {
	if limit <= 0 || limit > domain.MaxChatStreakListLimit {
		return domain.MaxChatStreakListLimit
	}
	return limit
}

// lockChatStreakTx reads a pair for update.
//
// The FOR UPDATE is the whole reason this is separate from scanChatStreak:
// without it two advances could both read days=3 and both write days=4, and one
// of the two days the pair actually talked would vanish.
func lockChatStreakTx(ctx context.Context, tx pgx.Tx, a, b int64) (domain.ChatStreak, bool, error) {
	streak, err := scanChatStreak(tx.QueryRow(ctx, `
SELECT `+chatStreakColumns+` FROM chat_streaks WHERE user_a = $1 AND user_b = $2 FOR UPDATE`, a, b))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ChatStreak{}, false, nil
	}
	if err != nil {
		return domain.ChatStreak{}, false, fmt.Errorf("lock chat streak: %w", err)
	}
	return streak, true, nil
}

// AdvanceChatStreak applies one private message between the pair.
//
// The transaction is what makes the mechanic correct under concurrency: the row
// lock serialises two simultaneous messages, and the second one then reads a
// last_day it has already caught up to and reports unchanged instead of
// advancing the counter twice.
func (s *ChatStreakStore) AdvanceChatStreak(ctx context.Context, userA, userB int64, now time.Time) (domain.ChatStreak, domain.ChatStreakOutcome, error) {
	if s == nil || s.db == nil {
		return domain.ChatStreak{}, domain.ChatStreakUnchanged, fmt.Errorf("chat streak store is not configured")
	}
	a, b, err := domain.NormalizeChatStreakPeers(userA, userB)
	if err != nil {
		return domain.ChatStreak{}, domain.ChatStreakUnchanged, err
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.ChatStreak{}, domain.ChatStreakUnchanged, fmt.Errorf("advance chat streak: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.ChatStreak{}, domain.ChatStreakUnchanged, fmt.Errorf("begin advance chat streak: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	prev, found, err := lockChatStreakTx(ctx, tx, a, b)
	if err != nil {
		return domain.ChatStreak{}, domain.ChatStreakUnchanged, err
	}
	if !found {
		next, outcome := domain.AdvanceChatStreak(domain.ChatStreak{UserA: a, UserB: b}, now)
		// First ever message for this pair. There is nothing to lock, so the
		// insert can race with a concurrent first message from the other side.
		tag, err := tx.Exec(ctx, `
INSERT INTO chat_streaks (user_a, user_b, days, last_day, frozen_days, updated_at, version)
VALUES ($1, $2, $3, $4, $5, $6, 1)
ON CONFLICT (user_a, user_b) DO NOTHING`,
			a, b, next.Days, chatStreakLastDayArg(next), next.FrozenDays, next.UpdatedAt.UTC())
		if err != nil {
			return domain.ChatStreak{}, domain.ChatStreakUnchanged, fmt.Errorf("insert chat streak: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// We lost the race: the other side's transaction inserted the pair's
			// first row while we were waiting on it. Reporting our own computed
			// "started" here would hand the caller two announcements for one day,
			// because the winner is announcing too -- so the streak feature would
			// greet people twice the first time they ever write. Re-read what
			// actually landed and report against that instead; same day, so it
			// comes back unchanged and stays silent.
			stored, _, err := lockChatStreakTx(ctx, tx, a, b)
			if err != nil {
				return domain.ChatStreak{}, domain.ChatStreakUnchanged, err
			}
			if err := tx.Commit(ctx); err != nil {
				return domain.ChatStreak{}, domain.ChatStreakUnchanged, fmt.Errorf("commit lost chat streak race: %w", err)
			}
			committed = true
			actual, actualOutcome := domain.AdvanceChatStreak(stored, now)
			return actual, actualOutcome, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.ChatStreak{}, domain.ChatStreakUnchanged, fmt.Errorf("commit chat streak: %w", err)
		}
		committed = true
		return next, outcome, nil
	}

	next, outcome := domain.AdvanceChatStreak(prev, now)
	if outcome == domain.ChatStreakUnchanged {
		// Commit rather than roll back: the lock is released either way, and an
		// unchanged streak must not bump updated_at or version -- a chatty pair
		// would otherwise look "recently active" on every dashboard that reads
		// those columns, and the leaderboard's recency column would be noise.
		if err := tx.Commit(ctx); err != nil {
			return domain.ChatStreak{}, domain.ChatStreakUnchanged, fmt.Errorf("commit unchanged chat streak: %w", err)
		}
		committed = true
		return prev, domain.ChatStreakUnchanged, nil
	}
	if _, err := tx.Exec(ctx, `
UPDATE chat_streaks
SET days = $3, last_day = $4, frozen_days = $5, updated_at = $6, version = version + 1
WHERE user_a = $1 AND user_b = $2`,
		a, b, next.Days, chatStreakLastDayArg(next), next.FrozenDays, next.UpdatedAt.UTC()); err != nil {
		return domain.ChatStreak{}, domain.ChatStreakUnchanged, fmt.Errorf("update chat streak: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ChatStreak{}, domain.ChatStreakUnchanged, fmt.Errorf("commit chat streak: %w", err)
	}
	committed = true
	next.Version = prev.Version + 1
	return next, outcome, nil
}

// ChatStreak returns the stored state for a pair.
func (s *ChatStreakStore) ChatStreak(ctx context.Context, userA, userB int64) (domain.ChatStreak, error) {
	if s == nil || s.db == nil {
		return domain.ChatStreak{}, fmt.Errorf("chat streak store is not configured")
	}
	a, b, err := domain.NormalizeChatStreakPeers(userA, userB)
	if err != nil {
		return domain.ChatStreak{}, err
	}
	streak, err := scanChatStreak(s.db.QueryRow(ctx, `
SELECT `+chatStreakColumns+` FROM chat_streaks WHERE user_a = $1 AND user_b = $2`, a, b))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ChatStreak{}, domain.ErrChatStreakNotFound
	}
	if err != nil {
		return domain.ChatStreak{}, fmt.Errorf("chat streak: %w", err)
	}
	return streak, nil
}

// StrongestChatStreak returns the longest live streak one viewer shares with
// one target. Both orderings are tried because the pair is stored normalised and
// the caller thinks in terms of "me and them", not "lower id and higher id".
func (s *ChatStreakStore) StrongestChatStreak(ctx context.Context, viewerUserID, targetUserID int64) (domain.ChatStreak, error) {
	if s == nil || s.db == nil {
		return domain.ChatStreak{}, fmt.Errorf("chat streak store is not configured")
	}
	a, b, err := domain.NormalizeChatStreakPeers(viewerUserID, targetUserID)
	if err != nil {
		return domain.ChatStreak{}, err
	}
	streak, err := scanChatStreak(s.db.QueryRow(ctx, `
SELECT `+chatStreakColumns+` FROM chat_streaks
WHERE user_a = $1 AND user_b = $2 AND last_day IS NOT NULL AND days > 0`, a, b))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ChatStreak{}, domain.ErrChatStreakNotFound
	}
	if err != nil {
		return domain.ChatStreak{}, fmt.Errorf("strongest chat streak: %w", err)
	}
	return streak, nil
}

// ListUserChatStreaks returns every streak one account is part of, longest
// first. Both sides of the pair are queried because the row does not record who
// started the streak, so "mine" cannot be expressed as one column.
func (s *ChatStreakStore) ListUserChatStreaks(ctx context.Context, userID int64, limit int) ([]domain.ChatStreak, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("chat streak store is not configured")
	}
	if userID <= 0 || domain.IsSystemUserID(userID) {
		return nil, domain.ErrChatStreakInvalid
	}
	rows, err := s.db.Query(ctx, `
SELECT `+chatStreakColumns+` FROM chat_streaks
WHERE user_a = $1 OR user_b = $1
ORDER BY days DESC, frozen_days DESC, user_a, user_b
LIMIT $2`, userID, chatStreakLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("list user chat streaks: %w", err)
	}
	out, err := scanChatStreakRows(rows)
	if err != nil {
		return nil, fmt.Errorf("list user chat streaks: %w", err)
	}
	domain.SortChatStreaksForDisplay(out)
	return out, nil
}

// TopChatStreaks is the live leaderboard, longest first. Whether a streak still
// counts is left to the caller: that depends on when it is read and on the skip
// budget the row carries, and a leaderboard that silently dropped rows would be
// lying about the very number people compete over.
func (s *ChatStreakStore) TopChatStreaks(ctx context.Context, limit int) ([]domain.ChatStreak, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("chat streak store is not configured")
	}
	rows, err := s.db.Query(ctx, `
SELECT `+chatStreakColumns+` FROM chat_streaks
WHERE last_day IS NOT NULL AND days > 0
ORDER BY days DESC, frozen_days DESC, user_a, user_b
LIMIT $1`, chatStreakLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("top chat streaks: %w", err)
	}
	out, err := scanChatStreakRows(rows)
	if err != nil {
		return nil, fmt.Errorf("top chat streaks: %w", err)
	}
	return out, nil
}

// GrantChatStreakFreeze tops one pair's skip budget up under a row lock.
//
// The read-modify-write has to be atomic for the same reason an advance does:
// an operator clicking twice must add the days twice, and two admins granting at
// once must not lose one of the grants. The bound is re-applied in SQL as well
// as in the domain helper, because the CHECK is the last line of defence and a
// clamped write is better than a rejected transaction.
func (s *ChatStreakStore) GrantChatStreakFreeze(ctx context.Context, userA, userB int64, days int, actor, reason string) (domain.ChatStreak, error) {
	if s == nil || s.db == nil {
		return domain.ChatStreak{}, fmt.Errorf("chat streak store is not configured")
	}
	a, b, err := domain.NormalizeChatStreakPeers(userA, userB)
	if err != nil {
		return domain.ChatStreak{}, err
	}
	if days <= 0 || days > domain.MaxChatStreakFreeze {
		return domain.ChatStreak{}, domain.ErrChatStreakFreezeInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.ChatStreak{}, fmt.Errorf("grant chat streak freeze: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.ChatStreak{}, fmt.Errorf("begin grant chat streak freeze: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	prev, found, err := lockChatStreakTx(ctx, tx, a, b)
	if err != nil {
		return domain.ChatStreak{}, err
	}
	if !found {
		// Granting to a pair that has never talked creates the row carrying only
		// the budget, so the grant is waiting when the pair does start. This is
		// the case an operator actually reaches for when they are about to warn
		// someone their streak is about to die.
		next, err := domain.NewChatStreak(a, b, days, time.Now())
		if err != nil {
			return domain.ChatStreak{}, err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO chat_streaks (user_a, user_b, days, last_day, frozen_days, updated_at, version)
VALUES ($1, $2, 0, NULL, $3, $4, 1)
ON CONFLICT (user_a, user_b) DO UPDATE
  SET frozen_days = LEAST(chat_streaks.frozen_days + EXCLUDED.frozen_days, $5),
      updated_at = EXCLUDED.updated_at,
      version = chat_streaks.version + 1`,
			a, b, next.FrozenDays, next.UpdatedAt.UTC(), domain.MaxChatStreakFreeze); err != nil {
			return domain.ChatStreak{}, fmt.Errorf("insert chat streak freeze: %w", err)
		}
		stored, _, err := lockChatStreakTx(ctx, tx, a, b)
		if err != nil {
			return domain.ChatStreak{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.ChatStreak{}, fmt.Errorf("commit chat streak freeze: %w", err)
		}
		committed = true
		return stored, nil
	}

	next, err := domain.GrantChatStreakFreeze(prev, days)
	if err != nil {
		return domain.ChatStreak{}, err
	}
	next.UpdatedAt = time.Now().UTC()
	if _, err := tx.Exec(ctx, `
UPDATE chat_streaks
SET frozen_days = $3, updated_at = $4, version = version + 1
WHERE user_a = $1 AND user_b = $2`,
		a, b, next.FrozenDays, next.UpdatedAt); err != nil {
		return domain.ChatStreak{}, fmt.Errorf("update chat streak freeze: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ChatStreak{}, fmt.Errorf("commit chat streak freeze: %w", err)
	}
	committed = true
	next.Version = prev.Version + 1
	next.Days = prev.Days
	next.LastDay = prev.LastDay
	return next, nil
}

// GrantUserChatStreakFreeze applies the grant to every live streak of one
// account. Zero touched rows is a normal answer, not an error: the account may
// not be talking to anyone yet.
//
// A single UPDATE rather than a loop over ListUserChatStreaks on purpose: the
// admin path must not read-modify-write N rows to add the same budget to each,
// or a pair could be granted twice under a retry.
func (s *ChatStreakStore) GrantUserChatStreakFreeze(ctx context.Context, userID int64, days int, actor, reason string) (int, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("chat streak store is not configured")
	}
	if userID <= 0 || domain.IsSystemUserID(userID) {
		return 0, domain.ErrChatStreakInvalid
	}
	if days <= 0 || days > domain.MaxChatStreakFreeze {
		return 0, domain.ErrChatStreakFreezeInvalid
	}
	tag, err := s.db.Exec(ctx, `
UPDATE chat_streaks
SET frozen_days = LEAST(frozen_days + $2, $3), updated_at = now(), version = version + 1
WHERE (user_a = $1 OR user_b = $1) AND last_day IS NOT NULL`,
		userID, days, domain.MaxChatStreakFreeze)
	if err != nil {
		return 0, fmt.Errorf("grant user chat streak freeze: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
