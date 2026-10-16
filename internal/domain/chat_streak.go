package domain

import (
	"errors"
	"sort"
	"time"
)

// Chat streak ("огонёк"): the number of consecutive calendar days on which two
// accounts exchanged at least one private message.
//
// This is gramsrv's own mechanic, not a reproduction of Telegram's flame. The
// TL schema has no streak object at all -- the only current_streak in the schema
// belongs to messages.emojiGameDiceInfo, the dice game -- so official Telegram
// computes its flame client-side from message history and gives a server
// nothing to drive. Everything here is therefore surfaced through surfaces that
// do render: a private service message, the user's emoji status, the cabinet.
//
// One row is one unordered pair of accounts. The streak is symmetric: a message
// in either direction advances it, because "переписка идёт" is a property of the
// dialog, not of one side of it.
const (
	// MaxChatStreakFreeze caps the skip budget a single grant can put on one
	// streak. A grant is an operator tool for "he went on holiday, do not wipe
	// his streak", not a way to reach a permanent state, so it needs a ceiling
	// that no accumulation of grants can climb past.
	MaxChatStreakFreeze = 365
	// MaxChatStreakDays is the largest streak a stored row can carry. Nothing in
	// the ladder depends on it -- the counter has no ceiling -- but the store
	// CHECK bounds the column and an operator-facing display that walks every
	// day would otherwise be unbounded.
	MaxChatStreakDays = 100_000
	// MaxChatStreakListLimit bounds every list query. A profile shows a handful
	// of streaks and the leaderboard a page; an unbounded read off a table
	// whose every row is touched by ordinary conversation is a slow query
	// waiting for a bad day.
	MaxChatStreakListLimit = 200
)

// chatStreakDayUTC is the timezone the day boundary is computed in.
//
// Telegram uses each participant's local midnight, which cannot be done here
// without a per-pair timezone preference that does not exist. UTC keeps the
// boundary a single shared instant, so the same row cannot read as two different
// days depending on which server evaluated it.
var chatStreakDayUTC = time.UTC

// ChatStreakOutcome tells the caller what an advance actually did, so the
// notification layer can decide whether there is anything to announce. A day
// that is already counted must stay silent -- "🔥 7 дней" repeated six times in
// one day because the pair wrote six messages is the failure mode that makes
// streak features annoying.
type ChatStreakOutcome string

const (
	// ChatStreakUnchanged means the pair already wrote today. Nothing changed.
	ChatStreakUnchanged ChatStreakOutcome = "unchanged"
	// ChatStreakStarted means this is the first ever counted day.
	ChatStreakStarted ChatStreakOutcome = "started"
	// ChatStreakAdvanced means the streak grew by one on consecutive days.
	ChatStreakAdvanced ChatStreakOutcome = "advanced"
	// ChatStreakResumed means the streak survived a gap because the skip budget
	// covered it, and the budget paid for the skipped days.
	ChatStreakResumed ChatStreakOutcome = "resumed"
	// ChatStreakBroken means the gap outgrew the budget. The counter restarts at
	// one and the budget is gone with it.
	ChatStreakBroken ChatStreakOutcome = "broken"
)

var (
	// ErrChatStreakInvalid reports a peer pair that can never carry a streak.
	ErrChatStreakInvalid = errors.New("chat streak peers invalid")
	// ErrChatStreakNotFound reports a pair that has never been counted. It is
	// distinct from a broken streak, which keeps its row: "never talked" is a
	// first-class answer for a profile to render, "talked and lost it" is not.
	ErrChatStreakNotFound = errors.New("chat streak not found")
	// ErrChatStreakFreezeInvalid reports a grant outside the allowed range.
	ErrChatStreakFreezeInvalid = errors.New("chat streak freeze invalid")
)

// ChatStreak is the stored state of one unordered pair.
//
// LastDay is UTC midnight, never a timestamp: the whole mechanic is a
// comparison of calendar days, and keeping the instant would invite a caller to
// subtract two midnights and get a number of hours.
type ChatStreak struct {
	// UserA and UserB are normalised so UserA < UserB. The pair is the identity:
	// the same two accounts always land on the same row regardless of who sent
	// the message that created it.
	UserA int64
	UserB int64
	// Days is the streak length. Zero means no streak is currently running --
	// the pair has never met, or the pair has not written today and the streak
	// has expired.
	Days int
	// LastDay is the UTC midnight of the most recently counted day.
	LastDay time.Time
	// FrozenDays is the remaining skip budget: how many days may be missed
	// before the streak breaks.
	FrozenDays int
	UpdatedAt  time.Time
	// Version makes concurrent advances from two directions lose predictably
	// instead of silently dropping one of them.
	Version int64
}

// ChatStreakPeer returns the other side of the pair for one participant, and
// reports false when userID is not part of it.
func (s ChatStreak) ChatStreakPeer(userID int64) (int64, bool) {
	switch userID {
	case s.UserA:
		return s.UserB, true
	case s.UserB:
		return s.UserA, true
	}
	return 0, false
}

// ChatStreakExpired reports whether the streak no longer counts as alive at the
// given instant: the pair has not written today and the skip budget cannot
// carry it to tomorrow.
//
// This is what a display reads. It deliberately does not mutate -- the stored
// row keeps Days until a message actually rewrites it, so a pair that misses a
// day still has its score to fall back on if the budget saves it.
func (s ChatStreak) ChatStreakExpired(now time.Time) bool {
	if s.Days == 0 || s.LastDay.IsZero() {
		return true
	}
	today := ChatStreakDay(now)
	if !today.After(s.LastDay) {
		return false
	}
	gap := int(today.Sub(s.LastDay) / (24 * time.Hour))
	return gap-1 > s.FrozenDays
}

// NormalizeChatStreakPeers orders a pair so the row key does not depend on
// direction, and rejects the pairs that can never carry a streak: an account
// with itself, and any pair touching a built-in service account. Service
// accounts are infrastructure, not participants, exactly as in RatableAccount.
func NormalizeChatStreakPeers(userA, userB int64) (int64, int64, error) {
	if userA <= 0 || userB <= 0 || userA == userB {
		return 0, 0, ErrChatStreakInvalid
	}
	if IsSystemUserID(userA) || IsSystemUserID(userB) {
		return 0, 0, ErrChatStreakInvalid
	}
	if userA > userB {
		userA, userB = userB, userA
	}
	return userA, userB, nil
}

// ChatStreakDay truncates an instant to its UTC calendar day.
func ChatStreakDay(at time.Time) time.Time {
	u := at.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, chatStreakDayUTC)
}

// NewChatStreak builds the starting row for a pair, carrying an existing skip
// budget. A grant made before the pair has ever written has to survive until
// the first message, otherwise "выдать заморозку" would silently do nothing for
// the exact case an operator reaches for it: fixing a streak that is about to
// break.
func NewChatStreak(userA, userB int64, frozenDays int, now time.Time) (ChatStreak, error) {
	a, b, err := NormalizeChatStreakPeers(userA, userB)
	if err != nil {
		return ChatStreak{}, err
	}
	if frozenDays < 0 || frozenDays > MaxChatStreakFreeze {
		return ChatStreak{}, ErrChatStreakFreezeInvalid
	}
	return ChatStreak{
		UserA:      a,
		UserB:      b,
		Days:       0,
		FrozenDays: frozenDays,
		UpdatedAt:  now.UTC(),
	}, nil
}

// AdvanceChatStreak applies one private message between the pair and returns
// the new state together with what it did.
//
// The rules, in order:
//
//   - First ever message: the streak starts at one.
//   - Already counted today: nothing changes. Two messages in one day are one
//     day, and re-announcing is noise.
//   - Yesterday: the streak grows by one.
//   - A gap the skip budget covers: the streak grows by one and the budget pays
//     for exactly the days that were missed.
//   - A gap the budget cannot cover: the streak restarts at one and the budget
//     is cleared, because a budget that failed to save the streak has no
//     business surviving it.
//
// A clock that moves backwards (gap zero or negative) is reported as unchanged
// rather than rewinding the counter, so a bad NTP step can cost a day but can
// never destroy a streak.
func AdvanceChatStreak(prev ChatStreak, now time.Time) (ChatStreak, ChatStreakOutcome) {
	day := ChatStreakDay(now)
	out := prev
	out.UpdatedAt = now.UTC()

	if prev.LastDay.IsZero() {
		out.Days = 1
		out.LastDay = day
		return out, ChatStreakStarted
	}
	if !day.After(prev.LastDay) {
		return prev, ChatStreakUnchanged
	}
	gap := int(day.Sub(prev.LastDay) / (24 * time.Hour))
	outcome := ChatStreakAdvanced
	switch {
	case gap == 1:
	case gap-1 <= prev.FrozenDays:
		out.FrozenDays = prev.FrozenDays - (gap - 1)
		outcome = ChatStreakResumed
	default:
		out.Days = 1
		out.FrozenDays = 0
		out.LastDay = day
		return out, ChatStreakBroken
	}
	out.Days = prev.Days + 1
	if out.Days > MaxChatStreakDays {
		out.Days = MaxChatStreakDays
	}
	out.LastDay = day
	return out, outcome
}

// GrantChatStreakFreeze tops a streak's skip budget up. It is additive up to
// MaxChatStreakFreeze and never takes the budget below what it already was, so
// granting to a pair that already holds a budget does not shorten it.
func GrantChatStreakFreeze(streak ChatStreak, days int) (ChatStreak, error) {
	if days <= 0 || days > MaxChatStreakFreeze {
		return ChatStreak{}, ErrChatStreakFreezeInvalid
	}
	out := streak
	out.FrozenDays = min(streak.FrozenDays+days, MaxChatStreakFreeze)
	return out, nil
}

// ChatStreakMilestone reports whether crossing to daysDays is one of the
// announce-worthy milestones, and at most once per crossing.
//
// The thresholds are the ones people screenshot. A streak nobody marks is a
// number nobody looks at; a streak announced every single day is a number
// everybody learns to scroll past.
func ChatStreakMilestone(days int) (int, bool) {
	switch days {
	case 3, 7, 14, 30, 60, 90, 120, 150, 200, 365, 500, 730, 1000:
		return days, true
	default:
		return days, false
	}
}

// SortChatStreaksForDisplay orders streaks the way a leaderboard reads: longest
// first, then the pair with more activity, then the lower user id, so the order
// is total and stable across calls.
func SortChatStreaksForDisplay(streaks []ChatStreak) {
	sort.Slice(streaks, func(i, j int) bool {
		a, b := streaks[i], streaks[j]
		if a.Days != b.Days {
			return a.Days > b.Days
		}
		if a.FrozenDays != b.FrozenDays {
			return a.FrozenDays > b.FrozenDays
		}
		if a.UserA != b.UserA {
			return a.UserA < b.UserA
		}
		return a.UserB < b.UserB
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
