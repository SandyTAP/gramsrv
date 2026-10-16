package store

import (
	"context"
	"time"

	"telesrv/internal/domain"
)

// ChatStreakStore owns the "огонёк" read model: how many consecutive days two
// accounts have been talking to each other in private.
//
// Every rule lives in domain.AdvanceChatStreak. This interface deliberately
// exposes no primitive that would let a caller decide the new state itself --
// the whole point of AdvanceChatStreak being atomic is that two people writing
// to each other at the same moment cannot lose a day between them, and that
// guarantee dies the moment a caller can read-modify-write on its own.
type ChatStreakStore interface {
	// AdvanceChatStreak applies one private message between the pair and
	// returns the resulting state plus what the message did. The caller decides
	// whether the outcome is worth announcing; the store never does.
	//
	// userA and userB may arrive in any order. Pairs that can never carry a
	// streak (a self-pair, a pair touching a built-in service account) report
	// domain.ErrChatStreakInvalid rather than silently storing a row nobody will
	// ever read.
	AdvanceChatStreak(ctx context.Context, userA, userB int64, now time.Time) (domain.ChatStreak, domain.ChatStreakOutcome, error)
	// ChatStreak returns the stored state for a pair, in any order.
	// A pair that has never been counted reports domain.ErrChatStreakNotFound.
	ChatStreak(ctx context.Context, userA, userB int64) (domain.ChatStreak, error)
	// StrongestChatStreak returns the longest live streak one viewer shares with
	// one target, in any order, and domain.ErrChatStreakNotFound when they share
	// none.
	//
	// It exists because "the badge this reader is allowed to see" is a question
	// about a pair, not about a user: there is no per-user streak, and asking
	// for "the viewer's streaks" and picking the best one in Go would mean
	// loading every conversation the reader has to answer a question about one
	// profile.
	StrongestChatStreak(ctx context.Context, viewerUserID, targetUserID int64) (domain.ChatStreak, error)
	// ListUserChatStreaks returns every streak one account is part of, longest
	// first, so a profile or an admin grant does not have to know that the pair
	// is symmetric and might be stored under either side.
	ListUserChatStreaks(ctx context.Context, userID int64, limit int) ([]domain.ChatStreak, error)
	// TopChatStreaks is the live leaderboard: rows with at least one counted
	// day, longest first. Expired rows are the caller's to judge, because
	// whether a streak still counts depends on when it is read and on the skip
	// budget that row carries.
	TopChatStreaks(ctx context.Context, limit int) ([]domain.ChatStreak, error)
	// GrantChatStreakFreeze tops one pair's skip budget up. It is the single
	// write an operator needs to repair "he went on holiday and lost it", and it
	// is deliberately not a counter bump: bumping days would let an operator
	// hand out a streak that no conversation ever earned.
	GrantChatStreakFreeze(ctx context.Context, userA, userB int64, days int, actor, reason string) (domain.ChatStreak, error)
	// GrantUserChatStreakFreeze applies the same grant to every live streak one
	// account is part of and reports how many rows it touched. Zero is a normal
	// answer, not an error: the account may not talk to anyone yet.
	GrantUserChatStreakFreeze(ctx context.Context, userID int64, days int, actor, reason string) (int, error)
}
