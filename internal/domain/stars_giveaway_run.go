package domain

import (
	"errors"
	"time"
)

// The "winner draw" half of a Stars giveaway.
//
// The purchase, launch-post and boost halves already exist (see
// app/stargifts and internal/rpc/payments_star_gifts.go). What is missing is the
// part that actually gives the prize away: deciding who won, once, atomically,
// and leaving a record that a later retry cannot double-pay.
//
// Everything here is pure so the rules can be pinned by unit tests rather than
// discovered by running money at a wall.

const (
	// MaxGiveawayWinners bounds one draw. A giveaway that names more winners than
	// this is a configuration mistake, not a large giveaway: the prize is split
	// per winner, so 10000 winners of 1000 stars each is a giveaway nobody can
	// read on a phone.
	MaxGiveawayWinners = 1000
	// MaxGiveawayDurationDays bounds until_date - created_at. A giveaway that
	// stays open for years is not a giveaway; it is an unbounded liability that
	// someone has to reconcile by hand when the channel is deleted.
	MaxGiveawayDurationDays = 365
)

var (
	// ErrGiveawayNotFound reports an unknown giveaway id.
	ErrGiveawayNotFound = errors.New("giveaway not found")
	// ErrGiveawayNotActive reports a draw or entry against a giveaway that has
	// already been completed or cancelled.
	ErrGiveawayNotActive = errors.New("giveaway is not active")
	// ErrGiveawayEnded reports an entry after until_date.
	ErrGiveawayEnded = errors.New("giveaway has ended")
	// ErrGiveawayNoEntries reports a draw with nobody eligible.
	ErrGiveawayNoEntries = errors.New("giveaway has no entries")
	// ErrGiveawayWinnersInvalid reports a winner set that cannot be paid.
	ErrGiveawayWinnersInvalid = errors.New("giveaway winners invalid")
)

// StarsGiveawayEntry is one participant.
//
// Participants are rows, not a counter, because the draw has to pick distinct
// accounts and because an operator has to be able to answer "who entered" when
// a winner disputes it.
type StarsGiveawayEntry struct {
	GiveawayID int64
	UserID     int64
	EnteredAt  int
}

// StarsGiveawayDraw is the immutable outcome of one giveaway.
//
// Winners is a sorted, deduplicated account list rather than a count, because
// the announcement message and the payments.giveawayInfoResults projection both
// need the identities, and because a count cannot be re-derived from a truncated
// entry table after the fact.
type StarsGiveawayDraw struct {
	GiveawayID     int64
	Winners        []int64
	WinnerCount    int
	PerUserStars   int64
	DrawnAt        int
	ActivatedCount int
}

// NewStarsGiveawayEntry validates an entry against the giveaway it is joining.
func NewStarsGiveawayEntry(giveawayID, userID int64, giveaway StarsGiveawayRun, now time.Time) (StarsGiveawayEntry, error) {
	if giveawayID <= 0 || userID <= 0 {
		return StarsGiveawayEntry{}, ErrGiveawayWinnersInvalid
	}
	if giveaway.State != StarsGiveawayActive {
		return StarsGiveawayEntry{}, ErrGiveawayNotActive
	}
	if giveaway.UntilDate > 0 && !now.UTC().Before(time.Unix(int64(giveaway.UntilDate), 0)) {
		return StarsGiveawayEntry{}, ErrGiveawayEnded
	}
	return StarsGiveawayEntry{
		GiveawayID: giveawayID,
		UserID:     userID,
		EnteredAt:  int(now.UTC().Unix()),
	}, nil
}

// DrawStarsGiveawayWinners picks the winners and returns the immutable outcome.
//
// The selection is seeded from the giveaway id rather than the wall clock so a
// retried draw of the same giveaway picks the same people. That is the property
// that makes a crash between "winners chosen" and "winners paid" recoverable:
// whoever retries re-derives the identical set instead of drawing a second,
// different set of winners to the same pool.
//
// Entries are walked in entry order and every len(winners)-th participant wins.
// That is not a cryptographic draw, and it does not need to be: the inputs are
// "who entered a channel giveaway" and the threat is an operator complaint, not
// a hostile entrant who can pick their own seat.
func DrawStarsGiveawayWinners(giveaway StarsGiveawayRun, entries []StarsGiveawayEntry, now time.Time) (StarsGiveawayDraw, error) {
	if giveaway.ID <= 0 {
		return StarsGiveawayDraw{}, ErrGiveawayNotFound
	}
	if giveaway.State != StarsGiveawayActive {
		return StarsGiveawayDraw{}, ErrGiveawayNotActive
	}
	if giveaway.Users <= 0 || giveaway.Users > MaxGiveawayWinners {
		return StarsGiveawayDraw{}, ErrGiveawayWinnersInvalid
	}
	if giveaway.PerUserStars <= 0 {
		return StarsGiveawayDraw{}, ErrGiveawayWinnersInvalid
	}
	if len(entries) == 0 {
		return StarsGiveawayDraw{}, ErrGiveawayNoEntries
	}

	// Distinct participants, in the order they entered. A duplicate row means a
	// retried entry slipped through, and a duplicate winner would be paid twice.
	unique := make([]int64, 0, len(entries))
	seen := make(map[int64]struct{}, len(entries))
	for _, entry := range entries {
		if entry.UserID <= 0 {
			continue
		}
		if _, dup := seen[entry.UserID]; dup {
			continue
		}
		seen[entry.UserID] = struct{}{}
		unique = append(unique, entry.UserID)
	}
	if len(unique) == 0 {
		return StarsGiveawayDraw{}, ErrGiveawayNoEntries
	}

	// Fewer entrants than winners: everyone who entered wins. The alternative is
	// to withhold the prize from a giveaway that ran exactly as advertised, and
	// "activatedCount" is the field the client uses to say so.
	wanted := giveaway.Users
	if wanted > len(unique) {
		wanted = len(unique)
	}

	// Rotation offset, so two giveaways on the same channel with the same
	// entrants do not hand the prize to the same accounts.
	offset := int(giveaway.ID % int64(len(unique)))

	// Spread the winners evenly across the entrant list, rotated by that offset.
	//
	// idx(i) = offset + floor(i * len / wanted). Because wanted <= len the
	// step floor(len/wanted) is at least 1, so consecutive indices are strictly
	// increasing: the walk can never revisit an entrant and therefore needs no
	// duplicate check and cannot loop. That matters because the obvious
	// "take every stride-th" formulation deadlocks into revisits whenever
	// len and the stride share a factor.
	winners := make([]int64, 0, wanted)
	for i := 0; i < wanted; i++ {
		idx := (offset + i*len(unique)/wanted) % len(unique)
		winners = append(winners, unique[idx])
	}

	return StarsGiveawayDraw{
		GiveawayID:     giveaway.ID,
		Winners:        winners,
		WinnerCount:    len(winners),
		PerUserStars:   giveaway.PerUserStars,
		DrawnAt:        int(now.UTC().Unix()),
		ActivatedCount: len(unique),
	}, nil
}

// StarsGiveawayRun is the giveaway state the draw reads.
//
// BuyerUserID is the account that paid the prize pool at launch. It is the
// recipient of entry fees, so it must be carried here: crediting a fee to the
// channel id instead would pay a random user account.
type StarsGiveawayRun struct {
	ID           int64
	BuyerUserID  int64
	ChannelID    int64
	LaunchMsgID  int
	Stars        int64
	Users        int
	PerUserStars int64
	UntilDate    int
	CreatedAt    int
	State        string
	// Draw outcome. DrawnAt is 0 until the worker picks winners; the counters
	// are only meaningful once it is non-zero. Carried here so a reader (the
	// results projection, an operator query) does not need a second round trip
	// or a hand-written SELECT of the jsonb column.
	DrawnAt        int
	WinnerCount    int
	ActivatedCount int
}

// StarsGiveawayState values. These are the strings stored in
// stars_giveaways.state, whose CHECK enumerates exactly these.
const (
	StarsGiveawayActive    = "active"
	StarsGiveawayCompleted = "completed"
	StarsGiveawayCancelled = "cancelled"
)
