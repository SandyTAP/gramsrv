package domain

import (
	"errors"
	"testing"
	"time"
)

func giveawayRunAt(id int64, users int, state string) StarsGiveawayRun {
	return StarsGiveawayRun{
		ID:           id,
		ChannelID:    932,
		LaunchMsgID:  10,
		Stars:        int64(users) * 100,
		Users:        users,
		PerUserStars: 100,
		UntilDate:    2000,
		CreatedAt:    1000,
		State:        state,
	}
}

func giveawayEntries(userIDs ...int64) []StarsGiveawayEntry {
	out := make([]StarsGiveawayEntry, 0, len(userIDs))
	for i, id := range userIDs {
		out = append(out, StarsGiveawayEntry{GiveawayID: 1, UserID: id, EnteredAt: 1000 + i})
	}
	return out
}

// TestDrawStarsGiveawayWinnersPicksExactlyTheAdvertisedCount: the prize is split
// per winner, so under-picking silently burns stars and over-picking pays more
// than the giveaway was funded with. The count is the contract.
func TestDrawStarsGiveawayWinnersPicksExactlyTheAdvertisedCount(t *testing.T) {
	run := giveawayRunAt(1, 5, StarsGiveawayActive)
	draw, err := DrawStarsGiveawayWinners(run, giveawayEntries(101, 102, 103, 104, 105, 106, 107, 108, 109, 110), time.Unix(2000, 0))
	if err != nil {
		t.Fatalf("Draw: %v", err)
	}
	if draw.WinnerCount != 5 {
		t.Fatalf("WinnerCount = %d, want 5", draw.WinnerCount)
	}
	if int64(draw.WinnerCount)*draw.PerUserStars != run.Stars {
		t.Fatalf("payout %d does not match the funded %d", int64(draw.WinnerCount)*draw.PerUserStars, run.Stars)
	}
	if draw.ActivatedCount != 10 {
		t.Fatalf("ActivatedCount = %d, want 10 entrants", draw.ActivatedCount)
	}
}

// TestDrawStarsGiveawayWinnersAreDistinct is the property a retried draw depends
// on: a duplicate winner is paid twice.
func TestDrawStarsGiveawayWinnersAreDistinct(t *testing.T) {
	// 7 entrants, 4 winners, and lengths that are not coprime -- the shape that
	// makes a stride walk revisit indices.
	for _, id := range []int64{1, 2, 3, 7, 11, 13} {
		run := giveawayRunAt(id, 4, StarsGiveawayActive)
		draw, err := DrawStarsGiveawayWinners(run, giveawayEntries(101, 102, 103, 104, 105, 106, 107), time.Unix(2000, 0))
		if err != nil {
			t.Fatalf("giveaway %d: %v", id, err)
		}
		seen := map[int64]struct{}{}
		for _, w := range draw.Winners {
			if _, dup := seen[w]; dup {
				t.Fatalf("giveaway %d: winner %d appears twice in %v", id, w, draw.Winners)
			}
			seen[w] = struct{}{}
		}
	}
	// Every ratio of entrants to winners, not just the coprime ones.
	for entrants := 1; entrants <= 40; entrants++ {
		for winners := 1; winners <= entrants; winners++ {
			ids := make([]int64, 0, entrants)
			for i := 0; i < entrants; i++ {
				ids = append(ids, int64(1000+i))
			}
			run := giveawayRunAt(int64(entrants*100+winners), winners, StarsGiveawayActive)
			draw, err := DrawStarsGiveawayWinners(run, giveawayEntries(ids...), time.Unix(2000, 0))
			if err != nil {
				t.Fatalf("entrants=%d winners=%d: %v", entrants, winners, err)
			}
			if draw.WinnerCount != winners {
				t.Fatalf("entrants=%d winners=%d: got %d", entrants, winners, draw.WinnerCount)
			}
			seen := map[int64]struct{}{}
			for _, w := range draw.Winners {
				if _, dup := seen[w]; dup {
					t.Fatalf("entrants=%d winners=%d: %d repeated in %v", entrants, winners, w, draw.Winners)
				}
				seen[w] = struct{}{}
			}
		}
	}
}

// TestDrawStarsGiveawayWinnersIsDeterministic: a crash between "chosen" and
// "paid" is recovered by re-drawing. If the second draw differs, the recovery
// pays a different set than the announcement promised.
func TestDrawStarsGiveawayWinnersIsDeterministic(t *testing.T) {
	run := giveawayRunAt(42, 3, StarsGiveawayActive)
	entries := giveawayEntries(201, 202, 203, 204, 205, 206, 207, 208)
	first, err := DrawStarsGiveawayWinners(run, entries, time.Unix(2000, 0))
	if err != nil {
		t.Fatalf("first draw: %v", err)
	}
	for i := 0; i < 5; i++ {
		again, err := DrawStarsGiveawayWinners(run, entries, time.Unix(2500, 0))
		if err != nil {
			t.Fatalf("redraw %d: %v", i, err)
		}
		if len(again.Winners) != len(first.Winners) {
			t.Fatalf("redraw %d: count changed", i)
		}
		for j := range first.Winners {
			if again.Winners[j] != first.Winners[j] {
				t.Fatalf("redraw %d: winners changed %v -> %v", i, first.Winners, again.Winners)
			}
		}
	}
}

// TestDrawStarsGiveawayWinnersDedupesEntries: a retried entry must not buy two
// chances, and must not shrink the prize pool's apparent size.
func TestDrawStarsGiveawayWinnersDedupesEntries(t *testing.T) {
	run := giveawayRunAt(1, 3, StarsGiveawayActive)
	entries := giveawayEntries(301, 301, 302, 302, 303, 303, 304)
	draw, err := DrawStarsGiveawayWinners(run, entries, time.Unix(2000, 0))
	if err != nil {
		t.Fatalf("Draw: %v", err)
	}
	if draw.ActivatedCount != 4 {
		t.Fatalf("ActivatedCount = %d, want 4 distinct entrants", draw.ActivatedCount)
	}
}

// TestDrawStarsGiveawayFewerEntrantsThanWinners: a giveaway with fewer entrants
// than prizes pays everyone rather than withholding it.
func TestDrawStarsGiveawayFewerEntrantsThanWinners(t *testing.T) {
	run := giveawayRunAt(1, 10, StarsGiveawayActive)
	draw, err := DrawStarsGiveawayWinners(run, giveawayEntries(401, 402), time.Unix(2000, 0))
	if err != nil {
		t.Fatalf("Draw: %v", err)
	}
	if draw.WinnerCount != 2 {
		t.Fatalf("WinnerCount = %d, want 2 (everyone who entered)", draw.WinnerCount)
	}
}

// TestDrawStarsGiveawayWinnersRejectsBadInput: the states that must never pay.
func TestDrawStarsGiveawayWinnersRejectsBadInput(t *testing.T) {
	at := time.Unix(2000, 0)
	entries := giveawayEntries(501, 502, 503)

	if _, err := DrawStarsGiveawayWinners(giveawayRunAt(0, 1, StarsGiveawayActive), entries, at); !errors.Is(err, ErrGiveawayNotFound) {
		t.Fatalf("unknown giveaway err = %v", err)
	}
	if _, err := DrawStarsGiveawayWinners(giveawayRunAt(1, 1, StarsGiveawayCompleted), entries, at); !errors.Is(err, ErrGiveawayNotActive) {
		t.Fatalf("completed giveaway err = %v", err)
	}
	if _, err := DrawStarsGiveawayWinners(giveawayRunAt(1, 1, StarsGiveawayCancelled), entries, at); !errors.Is(err, ErrGiveawayNotActive) {
		t.Fatalf("cancelled giveaway err = %v", err)
	}
	if _, err := DrawStarsGiveawayWinners(giveawayRunAt(1, 1, StarsGiveawayActive), nil, at); !errors.Is(err, ErrGiveawayNoEntries) {
		t.Fatalf("empty giveaway err = %v", err)
	}
	zeroPrize := giveawayRunAt(1, 1, StarsGiveawayActive)
	zeroPrize.PerUserStars = 0
	if _, err := DrawStarsGiveawayWinners(zeroPrize, entries, at); !errors.Is(err, ErrGiveawayWinnersInvalid) {
		t.Fatalf("zero prize err = %v", err)
	}
	tooMany := giveawayRunAt(1, MaxGiveawayWinners+1, StarsGiveawayActive)
	if _, err := DrawStarsGiveawayWinners(tooMany, entries, at); !errors.Is(err, ErrGiveawayWinnersInvalid) {
		t.Fatalf("oversized giveaway err = %v", err)
	}
}

// TestNewStarsGiveawayEntry: joining after the deadline or after the giveaway
// closed must be refused, otherwise entries keep appearing for a giveaway whose
// winners were already announced.
func TestNewStarsGiveawayEntry(t *testing.T) {
	run := giveawayRunAt(1, 1, StarsGiveawayActive)
	run.UntilDate = 2000
	at := time.Unix(1999, 0)

	entry, err := NewStarsGiveawayEntry(1, 900, run, at)
	if err != nil {
		t.Fatalf("join before the end: %v", err)
	}
	if entry.UserID != 900 || entry.EnteredAt != 1999 {
		t.Fatalf("entry = %+v", entry)
	}

	if _, err := NewStarsGiveawayEntry(1, 900, run, time.Unix(2000, 0)); !errors.Is(err, ErrGiveawayEnded) {
		t.Fatalf("join at until_date err = %v", err)
	}
	if _, err := NewStarsGiveawayEntry(1, 900, run, time.Unix(2001, 0)); !errors.Is(err, ErrGiveawayEnded) {
		t.Fatalf("join after until_date err = %v", err)
	}
	closed := giveawayRunAt(1, 1, StarsGiveawayCompleted)
	closed.UntilDate = 5000
	if _, err := NewStarsGiveawayEntry(1, 900, closed, at); !errors.Is(err, ErrGiveawayNotActive) {
		t.Fatalf("join a completed giveaway err = %v", err)
	}
	if _, err := NewStarsGiveawayEntry(1, 0, run, at); err == nil {
		t.Fatal("join with no user id was accepted")
	}
}
