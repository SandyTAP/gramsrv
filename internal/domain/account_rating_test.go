package domain

import (
	"testing"
	"time"
)

// TestAccountRatingLevelScale pins the ladder: 500 levels, the top one at
// 6 * 500^2 = 1.5M score, and every threshold strictly increasing.
func TestAccountRatingLevelScale(t *testing.T) {
	if MaxAccountRatingLevel != 500 {
		t.Fatalf("MaxAccountRatingLevel = %d, want 500", MaxAccountRatingLevel)
	}
	top := AccountRatingLevelThreshold(MaxAccountRatingLevel)
	if top != 1_500_000 {
		t.Fatalf("top level threshold = %d, want 1500000", top)
	}
	previous := int64(0)
	for level := 1; level <= MaxAccountRatingLevel; level++ {
		threshold := AccountRatingLevelThreshold(level)
		if threshold <= previous {
			t.Fatalf("level %d threshold = %d, want above the previous %d", level, threshold, previous)
		}
		previous = threshold
	}
	// Beyond the top the curve is clamped rather than extrapolated.
	if got := AccountRatingLevelThreshold(MaxAccountRatingLevel + 10); got != top {
		t.Fatalf("over-max threshold = %d, want the clamped %d", got, top)
	}
	if got := AccountRatingLevelThreshold(0); got != 0 {
		t.Fatalf("level 0 threshold = %d, want 0", got)
	}
}

// TestAccountRatingLevelForStarsBoundary pins the exact-threshold behaviour the
// progress bar depends on: a score that lands on a threshold is already on that
// level, and the pair (current, next) always brackets it.
func TestAccountRatingLevelForStarsBoundary(t *testing.T) {
	cases := []struct {
		stars         int64
		level         int
		current, next int64
		hasNext       bool
	}{
		{stars: 0, level: 0, current: 0, next: 6, hasNext: true},
		{stars: 5, level: 0, current: 0, next: 6, hasNext: true},
		{stars: 6, level: 1, current: 6, next: 24, hasNext: true},
		{stars: 23, level: 1, current: 6, next: 24, hasNext: true},
		{stars: 24, level: 2, current: 24, next: 54, hasNext: true},
		{stars: 600, level: 10, current: 600, next: 726, hasNext: true},
		{stars: 2_400, level: 20, current: 2_400, next: 2_646, hasNext: true},
		{stars: 15_000, level: 50, current: 15_000, next: 15_606, hasNext: true},
		{stars: 1_499_994, level: 499, current: 1_494_006, next: 1_500_000, hasNext: true},
		{stars: 1_500_000, level: 500, current: 1_500_000, next: 0, hasNext: false},
		{stars: 10_000_000, level: 500, current: 1_500_000, next: 0, hasNext: false},
		{stars: -5, level: 0, current: 0, next: 6, hasNext: true},
	}
	for _, tc := range cases {
		level, current, next, hasNext := AccountRatingLevelForStars(tc.stars)
		if level != tc.level || current != tc.current || next != tc.next || hasNext != tc.hasNext {
			t.Fatalf("AccountRatingLevelForStars(%d) = %d/%d/%d/%v, want %d/%d/%d/%v",
				tc.stars, level, current, next, hasNext,
				tc.level, tc.current, tc.next, tc.hasNext)
		}
	}
	// No score below the top threshold may report itself as the top level, and no
	// score above one may report anything else: the ladder has exactly
	// MaxAccountRatingLevel rungs.
	for level := 1; level < MaxAccountRatingLevel; level++ {
		levelAt, _, _, _ := AccountRatingLevelForStars(AccountRatingLevelThreshold(level))
		if levelAt != level {
			t.Fatalf("threshold %d resolved to level %d", AccountRatingLevelThreshold(level), levelAt)
		}
		if below, _, _, _ := AccountRatingLevelForStars(AccountRatingLevelThreshold(level) - 1); below != level-1 {
			t.Fatalf("just below threshold %d resolved to level %d, want %d",
				AccountRatingLevelThreshold(level), below, level-1)
		}
	}
}

// TestComputeAccountRatingTopLevelDropsNextThreshold mirrors what the clients
// draw: no next_level_stars once the ceiling is reached, so the profile shows a
// max-level badge instead of an endless progress bar.
func TestComputeAccountRatingTopLevelDropsNextThreshold(t *testing.T) {
	top := AccountRatingLevelThreshold(MaxAccountRatingLevel)
	rating := ComputeAccountRating(
		AccountRatingSignals{UserID: 7, StarsReceived: top},
		DefaultAccountRatingWeights(),
		time.Unix(1_700_000_000, 0).UTC(),
	)
	if rating.Level != MaxAccountRatingLevel || rating.HasNextLevel || rating.NextLevelStars != 0 {
		t.Fatalf("top rating = %+v, want level %d without next", rating, MaxAccountRatingLevel)
	}
	if rating.CurrentLevelStars != top || rating.Stars != top {
		t.Fatalf("top rating thresholds = %d/%d, want %d/%d",
			rating.CurrentLevelStars, rating.Stars, top, top)
	}
	// A pending level is derived from the same curve, so it cannot report a
	// next_level_stars the visible level would not have either.
	pending := rating
	pending.PendingStars = 1
	pending.PendingDate = time.Unix(1_700_000_000, 0).UTC()
	if snapshot, ok := pending.PendingLevel(); !ok ||
		snapshot.Level != MaxAccountRatingLevel || snapshot.HasNextLevelStars {
		t.Fatalf("pending top snapshot = %+v/%v, want level %d without next",
			snapshot, ok, MaxAccountRatingLevel)
	}
}
