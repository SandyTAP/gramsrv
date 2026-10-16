package domain

import (
	"testing"
	"time"
)

func chatStreakTestDay(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, chatStreakDayUTC)
}

func mustStreak(t *testing.T, a, b int64, frozen int) ChatStreak {
	t.Helper()
	s, err := NewChatStreak(a, b, frozen, chatStreakTestDay(2026, 1, 1))
	if err != nil {
		t.Fatalf("NewChatStreak: %v", err)
	}
	return s
}

// TestAdvanceChatStreak walks a pair through the whole lifecycle and pins every
// transition, including the ones that must not move.
func TestAdvanceChatStreak(t *testing.T) {
	base := mustStreak(t, 100, 200, 0)
	jan1 := chatStreakTestDay(2026, 1, 1)
	jan2 := chatStreakTestDay(2026, 1, 2)
	jan3 := chatStreakTestDay(2026, 1, 3)
	jan4 := chatStreakTestDay(2026, 1, 4)

	cases := []struct {
		name        string
		from        ChatStreak
		at          time.Time
		wantDays    int
		wantLast    time.Time
		wantOutcome ChatStreakOutcome
	}{
		{
			name:        "first message starts the streak",
			from:        base,
			at:          jan1,
			wantDays:    1,
			wantLast:    jan1,
			wantOutcome: ChatStreakStarted,
		},
		{
			name:        "second message on the same day is not a second day",
			from:        mustAdvance(t, base, jan1),
			at:          jan1.Add(9 * time.Hour),
			wantDays:    1,
			wantLast:    jan1,
			wantOutcome: ChatStreakUnchanged,
		},
		{
			name:        "next calendar day grows it",
			from:        mustAdvance(t, base, jan1),
			at:          jan2.Add(14 * time.Hour),
			wantDays:    2,
			wantLast:    jan2,
			wantOutcome: ChatStreakAdvanced,
		},
		{
			name:        "three in a row",
			from:        mustAdvance(t, mustAdvance(t, base, jan1), jan2),
			at:          jan3,
			wantDays:    3,
			wantLast:    jan3,
			wantOutcome: ChatStreakAdvanced,
		},
		{
			name:        "a gap with no budget restarts at one",
			from:        mustAdvance(t, mustAdvance(t, base, jan1), jan2),
			at:          jan4,
			wantDays:    1,
			wantLast:    jan4,
			wantOutcome: ChatStreakBroken,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, outcome := AdvanceChatStreak(tc.from, tc.at)
			if outcome != tc.wantOutcome {
				t.Fatalf("outcome = %q, want %q", outcome, tc.wantOutcome)
			}
			if got.Days != tc.wantDays {
				t.Fatalf("Days = %d, want %d", got.Days, tc.wantDays)
			}
			if !got.LastDay.Equal(tc.wantLast) {
				t.Fatalf("LastDay = %v, want %v", got.LastDay, tc.wantLast)
			}
		})
	}
}

// TestAdvanceChatStreakFreeze is the grant mechanism: the budget buys exactly
// the days that were missed, and a gap it cannot cover still breaks the streak
// and destroys the budget.
func TestAdvanceChatStreakFreeze(t *testing.T) {
	jan1 := chatStreakTestDay(2026, 1, 1)
	jan3 := chatStreakTestDay(2026, 1, 3)
	jan4 := chatStreakTestDay(2026, 1, 4)
	jan5 := chatStreakTestDay(2026, 1, 5)
	jan6 := chatStreakTestDay(2026, 1, 6)

	// Counted on Jan 1, active again on Jan 4: Jan 2 and Jan 3 were skipped, and
	// a budget of 2 covers exactly that.
	two := mustStreak(t, 100, 200, 2)
	resumed, outcome := AdvanceChatStreak(mustAdvance(t, two, jan1), jan4)
	if outcome != ChatStreakResumed {
		t.Fatalf("2-day budget over a 2-day gap: outcome = %q, want %q", outcome, ChatStreakResumed)
	}
	if resumed.Days != 2 {
		t.Fatalf("2-day budget: Days = %d, want 2", resumed.Days)
	}
	if resumed.FrozenDays != 0 {
		t.Fatalf("2-day budget: FrozenDays = %d, want it fully spent", resumed.FrozenDays)
	}

	// Three skipped days outgrow the same budget of two.
	short, outcome := AdvanceChatStreak(mustAdvance(t, two, jan1), jan5)
	if outcome != ChatStreakBroken || short.Days != 1 || short.FrozenDays != 0 {
		t.Fatalf("2-day budget over a 3-day gap = %d/%d/%q, want 1/0/broken",
			short.Days, short.FrozenDays, outcome)
	}

	// The budget is gone once it has been spent: one more missed day breaks it.
	spent, outcome := AdvanceChatStreak(resumed, jan6)
	if outcome != ChatStreakBroken {
		t.Fatalf("spent budget: outcome = %q, want %q", outcome, ChatStreakBroken)
	}
	if spent.Days != 1 {
		t.Fatalf("spent budget: Days = %d, want 1", spent.Days)
	}

	// A one-day budget covers one skipped day and stops there.
	one := mustStreak(t, 100, 200, 1)
	exact, outcome := AdvanceChatStreak(mustAdvance(t, one, jan1), jan3)
	if outcome != ChatStreakResumed || exact.Days != 2 || exact.FrozenDays != 0 {
		t.Fatalf("1-day budget over a 1-day gap = %d/%d/%q, want 2/0/resumed",
			exact.Days, exact.FrozenDays, outcome)
	}
	over, outcome := AdvanceChatStreak(mustAdvance(t, one, jan1), jan4)
	if outcome != ChatStreakBroken || over.Days != 1 {
		t.Fatalf("1-day budget over a 2-day gap = %d/%q, want 1/broken", over.Days, outcome)
	}
}

// TestAdvanceChatStreakClockGoesBackwards: a bad clock step may cost a day but
// must never destroy a streak.
func TestAdvanceChatStreakClockGoesBackwards(t *testing.T) {
	jan5 := chatStreakTestDay(2026, 1, 5)
	jan3 := chatStreakTestDay(2026, 1, 3)
	base := mustStreak(t, 100, 200, 0)
	running := mustAdvance(t, base, jan3)
	running = mustAdvance(t, running, chatStreakTestDay(2026, 1, 4))
	running = mustAdvance(t, running, jan5)

	got, outcome := AdvanceChatStreak(running, jan3)
	if outcome != ChatStreakUnchanged {
		t.Fatalf("outcome = %q, want %q", outcome, ChatStreakUnchanged)
	}
	if got.Days != running.Days || !got.LastDay.Equal(running.LastDay) {
		t.Fatalf("streak moved backwards: %+v vs %+v", got, running)
	}
}

// TestGrantChatStreakFreeze pins the bounds an operator can reach through the
// admin panel.
func TestGrantChatStreakFreeze(t *testing.T) {
	s := mustStreak(t, 100, 200, 3)
	got, err := GrantChatStreakFreeze(s, 4)
	if err != nil {
		t.Fatalf("GrantChatStreakFreeze: %v", err)
	}
	if got.FrozenDays != 7 {
		t.Fatalf("FrozenDays = %d, want 7", got.FrozenDays)
	}
	// Never past the cap, however many grants pile up.
	over, err := GrantChatStreakFreeze(got, MaxChatStreakFreeze)
	if err != nil {
		t.Fatalf("GrantChatStreakFreeze at cap: %v", err)
	}
	if over.FrozenDays != MaxChatStreakFreeze {
		t.Fatalf("FrozenDays = %d, want the %d cap", over.FrozenDays, MaxChatStreakFreeze)
	}
	for _, bad := range []int{0, -1, MaxChatStreakFreeze + 1} {
		if _, err := GrantChatStreakFreeze(s, bad); err == nil {
			t.Fatalf("grant of %d was accepted", bad)
		}
	}
}

// TestNormalizeChatStreakPeers: the row key must not depend on direction, and
// service accounts must never carry a streak.
func TestNormalizeChatStreakPeers(t *testing.T) {
	a, b, err := NormalizeChatStreakPeers(200, 100)
	if err != nil {
		t.Fatalf("NormalizeChatStreakPeers: %v", err)
	}
	if a != 100 || b != 200 {
		t.Fatalf("normalized = %d/%d, want 100/200", a, b)
	}
	for _, pair := range [][2]int64{{0, 1}, {1, 0}, {-1, 5}, {5, 5}, {777000, 100}} {
		if _, _, err := NormalizeChatStreakPeers(pair[0], pair[1]); err == nil {
			t.Fatalf("pair %v was accepted", pair)
		}
	}
}

// TestChatStreakExpired is what a display reads: a pair that has not written
// today is no longer "on fire" even while its stored score survives.
func TestChatStreakExpired(t *testing.T) {
	jan1 := chatStreakTestDay(2026, 1, 1)
	jan2 := chatStreakTestDay(2026, 1, 2)
	jan5 := chatStreakTestDay(2026, 1, 5)

	alive := mustAdvance(t, mustStreak(t, 100, 200, 0), jan1)
	if alive.ChatStreakExpired(jan1.Add(23 * time.Hour)) {
		t.Fatal("streak reported expired on its own day")
	}
	if alive.ChatStreakExpired(jan2) {
		t.Fatal("streak reported expired on the next day, which is still consecutive")
	}
	// Jan 5 after a Jan 1 message with no budget: three missed days.
	if !alive.ChatStreakExpired(jan5) {
		t.Fatal("streak reported alive after an uncovered gap")
	}
	frozen := mustAdvance(t, mustStreak(t, 100, 200, 3), jan1)
	if frozen.ChatStreakExpired(jan5) {
		t.Fatal("streak reported expired while the budget still covered the gap")
	}
	if !(ChatStreak{}).ChatStreakExpired(jan1) {
		t.Fatal("an empty streak must read as expired")
	}
}

// TestChatStreakMilestone: announced on the thresholds, silent in between.
func TestChatStreakMilestone(t *testing.T) {
	for _, want := range []int{3, 7, 30, 365, 1000} {
		if got, ok := ChatStreakMilestone(want); !ok || got != want {
			t.Fatalf("ChatStreakMilestone(%d) = %d/%v, want a milestone", want, got, ok)
		}
	}
	for _, quiet := range []int{0, 1, 2, 4, 8, 29, 31, 999} {
		if got, ok := ChatStreakMilestone(quiet); ok {
			t.Fatalf("ChatStreakMilestone(%d) announced at %d", quiet, got)
		}
	}
}

// TestSortChatStreaksForDisplay pins a total, stable order so a leaderboard does
// not shuffle between two identical reads.
func TestSortChatStreaksForDisplay(t *testing.T) {
	rows := []ChatStreak{
		{UserA: 300, UserB: 400, Days: 5, FrozenDays: 0},
		{UserA: 100, UserB: 900, Days: 5, FrozenDays: 2},
		{UserA: 100, UserB: 200, Days: 30, FrozenDays: 0},
		{UserA: 100, UserB: 300, Days: 5, FrozenDays: 2},
	}
	SortChatStreaksForDisplay(rows)
	want := []struct {
		userA, userB int64
		days         int
	}{
		{100, 200, 30},
		{100, 300, 5},
		{100, 900, 5},
		{300, 400, 5},
	}
	for i, w := range want {
		if rows[i].UserA != w.userA || rows[i].UserB != w.userB || rows[i].Days != w.days {
			t.Fatalf("row %d = %d/%d/%d, want %d/%d/%d",
				i, rows[i].UserA, rows[i].UserB, rows[i].Days, w.userA, w.userB, w.days)
		}
	}
}

func mustAdvance(t *testing.T, s ChatStreak, at time.Time) ChatStreak {
	t.Helper()
	out, _ := AdvanceChatStreak(s, at)
	return out
}
