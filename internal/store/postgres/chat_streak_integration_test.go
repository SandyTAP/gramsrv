package postgres

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
)

// chatStreakWidePool opens a pool wide enough for the workers to actually
// overlap.
//
// This is not cosmetic. testPool hands back the production-shaped pool, and on
// this host that pool ends up holding a single connection, so the sixteen
// workers of the race test queue up one at a time and each one finds the
// previous worker's row already committed. The race then cannot happen for
// physical reasons, the test passes, and it passes just as happily against the
// unfixed code -- which is exactly what it did before this pool existed. A
// concurrency test run on a one-connection pool is a test that tests nothing.
func chatStreakWidePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	// testPool owns the three rules this file must not re-invent: skip when the
	// DSN is unset, refuse a DSN whose database is not a test database, and run
	// the migrations. Calling it first is cheaper than copying that logic --
	// skipping it once let the race test connect to a local socket and fail the
	// whole package in the plain `go test ./...` run.
	_ = testPool(t)

	cfg, err := pgxpool.ParseConfig(os.Getenv("TELESRV_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatalf("parse TELESRV_TEST_POSTGRES_DSN: %v", err)
	}
	cfg.MaxConns = chatStreakRaceWorkers + 8
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("open wide pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// chatStreakReset clears the table so every test in this file is re-runnable.
//
// The pool is a long-lived dedicated test database, not a per-test fixture, so a
// pair created by the previous run is still there on the next one -- and a
// leftover row turns "first message" into "second message that day", which fails
// in a way that looks exactly like a bug in the store. Truncating is safe here
// precisely because the database exists only to run these tests.
func chatStreakReset(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `TRUNCATE chat_streaks`); err != nil {
		t.Fatalf("truncate chat_streaks: %v", err)
	}
}

// chatStreakRaceWorkers is the number of simultaneous first messages thrown at
// one pair inside a round.
const chatStreakRaceWorkers = 8

// chatStreakRaceRounds is how many independent pairs a single run races.
//
// The interleaving is probabilistic: whether two transactions both reach their
// INSERT before either commits depends on how the scheduler and Postgres happen
// to interleave them. A single round caught the bug on some runs and missed it
// on others, which makes it worse than no test at all -- a regression test that
// passes against the broken code teaches you to trust it. Rounds are cheap and
// the failure signal is counted, not sampled, so the whole run fails whenever
// any round over-reports.
const chatStreakRaceRounds = 12

// TestChatStreakStoreConcurrentFirstMessage is the regression test for the race
// a code review would not catch: two people write to each other for the first
// time in the same instant.
//
// Every worker hammers the SAME pair, because that is the only shape that
// conflicts. Give each worker its own pair and there is nothing to race, so the
// test proves nothing while looking like it proves something.
//
// There is no row to lock yet, so all of them compute "started" independently
// and INSERT ... ON CONFLICT DO NOTHING makes every loser a silent no-op at the
// SQL level. What it does not do is stop the losers from *reporting* started --
// the announcement is the caller's decision, and a caller that announced on
// every result would post two "Огонёк: 1 день" messages into one conversation
// for one day.
//
// The contract: exactly one "started", every other worker "unchanged", one row
// at day one, and that row under the normalised (min, max) pair.
//
// Skips without TELESRV_TEST_POSTGRES_DSN, like every other Postgres test here.
func TestChatStreakStoreConcurrentFirstMessage(t *testing.T) {
	pool := chatStreakWidePool(t)
	chatStreakReset(t, pool)
	store := NewChatStreakStore(pool)

	ctx := context.Background()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	var (
		totalStarted int
		failures     []error
	)
	for round := 0; round < chatStreakRaceRounds; round++ {
		a := int64(1_000_000 + round*10)
		b := a + 1

		start := make(chan struct{})
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			started int
			others  []domain.ChatStreakOutcome
		)
		for i := 0; i < chatStreakRaceWorkers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				// Alternate the direction so the (min,max) normalisation races
				// against itself as well as the insert.
				from, to := a, b
				if i%2 == 1 {
					from, to = b, a
				}
				<-start
				streak, outcome, err := store.AdvanceChatStreak(ctx, from, to, now)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					failures = append(failures, fmt.Errorf("round %d worker %d: %w", round, i, err))
					return
				}
				if streak.Days != 1 {
					failures = append(failures, fmt.Errorf("round %d worker %d: days = %d, want 1", round, i, streak.Days))
					return
				}
				if outcome == domain.ChatStreakStarted {
					started++
					return
				}
				others = append(others, outcome)
			}(i)
		}
		close(start)
		wg.Wait()

		for _, outcome := range others {
			if outcome != domain.ChatStreakUnchanged {
				failures = append(failures, fmt.Errorf("round %d: a losing worker reported %q", round, outcome))
			}
		}
		totalStarted += started

		var count int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM chat_streaks WHERE user_a = $1 AND user_b = $2`, a, b).Scan(&count); err != nil {
			failures = append(failures, fmt.Errorf("round %d: count rows: %w", round, err))
			continue
		}
		if count != 1 {
			failures = append(failures, fmt.Errorf("round %d: rows for the pair = %d, want 1", round, count))
		}
	}

	if len(failures) > 0 {
		t.Fatalf("concurrent first messages: %v", failures)
	}
	if totalStarted != chatStreakRaceRounds {
		t.Fatalf("reported started %d times over %d rounds, want exactly one per round: %d duplicate announcements would be posted",
			totalStarted, chatStreakRaceRounds, totalStarted-chatStreakRaceRounds)
	}
}

// TestChatStreakStoreSameDayIsIdempotent pins the rule the notification layer
// relies on: one calendar day counted twice reports unchanged, so the caller
// stays silent.
func TestChatStreakStoreSameDayIsIdempotent(t *testing.T) {
	pool := testPool(t)
	chatStreakReset(t, pool)
	store := NewChatStreakStore(pool)
	ctx := context.Background()
	a, b := int64(910001), int64(910002)

	day1 := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	if _, outcome, err := store.AdvanceChatStreak(ctx, a, b, day1); err != nil || outcome != domain.ChatStreakStarted {
		t.Fatalf("first = %q/%v, want started/nil", outcome, err)
	}
	// Later the same day, from the other direction.
	if _, outcome, err := store.AdvanceChatStreak(ctx, b, a, day1.Add(9*time.Hour)); err != nil || outcome != domain.ChatStreakUnchanged {
		t.Fatalf("same day = %q/%v, want unchanged/nil", outcome, err)
	}
	stored, err := store.ChatStreak(ctx, a, b)
	if err != nil {
		t.Fatalf("ChatStreak: %v", err)
	}
	if stored.Days != 1 {
		t.Fatalf("Days = %d, want 1: one calendar day is one day", stored.Days)
	}
	day2 := day1.AddDate(0, 0, 1)
	streak, outcome, err := store.AdvanceChatStreak(ctx, b, a, day2)
	if err != nil || outcome != domain.ChatStreakAdvanced || streak.Days != 2 {
		t.Fatalf("day two = %d/%q/%v, want 2/advanced/nil", streak.Days, outcome, err)
	}
	// The pair is addressable from either side and reads back normalised.
	mirrored, err := store.ChatStreak(ctx, b, a)
	if err != nil || mirrored.UserA != a || mirrored.UserB != b {
		t.Fatalf("reverse lookup = %+v/%v, want the same normalised pair", mirrored, err)
	}
}

// TestChatStreakStoreFreezeRescuesThenBreaks exercises the admin grant against
// the real store: a covered gap resumes and spends the budget, an uncovered one
// restarts and destroys it.
func TestChatStreakStoreFreezeRescuesThenBreaks(t *testing.T) {
	pool := testPool(t)
	chatStreakReset(t, pool)
	store := NewChatStreakStore(pool)
	ctx := context.Background()
	a, b := int64(920001), int64(920002)

	day1 := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	if _, _, err := store.AdvanceChatStreak(ctx, a, b, day1); err != nil {
		t.Fatalf("day1: %v", err)
	}
	granted, err := store.GrantChatStreakFreeze(ctx, a, b, 2, "tester", "holiday")
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if granted.FrozenDays != 2 || granted.Days != 1 {
		t.Fatalf("after grant = %+v, want days 1 frozen 2", granted)
	}
	resumed, outcome, err := store.AdvanceChatStreak(ctx, b, a, day1.AddDate(0, 0, 3))
	if err != nil || outcome != domain.ChatStreakResumed {
		t.Fatalf("covered gap = %q/%v, want resumed/nil", outcome, err)
	}
	if resumed.Days != 2 || resumed.FrozenDays != 0 {
		t.Fatalf("resumed = %+v, want days 2 and the budget spent", resumed)
	}
	broken, outcome, err := store.AdvanceChatStreak(ctx, a, b, day1.AddDate(0, 0, 5))
	if err != nil || outcome != domain.ChatStreakBroken {
		t.Fatalf("uncovered gap = %q/%v, want broken/nil", outcome, err)
	}
	if broken.Days != 1 || broken.FrozenDays != 0 {
		t.Fatalf("broken = %+v, want days 1 and no budget left", broken)
	}
}

// TestChatStreakStoreFreezeBeforeFirstMessage: the grant an operator gives to a
// pair that has never spoken has to survive until the pair does, or "he is about
// to lose it" becomes "the button did nothing".
func TestChatStreakStoreFreezeBeforeFirstMessage(t *testing.T) {
	pool := testPool(t)
	chatStreakReset(t, pool)
	store := NewChatStreakStore(pool)
	ctx := context.Background()
	a, b := int64(925001), int64(925002)

	granted, err := store.GrantChatStreakFreeze(ctx, a, b, 3, "tester", "warning")
	if err != nil {
		t.Fatalf("grant before any message: %v", err)
	}
	if granted.Days != 0 || granted.FrozenDays != 3 || !granted.LastDay.IsZero() {
		t.Fatalf("pre-message grant = %+v, want days 0 frozen 3 no last day", granted)
	}
	day1 := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	streak, outcome, err := store.AdvanceChatStreak(ctx, a, b, day1)
	if err != nil || outcome != domain.ChatStreakStarted {
		t.Fatalf("first message = %q/%v, want started/nil", outcome, err)
	}
	if streak.FrozenDays != 3 {
		t.Fatalf("FrozenDays = %d after the first message, want the grant to survive as 3", streak.FrozenDays)
	}
	// And the budget is usable straight away.
	resumed, outcome, err := store.AdvanceChatStreak(ctx, a, b, day1.AddDate(0, 0, 3))
	if err != nil || outcome != domain.ChatStreakResumed || resumed.FrozenDays != 1 {
		t.Fatalf("covered gap = %d/%q/%v, want days 2 resumed frozen 1", resumed.FrozenDays, outcome, err)
	}
}

// TestChatStreakStoreRejectsIneligiblePairs: a self-pair and a service account
// must not produce rows, and the table CHECK must reject an unordered pair
// written behind the store's back.
func TestChatStreakStoreRejectsIneligiblePairs(t *testing.T) {
	pool := testPool(t)
	chatStreakReset(t, pool)
	store := NewChatStreakStore(pool)
	ctx := context.Background()
	now := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)

	for _, pair := range [][2]int64{{930001, 930001}, {domain.OfficialSystemUserID, 930002}, {930002, 0}} {
		if _, _, err := store.AdvanceChatStreak(ctx, pair[0], pair[1], now); err == nil {
			t.Fatalf("pair %v was accepted", pair)
		}
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO chat_streaks (user_a, user_b, days) VALUES (940002, 940001, 1)`); err == nil {
		t.Fatal("the table accepted an unordered pair")
	}
}

// TestChatStreakStoreListAndLeaderboard: both surfaces read the pair from either
// side, and the leaderboard skips rows that were granted a budget but never
// counted a day.
func TestChatStreakStoreListAndLeaderboard(t *testing.T) {
	pool := testPool(t)
	chatStreakReset(t, pool)
	store := NewChatStreakStore(pool)
	ctx := context.Background()
	day1 := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)

	for _, pair := range [][2]int64{{950001, 950002}, {950003, 950004}} {
		for d := 0; d < 5; d++ {
			if _, _, err := store.AdvanceChatStreak(ctx, pair[0], pair[1], day1.AddDate(0, 0, d)); err != nil {
				t.Fatalf("pair %v day %d: %v", pair, d, err)
			}
		}
	}
	// A pair with a budget but no conversation yet.
	if _, err := store.GrantChatStreakFreeze(ctx, 950005, 950006, 5, "tester", "pre"); err != nil {
		t.Fatalf("pre-message grant: %v", err)
	}

	for _, userID := range []int64{950002, 950004} {
		mine, err := store.ListUserChatStreaks(ctx, userID, 0)
		if err != nil {
			t.Fatalf("list for %d: %v", userID, err)
		}
		if len(mine) != 1 || mine[0].Days != 5 {
			t.Fatalf("streaks for %d = %+v, want the one 5-day pair", userID, mine)
		}
	}
	// The second member of each pair sees the same row.
	mine, err := store.ListUserChatStreaks(ctx, 950001, 0)
	if err != nil || len(mine) != 1 || mine[0].UserB != 950002 {
		t.Fatalf("streaks for 950001 = %+v/%v", mine, err)
	}

	top, err := store.TopChatStreaks(ctx, 10)
	if err != nil {
		t.Fatalf("top: %v", err)
	}
	for _, streak := range top {
		if streak.Days == 0 || streak.LastDay.IsZero() {
			t.Fatalf("leaderboard contains a never-counted row: %+v", streak)
		}
	}
}
