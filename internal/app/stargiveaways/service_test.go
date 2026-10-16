package stargiveaways

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// fakeStore is an in-memory DrawStore that mimics the real store's two
// load-bearing guarantees: entries are idempotent per (giveaway, user), and a
// draw commits exactly once.
type fakeStore struct {
	mu        sync.Mutex
	runs      map[int64]domain.StarsGiveawayRun
	entries   map[int64][]domain.StarsGiveawayEntry
	winners   map[int64][]int64
	drawn     map[int64]bool
	reward    map[int64]map[int64]string
	rewardErr map[int64]map[int64]string
	commitErr error
	commits   int
}

func newFakeStore(runs ...domain.StarsGiveawayRun) *fakeStore {
	f := &fakeStore{
		runs:      map[int64]domain.StarsGiveawayRun{},
		entries:   map[int64][]domain.StarsGiveawayEntry{},
		winners:   map[int64][]int64{},
		drawn:     map[int64]bool{},
		reward:    map[int64]map[int64]string{},
		rewardErr: map[int64]map[int64]string{},
	}
	for _, run := range runs {
		f.runs[run.ID] = run
	}
	return f
}

func (f *fakeStore) DueStarsGiveaways(_ context.Context, now, _ int) ([]domain.StarsGiveawayRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.StarsGiveawayRun
	for _, run := range f.runs {
		if (run.State == domain.StarsGiveawayActive && run.UntilDate <= now) ||
			(run.State == domain.StarsGiveawayCompleted && len(f.pendingLocked(run.ID)) > 0) {
			out = append(out, run)
		}
	}
	return out, nil
}

func (f *fakeStore) pendingLocked(giveawayID int64) []int64 {
	var out []int64
	for _, w := range f.winners[giveawayID] {
		// Only rows still in 'won' are outstanding, exactly like the real
		// StarsGiveawayPendingPayouts filter on reward_state.
		st := f.reward[giveawayID][w]
		if st == "" || st == "won" || st == "failed" {
			out = append(out, w)
		}
	}
	return out
}

func (f *fakeStore) ListStarsGiveawayEntries(_ context.Context, giveawayID int64, _ int) ([]domain.StarsGiveawayEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.StarsGiveawayEntry(nil), f.entries[giveawayID]...), nil
}

func (f *fakeStore) CommitStarsGiveawayDraw(_ context.Context, draw domain.StarsGiveawayDraw, winners []int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commitErr != nil {
		return false, f.commitErr
	}
	if f.drawn[draw.GiveawayID] {
		return false, nil
	}
	f.drawn[draw.GiveawayID] = true
	f.commits++
	f.winners[draw.GiveawayID] = append([]int64(nil), winners...)
	run := f.runs[draw.GiveawayID]
	run.State = domain.StarsGiveawayCompleted
	f.runs[draw.GiveawayID] = run
	return true, nil
}

func (f *fakeStore) StarsGiveawayPendingPayouts(_ context.Context, giveawayID int64, _ int) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pendingLocked(giveawayID), nil
}

func (f *fakeStore) MarkStarsGiveawayEntryPaid(_ context.Context, giveawayID, userID int64, _ int, failure string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reward[giveawayID] == nil {
		f.reward[giveawayID] = map[int64]string{}
		f.rewardErr[giveawayID] = map[int64]string{}
	}
	if failure != "" {
		f.reward[giveawayID][userID] = "failed"
		f.rewardErr[giveawayID][userID] = failure
	} else {
		f.reward[giveawayID][userID] = "paid"
	}
	return nil
}

func (f *fakeStore) StarsGiveawayByID(_ context.Context, giveawayID int64) (domain.StarsGiveawayRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	run, ok := f.runs[giveawayID]
	if !ok {
		return domain.StarsGiveawayRun{}, domain.ErrGiveawayNotFound
	}
	return run, nil
}

// fakeCredit records credits and can be told to fail for one account.
type fakeCredit struct {
	mu       sync.Mutex
	granted  map[int64]int64
	failWith map[int64]error
}

func newFakeCredit() *fakeCredit {
	return &fakeCredit{granted: map[int64]int64{}, failWith: map[int64]error{}}
}

func (c *fakeCredit) Credit(_ context.Context, userID, amount int64, _ domain.StarsTransactionReason, _ domain.Peer, _, _ string) (domain.StarsBalance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.failWith[userID]; err != nil {
		return domain.StarsBalance{}, err
	}
	c.granted[userID] += amount
	return domain.StarsBalance{Balance: c.granted[userID]}, nil
}

func endedGiveaway(id int64, stars int64, users int) domain.StarsGiveawayRun {
	return domain.StarsGiveawayRun{
		ID:           id,
		ChannelID:    932,
		LaunchMsgID:  7,
		Stars:        stars,
		Users:        users,
		PerUserStars: stars / int64(users),
		UntilDate:    1000,
		CreatedAt:    100,
		State:        domain.StarsGiveawayActive,
	}
}

func newService(store *fakeStore, credit *fakeCredit, now func() time.Time) *Service {
	return New(store, credit, Options{Interval: time.Minute, BatchSize: 10, Now: now, Log: func(error) {}})
}

// TestSweepDrawsAndPays: the happy path a 5000-star / 5-winner giveaway takes.
func TestSweepDrawsAndPays(t *testing.T) {
	run := endedGiveaway(1, 5000, 5)
	store := newFakeStore(run)
	for _, id := range []int64{101, 102, 103, 104, 105, 106, 107} {
		store.entries[1] = append(store.entries[1], domain.StarsGiveawayEntry{GiveawayID: 1, UserID: id, EnteredAt: 200})
	}
	credit := newFakeCredit()
	svc := newService(store, credit, func() time.Time { return time.Unix(2000, 0) })

	if err := svc.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(store.winners[1]) != 5 {
		t.Fatalf("winners = %v, want 5", store.winners[1])
	}
	// The prize is split per winner, so 5 x 1000 must be paid, no more.
	var total int64
	for _, amt := range credit.granted {
		total += amt
	}
	if total != 5000 {
		t.Fatalf("granted %d stars, want exactly the funded 5000", total)
	}
	for id, amt := range credit.granted {
		if amt != 1000 {
			t.Fatalf("user %d got %d, want 1000 each", id, amt)
		}
	}
}

// TestSweepPaysExactlyOnce: re-running the worker must not pay again. This is
// the property the WHERE state='active' guard buys, and it is the difference
// between a giveaway and a money printer.
func TestSweepPaysExactlyOnce(t *testing.T) {
	store := newFakeStore(endedGiveaway(1, 5000, 5))
	for _, id := range []int64{101, 102, 103, 104, 105, 106, 107} {
		store.entries[1] = append(store.entries[1], domain.StarsGiveawayEntry{GiveawayID: 1, UserID: id, EnteredAt: 200})
	}
	credit := newFakeCredit()
	svc := newService(store, credit, func() time.Time { return time.Unix(2000, 0) })

	for i := 0; i < 5; i++ {
		if err := svc.Sweep(context.Background()); err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
	}
	var total int64
	for _, amt := range credit.granted {
		total += amt
	}
	if total != 5000 {
		t.Fatalf("after 5 sweeps granted %d stars, want 5000", total)
	}
	if store.commits != 1 {
		t.Fatalf("commits = %d, want exactly 1", store.commits)
	}
}

// TestSweepRetriesFailedPayout: a credit error must not lose the prize, and must
// not stop the other winners being paid.
func TestSweepRetriesFailedPayout(t *testing.T) {
	// 6000/3 rather than 5000/3: the real table enforces
	// users * per_user_stars = stars, so a giveaway whose prize does not divide
	// evenly cannot exist. Using 5000/3 here would assert against a row the
	// database would reject.
	store := newFakeStore(endedGiveaway(1, 6000, 3))
	for _, id := range []int64{101, 102, 103, 104, 105, 106, 107} {
		store.entries[1] = append(store.entries[1], domain.StarsGiveawayEntry{GiveawayID: 1, UserID: id, EnteredAt: 200})
	}
	credit := newFakeCredit()
	svc := newService(store, credit, func() time.Time { return time.Unix(2000, 0) })

	// Draw first with the credit path broken for one account.
	draw, err := domain.DrawStarsGiveawayWinners(store.runs[1], store.entries[1], time.Unix(2000, 0))
	if err != nil {
		t.Fatalf("Draw: %v", err)
	}
	failing := draw.Winners[0]
	credit.failWith[failing] = errors.New("ledger unavailable")

	if err := svc.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(store.winners[1]) != 3 {
		t.Fatalf("winners = %v", store.winners[1])
	}
	// The two healthy winners are paid despite the third failing.
	paid := 0
	for _, amt := range credit.granted {
		_ = amt
		paid++
	}
	if paid != 2 {
		t.Fatalf("paid %d winners, want 2 healthy ones", paid)
	}

	// The failure is recorded on the row so it stays in the work queue.
	if store.rewardErr[1][failing] == "" {
		t.Fatal("failed winner was not recorded as failed")
	}

	// Once the ledger recovers, the next sweep pays the straggler.
	delete(credit.failWith, failing)
	if err := svc.Sweep(context.Background()); err != nil {
		t.Fatalf("recovery sweep: %v", err)
	}
	if credit.granted[failing] == 0 {
		t.Fatal("failed winner was never retried after the ledger recovered")
	}
	var total int64
	for _, amt := range credit.granted {
		total += amt
	}
	if total != 6000 {
		t.Fatalf("granted %d stars, want exactly 6000 after recovery", total)
	}
}

// TestSweepLeavesUnenteredGiveawayAlone: no participants means no draw, and no
// zero-winner completion.
func TestSweepLeavesUnenteredGiveawayAlone(t *testing.T) {
	store := newFakeStore(endedGiveaway(1, 5000, 5))
	credit := newFakeCredit()
	svc := newService(store, credit, func() time.Time { return time.Unix(2000, 0) })

	if err := svc.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if store.commits != 0 {
		t.Fatalf("commits = %d, want 0 for a giveaway nobody entered", store.commits)
	}
	if len(credit.granted) != 0 {
		t.Fatalf("granted %v to nobody", credit.granted)
	}
}

// TestSweepSkipsRunningGiveaway: a giveaway that has not ended must not be
// drawn, however many people have entered.
func TestSweepSkipsRunningGiveaway(t *testing.T) {
	run := endedGiveaway(1, 5000, 5)
	run.UntilDate = 5000
	store := newFakeStore(run)
	for _, id := range []int64{101, 102, 103} {
		store.entries[1] = append(store.entries[1], domain.StarsGiveawayEntry{GiveawayID: 1, UserID: id, EnteredAt: 200})
	}
	credit := newFakeCredit()
	svc := newService(store, credit, func() time.Time { return time.Unix(2000, 0) })

	if err := svc.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if store.commits != 0 {
		t.Fatalf("commits = %d, want 0 before the deadline", store.commits)
	}
}
