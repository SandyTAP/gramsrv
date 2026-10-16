package stargiveaways

import (
	"context"
	"errors"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// chargeStore records charges and moves a fake ledger.
type chargeStore struct {
	EntryStore
	run      domain.StarsGiveawayRun
	charges  []domain.ChargeGiveawayEntry
	entries  map[int64]bool
	balances map[int64]int64
	refunded []domain.ChargeGiveawayEntry
}

func newChargeStore(run domain.StarsGiveawayRun) *chargeStore {
	return &chargeStore{
		run:      run,
		entries:  map[int64]bool{},
		balances: map[int64]int64{},
	}
}

func (c *chargeStore) StarsGiveawayByID(context.Context, int64) (domain.StarsGiveawayRun, error) {
	return c.run, nil
}

func (c *chargeStore) PayStarsGiveawayEntry(_ context.Context, charge domain.ChargeGiveawayEntry) (domain.StarsGiveawayEntry, bool, error) {
	// Mirror the real ON CONFLICT DO NOTHING: a repeat join is detected before
	// any money moves.
	if c.entries[charge.ParticipantUserID] {
		return domain.StarsGiveawayEntry{}, false, nil
	}
	if err := domain.ValidateGiveawayEntryCharge(charge); err != nil {
		return domain.StarsGiveawayEntry{}, false, err
	}
	c.entries[charge.ParticipantUserID] = true
	c.charges = append(c.charges, charge)
	c.balances[charge.ParticipantUserID] -= charge.FeeStars
	c.balances[charge.HolderUserID] += charge.FeeStars
	return domain.StarsGiveawayEntry{GiveawayID: charge.GiveawayID, UserID: charge.ParticipantUserID}, true, nil
}

func (c *chargeStore) ActiveStarsGiveawayByMessage(context.Context, int64, int, int) (domain.StarsGiveawayRun, bool, error) {
	return c.run, true, nil
}

func (c *chargeStore) RefundStarsGiveawayEntry(_ context.Context, giveawayID, participant, holder, fee int64, _ int) error {
	c.refunded = append(c.refunded, domain.ChargeGiveawayEntry{
		ParticipantUserID: participant, HolderUserID: holder, GiveawayID: giveawayID, FeeStars: fee,
	})
	c.balances[participant] += fee
	c.balances[holder] -= fee
	delete(c.entries, participant)
	return nil
}

func paidGiveaway() domain.StarsGiveawayRun {
	return domain.StarsGiveawayRun{
		ID: 1, BuyerUserID: 777, ChannelID: 932,
		Stars: 5000, Users: 5, PerUserStars: 1000,
		UntilDate: 5000, CreatedAt: 100, State: domain.StarsGiveawayActive,
	}
}

// TestJoinPaidChargesTenStars: the entry fee is the configured flat amount and
// it moves to the holder, so the ledger is conserved.
func TestJoinPaidChargesTenStars(t *testing.T) {
	store := newChargeStore(paidGiveaway())
	svc := NewEnterService(store, func() time.Time { return time.Unix(1000, 0) })

	if _, err := svc.JoinPaid(context.Background(), 1, 501, 0); err != nil {
		t.Fatalf("JoinPaid: %v", err)
	}
	if len(store.charges) != 1 {
		t.Fatalf("charges = %d, want 1", len(store.charges))
	}
	if got := store.charges[0].FeeStars; got != domain.StarsGiveawayEntryFeeStars {
		t.Fatalf("fee = %d, want %d", got, domain.StarsGiveawayEntryFeeStars)
	}
	if store.balances[501] != -10 {
		t.Fatalf("participant balance = %d, want -10", store.balances[501])
	}
	if store.balances[777] != 10 {
		t.Fatalf("holder balance = %d, want +10", store.balances[777])
	}
	// Conserved: the two legs must always cancel.
	if store.balances[501]+store.balances[777] != 0 {
		t.Fatalf("ledger not conserved: %+v", store.balances)
	}
}

// TestJoinPaidRepeatIsFree: tapping twice must not cost twice. A paid entry that
// charges on every retry is a billing bug users notice immediately.
func TestJoinPaidRepeatIsFree(t *testing.T) {
	store := newChargeStore(paidGiveaway())
	svc := NewEnterService(store, func() time.Time { return time.Unix(1000, 0) })

	for i := 0; i < 4; i++ {
		if _, err := svc.JoinPaid(context.Background(), 1, 501, 0); err != nil {
			t.Fatalf("JoinPaid %d: %v", i, err)
		}
	}
	if store.balances[501] != -10 {
		t.Fatalf("participant charged %d over 4 taps, want -10 total", store.balances[501])
	}
	if len(store.charges) != 1 {
		t.Fatalf("charges = %d, want 1", len(store.charges))
	}
}

// TestJoinPaidRejectsSelfEntry: the holder funded the prize and must not also
// win it back by entering.
func TestJoinPaidRejectsSelfEntry(t *testing.T) {
	store := newChargeStore(paidGiveaway())
	svc := NewEnterService(store, func() time.Time { return time.Unix(1000, 0) })

	_, err := svc.JoinPaid(context.Background(), 1, 777, 0)
	if !errors.Is(err, domain.ErrGiveawaySelfEntry) {
		t.Fatalf("self entry err = %v", err)
	}
	if store.balances[777] != 0 {
		t.Fatalf("holder balance moved by %d", store.balances[777])
	}
}

// TestJoinPaidRejectsClosedGiveaway: no entries after the deadline.
func TestJoinPaidRejectsClosedGiveaway(t *testing.T) {
	for _, state := range []string{domain.StarsGiveawayCompleted, domain.StarsGiveawayCancelled} {
		run := paidGiveaway()
		run.State = state
		svc := NewEnterService(newChargeStore(run), func() time.Time { return time.Unix(1000, 0) })
		if _, err := svc.JoinPaid(context.Background(), 1, 501, 0); !errors.Is(err, domain.ErrGiveawayNotActive) {
			t.Fatalf("state %s err = %v", state, err)
		}
	}
}

// TestRefundEntryRestoresLedger: a refunded entry must leave no balance change
// and no participant, or a user who never posted would still be in the draw.
func TestRefundEntryRestoresLedger(t *testing.T) {
	store := newChargeStore(paidGiveaway())
	svc := NewEnterService(store, func() time.Time { return time.Unix(1000, 0) })

	if _, err := svc.JoinPaid(context.Background(), 1, 501, 0); err != nil {
		t.Fatalf("JoinPaid: %v", err)
	}
	if err := svc.RefundEntry(context.Background(), 1, 501, 0); err != nil {
		t.Fatalf("RefundEntry: %v", err)
	}
	if store.balances[501] != 0 || store.balances[777] != 0 {
		t.Fatalf("refund left balances %+v, want both zero", store.balances)
	}
	if store.entries[501] {
		t.Fatal("refunded user is still a participant")
	}
	if len(store.refunded) != 1 || store.refunded[0].FeeStars != domain.StarsGiveawayEntryFeeStars {
		t.Fatalf("refund = %+v", store.refunded)
	}
}
