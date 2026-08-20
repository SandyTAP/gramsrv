package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "telesrv/internal/store"
)

func TestDeliveryOutboxClientAckIsIdempotentButFencesReclaimedAttemptPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1887"+suffix+"01", "DeliveryAck", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM edge_delivery_outbox WHERE target_user_id = $1", owner.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `DELETE FROM edge_delivery_outbox`); err != nil {
		t.Fatalf("isolate edge delivery outbox: %v", err)
	}

	outbox := NewDeliveryOutboxStore(tx, WithDeliveryLeaseTimeout(time.Second))
	claimOne := func() storepkg.DeliveryOutboxItem {
		t.Helper()
		claimed, err := outbox.ClaimPending(ctx, 1)
		if err != nil {
			t.Fatalf("claim delivery outbox: %v", err)
		}
		if len(claimed) != 1 || claimed[0].TargetUserID != owner.ID {
			t.Fatalf("claimed = %+v, want one item for user %d", claimed, owner.ID)
		}
		return claimed[0]
	}
	ackFor := func(item storepkg.DeliveryOutboxItem, serverMsgID int64) storepkg.DeliveryOutboxClientAck {
		return storepkg.DeliveryOutboxClientAck{
			OutboxID:     item.ID,
			TargetUserID: item.TargetUserID,
			Attempt:      item.Attempts,
			AuthKeyID:    [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
			SessionID:    9901,
			ServerMsgID:  serverMsgID,
			AckedAt:      time.Unix(1700002400, serverMsgID),
		}
	}
	enqueue := func(payload byte) {
		t.Helper()
		if _, err := outbox.Enqueue(ctx, storepkg.DeliveryOutboxEnqueue{
			TargetUserID: owner.ID,
			Payload:      []byte{payload},
		}); err != nil {
			t.Fatalf("enqueue delivery outbox: %v", err)
		}
	}

	enqueue(0x01)
	first := claimOne()
	firstAck := ackFor(first, 9001)
	if err := outbox.MarkClientAcked(ctx, firstAck); err != nil {
		t.Fatalf("early ack while dispatching: %v", err)
	}
	if err := outbox.MarkClientAcked(ctx, firstAck); err != nil {
		t.Fatalf("duplicate delivery ack should be idempotent: %v", err)
	}

	enqueue(0x02)
	second := claimOne()
	if _, err := tx.Exec(ctx, `
UPDATE edge_delivery_outbox
SET updated_at = now() - interval '2 hours'
WHERE target_user_id = $1 AND id = $2`, owner.ID, second.ID); err != nil {
		t.Fatalf("age delivery lease: %v", err)
	}
	reclaimed := claimOne()
	if reclaimed.ID != second.ID || reclaimed.Attempts != second.Attempts+1 {
		t.Fatalf("reclaimed = %+v, want same id %d attempts %d", reclaimed, second.ID, second.Attempts+1)
	}
	if err := outbox.MarkClientAcked(ctx, ackFor(second, 9002)); !errors.Is(err, storepkg.ErrDispatchLeaseLost) {
		t.Fatalf("stale ack err = %v, want ErrDispatchLeaseLost", err)
	}
	if err := outbox.MarkClientAcked(ctx, ackFor(reclaimed, 9003)); err != nil {
		t.Fatalf("current ack: %v", err)
	}
}

func TestDeliveryOutboxCompletionRequiresCurrentDispatchingAttemptPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1887"+suffix+"02", "DeliveryFence", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM edge_delivery_outbox WHERE target_user_id = $1", owner.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `DELETE FROM edge_delivery_outbox`); err != nil {
		t.Fatalf("isolate edge delivery outbox: %v", err)
	}

	outbox := NewDeliveryOutboxStore(tx, WithDeliveryLeaseTimeout(time.Second))
	if _, err := outbox.Enqueue(ctx, storepkg.DeliveryOutboxEnqueue{TargetUserID: owner.ID, Payload: []byte{0x01}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := outbox.ClaimPending(ctx, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed = %+v, want one item", claimed)
	}
	item := claimed[0]
	if _, err := tx.Exec(ctx, `
UPDATE edge_delivery_outbox
SET updated_at = now() - interval '2 hours'
WHERE target_user_id = $1 AND id = $2`, item.TargetUserID, item.ID); err != nil {
		t.Fatalf("age delivery lease: %v", err)
	}
	reclaimed, err := outbox.ClaimPending(ctx, 1)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != item.ID || reclaimed[0].Attempts != item.Attempts+1 {
		t.Fatalf("reclaimed = %+v, want same id %d attempts %d", reclaimed, item.ID, item.Attempts+1)
	}
	if err := outbox.MarkDelivered(ctx, item); !errors.Is(err, storepkg.ErrDispatchLeaseLost) {
		t.Fatalf("delivered stale attempt err = %v, want ErrDispatchLeaseLost", err)
	}
	if err := outbox.MarkFailed(ctx, item, "stale worker"); !errors.Is(err, storepkg.ErrDispatchLeaseLost) {
		t.Fatalf("failed stale attempt err = %v, want ErrDispatchLeaseLost", err)
	}
	if err := outbox.MarkAbandoned(ctx, item, "stale worker"); !errors.Is(err, storepkg.ErrDispatchLeaseLost) {
		t.Fatalf("abandoned stale attempt err = %v, want ErrDispatchLeaseLost", err)
	}
	if err := outbox.MarkClientAcked(ctx, storepkg.DeliveryOutboxClientAck{
		OutboxID:     reclaimed[0].ID,
		TargetUserID: reclaimed[0].TargetUserID,
		Attempt:      reclaimed[0].Attempts,
		AuthKeyID:    [8]byte{8, 7, 6, 5, 4, 3, 2, 1},
		SessionID:    9902,
		ServerMsgID:  9101,
		AckedAt:      time.Unix(1700002401, 0),
	}); err != nil {
		t.Fatalf("ack current dispatching row: %v", err)
	}

	if _, err := outbox.Enqueue(ctx, storepkg.DeliveryOutboxEnqueue{TargetUserID: owner.ID, Payload: []byte{0x02}}); err != nil {
		t.Fatalf("enqueue abandon target: %v", err)
	}
	claimedAgain, err := outbox.ClaimPending(ctx, 1)
	if err != nil {
		t.Fatalf("claim abandon target: %v", err)
	}
	if len(claimedAgain) != 1 {
		t.Fatalf("claimed abandon target = %+v, want one item", claimedAgain)
	}
	stale := claimedAgain[0]
	stale.Attempts--
	if err := outbox.MarkAbandoned(ctx, stale, "stale worker"); !errors.Is(err, storepkg.ErrDispatchLeaseLost) {
		t.Fatalf("stale abandon err = %v, want ErrDispatchLeaseLost", err)
	}
	if err := outbox.MarkAbandoned(ctx, claimedAgain[0], "online delivery attempts exhausted"); err != nil {
		t.Fatalf("abandon dispatching row: %v", err)
	}
	var remaining int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1 AND id = $2`, claimedAgain[0].TargetUserID, claimedAgain[0].ID).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("remaining abandoned delivery rows = %d err=%v, want 0", remaining, err)
	}
}
