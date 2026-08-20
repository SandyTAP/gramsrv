package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"telesrv/internal/store"
)

func TestDeliveryOutboxClientAckIsIdempotentButFencesReclaimedAttempt(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()

	claimOne := func(targetUserID int64) store.DeliveryOutboxItem {
		t.Helper()
		claimed, err := outbox.ClaimPending(ctx, 1)
		if err != nil {
			t.Fatalf("claim delivery outbox: %v", err)
		}
		if len(claimed) != 1 || claimed[0].TargetUserID != targetUserID {
			t.Fatalf("claimed = %+v, want one item for user %d", claimed, targetUserID)
		}
		return claimed[0]
	}
	ackFor := func(item store.DeliveryOutboxItem, serverMsgID int64) store.DeliveryOutboxClientAck {
		return store.DeliveryOutboxClientAck{
			OutboxID:     item.ID,
			TargetUserID: item.TargetUserID,
			Attempt:      item.Attempts,
			AuthKeyID:    [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
			SessionID:    9901,
			ServerMsgID:  serverMsgID,
			AckedAt:      time.Unix(1700002300, serverMsgID),
		}
	}

	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{TargetUserID: 1001, Payload: []byte{0x01}}); err != nil {
		t.Fatalf("enqueue early ack item: %v", err)
	}
	first := claimOne(1001)
	firstAck := ackFor(first, 9001)
	if err := outbox.MarkClientAcked(ctx, firstAck); err != nil {
		t.Fatalf("early ack while dispatching: %v", err)
	}
	if err := outbox.MarkClientAcked(ctx, firstAck); err != nil {
		t.Fatalf("duplicate delivery ack should be idempotent: %v", err)
	}

	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{TargetUserID: 1001, Payload: []byte{0x02}}); err != nil {
		t.Fatalf("enqueue stale attempt item: %v", err)
	}
	second := claimOne(1001)
	ageDeliveryAttempt(outbox, second.ID, second.TargetUserID, defaultMemoryDeliveryLease+time.Second)
	reclaimed := claimOne(1001)
	if reclaimed.ID != second.ID || reclaimed.Attempts != second.Attempts+1 {
		t.Fatalf("reclaimed = %+v, want same id %d attempts %d", reclaimed, second.ID, second.Attempts+1)
	}
	if err := outbox.MarkClientAcked(ctx, ackFor(second, 9002)); !errors.Is(err, store.ErrDispatchLeaseLost) {
		t.Fatalf("stale ack err = %v, want ErrDispatchLeaseLost", err)
	}
	if err := outbox.MarkClientAcked(ctx, ackFor(reclaimed, 9003)); err != nil {
		t.Fatalf("current ack: %v", err)
	}
}

func TestDeliveryOutboxCompletionRequiresCurrentDispatchingAttempt(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{TargetUserID: 1002, Payload: []byte{0x01}}); err != nil {
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
	ageDeliveryAttempt(outbox, item.ID, item.TargetUserID, defaultMemoryDeliveryLease+time.Second)
	reclaimed := claimOneForTest(t, ctx, outbox, item.TargetUserID)
	if reclaimed.ID != item.ID || reclaimed.Attempts != item.Attempts+1 {
		t.Fatalf("reclaimed = %+v, want same id %d attempts %d", reclaimed, item.ID, item.Attempts+1)
	}
	if err := outbox.MarkDelivered(ctx, item); !errors.Is(err, store.ErrDispatchLeaseLost) {
		t.Fatalf("delivered stale attempt err = %v, want ErrDispatchLeaseLost", err)
	}
	if err := outbox.MarkFailed(ctx, item, "stale worker"); !errors.Is(err, store.ErrDispatchLeaseLost) {
		t.Fatalf("failed stale attempt err = %v, want ErrDispatchLeaseLost", err)
	}
	if err := outbox.MarkAbandoned(ctx, item, "stale worker"); !errors.Is(err, store.ErrDispatchLeaseLost) {
		t.Fatalf("abandoned stale attempt err = %v, want ErrDispatchLeaseLost", err)
	}
	if err := outbox.MarkClientAcked(ctx, store.DeliveryOutboxClientAck{
		OutboxID:     reclaimed.ID,
		TargetUserID: reclaimed.TargetUserID,
		Attempt:      reclaimed.Attempts,
		AuthKeyID:    [8]byte{8, 7, 6, 5, 4, 3, 2, 1},
		SessionID:    9902,
		ServerMsgID:  9101,
		AckedAt:      time.Unix(1700002301, 0),
	}); err != nil {
		t.Fatalf("ack current dispatching row: %v", err)
	}
}

func claimOneForTest(t *testing.T, ctx context.Context, outbox *DeliveryOutboxStore, targetUserID int64) store.DeliveryOutboxItem {
	t.Helper()
	claimed, err := outbox.ClaimPending(ctx, 1)
	if err != nil {
		t.Fatalf("claim delivery outbox: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TargetUserID != targetUserID {
		t.Fatalf("claimed = %+v, want one item for user %d", claimed, targetUserID)
	}
	return claimed[0]
}

func ageDeliveryAttempt(outbox *DeliveryOutboxStore, outboxID, targetUserID int64, age time.Duration) {
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	for i := range outbox.items {
		if outbox.items[i].ID == outboxID && outbox.items[i].TargetUserID == targetUserID {
			outbox.items[i].updatedAt = time.Now().Add(-age)
			return
		}
	}
}
