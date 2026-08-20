package egress

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

func TestDeliveryDispatcherMarksServiceConfirmedPushDelivered(t *testing.T) {
	const userID = int64(1000000002)
	outbox := &captureDeliveryOutbox{items: []store.DeliveryOutboxItem{{
		ID:               91,
		TargetUserID:     userID,
		ExcludeAuthKeyID: [8]byte{1},
		ExcludeSessionID: 99,
		Payload:          []byte{1, 2, 3},
	}}}
	deliverer := &captureOutboxDeliverer{
		result: edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryDelivered, Sent: 1},
	}
	metrics := &captureOutboxMetrics{}
	dispatcher := NewDeliveryDispatcher(outbox, deliverer, zaptest.NewLogger(t), WithDeliveryMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if len(deliverer.requests) != 1 {
		t.Fatalf("edge delivery calls=%d, want 1", len(deliverer.requests))
	}
	if !outbox.delivered || outbox.failed {
		t.Fatalf("edge delivery delivered=%v failed=%v, want delivered after Edge service ACK", outbox.delivered, outbox.failed)
	}
	if metrics.delivered != 1 || metrics.failed != 0 {
		t.Fatalf("metrics delivered=%d failed=%d, want 1/0", metrics.delivered, metrics.failed)
	}
}

func TestDeliveryDispatcherAbandonsIndeterminateAtAttemptLimit(t *testing.T) {
	const userID = int64(1000000002)
	outbox := &captureDeliveryOutbox{items: []store.DeliveryOutboxItem{{
		ID:               91,
		TargetUserID:     userID,
		ExcludeAuthKeyID: [8]byte{1},
		ExcludeSessionID: 99,
		Attempts:         maxOnlineDeliveryAttempts,
		Payload:          []byte{1, 2, 3},
	}}}
	deliverer := &captureOutboxDeliverer{
		result: edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryIndeterminate},
	}
	metrics := &captureOutboxMetrics{}
	dispatcher := NewDeliveryDispatcher(outbox, deliverer, zaptest.NewLogger(t), WithDeliveryMetrics(metrics))
	dispatcher.DispatchOnce(context.Background())

	if len(deliverer.requests) != 1 {
		t.Fatalf("edge delivery calls=%d, want 1 final attempt", len(deliverer.requests))
	}
	req := deliverer.lastRequest()
	if got, want := req.DeliveryRef, (edgecontrol.OutboxDeliveryRef{OutboxID: 91, TargetUserID: userID, Pts: 0, Attempt: maxOnlineDeliveryAttempts}); got != want {
		t.Fatalf("edge delivery ref = %+v, want %+v", got, want)
	}
	if !outbox.abandoned || outbox.abandonedID != 91 {
		t.Fatalf("edge delivery abandoned=%v id=%d, want exhausted task abandoned", outbox.abandoned, outbox.abandonedID)
	}
	if outbox.delivered || outbox.failed {
		t.Fatalf("edge delivery delivered=%v failed=%v, want abandon only", outbox.delivered, outbox.failed)
	}
	if metrics.failed != 1 {
		t.Fatalf("metrics.failed=%d, want 1", metrics.failed)
	}
}

func TestDeliveryDispatcherAbandonsExceededOnlineAttemptsWithoutPush(t *testing.T) {
	const userID = int64(1000000002)
	outbox := &captureDeliveryOutbox{items: []store.DeliveryOutboxItem{{
		ID:               91,
		TargetUserID:     userID,
		ExcludeAuthKeyID: [8]byte{1},
		ExcludeSessionID: 99,
		Attempts:         maxOnlineDeliveryAttempts + 1,
		Payload:          []byte{1, 2, 3},
	}}}
	deliverer := &captureOutboxDeliverer{
		result: edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryDelivered, Sent: 1},
	}
	dispatcher := NewDeliveryDispatcher(outbox, deliverer, zaptest.NewLogger(t))
	dispatcher.DispatchOnce(context.Background())

	if len(deliverer.requests) != 0 {
		t.Fatalf("edge delivery calls=%d, want no push after exhausted online attempts", len(deliverer.requests))
	}
	if !outbox.abandoned {
		t.Fatal("exhausted edge delivery attempts did not abandon online task")
	}
	if outbox.delivered || outbox.failed {
		t.Fatalf("edge delivery delivered=%v failed=%v, want abandon only", outbox.delivered, outbox.failed)
	}
}

type captureDeliveryOutbox struct {
	items          []store.DeliveryOutboxItem
	delivered      bool
	abandoned      bool
	abandonedID    int64
	abandonedError string
	failed         bool
}

func (s *captureDeliveryOutbox) Enqueue(_ context.Context, item store.DeliveryOutboxEnqueue) (store.DeliveryOutboxItem, error) {
	out := store.DeliveryOutboxItem{
		ID:               int64(len(s.items) + 1),
		TargetUserID:     item.TargetUserID,
		ExcludeAuthKeyID: item.ExcludeAuthKeyID,
		ExcludeSessionID: item.ExcludeSessionID,
		Payload:          append([]byte(nil), item.Payload...),
	}
	s.items = append(s.items, out)
	return out, nil
}

func (s *captureDeliveryOutbox) ClaimPending(context.Context, int) ([]store.DeliveryOutboxItem, error) {
	items := s.items
	s.items = nil
	return items, nil
}

func (s *captureDeliveryOutbox) MarkDelivered(context.Context, store.DeliveryOutboxItem) error {
	s.delivered = true
	return nil
}

func (s *captureDeliveryOutbox) MarkAbandoned(_ context.Context, item store.DeliveryOutboxItem, lastError string) error {
	s.abandoned = true
	s.abandonedID = item.ID
	s.abandonedError = lastError
	return nil
}

func (s *captureDeliveryOutbox) MarkFailed(context.Context, store.DeliveryOutboxItem, string) error {
	s.failed = true
	return nil
}

func (s *captureDeliveryOutbox) DeleteFailed(context.Context, time.Duration, int) (int, error) {
	return 0, nil
}
