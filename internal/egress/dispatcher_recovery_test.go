package egress

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

func TestOutboxDispatcherCompletesOnEdgeAcceptedAndIgnoresLateClientAck(t *testing.T) {
	ctx := context.Background()
	item := store.DispatchOutboxItem{
		ID:           77,
		TargetUserID: 42,
		Pts:          11,
		EventType:    domain.UpdateEventNewMessage,
	}
	events := singleUpdateEventStore{event: domain.UpdateEvent{
		UserID: item.TargetUserID,
		Pts:    item.Pts,
		Type:   item.EventType,
	}}
	outbox := newReclaimingOutboxStore(item)
	deliverer := &recordingDeliveredOutbox{}
	dispatcher := NewOutboxDispatcher(events, outbox, deliverer, zaptest.NewLogger(t),
		WithOutboxBatch(1),
		WithOutboxUpdateBuilder(func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
			updates := make([][]byte, len(requests))
			for i := range updates {
				raw, err := edgecontrol.EncodeOutboxUpdate(&tg.Updates{})
				if err != nil {
					t.Fatalf("EncodeOutboxUpdate: %v", err)
				}
				updates[i] = raw
			}
			return updates, nil
		}),
	)

	if !dispatcher.DispatchOnce(ctx) {
		t.Fatal("first dispatch did not claim outbox item")
	}
	if got, want := outbox.status(), "delivered"; got != want {
		t.Fatalf("status after first dispatch = %q, want %q", got, want)
	}
	if got, want := deliverer.refs(), []edgecontrol.OutboxDeliveryRef{{OutboxID: 77, TargetUserID: 42, Pts: 11, Attempt: 1}}; !deliveryRefsEqual(got, want) {
		t.Fatalf("delivery refs after first dispatch = %+v, want %+v", got, want)
	}

	outbox.allowReclaim()
	if dispatcher.DispatchOnce(ctx) {
		t.Fatal("service-confirmed outbox item was claimed again")
	}
	lateAck := store.DispatchOutboxClientAck{
		OutboxID:     item.ID,
		TargetUserID: item.TargetUserID,
		Pts:          item.Pts,
		Attempt:      1,
		AuthKeyID:    [8]byte{1},
		SessionID:    1001,
		ServerMsgID:  9001,
		AckedAt:      time.Unix(1700000001, 0),
	}
	if err := outbox.MarkClientAcked(ctx, lateAck); err != nil {
		t.Fatalf("late client ack must be idempotent after service-confirmed delivery: %v", err)
	}
	if !outbox.completed() {
		t.Fatal("delivered outbox item lost completion state after late client ack")
	}
	if dispatcher.DispatchOnce(ctx) {
		t.Fatal("completed outbox item was claimed again")
	}
}

type singleUpdateEventStore struct {
	event domain.UpdateEvent
}

func (s singleUpdateEventStore) Append(context.Context, int64, domain.UpdateEvent) error {
	return nil
}

func (s singleUpdateEventStore) AppendAllocated(_ context.Context, _ int64, event domain.UpdateEvent) (domain.UpdateEvent, error) {
	return event, nil
}

func (s singleUpdateEventStore) ListAfter(_ context.Context, userID int64, pts int, limit int) ([]domain.UpdateEvent, error) {
	if limit <= 0 || s.event.UserID != userID || s.event.Pts <= pts {
		return nil, nil
	}
	return []domain.UpdateEvent{s.event}, nil
}

func (s singleUpdateEventStore) MaxContiguousPts(context.Context, int64) (int, error) {
	return s.event.Pts, nil
}

type reclaimingOutboxStore struct {
	mu           sync.Mutex
	item         store.DispatchOutboxItem
	state        string
	attempt      int
	reclaimReady bool
	done         bool
}

func newReclaimingOutboxStore(item store.DispatchOutboxItem) *reclaimingOutboxStore {
	return &reclaimingOutboxStore{item: item, state: "pending"}
}

func (s *reclaimingOutboxStore) ClaimPending(context.Context, int) ([]store.DispatchOutboxItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return nil, nil
	}
	switch s.state {
	case "pending":
	case "dispatching":
		if !s.reclaimReady {
			return nil, nil
		}
		s.reclaimReady = false
	default:
		return nil, nil
	}
	s.attempt++
	s.state = "dispatching"
	item := s.item
	item.Attempts = s.attempt
	return []store.DispatchOutboxItem{item}, nil
}

func (s *reclaimingOutboxStore) MarkDelivered(_ context.Context, item store.DispatchOutboxItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.matchesLocked(item) || s.state != "dispatching" {
		return store.ErrDispatchLeaseLost
	}
	s.done = true
	s.state = "delivered"
	return nil
}

func (s *reclaimingOutboxStore) MarkAbandoned(_ context.Context, item store.DispatchOutboxItem, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.matchesLocked(item) || s.state != "dispatching" {
		return store.ErrDispatchLeaseLost
	}
	s.done = true
	s.state = "abandoned"
	return nil
}

func (s *reclaimingOutboxStore) MarkFailed(_ context.Context, item store.DispatchOutboxItem, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.matchesLocked(item) {
		return store.ErrDispatchLeaseLost
	}
	s.state = "pending"
	return nil
}

func (s *reclaimingOutboxStore) DeleteFailed(context.Context, time.Duration, int) (int, error) {
	return 0, nil
}

func (s *reclaimingOutboxStore) MarkClientAcked(_ context.Context, ack store.DispatchOutboxClientAck) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return nil
	}
	if ack.OutboxID != s.item.ID ||
		ack.TargetUserID != s.item.TargetUserID ||
		ack.Pts != s.item.Pts ||
		ack.Attempt != s.attempt ||
		s.state != "dispatching" {
		return store.ErrDispatchLeaseLost
	}
	s.done = true
	s.state = "acked"
	return nil
}

func (s *reclaimingOutboxStore) allowReclaim() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reclaimReady = true
}

func (s *reclaimingOutboxStore) status() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *reclaimingOutboxStore) completed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

func (s *reclaimingOutboxStore) matchesLocked(item store.DispatchOutboxItem) bool {
	return item.ID == s.item.ID &&
		item.TargetUserID == s.item.TargetUserID &&
		item.Pts == s.item.Pts &&
		item.Attempts == s.attempt
}

type recordingDeliveredOutbox struct {
	mu   sync.Mutex
	seen []edgecontrol.OutboxDeliveryRef
}

func (d *recordingDeliveredOutbox) PushOutboxUpdate(_ context.Context, req edgecontrol.OutboxPushRequest) (edgecontrol.OutboxPushResult, error) {
	d.mu.Lock()
	d.seen = append(d.seen, req.DeliveryRef)
	d.mu.Unlock()
	return edgecontrol.OutboxPushResult{Sent: 1, Status: edgecontrol.OutboxDeliveryDelivered}, nil
}

func (d *recordingDeliveredOutbox) refs() []edgecontrol.OutboxDeliveryRef {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]edgecontrol.OutboxDeliveryRef(nil), d.seen...)
}

func deliveryRefsEqual(a, b []edgecontrol.OutboxDeliveryRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
