package egress

import (
	"context"
	"errors"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

func TestNewServiceRequiresDurableDependencies(t *testing.T) {
	events := emptyEventStore{}
	outbox := emptyOutboxStore{}
	deliverer := noopDeliverer{}

	tests := []struct {
		name      string
		events    store.UpdateEventStore
		outbox    store.DispatchOutboxStore
		deliverer edgecontrol.OutboxDeliverer
	}{
		{name: "events", outbox: outbox, deliverer: deliverer},
		{name: "outbox", events: events, deliverer: deliverer},
		{name: "deliverer", events: events, outbox: outbox},
		{name: "builder", events: events, outbox: outbox, deliverer: deliverer},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, err := NewService(tt.events, tt.outbox, tt.deliverer, nil, nil, nil, Config{})
			if !errors.Is(err, ErrMissingDependency) {
				t.Fatalf("NewService err = %v, want ErrMissingDependency", err)
			}
			if svc != nil {
				t.Fatalf("NewService svc = %#v, want nil", svc)
			}
		})
	}
}

func TestNewServiceAcceptsExplicitOutboxBuilder(t *testing.T) {
	builder := func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
		return make([][]byte, len(requests)), nil
	}
	svc, err := NewService(emptyEventStore{}, emptyOutboxStore{}, noopDeliverer{}, builder, nil, nil, Config{})
	if err != nil {
		t.Fatalf("NewService err = %v, want nil", err)
	}
	if svc == nil {
		t.Fatal("NewService svc = nil")
	}
}

type emptyEventStore struct{}

func (emptyEventStore) Append(context.Context, int64, domain.UpdateEvent) error {
	return nil
}

func (emptyEventStore) AppendAllocated(_ context.Context, _ int64, event domain.UpdateEvent) (domain.UpdateEvent, error) {
	return event, nil
}

func (emptyEventStore) ListAfter(context.Context, int64, int, int) ([]domain.UpdateEvent, error) {
	return nil, nil
}

func (emptyEventStore) MaxContiguousPts(context.Context, int64) (int, error) {
	return 0, nil
}

type emptyOutboxStore struct{}

func (emptyOutboxStore) ClaimPending(context.Context, int) ([]store.DispatchOutboxItem, error) {
	return nil, nil
}

func (emptyOutboxStore) MarkDelivered(context.Context, store.DispatchOutboxItem) error {
	return nil
}

func (emptyOutboxStore) MarkAbandoned(context.Context, store.DispatchOutboxItem, string) error {
	return nil
}

func (emptyOutboxStore) MarkFailed(context.Context, store.DispatchOutboxItem, string) error {
	return nil
}

func (emptyOutboxStore) DeleteFailed(context.Context, time.Duration, int) (int, error) {
	return 0, nil
}

type noopDeliverer struct{}

func (noopDeliverer) PushOutboxUpdate(context.Context, edgecontrol.OutboxPushRequest) (edgecontrol.OutboxPushResult, error) {
	return edgecontrol.OutboxPushResult{Status: edgecontrol.OutboxDeliveryNoKnownOnlineTargets}, nil
}
