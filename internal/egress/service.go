// Package egress owns durable outbox claim and Edge delivery orchestration.
package egress

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

var ErrMissingDependency = errors.New("egress: missing dependency")

// Config is the process-local worker configuration for durable outbox dispatch.
type Config struct {
	Workers     int
	Batch       int
	PushTimeout time.Duration
}

// Service is the dedicated Durable Egress runtime. It claims durable outbox rows,
// builds receiver-specific update projections through the injected builder, and
// delivers them through the Edge outbox fabric.
type Service struct {
	dispatcher *OutboxDispatcher
}

type DeliveryService struct {
	dispatcher *DeliveryDispatcher
}

// NewService wires a Durable Egress service from explicit production
// dependencies. The deliverer should normally be an Edge OutboxFabric backed by
// Redis location and command bus.
func NewService(
	events store.UpdateEventStore,
	outbox store.DispatchOutboxStore,
	deliverer edgecontrol.OutboxDeliverer,
	builder OutboxUpdateBuilder,
	metrics Metrics,
	log *zap.Logger,
	cfg Config,
) (*Service, error) {
	if events == nil || outbox == nil || deliverer == nil || builder == nil {
		return nil, ErrMissingDependency
	}
	opts := []OutboxOption{
		WithOutboxWorkers(cfg.Workers),
		WithOutboxBatch(cfg.Batch),
		WithOutboxPushTimeout(cfg.PushTimeout),
		WithOutboxUpdateBuilder(builder),
		WithOutboxMetrics(metrics),
	}
	return &Service{
		dispatcher: NewOutboxDispatcher(events, outbox, deliverer, log, opts...),
	}, nil
}

func NewDeliveryService(
	outbox store.DeliveryOutboxStore,
	deliverer edgecontrol.OutboxDeliverer,
	metrics Metrics,
	log *zap.Logger,
	cfg Config,
) (*DeliveryService, error) {
	if outbox == nil || deliverer == nil {
		return nil, ErrMissingDependency
	}
	opts := []DeliveryOption{
		WithDeliveryWorkers(cfg.Workers),
		WithDeliveryBatch(cfg.Batch),
		WithDeliveryPushTimeout(cfg.PushTimeout),
		WithDeliveryMetrics(metrics),
	}
	return &DeliveryService{
		dispatcher: NewDeliveryDispatcher(outbox, deliverer, log, opts...),
	}, nil
}

// RunWithWake starts durable outbox workers and wakes claimers only when the
// storage layer reports newly ready heads.
func (s *Service) RunWithWake(ctx context.Context, wake <-chan struct{}) {
	if s == nil || s.dispatcher == nil {
		return
	}
	s.dispatcher.RunWithWake(ctx, wake)
}

// DispatchOnce claims and dispatches one batch. It exists for deterministic
// tests and operational probes; the normal service path is RunWithWake.
func (s *Service) DispatchOnce(ctx context.Context) bool {
	if s == nil || s.dispatcher == nil {
		return false
	}
	return s.dispatcher.DispatchOnce(ctx)
}

func (s *DeliveryService) RunWithWake(ctx context.Context, wake <-chan struct{}) {
	if s == nil || s.dispatcher == nil {
		return
	}
	s.dispatcher.RunWithWake(ctx, wake)
}

func (s *DeliveryService) DispatchOnce(ctx context.Context) bool {
	if s == nil || s.dispatcher == nil {
		return false
	}
	return s.dispatcher.DispatchOnce(ctx)
}
