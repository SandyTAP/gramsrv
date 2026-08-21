package rpc

import (
	"context"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

// SuggestedPostDispatcher publishes scheduled suggestions and resolves paid
// escrow after the minimum live age (or refunds it when the post is deleted).
// Store-side per-key claims and row locks make multiple server instances safe.
type SuggestedPostDispatcher struct {
	router   *Router
	log      *zap.Logger
	interval time.Duration
	batch    int
	enqueue  func(context.Context, int64, domain.ToggleSuggestedPostApprovalResult) error
}

func NewSuggestedPostDispatcher(router *Router, log *zap.Logger) *SuggestedPostDispatcher {
	if log == nil {
		log = zap.NewNop()
	}
	return &SuggestedPostDispatcher{
		router: router, log: log, interval: time.Second, batch: 50,
		enqueue: router.enqueueSuggestedPostApprovalFanout,
	}
}

func (d *SuggestedPostDispatcher) Run(ctx context.Context) {
	if d == nil || d.router == nil {
		return
	}
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.DispatchOnce(ctx)
		}
	}
}

func (d *SuggestedPostDispatcher) DispatchOnce(ctx context.Context) bool {
	service, ok := d.router.deps.Channels.(suggestedPostApprovalService)
	if !ok {
		return false
	}
	results, err := service.ProcessSuggestedPostLifecycle(ctx, domain.SuggestedPostLifecycleRequest{Now: int(d.router.clock.Now().Unix()), Limit: d.batch})
	if err != nil {
		d.log.Warn("process suggested post lifecycle", zap.Error(err))
	}
	enqueued := false
	for _, result := range results {
		if err := d.enqueue(ctx, 0, result); err != nil {
			d.log.Warn("enqueue suggested post approval fanout", zap.Error(err))
			continue
		}
		enqueued = true
	}
	return enqueued
}
