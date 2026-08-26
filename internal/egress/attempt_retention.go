package egress

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/store"
)

// Finalized attempts are fencing tombstones, not queue work. Keeping them for
// one day makes delayed and duplicated Edge evidence observable while a single
// bounded worker prevents the tombstone tables from growing forever. This
// ticker is deliberately absent from every correctness path: claim, evidence
// expiry, retry, and finalization are all driven by durable DB time and wakeups.
const (
	finalizedAttemptRetention    = 24 * time.Hour
	finalizedAttemptGCInterval   = time.Minute
	finalizedAttemptGCBatch      = 4096
	finalizedAttemptGCMaxBatches = 8
)

type attemptTombstoneStore interface {
	DeleteFinalizedAttempts(context.Context, time.Time, int) (int, error)
}

type attemptTombstoneQueue struct {
	kind  store.OutboxQueueKind
	state attemptTombstoneStore
}

func (s *Service) runFinalizedAttemptGC(ctx context.Context, wait *sync.WaitGroup) {
	if s == nil || s.coordinator == nil || wait == nil {
		return
	}
	queues := []attemptTombstoneQueue{
		{kind: store.OutboxQueueDispatchPTS, state: s.coordinator.dispatch},
		{kind: store.OutboxQueueAbsoluteDelivery, state: s.coordinator.absolute},
		{kind: store.OutboxQueueChannelPTS, state: s.coordinator.channel},
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		ticker := time.NewTicker(finalizedAttemptGCInterval)
		defer ticker.Stop()

		run := func() {
			cutoff := time.Now().UTC().Add(-finalizedAttemptRetention)
			sweepFinalizedAttemptTombstones(
				ctx,
				queues,
				cutoff,
				finalizedAttemptGCBatch,
				finalizedAttemptGCMaxBatches,
				func(kind store.OutboxQueueKind, err error) {
					s.log.Warn("delete finalized delivery attempt tombstones",
						zap.Uint8("queue_kind", uint8(kind)),
						zap.Error(err),
					)
				},
			)
		}

		// Capacity cleanup may start immediately, but it never gates service
		// readiness or any delivery state transition.
		run()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run()
			}
		}
	}()
}

func sweepFinalizedAttemptTombstones(
	ctx context.Context,
	queues []attemptTombstoneQueue,
	cutoff time.Time,
	batch int,
	maxBatches int,
	onError func(store.OutboxQueueKind, error),
) {
	if ctx == nil || cutoff.IsZero() || batch <= 0 || maxBatches <= 0 {
		return
	}
	for _, queue := range queues {
		if queue.state == nil {
			continue
		}
		for i := 0; i < maxBatches && ctx.Err() == nil; i++ {
			deleted, err := queue.state.DeleteFinalizedAttempts(ctx, cutoff, batch)
			if err != nil {
				if onError != nil {
					onError(queue.kind, err)
				}
				break
			}
			if deleted < batch {
				break
			}
		}
	}
}
