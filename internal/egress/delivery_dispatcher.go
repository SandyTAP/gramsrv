package egress

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/iamxvbaba/td/proto"
	"go.uber.org/zap"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

type DeliveryDispatcher struct {
	outbox      store.DeliveryOutboxStore
	delivery    edgecontrol.OutboxDeliverer
	log         *zap.Logger
	metrics     Metrics
	batch       int
	workers     int
	pushTimeout time.Duration
}

type DeliveryOption func(*DeliveryDispatcher)

func WithDeliveryBatch(n int) DeliveryOption {
	return func(d *DeliveryDispatcher) {
		if n > 0 {
			d.batch = n
		}
	}
}

func WithDeliveryWorkers(n int) DeliveryOption {
	return func(d *DeliveryDispatcher) {
		if n > 0 {
			d.workers = n
		}
	}
}

func WithDeliveryPushTimeout(timeout time.Duration) DeliveryOption {
	return func(d *DeliveryDispatcher) {
		if timeout > 0 {
			d.pushTimeout = timeout
		}
	}
}

func WithDeliveryMetrics(m Metrics) DeliveryOption {
	return func(d *DeliveryDispatcher) {
		if m != nil {
			d.metrics = m
		}
	}
}

func NewDeliveryDispatcher(outbox store.DeliveryOutboxStore, delivery edgecontrol.OutboxDeliverer, log *zap.Logger, opts ...DeliveryOption) *DeliveryDispatcher {
	if log == nil {
		log = zap.NewNop()
	}
	d := &DeliveryDispatcher{
		outbox:   outbox,
		delivery: delivery,
		log:      log,
		metrics:  nopMetrics{},
		batch:    defaultOutboxBatch,
		workers:  defaultOutboxWorkers,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(d)
		}
	}
	return d
}

func (d *DeliveryDispatcher) RunWithWake(ctx context.Context, wake <-chan struct{}) {
	if d == nil || d.outbox == nil || d.delivery == nil {
		return
	}
	if wake == nil {
		d.log.Error("edge delivery outbox wake channel is required")
		return
	}
	claimer, sharded := d.outbox.(shardedDeliveryClaimer)
	workers := normalizedOutboxWorkers(d.workers, sharded)
	workerWakes := make([]chan struct{}, workers)
	for i := range workerWakes {
		workerWakes[i] = make(chan struct{}, 1)
	}
	go broadcastOutboxWake(ctx, wake, workerWakes)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		var shardIDs []int
		if sharded {
			shardIDs = logicalShardsForWorker(i, workers)
		}
		workerWake := (<-chan struct{})(nil)
		if workerWakes[i] != nil {
			workerWake = workerWakes[i]
		}
		go func() {
			defer wg.Done()
			d.runWorker(ctx, claimer, shardIDs, workerWake)
		}()
	}
	wg.Wait()
}

func (d *DeliveryDispatcher) runWorker(ctx context.Context, claimer shardedDeliveryClaimer, shardIDs []int, wake <-chan struct{}) {
	dispatch := d.DispatchOnce
	if claimer != nil && len(shardIDs) > 0 {
		dispatch = func(ctx context.Context) bool {
			return d.dispatchOnceShards(ctx, claimer, shardIDs)
		}
	}
	runWakeDrivenDispatchLoop(ctx, wake, dispatch)
}

type shardedDeliveryClaimer interface {
	ClaimPendingShards(ctx context.Context, shardCount int, shardIDs []int, limit int) ([]store.DeliveryOutboxItem, error)
}

func (d *DeliveryDispatcher) DispatchOnce(ctx context.Context) bool {
	items, err := d.outbox.ClaimPending(ctx, d.batch)
	return d.dispatchClaimed(ctx, items, err)
}

func (d *DeliveryDispatcher) dispatchOnceShards(ctx context.Context, claimer shardedDeliveryClaimer, shardIDs []int) bool {
	items, err := claimer.ClaimPendingShards(ctx, outboxLogicalShards, shardIDs, d.batch)
	return d.dispatchClaimed(ctx, items, err)
}

func (d *DeliveryDispatcher) dispatchClaimed(ctx context.Context, items []store.DeliveryOutboxItem, err error) bool {
	if err != nil {
		d.log.Warn("claim edge delivery outbox", zap.Error(err))
		return false
	}
	if len(items) == 0 {
		return false
	}
	sortDeliveryItems(items)
	d.metrics.OutboxClaimed(len(items))
	blockedUsers := make(map[int64]struct{})
	for _, item := range items {
		if _, blocked := blockedUsers[item.TargetUserID]; blocked {
			continue
		}
		if !d.dispatchItem(ctx, item) {
			blockedUsers[item.TargetUserID] = struct{}{}
		}
	}
	return true
}

func sortDeliveryItems(items []store.DeliveryOutboxItem) {
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.TargetUserID != b.TargetUserID {
			return a.TargetUserID < b.TargetUserID
		}
		return a.ID < b.ID
	})
}

func (d *DeliveryDispatcher) dispatchItem(ctx context.Context, item store.DeliveryOutboxItem) bool {
	start := time.Now()
	if outboxClaimExceededOnlineAttempts(item.Attempts) {
		d.markDeliveryAbandoned(ctx, item, nil)
		return false
	}
	if err := validateDeliveryExclusionPair(item); err != nil {
		d.markDeliveryFailed(ctx, item, err)
		return false
	}
	sent, status, retriable, err := d.pushDelivery(ctx, item)
	if err != nil {
		if retriable {
			if outboxAttemptReachedOnlineLimit(item.Attempts) {
				d.markDeliveryAbandoned(ctx, item, err)
			} else {
				d.log.Debug("edge delivery outbox deferred (push not confirmed)",
					zap.Int64("target_user_id", item.TargetUserID),
					zap.Int64("outbox_id", item.ID),
					zap.Int("attempts", item.Attempts),
				)
			}
			return false
		}
		d.markDeliveryFailed(ctx, item, err)
		return false
	}
	_ = status
	_ = sent
	if err := d.outbox.MarkDelivered(ctx, item); err != nil {
		if errors.Is(err, store.ErrDispatchLeaseLost) {
			d.log.Debug("mark edge delivery delivered fenced", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Int("attempts", item.Attempts), zap.Error(err))
			d.metrics.OutboxDelivered(time.Since(start))
			return true
		}
		d.log.Warn("mark edge delivery delivered", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Error(err))
		return false
	}
	d.metrics.OutboxDelivered(time.Since(start))
	return true
}

func (d *DeliveryDispatcher) pushDelivery(ctx context.Context, item store.DeliveryOutboxItem) (sent int, status edgecontrol.OutboxDeliveryStatus, retriable bool, err error) {
	if len(item.Payload) == 0 {
		return 0, edgecontrol.OutboxDeliveryUnknown, false, fmt.Errorf("empty edge delivery payload")
	}
	if d.delivery == nil {
		return 0, edgecontrol.OutboxDeliveryIndeterminate, true, edgecontrol.ErrOutboxDeliveryIndeterminate
	}
	result, err := d.delivery.PushOutboxUpdate(ctx, edgecontrol.OutboxPushRequest{
		TargetUserID:     item.TargetUserID,
		DeliveryRef:      deliveryOutboxRef(item),
		ExcludeAuthKeyID: item.ExcludeAuthKeyID,
		ExcludeSessionID: item.ExcludeSessionID,
		MessageType:      proto.MessageFromServer,
		UpdateBytes:      append([]byte(nil), item.Payload...),
		DeliveryTimeout:  d.pushTimeout,
	})
	if err != nil {
		return result.Sent, result.Status, outboxPushInterrupted(err), err
	}
	switch result.Status {
	case edgecontrol.OutboxDeliveryDelivered, edgecontrol.OutboxDeliveryNoKnownOnlineTargets:
		return result.Sent, result.Status, false, nil
	case edgecontrol.OutboxDeliveryIndeterminate:
		return result.Sent, result.Status, true, edgecontrol.ErrOutboxDeliveryIndeterminate
	default:
		if result.Sent > 0 {
			return result.Sent, edgecontrol.OutboxDeliveryDelivered, false, nil
		}
		return result.Sent, edgecontrol.OutboxDeliveryIndeterminate, true, edgecontrol.ErrOutboxDeliveryIndeterminate
	}
}

func (d *DeliveryDispatcher) markDeliveryAbandoned(ctx context.Context, item store.DeliveryOutboxItem, cause error) {
	err := onlineDeliveryAttemptsExhaustedError(item.Attempts, cause)
	if markErr := d.outbox.MarkAbandoned(ctx, item, err.Error()); markErr != nil {
		d.log.Warn("mark edge delivery abandoned",
			zap.Int64("target_user_id", item.TargetUserID),
			zap.Int64("outbox_id", item.ID),
			zap.Int("attempts", item.Attempts),
			zap.Error(markErr),
		)
		return
	}
	d.metrics.OutboxFailed(err)
	d.log.Error("edge delivery outbox online delivery abandoned",
		zap.Int64("target_user_id", item.TargetUserID),
		zap.Int64("outbox_id", item.ID),
		zap.Int("attempts", item.Attempts),
		zap.Int("max_attempts", maxOnlineDeliveryAttempts),
		zap.Error(err),
	)
}

func (d *DeliveryDispatcher) markDeliveryFailed(ctx context.Context, item store.DeliveryOutboxItem, err error) {
	d.metrics.OutboxFailed(err)
	if markErr := d.outbox.MarkFailed(ctx, item, err.Error()); markErr != nil {
		d.log.Warn("mark edge delivery failed", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Error(markErr))
		return
	}
	d.log.Warn("edge delivery failed", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Error(err))
}

func validateDeliveryExclusionPair(item store.DeliveryOutboxItem) error {
	if (item.ExcludeAuthKeyID != ([8]byte{})) != (item.ExcludeSessionID != 0) {
		return ErrInvalidOutboxExclusionPair
	}
	return nil
}

func deliveryOutboxRef(item store.DeliveryOutboxItem) edgecontrol.OutboxDeliveryRef {
	return edgecontrol.OutboxDeliveryRef{
		OutboxID:     item.ID,
		TargetUserID: item.TargetUserID,
		Pts:          0,
		Attempt:      item.Attempts,
	}
}
