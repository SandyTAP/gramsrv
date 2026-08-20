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

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

const (
	defaultOutboxBatch        = 100
	defaultOutboxWorkers      = 4
	maxOnlineDeliveryAttempts = 5
	// outboxLogicalShards 是稳定 user→lane 哈希空间。它不随运行时 worker 数变化，
	// worker 只独占其中一组 shard，保证同一用户始终单 lane 串行、不同用户可并行。
	outboxLogicalShards = store.DispatchOutboxLogicalShards
)

var (
	errMissingOutboxEvent                    = errors.New("missing outbox update event")
	errOutboxUpdateBuilderMissing            = errors.New("outbox update builder is required")
	errOutboxUpdateBuilderCount              = errors.New("outbox update builder returned mismatched count")
	errOutboxUpdateBuilderEmpty              = errors.New("outbox update builder returned an empty non-noop update")
	errOutboxOnlineDeliveryAttemptsExhausted = errors.New("outbox online delivery attempts exhausted")
	ErrInvalidOutboxExclusionPair            = errors.New("outbox exclusion requires both raw auth key and session id")
)

// Metrics receives durable Egress outbox dispatcher metrics.
type Metrics interface {
	OutboxClaimed(count int)
	OutboxDelivered(d time.Duration)
	OutboxFailed(err error)
}

type nopMetrics struct{}

func (nopMetrics) OutboxClaimed(int) {}

func (nopMetrics) OutboxDelivered(time.Duration) {}

func (nopMetrics) OutboxFailed(error) {}

// OutboxDispatcher 把 PG transactional outbox 中的 update 批量推给在线 session。
// 多 worker 并发 claim：ClaimPending 用 FOR UPDATE SKIP LOCKED，worker 间认领不重叠。
type OutboxDispatcher struct {
	events        store.UpdateEventStore
	outbox        store.DispatchOutboxStore
	delivery      edgecontrol.OutboxDeliverer
	log           *zap.Logger
	metrics       Metrics
	updateBuilder OutboxUpdateBuilder
	batch         int
	workers       int
	pushTimeout   time.Duration
}

// OutboxOption 调整 OutboxDispatcher 的运行参数。
type OutboxOption func(*OutboxDispatcher)

// OutboxUpdateRequest 是 outbox worker 构造在线 updates 时的批量输入。
type OutboxUpdateRequest struct {
	TargetUserID int64
	Event        domain.UpdateEvent
}

// OutboxUpdateBuilder 按接收者视角批量把 domain.UpdateEvent 转为已编码的 TL
// updates bytes。Egress 只转交 bytes，不持有协议 typed object。
type OutboxUpdateBuilder func(ctx context.Context, requests []OutboxUpdateRequest) ([][]byte, error)

// WithOutboxUpdateBuilder 注入按接收者视角的批量 updates 构建器。
func WithOutboxUpdateBuilder(builder OutboxUpdateBuilder) OutboxOption {
	return func(d *OutboxDispatcher) {
		d.updateBuilder = builder
	}
}

// WithOutboxBatch 设置每次 claim 的最大条数；<=0 时保持默认。
func WithOutboxBatch(n int) OutboxOption {
	return func(d *OutboxDispatcher) {
		if n > 0 {
			d.batch = n
		}
	}
}

// WithOutboxWorkers 设置并发 claim worker 数；<=0 时保持默认。
func WithOutboxWorkers(n int) OutboxOption {
	return func(d *OutboxDispatcher) {
		if n > 0 {
			d.workers = n
		}
	}
}

// WithOutboxPushTimeout 设置 updates fanout 入队等待时间；<=0 时使用同步可靠推送。
func WithOutboxPushTimeout(timeout time.Duration) OutboxOption {
	return func(d *OutboxDispatcher) {
		if timeout > 0 {
			d.pushTimeout = timeout
		}
	}
}

// WithOutboxMetrics 注入指标实现；nil 时保持 NopMetrics。
func WithOutboxMetrics(m Metrics) OutboxOption {
	return func(d *OutboxDispatcher) {
		if m != nil {
			d.metrics = m
		}
	}
}

// NewOutboxDispatcher 创建在线 update 推送 worker。生产只接受显式 Edge
// OutboxDeliverer，避免 durable outbox 消费方重新依赖 Core-local session 状态。
func NewOutboxDispatcher(events store.UpdateEventStore, outbox store.DispatchOutboxStore, delivery edgecontrol.OutboxDeliverer, log *zap.Logger, opts ...OutboxOption) *OutboxDispatcher {
	if log == nil {
		log = zap.NewNop()
	}
	d := &OutboxDispatcher{
		events:   events,
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

// RunWithWake 启动 workers 个并发 worker，并只在 durable storage ready 事件后
// claim/drain。PG outbox 仍是唯一事实源；这里不保留定时轮询兜底。
func (d *OutboxDispatcher) RunWithWake(ctx context.Context, wake <-chan struct{}) {
	if d == nil || d.events == nil || d.outbox == nil || d.delivery == nil {
		return
	}
	if wake == nil {
		d.log.Error("dispatch outbox wake channel is required")
		return
	}
	claimer, sharded := d.outbox.(shardedOutboxClaimer)
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

func broadcastOutboxWake(ctx context.Context, wake <-chan struct{}, workers []chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-wake:
			if !ok {
				return
			}
			for _, worker := range workers {
				if worker == nil {
					continue
				}
				select {
				case worker <- struct{}{}:
				default:
				}
			}
		}
	}
}

func normalizedOutboxWorkers(workers int, sharded bool) int {
	if workers < 1 {
		workers = 1
	}
	if !sharded {
		// 测试替身或旧 store 没有 user shard claim 时，强制单 worker，避免同一用户
		// 的不同 pts 被并行领取。
		return 1
	}
	// 多于 logical shard 的 worker 没有独占 lane。若让空 shard worker 回退全局
	// claim，会与全部分片 worker 重叠，因此必须在启动前钳制。
	if workers > outboxLogicalShards {
		return outboxLogicalShards
	}
	return workers
}

// OutboxLogicalShards is the stable durable outbox user lane count.
const OutboxLogicalShards = outboxLogicalShards

// LogicalShardsForWorker exposes the stable shard assignment for package-level
// regression tests and smoke probes.
func LogicalShardsForWorker(worker, workers int) []int {
	return logicalShardsForWorker(worker, workers)
}

// NormalizedOutboxWorkers returns the effective durable outbox worker count.
func NormalizedOutboxWorkers(workers int, sharded bool) int {
	return normalizedOutboxWorkers(workers, sharded)
}

func logicalShardsForWorker(worker, workers int) []int {
	if workers <= 0 || worker < 0 || worker >= workers {
		return nil
	}
	out := make([]int, 0, (outboxLogicalShards+workers-1)/workers)
	for shard := worker; shard < outboxLogicalShards; shard += workers {
		out = append(out, shard)
	}
	return out
}

// runWorker 是单个 claim 循环；多 worker 靠 ClaimPending 的 SKIP LOCKED 互不重叠。
// 每次 ready wake 后 drain 到空，避免用定时器承担 outbox 正确性。
func (d *OutboxDispatcher) runWorker(ctx context.Context, claimer shardedOutboxClaimer, shardIDs []int, wake <-chan struct{}) {
	dispatch := d.DispatchOnce
	if claimer != nil && len(shardIDs) > 0 {
		dispatch = func(ctx context.Context) bool {
			return d.dispatchOnceShards(ctx, claimer, shardIDs)
		}
	}
	runWakeDrivenDispatchLoop(ctx, wake, dispatch)
}

// batchEventLoader 是 UpdateEventStore 的可选批量能力：一次取多条 (user,pts) 事件。
type batchEventLoader interface {
	BatchByCursor(ctx context.Context, cursors []store.EventCursor) ([]domain.UpdateEvent, error)
}

// batchOutboxMarker 是 DispatchOutboxStore 的可选批量能力：一次标记多行 delivered。
type batchOutboxMarker interface {
	MarkDeliveredBatch(ctx context.Context, items []store.DispatchOutboxItem) error
}

type shardedOutboxClaimer interface {
	ClaimPendingShards(ctx context.Context, shardCount int, shardIDs []int, limit int) ([]store.DispatchOutboxItem, error)
}

// DispatchOnce claim 一批 outbox 并投递，测试可直接调用。返回本次是否 claim 到事件，
// 供 runWorker 决定快轮询还是空闲退避。
// store 同时具备批量取事件 + 批量标记能力时走批量路径（每批 ~3 次 PG 往返），否则逐条回退。
func (d *OutboxDispatcher) DispatchOnce(ctx context.Context) bool {
	items, err := d.outbox.ClaimPending(ctx, d.batch)
	return d.dispatchClaimed(ctx, items, err)
}

func (d *OutboxDispatcher) dispatchOnceShards(ctx context.Context, claimer shardedOutboxClaimer, shardIDs []int) bool {
	items, err := claimer.ClaimPendingShards(ctx, outboxLogicalShards, shardIDs, d.batch)
	return d.dispatchClaimed(ctx, items, err)
}

func (d *OutboxDispatcher) dispatchClaimed(ctx context.Context, items []store.DispatchOutboxItem, err error) bool {
	if err != nil {
		d.log.Warn("claim dispatch outbox", zap.Error(err))
		return false
	}
	if len(items) == 0 {
		return false
	}
	sortDispatchItems(items)
	d.metrics.OutboxClaimed(len(items))
	if loader, ok := d.events.(batchEventLoader); ok {
		if marker, ok := d.outbox.(batchOutboxMarker); ok {
			d.dispatchBatch(ctx, items, loader, marker)
			return true
		}
	}
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

func sortDispatchItems(items []store.DispatchOutboxItem) {
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.TargetUserID != b.TargetUserID {
			return a.TargetUserID < b.TargetUserID
		}
		if a.Pts != b.Pts {
			return a.Pts < b.Pts
		}
		return a.ID < b.ID
	})
}

type outboxEventKey struct {
	userID int64
	pts    int
}

// dispatchBatch 批量加载已 claim 事件、逐条 push、批量标记 delivered；失败项单独退避重试。
func (d *OutboxDispatcher) dispatchBatch(ctx context.Context, items []store.DispatchOutboxItem, loader batchEventLoader, marker batchOutboxMarker) {
	valid := make([]store.DispatchOutboxItem, 0, len(items))
	blockedUsers := make(map[int64]struct{})
	for _, item := range items {
		if _, blocked := blockedUsers[item.TargetUserID]; blocked {
			continue
		}
		if outboxClaimExceededOnlineAttempts(item.Attempts) {
			d.markDispatchAbandoned(ctx, item, nil)
			blockedUsers[item.TargetUserID] = struct{}{}
			continue
		}
		if err := validateOutboxExclusionPair(item); err != nil {
			d.markDispatchFailed(ctx, item, err)
			blockedUsers[item.TargetUserID] = struct{}{}
			continue
		}
		valid = append(valid, item)
	}
	if len(valid) == 0 {
		return
	}
	items = valid
	cursors := make([]store.EventCursor, len(items))
	for i, item := range items {
		cursors[i] = store.EventCursor{UserID: item.TargetUserID, Pts: item.Pts}
	}
	events, err := loader.BatchByCursor(ctx, cursors)
	if err != nil {
		// 批量取失败则整批回退逐条路径，让每条各自重试/标失败，不丢进度。
		d.log.Warn("batch load dispatch events", zap.Error(err))
		blockedUsers := make(map[int64]struct{})
		for _, item := range items {
			if _, blocked := blockedUsers[item.TargetUserID]; blocked {
				continue
			}
			if !d.dispatchItem(ctx, item) {
				blockedUsers[item.TargetUserID] = struct{}{}
			}
		}
		return
	}
	byKey := make(map[outboxEventKey]domain.UpdateEvent, len(events))
	for _, event := range events {
		byKey[outboxEventKey{event.UserID, event.Pts}] = event
	}
	start := time.Now()
	ready := make([]outboxDispatchReady, 0, len(items))
	requests := make([]OutboxUpdateRequest, 0, len(items))
	clear(blockedUsers)
	for _, item := range items {
		if _, blocked := blockedUsers[item.TargetUserID]; blocked {
			continue
		}
		event, ok := byKey[outboxEventKey{item.TargetUserID, item.Pts}]
		if !ok {
			d.markDispatchFailed(ctx, item, errMissingOutboxEvent)
			blockedUsers[item.TargetUserID] = struct{}{}
			continue
		}
		ready = append(ready, outboxDispatchReady{item: item})
		requests = append(requests, OutboxUpdateRequest{TargetUserID: item.TargetUserID, Event: event})
	}
	builtUpdates, err := d.buildOutboxUpdates(ctx, requests)
	if err != nil {
		d.log.Warn("build dispatch outbox updates; isolating claimed items", zap.Error(err))
		clear(blockedUsers)
		for i, entry := range ready {
			item := entry.item
			if _, blocked := blockedUsers[item.TargetUserID]; blocked {
				continue
			}
			// BatchByCursor already hydrated the exact durable event. Reuse it for
			// singleton isolation so one malformed event cannot poison unrelated
			// users, without falling back to ListAfter or weakening lane ordering.
			if !d.dispatchHydratedItem(ctx, item, requests[i].Event, time.Now()) {
				blockedUsers[item.TargetUserID] = struct{}{}
			}
		}
		return
	}
	delivered := make([]store.DispatchOutboxItem, 0, len(items))
	clear(blockedUsers)
	for i, entry := range ready {
		item := entry.item
		if _, blocked := blockedUsers[item.TargetUserID]; blocked {
			continue
		}
		update := builtUpdates[i]
		if update == nil {
			delivered = append(delivered, item)
			continue
		}
		sent, status, retriable, err := d.pushOutboxUpdate(ctx, item, update)
		if err != nil {
			blockedUsers[item.TargetUserID] = struct{}{}
			if retriable {
				if outboxAttemptReachedOnlineLimit(item.Attempts) {
					d.markDispatchAbandoned(ctx, item, err)
				} else {
					// 出站队列拥塞或跨 Edge ACK 未确认：留 dispatching 行靠租约过期重投。
					// attempts 由下一次 durable claim 递增；达到上限后 abandon 在线任务。
					d.log.Debug("dispatch outbox deferred (push not confirmed)",
						zap.Int64("target_user_id", item.TargetUserID),
						zap.Int64("outbox_id", item.ID),
						zap.Int("pts", item.Pts),
						zap.Int("attempts", item.Attempts),
					)
				}
				// 不加入 delivered，故不会被 MarkDeliveredBatch 删除。
				continue
			}
			d.markDispatchFailed(ctx, item, err)
			continue
		}
		_ = status
		_ = sent
		delivered = append(delivered, item)
	}
	if len(delivered) == 0 {
		return
	}
	if err := marker.MarkDeliveredBatch(ctx, delivered); err != nil {
		// 批量标记失败则逐条标记，避免整批已投递却卡在 dispatching 等租约过期重投。
		// ErrDispatchLeaseLost 可能是 late client ACK 或另一个 fenced completion 已先删除
		// 当前 attempt；它是幂等竞争，不应污染故障日志。
		if errors.Is(err, store.ErrDispatchLeaseLost) {
			d.log.Debug("mark dispatch delivered batch fenced", zap.Int("items", len(delivered)), zap.Error(err))
		} else {
			d.log.Warn("mark dispatch delivered batch", zap.Error(err))
		}
		for _, item := range delivered {
			if markErr := d.outbox.MarkDelivered(ctx, item); markErr != nil {
				if errors.Is(markErr, store.ErrDispatchLeaseLost) {
					d.log.Debug("mark dispatch delivered fenced", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Int("attempts", item.Attempts), zap.Error(markErr))
				} else {
					d.log.Warn("mark dispatch delivered", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Error(markErr))
				}
			}
		}
	}
	per := time.Since(start) / time.Duration(len(delivered))
	for range delivered {
		d.metrics.OutboxDelivered(per)
	}
}

type outboxDispatchReady struct {
	item store.DispatchOutboxItem
}

func (d *OutboxDispatcher) dispatchItem(ctx context.Context, item store.DispatchOutboxItem) bool {
	start := time.Now()
	if outboxClaimExceededOnlineAttempts(item.Attempts) {
		d.markDispatchAbandoned(ctx, item, nil)
		return false
	}
	if err := validateOutboxExclusionPair(item); err != nil {
		d.markDispatchFailed(ctx, item, err)
		return false
	}
	events, err := d.events.ListAfter(ctx, item.TargetUserID, item.Pts-1, 1)
	if err != nil {
		d.markDispatchFailed(ctx, item, err)
		return false
	}
	if len(events) == 0 || events[0].Pts != item.Pts {
		d.markDispatchFailed(ctx, item, errMissingOutboxEvent)
		return false
	}
	return d.dispatchHydratedItem(ctx, item, events[0], start)
}

// dispatchHydratedItem preserves the singleton delivery/ACK/fencing semantics
// while allowing the batch error path to reuse an event loaded by BatchByCursor.
func (d *OutboxDispatcher) dispatchHydratedItem(ctx context.Context, item store.DispatchOutboxItem, event domain.UpdateEvent, start time.Time) bool {
	update, err := d.buildOutboxUpdate(ctx, item, event)
	if err != nil {
		d.markDispatchFailed(ctx, item, err)
		return false
	}
	if update == nil {
		if err := d.outbox.MarkDelivered(ctx, item); err != nil {
			d.log.Warn("mark noop dispatch delivered", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Error(err))
			return false
		}
		d.metrics.OutboxDelivered(time.Since(start))
		return true
	}
	sent, status, retriable, err := d.pushOutboxUpdate(ctx, item, update)
	if err != nil {
		if retriable {
			if outboxAttemptReachedOnlineLimit(item.Attempts) {
				d.markDispatchAbandoned(ctx, item, err)
			} else {
				// 出站队列拥塞或跨 Edge ACK 未确认：保留 dispatching 行，靠租约过期
				// 重新 claim 重投；attempts 由下一次 claim 递增并有上限。
				d.log.Debug("dispatch outbox deferred (push not confirmed)",
					zap.Int64("target_user_id", item.TargetUserID),
					zap.Int64("outbox_id", item.ID),
					zap.Int("pts", item.Pts),
					zap.Int("attempts", item.Attempts),
				)
			}
			return false
		}
		d.markDispatchFailed(ctx, item, err)
		return false
	}
	_ = status
	if err := d.outbox.MarkDelivered(ctx, item); err != nil {
		d.log.Warn("mark dispatch delivered", zap.Int64("target_user_id", item.TargetUserID), zap.Int64("outbox_id", item.ID), zap.Error(err))
		return false
	}
	d.metrics.OutboxDelivered(time.Since(start))
	d.log.Debug("dispatch outbox delivered",
		zap.Int64("target_user_id", item.TargetUserID),
		zap.Int64("outbox_id", item.ID),
		zap.Int("pts", item.Pts),
		zap.Int("sessions", sent),
	)
	return true
}

func (d *OutboxDispatcher) buildOutboxUpdate(ctx context.Context, item store.DispatchOutboxItem, event domain.UpdateEvent) ([]byte, error) {
	updates, err := d.buildOutboxUpdates(ctx, []OutboxUpdateRequest{{TargetUserID: item.TargetUserID, Event: event}})
	if err != nil {
		return nil, err
	}
	if len(updates) == 0 {
		return nil, nil
	}
	return updates[0], nil
}

func (d *OutboxDispatcher) buildOutboxUpdates(ctx context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
	out := make([][]byte, len(requests))
	if len(requests) == 0 {
		return out, nil
	}
	if d.updateBuilder == nil {
		return nil, errOutboxUpdateBuilderMissing
	}
	built, err := d.updateBuilder(ctx, requests)
	if err != nil {
		return nil, err
	}
	if len(built) != len(requests) {
		return nil, fmt.Errorf("%w: got %d want %d", errOutboxUpdateBuilderCount, len(built), len(requests))
	}
	for i := range built {
		if built[i] == nil {
			if requests[i].Event.Type == domain.UpdateEventNoop {
				continue
			}
			return nil, fmt.Errorf("%w: index=%d user_id=%d pts=%d event_type=%s", errOutboxUpdateBuilderEmpty, i, requests[i].TargetUserID, requests[i].Event.Pts, requests[i].Event.Type)
		}
		if len(built[i]) == 0 {
			return nil, fmt.Errorf("%w: index=%d user_id=%d pts=%d event_type=%s", errOutboxUpdateBuilderEmpty, i, requests[i].TargetUserID, requests[i].Event.Pts, requests[i].Event.Type)
		}
	}
	return built, nil
}

// pushOutboxUpdate 投递一条 outbox update，返回 (送达的在线 session 数, 是否可重试, err)。
// Edge deliverer 必须把本地/远端 ACK 收敛成三态合同；dispatcher 只据此推进 durable outbox。
func (d *OutboxDispatcher) pushOutboxUpdate(ctx context.Context, item store.DispatchOutboxItem, updateBytes []byte) (sent int, status edgecontrol.OutboxDeliveryStatus, retriable bool, err error) {
	if err := validateOutboxExclusionPair(item); err != nil {
		return 0, edgecontrol.OutboxDeliveryUnknown, false, ErrInvalidOutboxExclusionPair
	}
	if len(updateBytes) == 0 {
		return 0, edgecontrol.OutboxDeliveryUnknown, false, fmt.Errorf("empty outbox update bytes")
	}

	if d.delivery == nil {
		return 0, edgecontrol.OutboxDeliveryIndeterminate, true, edgecontrol.ErrOutboxDeliveryIndeterminate
	}
	result, err := d.delivery.PushOutboxUpdate(ctx, edgecontrol.OutboxPushRequest{
		TargetUserID:     item.TargetUserID,
		DeliveryRef:      outboxDeliveryRef(item),
		ExcludeAuthKeyID: item.ExcludeAuthKeyID,
		ExcludeSessionID: item.ExcludeSessionID,
		MessageType:      proto.MessageFromServer,
		UpdateBytes:      append([]byte(nil), updateBytes...),
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

func validateOutboxExclusionPair(item store.DispatchOutboxItem) error {
	var zeroAuthKeyID [8]byte
	if (item.ExcludeAuthKeyID != zeroAuthKeyID) != (item.ExcludeSessionID != 0) {
		return ErrInvalidOutboxExclusionPair
	}
	return nil
}

func outboxDeliveryRef(item store.DispatchOutboxItem) edgecontrol.OutboxDeliveryRef {
	return edgecontrol.OutboxDeliveryRef{
		OutboxID:     item.ID,
		TargetUserID: item.TargetUserID,
		Pts:          item.Pts,
		Attempt:      item.Attempts,
	}
}

func outboxPushInterrupted(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, edgecontrol.ErrOutboxDeliveryIndeterminate)
}

// OutboxPushInterrupted reports whether a push error should preserve the claim
// for lease-based retry instead of marking a deterministic failure.
func OutboxPushInterrupted(err error) bool {
	return outboxPushInterrupted(err)
}

func outboxClaimExceededOnlineAttempts(attempts int) bool {
	return attempts > maxOnlineDeliveryAttempts
}

func outboxAttemptReachedOnlineLimit(attempts int) bool {
	return attempts >= maxOnlineDeliveryAttempts
}

func onlineDeliveryAttemptsExhaustedError(attempts int, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w after %d attempts", errOutboxOnlineDeliveryAttemptsExhausted, attempts)
	}
	return fmt.Errorf("%w after %d attempts: %v", errOutboxOnlineDeliveryAttemptsExhausted, attempts, cause)
}

func (d *OutboxDispatcher) markDispatchAbandoned(ctx context.Context, item store.DispatchOutboxItem, cause error) {
	err := onlineDeliveryAttemptsExhaustedError(item.Attempts, cause)
	if markErr := d.outbox.MarkAbandoned(ctx, item, err.Error()); markErr != nil {
		d.log.Warn("mark dispatch abandoned",
			zap.Int64("target_user_id", item.TargetUserID),
			zap.Int64("outbox_id", item.ID),
			zap.Int("pts", item.Pts),
			zap.Int("attempts", item.Attempts),
			zap.Error(markErr),
		)
		return
	}
	d.metrics.OutboxFailed(err)
	d.log.Error("dispatch outbox online delivery abandoned",
		zap.Int64("target_user_id", item.TargetUserID),
		zap.Int64("outbox_id", item.ID),
		zap.Int("pts", item.Pts),
		zap.Int("attempts", item.Attempts),
		zap.Int("max_attempts", maxOnlineDeliveryAttempts),
		zap.Error(err),
	)
}

func (d *OutboxDispatcher) markDispatchFailed(ctx context.Context, item store.DispatchOutboxItem, err error) {
	if err == nil {
		err = errMissingOutboxEvent
	}
	if markErr := d.outbox.MarkFailed(ctx, item, err.Error()); markErr != nil {
		d.log.Warn("mark dispatch failed",
			zap.Int64("target_user_id", item.TargetUserID),
			zap.Int64("outbox_id", item.ID),
			zap.Error(markErr),
		)
		return
	}
	d.metrics.OutboxFailed(err)
	d.log.Debug("dispatch outbox failed",
		zap.Int64("target_user_id", item.TargetUserID),
		zap.Int64("outbox_id", item.ID),
		zap.Int("pts", item.Pts),
		zap.Error(err),
	)
}
