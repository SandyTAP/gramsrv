package egress

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

const (
	defaultDomainActorPartitions = 256
	defaultDomainActorMailbox    = 128
	defaultDomainActorBytes      = int64(256 << 20)
	defaultDomainIOWorkers       = 256
	ptsClaimByteEstimate         = 64 << 10
	claimItemRetainedBytes       = int64(256)
	claimAttemptTargetBytes      = int64(64)
	deliveryReservationReleased  = uint64(1) << 63
	deliveryReservationByteMask  = deliveryReservationReleased - 1
	maxClaimWindowRetainedBytes  = int64(edgecontrol.MaxDeliveryBatchBytes) +
		int64(edgecontrol.MaxDeliveryBatchItems)*claimItemRetainedBytes +
		int64(edgecontrol.MaxDeliveryBatchItems)*int64(edgecontrol.MaxDeliveryTargets)*
			(claimAttemptTargetBytes+int64(edgecontrol.MaxDeliveryInstanceIDBytes))
	minimumProductionActorBytes = maxClaimWindowRetainedBytes + maxProjectedWindowRetainedBytes +
		maxDeliveryPlanRetainedBytes + maxAttemptTargetLedgerRetainedBytes + maxReplayIndexRetainedBytes
)

type deliveryActorWork struct {
	window      store.OutboxClaimWindow
	bytes       int64
	reservation *deliveryActorReservation
}

type deliveryActorReservation struct {
	owner *deliveryActorExecutor
	state atomic.Uint64
}

type deliveryOrderingDomain struct {
	queueKind store.OutboxQueueKind
	streamID  int64
}

type deliveryDomainState struct {
	pending  []deliveryActorWork
	inFlight *deliveryActorWork
}

type deliveryActorCompletion struct {
	domain deliveryOrderingDomain
}

type deliveryActorShard struct {
	queue     chan deliveryActorWork
	completed chan deliveryActorCompletion
	domains   map[deliveryOrderingDomain]*deliveryDomainState
	executor  *deliveryActorExecutor
}

type deliveryActorIOTask struct {
	shard  *deliveryActorShard
	domain deliveryOrderingDomain
	work   deliveryActorWork
}

type deliveryActorExecutor struct {
	coordinator *deliveryCoordinator
	process     func(context.Context, store.OutboxClaimWindow, *deliveryActorReservation)
	shards      []deliveryActorShard
	ioQueue     chan deliveryActorIOTask
	ioWorkers   int
	maxBytes    int64
	usedBytes   atomic.Int64
	maxTasks    int64
	usedTasks   atomic.Int64
	capacity    chan struct{}
	lifecycle   sync.RWMutex
	stopped     bool
}

func newDeliveryActorExecutor(coordinator *deliveryCoordinator, partitions, mailbox int, maxBytes int64) *deliveryActorExecutor {
	if partitions <= 0 {
		partitions = defaultDomainActorPartitions
	}
	if mailbox <= 0 {
		mailbox = defaultDomainActorMailbox
	}
	if maxBytes <= 0 {
		maxBytes = defaultDomainActorBytes
	}
	maxTasks := int64(partitions) * int64(mailbox)
	if maxTasks <= 0 {
		maxTasks = 1
	}
	ioWorkers := partitions
	if ioWorkers > defaultDomainIOWorkers {
		ioWorkers = defaultDomainIOWorkers
	}
	if ioWorkers <= 0 {
		ioWorkers = 1
	}
	executor := &deliveryActorExecutor{
		coordinator: coordinator, shards: make([]deliveryActorShard, partitions), maxBytes: maxBytes,
		maxTasks: maxTasks, ioWorkers: ioWorkers,
		ioQueue:  make(chan deliveryActorIOTask, int(maxTasks)),
		capacity: make(chan struct{}, 1),
	}
	if coordinator != nil {
		executor.process = coordinator.processWindowBudgeted
	}
	for i := range executor.shards {
		executor.shards[i] = deliveryActorShard{
			queue: make(chan deliveryActorWork, mailbox),
			// Completion advances per-domain serialization and releases the shared
			// reservation. A single slot decouples the worker from a shard briefly
			// submitting another domain to ioQueue; the idempotent reservation token
			// makes cancellation safe without making the runtime unbounded.
			completed: make(chan deliveryActorCompletion, 1),
			domains:   make(map[deliveryOrderingDomain]*deliveryDomainState),
			executor:  executor,
		}
	}
	return executor
}

func (e *deliveryActorExecutor) run(ctx context.Context, wait *sync.WaitGroup) {
	for i := 0; i < e.ioWorkers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			e.runIOWorker(ctx)
		}()
	}
	for i := range e.shards {
		wait.Add(1)
		go func(shard *deliveryActorShard) {
			defer wait.Done()
			shard.run(ctx)
		}(&e.shards[i])
	}
}

func (s *deliveryActorShard) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.executor.stopAdmissions()
			s.releasePending()
			return
		case work := <-s.queue:
			domain := orderingDomainOf(work.window)
			state := s.domains[domain]
			if state == nil {
				state = &deliveryDomainState{}
				s.domains[domain] = state
			}
			state.pending = append(state.pending, work)
			s.dispatchNext(ctx, domain, state)
		case completion := <-s.completed:
			state := s.domains[completion.domain]
			if state == nil || state.inFlight == nil {
				continue
			}
			s.executor.releaseWork(*state.inFlight)
			state.inFlight = nil
			s.dispatchNext(ctx, completion.domain, state)
			if state.inFlight == nil && len(state.pending) == 0 {
				delete(s.domains, completion.domain)
			}
		}
	}
}

func (s *deliveryActorShard) dispatchNext(ctx context.Context, domain deliveryOrderingDomain, state *deliveryDomainState) {
	if state == nil || state.inFlight != nil || len(state.pending) == 0 {
		return
	}
	work := state.pending[0]
	select {
	case s.executor.ioQueue <- deliveryActorIOTask{shard: s, domain: domain, work: work}:
		state.pending[0] = deliveryActorWork{}
		state.pending = state.pending[1:]
		state.inFlight = &work
	case <-ctx.Done():
	}
}

func (s *deliveryActorShard) releasePending() {
	// Cancellation may race a worker publishing a buffered completion. Every
	// copy shares an idempotent reservation token, so the shard can release all
	// work it still references while the worker safely attempts the same thing.
	for _, state := range s.domains {
		if state.inFlight != nil {
			s.executor.releaseWork(*state.inFlight)
			state.inFlight = nil
		}
		for _, work := range state.pending {
			s.executor.releaseWork(work)
		}
		state.pending = nil
	}
	for {
		select {
		case work := <-s.queue:
			s.executor.releaseWork(work)
		default:
			return
		}
	}
}

func (e *deliveryActorExecutor) runIOWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			e.drainCanceledIOTasks()
			return
		case task := <-e.ioQueue:
			if e.process != nil {
				e.process(ctx, task.work.window, task.work.reservation)
			}
			if ctx.Err() != nil {
				e.releaseWork(task.work)
				continue
			}
			select {
			case task.shard.completed <- deliveryActorCompletion{domain: task.domain}:
			case <-ctx.Done():
				e.releaseWork(task.work)
			}
		}
	}
}

func (e *deliveryActorExecutor) drainCanceledIOTasks() {
	for {
		select {
		case task := <-e.ioQueue:
			e.releaseWork(task.work)
		default:
			return
		}
	}
}

func orderingDomainOf(window store.OutboxClaimWindow) deliveryOrderingDomain {
	return deliveryOrderingDomain{queueKind: window.QueueKind, streamID: window.StreamID}
}

func (e *deliveryActorExecutor) submit(ctx context.Context, window store.OutboxClaimWindow) bool {
	if e == nil || len(e.shards) == 0 || len(window.Items) == 0 {
		return false
	}
	e.lifecycle.RLock()
	defer e.lifecycle.RUnlock()
	if e.stopped || ctx.Err() != nil {
		return false
	}
	bytes := claimWindowBytes(window)
	if !e.reserveContext(ctx, bytes) {
		return false
	}
	reservation := newDeliveryActorReservation(e, bytes)
	work := deliveryActorWork{window: window, bytes: bytes, reservation: reservation}
	index := orderingDomainHash(window.QueueKind, window.StreamID) % uint64(len(e.shards))
	select {
	case e.shards[index].queue <- work:
		return true
	case <-ctx.Done():
		e.releaseWork(work)
		return false
	}
}

func (e *deliveryActorExecutor) stopAdmissions() {
	if e == nil {
		return
	}
	e.lifecycle.Lock()
	e.stopped = true
	e.lifecycle.Unlock()
}

func (e *deliveryActorExecutor) reserveContext(ctx context.Context, bytes int64) bool {
	if bytes <= 0 || bytes > e.maxBytes {
		return false
	}
	for {
		if e.reserve(bytes) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-e.capacity:
		}
	}
}

func (e *deliveryActorExecutor) release(bytes int64) {
	if e == nil || bytes <= 0 {
		return
	}
	e.releaseBytes(bytes)
	e.usedTasks.Add(-1)
	select {
	case e.capacity <- struct{}{}:
	default:
	}
}

func (e *deliveryActorExecutor) releaseBytes(bytes int64) {
	if e == nil || bytes <= 0 {
		return
	}
	e.usedBytes.Add(-bytes)
	select {
	case e.capacity <- struct{}{}:
	default:
	}
}

func (e *deliveryActorExecutor) releaseWork(work deliveryActorWork) {
	if work.reservation == nil {
		e.release(work.bytes)
		return
	}
	if bytes, ok := work.reservation.take(); ok {
		e.release(bytes)
	}
}

func (e *deliveryActorExecutor) reserve(bytes int64) bool {
	if bytes <= 0 || bytes > e.maxBytes {
		return false
	}
	for {
		used := e.usedTasks.Load()
		if used >= e.maxTasks {
			return false
		}
		if e.usedTasks.CompareAndSwap(used, used+1) {
			break
		}
	}
	if !e.reserveBytes(bytes) {
		e.usedTasks.Add(-1)
		return false
	}
	return true
}

func (e *deliveryActorExecutor) reserveBytes(bytes int64) bool {
	if e == nil || bytes <= 0 || bytes > e.maxBytes {
		return false
	}
	for {
		used := e.usedBytes.Load()
		if used > e.maxBytes-bytes {
			return false
		}
		if e.usedBytes.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}

func newDeliveryActorReservation(owner *deliveryActorExecutor, bytes int64) *deliveryActorReservation {
	r := &deliveryActorReservation{owner: owner}
	if bytes > 0 {
		r.state.Store(uint64(bytes))
	}
	return r
}

// retain charges memory allocated after claim (projection bytes and immutable
// delivery-plan clones) to the same global actor budget. It is deliberately
// non-blocking: waiting while every worker retains a large claim could create a
// capacity deadlock in which no work can finish and release bytes.
func (r *deliveryActorReservation) retain(bytes int64) bool {
	if r == nil || r.owner == nil || bytes <= 0 || !r.owner.reserveBytes(bytes) {
		return false
	}
	addition := uint64(bytes)
	for {
		state := r.state.Load()
		if state&deliveryReservationReleased != 0 || state&deliveryReservationByteMask > deliveryReservationByteMask-addition {
			r.owner.releaseBytes(bytes)
			return false
		}
		if r.state.CompareAndSwap(state, state+addition) {
			return true
		}
	}
}

func (r *deliveryActorReservation) releaseRetained(bytes int64) bool {
	if r == nil || r.owner == nil || bytes <= 0 {
		return false
	}
	subtraction := uint64(bytes)
	for {
		state := r.state.Load()
		retained := state & deliveryReservationByteMask
		if state&deliveryReservationReleased != 0 || retained < subtraction {
			return false
		}
		if r.state.CompareAndSwap(state, state-subtraction) {
			r.owner.releaseBytes(bytes)
			return true
		}
	}
}

func (r *deliveryActorReservation) take() (int64, bool) {
	if r == nil {
		return 0, false
	}
	for {
		state := r.state.Load()
		if state&deliveryReservationReleased != 0 {
			return 0, false
		}
		if r.state.CompareAndSwap(state, deliveryReservationReleased) {
			return int64(state & deliveryReservationByteMask), true
		}
	}
}

func claimWindowBytes(window store.OutboxClaimWindow) int64 {
	bytes := int64(256+len(window.Owner)) + claimItemRetainedBytes*int64(len(window.Items))
	for _, item := range window.Items {
		bytes += int64(len(item.SourceInstanceID))
		for _, target := range item.Targets {
			bytes += claimAttemptTargetBytes + int64(len(target.TargetInstanceID))
		}
		switch payload := item.Payload.(type) {
		case store.AbsoluteDeliveryPayload:
			bytes += int64(len(payload.TL))
		case store.DispatchOutboxPayload:
			bytes += ptsClaimByteEstimate
		case store.ChannelDeliveryPayload:
			bytes += int64(512 + 8*(len(payload.AudienceUserIDs)+len(payload.AffectedUserIDs)))
		}
	}
	return bytes
}

func orderingDomainHash(kind store.OutboxQueueKind, streamID int64) uint64 {
	x := uint64(streamID) ^ uint64(kind)*0x9e3779b97f4a7c15
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

type queueRunnerConfig struct {
	kind               store.OutboxQueueKind
	store              store.DurableOutboxStateStore
	wake               <-chan struct{}
	logicalCount       int
	workers            int
	laneLimit          int
	windowSize         int
	windowBytes        int
	lease              time.Duration
	physicalDuration   time.Duration
	clockSkewAllowance time.Duration
}

func (s *Service) runQueue(ctx context.Context, cfg queueRunnerConfig, actors *deliveryActorExecutor, wait *sync.WaitGroup) {
	if cfg.store == nil || cfg.wake == nil {
		s.log.Error("durable queue dependency missing", zap.Uint8("queue_kind", uint8(cfg.kind)))
		return
	}
	workers := normalizedOutboxWorkers(cfg.workers)
	workerWakes := make([]chan struct{}, workers)
	for i := range workerWakes {
		workerWakes[i] = make(chan struct{}, 1)
		workerWakes[i] <- struct{}{}
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		broadcastOutboxWake(ctx, cfg.wake, workerWakes)
	}()
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			s.runQueueWorker(ctx, cfg, worker, workerWakes[worker], actors)
		}()
	}
}

func (s *Service) runQueueWorker(
	ctx context.Context,
	cfg queueRunnerConfig,
	worker int,
	wake <-chan struct{},
	actors *deliveryActorExecutor,
) {
	shards := logicalShardsForWorker(worker, cfg.workers)
	owner := deliveryWorkerOwner(s.coordinator.instanceID, cfg.kind, worker)
	recovery := store.OutboxRecoverBoundRequest{
		QueueKind: cfg.kind, Mode: store.OutboxBoundRecoveryExactOwnerLive, Owner: owner,
		LogicalShardCount: cfg.logicalCount, LogicalShardIDs: shards,
		LaneLimit: cfg.laneLimit, LeaseDuration: cfg.lease,
	}
	if windows, err := cfg.store.RecoverBoundWindows(ctx, recovery); err != nil {
		s.log.Error("recover bound delivery windows", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
	} else {
		for _, window := range windows {
			if !actors.submit(ctx, window) {
				return
			}
		}
	}
	claim := store.OutboxClaimRequest{
		QueueKind: cfg.kind, LogicalShardCount: cfg.logicalCount, LogicalShardIDs: shards,
		LaneLimit: cfg.laneLimit, WindowSize: cfg.windowSize, WindowByteLimit: cfg.windowBytes,
		LeaseDuration: cfg.lease, PhysicalDuration: cfg.physicalDuration,
		ClockSkewAllowance: cfg.clockSkewAllowance, Owner: owner,
	}
	expired := store.OutboxEvidenceExpiryRequest{
		QueueKind:         cfg.kind,
		LogicalShardCount: cfg.logicalCount, LogicalShardIDs: shards,
		LaneLimit: cfg.laneLimit, MaxAttempts: maxOnlineDeliveryAttempts,
	}
	dispatch := func(ctx context.Context) bool {
		// The DB clock waits until recover_after (wire NotAfter + skew allowance)
		// before resolving a missing receipt. Finalize before claiming a new fence;
		// the domain actor queues any successor behind work still completing.
		expiredFinalizers, err := cfg.store.ExpireEvidenceDeadlines(ctx, expired)
		if err != nil {
			s.log.Warn("expire durable delivery evidence", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
			return false
		}
		if len(expiredFinalizers) > 0 {
			finalized, err := cfg.store.FinalizeAttempts(ctx, expiredFinalizers)
			if err != nil {
				s.log.Warn("finalize expired durable delivery evidence", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
				return false
			}
			for _, next := range finalized.Next {
				if !actors.submit(ctx, next) {
					return false
				}
			}
		}
		windows, err := cfg.store.ClaimWindows(ctx, claim)
		if err != nil {
			s.log.Warn("claim durable delivery windows", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
			return false
		}
		if len(windows) == 0 {
			return false
		}
		for _, window := range windows {
			s.coordinator.metrics.OutboxClaimed(len(window.Items))
			if actors.submit(ctx, window) {
				continue
			}
			s.coordinator.resolveItems(ctx, cfg.store, window.Owner, window.Items, errorsNewActorCapacity(), false)
		}
		return true
	}
	nextReady := func(ctx context.Context) (store.OutboxNextReady, bool, error) {
		return cfg.store.NextReadyAt(ctx, cfg.kind, cfg.logicalCount, shards)
	}
	runWakeScheduledDispatchLoop(ctx, wake, dispatch, nextReady, func(err error) {
		s.log.Warn("schedule durable delivery queue", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
	})
}

func deliveryWorkerOwner(instanceID string, kind store.OutboxQueueKind, worker int) string {
	return fmt.Sprintf("%s/q%d/w%d", instanceID, kind, worker)
}

func errorsNewActorCapacity() error {
	return fmt.Errorf("egress: domain actor mailbox capacity exhausted")
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
				select {
				case worker <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (s *Service) runFinalizers(
	ctx context.Context,
	cfg queueRunnerConfig,
	wake <-chan struct{},
	actors *deliveryActorExecutor,
	wait *sync.WaitGroup,
) {
	workers := normalizedOutboxWorkers(cfg.workers)
	workerWakes := make([]chan struct{}, workers)
	for i := range workerWakes {
		workerWakes[i] = make(chan struct{}, 1)
		workerWakes[i] <- struct{}{}
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		broadcastOutboxWake(ctx, wake, workerWakes)
	}()
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			s.runFinalizerWorker(ctx, cfg, worker, workerWakes[worker], actors)
		}()
	}
}

func (s *Service) runFinalizerWorker(
	ctx context.Context,
	cfg queueRunnerConfig,
	worker int,
	wake <-chan struct{},
	actors *deliveryActorExecutor,
) {
	shards := logicalShardsForWorker(worker, cfg.workers)
	request := store.OutboxRecoverFinalizableRequest{
		QueueKind: cfg.kind, LogicalShardCount: cfg.logicalCount,
		LogicalShardIDs: shards, LaneLimit: cfg.laneLimit,
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-wake:
			if !ok {
				return
			}
		}
		backoff := outboxScheduleErrorBackoff
		for {
			requests, err := cfg.store.RecoverFinalizableAttempts(ctx, request)
			if err == nil && len(requests) == 0 {
				break
			}
			if err == nil {
				var finalized store.OutboxFinalizeBatch
				finalized, err = cfg.store.FinalizeAttempts(ctx, requests)
				if err == nil {
					for _, window := range finalized.Next {
						if !actors.submit(ctx, window) {
							return
						}
					}
					continue
				}
			}
			s.log.Warn("recover durable finalization", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			if backoff < maxOutboxScheduleBackoff {
				backoff *= 2
				if backoff > maxOutboxScheduleBackoff {
					backoff = maxOutboxScheduleBackoff
				}
			}
		}
	}
}

func fanoutQueueWake(ctx context.Context, source <-chan struct{}, outputs ...chan struct{}) {
	for _, output := range outputs {
		select {
		case output <- struct{}{}:
		default:
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-source:
			if !ok {
				return
			}
			for _, output := range outputs {
				select {
				case output <- struct{}{}:
				default:
				}
			}
		}
	}
}
