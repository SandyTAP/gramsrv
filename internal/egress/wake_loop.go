package egress

import (
	"context"
	"time"

	"telesrv/internal/store"
)

const (
	minimumOutboxScheduleDelay = time.Millisecond
	outboxScheduleErrorBackoff = 100 * time.Millisecond
	maxOutboxScheduleBackoff   = 5 * time.Second
)

type outboxNextReady func(context.Context) (store.OutboxNextReady, bool, error)

// runWakeScheduledDispatchLoop combines low-latency durable-storage wakeups
// with one database-derived lease/retry deadline. A NOTIFY always cancels the
// current deadline; after every drain the worker rebuilds it from storage.
// There is no fixed queue scan interval.
func runWakeScheduledDispatchLoop(
	ctx context.Context,
	wake <-chan struct{},
	dispatch func(context.Context) bool,
	nextReady outboxNextReady,
	onScheduleError func(error),
) {
	if wake == nil || dispatch == nil || nextReady == nil {
		return
	}
	var timer *time.Timer
	var timerC <-chan time.Time
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}
	defer stopTimer()

	errorBackoff := outboxScheduleErrorBackoff
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-wake:
			if !ok {
				return
			}
		case <-timerC:
		}
		stopTimer()

		for {
			if ctx.Err() != nil {
				return
			}
			if !dispatch(ctx) {
				break
			}
		}

		queryStarted := time.Now()
		next, ok, err := nextReady(ctx)
		if err != nil {
			if onScheduleError != nil {
				onScheduleError(err)
			}
			timer = time.NewTimer(errorBackoff)
			timerC = timer.C
			if errorBackoff < maxOutboxScheduleBackoff {
				errorBackoff *= 2
				if errorBackoff > maxOutboxScheduleBackoff {
					errorBackoff = maxOutboxScheduleBackoff
				}
			}
			continue
		}
		errorBackoff = outboxScheduleErrorBackoff
		if !ok {
			continue
		}
		delay := next.Delay() - time.Since(queryStarted)
		if delay < minimumOutboxScheduleDelay {
			delay = minimumOutboxScheduleDelay
		}
		timer = time.NewTimer(delay)
		timerC = timer.C
	}
}
