package egress

import "context"

// runWakeDrivenDispatchLoop waits for durable storage readiness signals, then
// drains claimable work until the store reports an empty batch.
func runWakeDrivenDispatchLoop(ctx context.Context, wake <-chan struct{}, dispatch func(context.Context) bool) {
	if wake == nil || dispatch == nil {
		return
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
		for {
			if ctx.Err() != nil {
				return
			}
			if !dispatch(ctx) {
				break
			}
		}
	}
}
