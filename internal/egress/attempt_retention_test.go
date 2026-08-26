package egress

import (
	"context"
	"errors"
	"testing"
	"time"

	"telesrv/internal/store"
)

type attemptTombstoneStoreStub struct {
	results []int
	err     error
	calls   int
	cutoffs []time.Time
	limits  []int
}

func (s *attemptTombstoneStoreStub) DeleteFinalizedAttempts(_ context.Context, cutoff time.Time, limit int) (int, error) {
	s.calls++
	s.cutoffs = append(s.cutoffs, cutoff)
	s.limits = append(s.limits, limit)
	if s.err != nil {
		return 0, s.err
	}
	if len(s.results) == 0 {
		return 0, nil
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result, nil
}

func TestSweepFinalizedAttemptTombstonesIsBoundedAndQueueIsolated(t *testing.T) {
	cutoff := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	pts := &attemptTombstoneStoreStub{results: []int{2, 2, 2, 2}}
	absoluteErr := errors.New("absolute unavailable")
	absolute := &attemptTombstoneStoreStub{err: absoluteErr}
	channel := &attemptTombstoneStoreStub{results: []int{2, 1}}
	var errorsSeen []store.OutboxQueueKind

	sweepFinalizedAttemptTombstones(context.Background(), []attemptTombstoneQueue{
		{kind: store.OutboxQueueDispatchPTS, state: pts},
		{kind: store.OutboxQueueAbsoluteDelivery, state: absolute},
		{kind: store.OutboxQueueChannelPTS, state: channel},
	}, cutoff, 2, 3, func(kind store.OutboxQueueKind, err error) {
		if !errors.Is(err, absoluteErr) {
			t.Fatalf("unexpected sweep error: %v", err)
		}
		errorsSeen = append(errorsSeen, kind)
	})

	if pts.calls != 3 {
		t.Fatalf("PTS batches = %d, want hard maximum 3", pts.calls)
	}
	if absolute.calls != 1 {
		t.Fatalf("absolute batches = %d, want stop after first error", absolute.calls)
	}
	if channel.calls != 2 {
		t.Fatalf("channel batches = %d, want stop after short batch", channel.calls)
	}
	if len(errorsSeen) != 1 || errorsSeen[0] != store.OutboxQueueAbsoluteDelivery {
		t.Fatalf("error queues = %v, want absolute only", errorsSeen)
	}
	for _, stub := range []*attemptTombstoneStoreStub{pts, absolute, channel} {
		for i := range stub.cutoffs {
			if !stub.cutoffs[i].Equal(cutoff) || stub.limits[i] != 2 {
				t.Fatalf("sweep args cutoff=%v limit=%d", stub.cutoffs[i], stub.limits[i])
			}
		}
	}
}

func TestSweepFinalizedAttemptTombstonesStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stub := &attemptTombstoneStoreStub{results: []int{1}}
	sweepFinalizedAttemptTombstones(ctx, []attemptTombstoneQueue{{
		kind: store.OutboxQueueDispatchPTS, state: stub,
	}}, time.Now().UTC(), 1, 1, nil)
	if stub.calls != 0 {
		t.Fatalf("delete calls after cancellation = %d, want 0", stub.calls)
	}
}
