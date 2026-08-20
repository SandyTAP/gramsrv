package edge

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	egresssvc "telesrv/internal/egress"
	"telesrv/internal/mtprotoedge"
)

const (
	outboxClientAckQueueSize = 8192
	outboxClientAckWorkers   = 4
)

type outboxClientAckSink interface {
	AckOutboxDelivery(ctx context.Context, ack egresssvc.DeliveryAck) error
}

type outboxClientAckReporter struct {
	sink    outboxClientAckSink
	log     *zap.Logger
	timeout time.Duration
	ch      chan mtprotoedge.OutboxClientAck
	done    chan struct{}
}

func newOutboxClientAckReporter(sink outboxClientAckSink, timeout time.Duration, log *zap.Logger) *outboxClientAckReporter {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &outboxClientAckReporter{
		sink:    sink,
		log:     log,
		timeout: timeout,
		ch:      make(chan mtprotoedge.OutboxClientAck, outboxClientAckQueueSize),
		done:    make(chan struct{}),
	}
}

func (w *outboxClientAckReporter) Submit(ack mtprotoedge.OutboxClientAck) {
	if w == nil || w.sink == nil {
		return
	}
	select {
	case w.ch <- ack:
	case <-w.done:
	}
}

func (w *outboxClientAckReporter) Run(ctx context.Context) {
	if w == nil || w.sink == nil {
		return
	}
	defer close(w.done)
	for i := 0; i < outboxClientAckWorkers; i++ {
		go w.runWorker(ctx)
	}
	<-ctx.Done()
}

func (w *outboxClientAckReporter) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ack := <-w.ch:
			w.report(ctx, ack)
		}
	}
}

func (w *outboxClientAckReporter) report(ctx context.Context, ack mtprotoedge.OutboxClientAck) {
	writeCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	err := w.sink.AckOutboxDelivery(writeCtx, egresssvc.DeliveryAck{
		OutboxID:     ack.Ref.OutboxID,
		TargetUserID: ack.Ref.TargetUserID,
		Pts:          ack.Ref.Pts,
		Attempt:      ack.Ref.Attempt,
		AuthKeyID:    ack.AuthKeyID,
		SessionID:    ack.SessionID,
		ServerMsgID:  ack.ServerMsgID,
		AckedAt:      ack.AckedAt,
	})
	if err == nil {
		return
	}
	fields := []zap.Field{
		zap.Int64("target_user_id", ack.Ref.TargetUserID),
		zap.Int64("outbox_id", ack.Ref.OutboxID),
		zap.Int("pts", ack.Ref.Pts),
		zap.Int("attempt", ack.Ref.Attempt),
		zap.Int64("session_id", ack.SessionID),
		zap.Int64("server_msg_id", ack.ServerMsgID),
		zap.Error(err),
	}
	if errors.Is(err, egresssvc.ErrAckLeaseLost) {
		w.log.Debug("ignore stale outbox client ack", fields...)
		return
	}
	w.log.Warn("report outbox client ack", fields...)
}
