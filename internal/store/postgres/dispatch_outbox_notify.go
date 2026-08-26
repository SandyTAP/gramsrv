package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

// DispatchOutboxReadyNotifyChannel is emitted transactionally by dispatch_outbox
// lane triggers when at least one account ordering domain is ready to claim.
const DispatchOutboxReadyNotifyChannel = "telesrv_dispatch_outbox_ready"
const DispatchOutboxFinalizeNotifyChannel = "telesrv_dispatch_outbox_finalize"
const EdgeDeliveryOutboxReadyNotifyChannel = "telesrv_edge_delivery_outbox_ready"
const EdgeDeliveryOutboxFinalizeNotifyChannel = "telesrv_edge_delivery_outbox_finalize"
const BootstrapUpdateReadyNotifyChannel = "telesrv_bootstrap_update_ready"

// DispatchOutboxReadyListener consumes ready notifications for the durable
// outbox. The outbox table remains the queue of record; LISTEN readiness wakes
// claimers, and the listener wakes once after each successful LISTEN.
type DispatchOutboxReadyListener struct {
	dsn string
	log *zap.Logger
}

type EdgeDeliveryOutboxReadyListener struct {
	dsn string
	log *zap.Logger
}

type BootstrapUpdateReadyListener struct {
	dsn string
	log *zap.Logger
}

func NewDispatchOutboxReadyListener(dsn string, log *zap.Logger) *DispatchOutboxReadyListener {
	if log == nil {
		log = zap.NewNop()
	}
	return &DispatchOutboxReadyListener{dsn: dsn, log: log}
}

func NewEdgeDeliveryOutboxReadyListener(dsn string, log *zap.Logger) *EdgeDeliveryOutboxReadyListener {
	if log == nil {
		log = zap.NewNop()
	}
	return &EdgeDeliveryOutboxReadyListener{dsn: dsn, log: log}
}

func NewBootstrapUpdateReadyListener(dsn string, log *zap.Logger) *BootstrapUpdateReadyListener {
	if log == nil {
		log = zap.NewNop()
	}
	return &BootstrapUpdateReadyListener{dsn: dsn, log: log}
}

// Run blocks until ctx is canceled. wake is called once after LISTEN is ready
// and then for every notification received.
func (l *DispatchOutboxReadyListener) Run(ctx context.Context, wake func()) {
	if l == nil || wake == nil {
		return
	}
	backoff := channelListenerInitialBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		err := l.listenAndConsume(ctx, wake)
		if ctx.Err() != nil {
			return
		}
		l.log.Warn("dispatch outbox ready listener disconnected; reconnecting",
			zap.Error(err), zap.Duration("backoff", backoff))
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > channelListenerMaxBackoff {
			backoff = channelListenerMaxBackoff
		}
	}
}

func (l *EdgeDeliveryOutboxReadyListener) Run(ctx context.Context, wake func()) {
	if l == nil || wake == nil {
		return
	}
	backoff := channelListenerInitialBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		err := listenAndConsumeOutboxSignals(ctx, l.dsn, []string{
			EdgeDeliveryOutboxReadyNotifyChannel,
			EdgeDeliveryOutboxFinalizeNotifyChannel,
		}, l.log, wake)
		if ctx.Err() != nil {
			return
		}
		l.log.Warn("edge delivery outbox ready listener disconnected; reconnecting",
			zap.Error(err), zap.Duration("backoff", backoff))
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > channelListenerMaxBackoff {
			backoff = channelListenerMaxBackoff
		}
	}
}

func (l *BootstrapUpdateReadyListener) Run(ctx context.Context, wake func()) {
	if l == nil || wake == nil {
		return
	}
	backoff := channelListenerInitialBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		err := listenAndConsumeOutboxReady(ctx, l.dsn, BootstrapUpdateReadyNotifyChannel, l.log, wake)
		if ctx.Err() != nil {
			return
		}
		l.log.Warn("bootstrap update ready listener disconnected; reconnecting",
			zap.Error(err), zap.Duration("backoff", backoff))
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > channelListenerMaxBackoff {
			backoff = channelListenerMaxBackoff
		}
	}
}

func (l *DispatchOutboxReadyListener) listenAndConsume(ctx context.Context, wake func()) error {
	return listenAndConsumeOutboxSignals(ctx, l.dsn, []string{
		DispatchOutboxReadyNotifyChannel,
		DispatchOutboxFinalizeNotifyChannel,
	}, l.log, wake)
}

func listenAndConsumeOutboxReady(ctx context.Context, dsn, channel string, log *zap.Logger, wake func()) error {
	return listenAndConsumeOutboxSignals(ctx, dsn, []string{channel}, log, wake)
}

func listenAndConsumeOutboxSignals(ctx context.Context, dsn string, channels []string, log *zap.Logger, wake func()) error {
	if log == nil {
		log = zap.NewNop()
	}
	if len(channels) == 0 {
		return fmt.Errorf("outbox listener requires at least one channel")
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	for _, channel := range channels {
		if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
			return err
		}
	}
	wake()
	log.Info("outbox signal listener active", zap.Strings("notify_channels", channels))

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if notification == nil {
			continue
		}
		for _, channel := range channels {
			if notification.Channel == channel {
				wake()
				break
			}
		}
	}
}
