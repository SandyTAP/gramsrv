package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

const (
	ChannelDeliveryReadyNotifyChannel    = "telesrv_channel_delivery_ready"
	ChannelDeliveryFinalizeNotifyChannel = "telesrv_channel_delivery_finalize"
)

// ChannelDeliveryReadyListener consumes both channel lane-ready and
// confirmed-attempt notifications.  PostgreSQL remains the queue of record;
// notifications only wake claim/finalizer loops.
type ChannelDeliveryReadyListener struct {
	dsn string
	log *zap.Logger
}

func NewChannelDeliveryReadyListener(dsn string, log *zap.Logger) *ChannelDeliveryReadyListener {
	if log == nil {
		log = zap.NewNop()
	}
	return &ChannelDeliveryReadyListener{dsn: dsn, log: log}
}

// Run blocks until ctx is canceled.  It wakes once after both LISTEN commands
// are active (including every reconnect), then once for every notification.
func (l *ChannelDeliveryReadyListener) Run(ctx context.Context, wake func()) {
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
		l.log.Warn("channel delivery ready listener disconnected; reconnecting",
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

func (l *ChannelDeliveryReadyListener) listenAndConsume(ctx context.Context, wake func()) error {
	conn, err := pgx.Connect(ctx, l.dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN "+ChannelDeliveryReadyNotifyChannel); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "LISTEN "+ChannelDeliveryFinalizeNotifyChannel); err != nil {
		return err
	}
	wake()
	l.log.Info("channel delivery ready listener active",
		zap.String("ready_notify_channel", ChannelDeliveryReadyNotifyChannel),
		zap.String("finalize_notify_channel", ChannelDeliveryFinalizeNotifyChannel))

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if notification == nil {
			continue
		}
		switch notification.Channel {
		case ChannelDeliveryReadyNotifyChannel, ChannelDeliveryFinalizeNotifyChannel:
			wake()
		}
	}
}
