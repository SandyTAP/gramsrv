package store

import (
	"context"
	"time"
)

// DeliveryOutboxItem is a durable non-PTS online delivery task. Payload is an
// already encoded TL updates object; the queue layer treats it as opaque bytes.
type DeliveryOutboxItem struct {
	ID               int64
	TargetUserID     int64
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	Attempts         int
	Payload          []byte
}

type DeliveryOutboxEnqueue struct {
	TargetUserID     int64
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	Payload          []byte
}

type DeliveryOutboxClientAck struct {
	OutboxID     int64
	TargetUserID int64
	Attempt      int
	AuthKeyID    [8]byte
	SessionID    int64
	ServerMsgID  int64
	AckedAt      time.Time
}

// DeliveryOutboxStore persists non-PTS delivery facts for independent Egress.
type DeliveryOutboxStore interface {
	Enqueue(ctx context.Context, item DeliveryOutboxEnqueue) (DeliveryOutboxItem, error)
	ClaimPending(ctx context.Context, limit int) ([]DeliveryOutboxItem, error)
	MarkDelivered(ctx context.Context, item DeliveryOutboxItem) error
	MarkAbandoned(ctx context.Context, item DeliveryOutboxItem, lastError string) error
	MarkFailed(ctx context.Context, item DeliveryOutboxItem, lastError string) error
	DeleteFailed(ctx context.Context, olderThan time.Duration, limit int) (int, error)
}

type DeliveryOutboxClientAckStore interface {
	MarkClientAcked(ctx context.Context, ack DeliveryOutboxClientAck) error
}
