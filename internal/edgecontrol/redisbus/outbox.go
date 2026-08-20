package redisbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/edgecontrol"
)

const (
	defaultPrefix                = "edge:control"
	defaultSubscriberCommandTime = 2 * time.Second
)

var ErrNilClient = errors.New("edge redis bus: nil redis client")

type Bus struct {
	c      redis.UniversalClient
	prefix string
}

type Option func(*Bus)

func WithPrefix(prefix string) Option {
	return func(b *Bus) {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" {
			b.prefix = strings.TrimRight(prefix, ":")
		}
	}
}

func New(c redis.UniversalClient, opts ...Option) *Bus {
	b := &Bus{c: c, prefix: defaultPrefix}
	for _, opt := range opts {
		if opt != nil {
			opt(b)
		}
	}
	return b
}

func (b *Bus) SendOutboxPush(ctx context.Context, targetInstanceID string, cmd edgecontrol.OutboxPushCommand) (edgecontrol.OutboxPushAck, error) {
	if b == nil || b.c == nil {
		return edgecontrol.OutboxPushAck{}, ErrNilClient
	}
	if strings.TrimSpace(targetInstanceID) == "" || strings.TrimSpace(cmd.CommandID) == "" {
		return edgecontrol.OutboxPushAck{}, fmt.Errorf("edge redis bus: target instance and command id are required")
	}
	cmd.TargetInstanceID = targetInstanceID
	ackChannel := b.outboxAckChannel(cmd.CommandID)
	pubsub := b.c.Subscribe(ctx, ackChannel)
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil {
		return edgecontrol.OutboxPushAck{}, fmt.Errorf("edge redis bus subscribe outbox ack: %w", err)
	}
	raw, err := json.Marshal(cmd)
	if err != nil {
		return edgecontrol.OutboxPushAck{}, fmt.Errorf("edge redis bus encode outbox command: %w", err)
	}
	receivers, err := b.c.Publish(ctx, b.outboxCommandChannel(targetInstanceID), raw).Result()
	if err != nil {
		return edgecontrol.OutboxPushAck{}, fmt.Errorf("edge redis bus publish outbox command: %w", err)
	}
	if receivers == 0 {
		return edgecontrol.OutboxPushAck{}, edgecontrol.ErrOutboxTargetUnavailable
	}
	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return edgecontrol.OutboxPushAck{}, ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return edgecontrol.OutboxPushAck{}, fmt.Errorf("edge redis bus outbox ack subscription closed")
			}
			var ack edgecontrol.OutboxPushAck
			if err := json.Unmarshal([]byte(msg.Payload), &ack); err != nil {
				return edgecontrol.OutboxPushAck{}, fmt.Errorf("edge redis bus decode outbox ack: %w", err)
			}
			if ack.CommandID != cmd.CommandID {
				continue
			}
			return ack, nil
		}
	}
}

func (b *Bus) SubscribeOutboxPushes(ctx context.Context, instanceID string, handle edgecontrol.OutboxPushCommandHandler) error {
	if b == nil || b.c == nil {
		return ErrNilClient
	}
	if strings.TrimSpace(instanceID) == "" {
		return fmt.Errorf("edge redis bus: instance id is required")
	}
	if handle == nil {
		return fmt.Errorf("edge redis bus: outbox push handler is required")
	}
	pubsub := b.c.Subscribe(ctx, b.outboxCommandChannel(instanceID))
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("edge redis bus subscribe outbox commands: %w", err)
	}
	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return fmt.Errorf("edge redis bus outbox command subscription closed")
			}
			var cmd edgecontrol.OutboxPushCommand
			if err := json.Unmarshal([]byte(msg.Payload), &cmd); err != nil {
				continue
			}
			go b.handleOutboxCommand(ctx, cmd, handle)
		}
	}
}

func (b *Bus) SendSessionControl(ctx context.Context, targetInstanceID string, cmd edgecontrol.SessionControlCommand) (edgecontrol.SessionControlAck, error) {
	if b == nil || b.c == nil {
		return edgecontrol.SessionControlAck{}, ErrNilClient
	}
	if strings.TrimSpace(targetInstanceID) == "" || strings.TrimSpace(cmd.CommandID) == "" {
		return edgecontrol.SessionControlAck{}, fmt.Errorf("edge redis bus: target instance and command id are required")
	}
	cmd.TargetInstanceID = targetInstanceID
	ackChannel := b.sessionControlAckChannel(cmd.CommandID)
	pubsub := b.c.Subscribe(ctx, ackChannel)
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil {
		return edgecontrol.SessionControlAck{}, fmt.Errorf("edge redis bus subscribe session control ack: %w", err)
	}
	raw, err := json.Marshal(cmd)
	if err != nil {
		return edgecontrol.SessionControlAck{}, fmt.Errorf("edge redis bus encode session control command: %w", err)
	}
	if err := b.c.Publish(ctx, b.sessionControlCommandChannel(targetInstanceID), raw).Err(); err != nil {
		return edgecontrol.SessionControlAck{}, fmt.Errorf("edge redis bus publish session control command: %w", err)
	}
	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return edgecontrol.SessionControlAck{}, ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return edgecontrol.SessionControlAck{}, fmt.Errorf("edge redis bus session control ack subscription closed")
			}
			var ack edgecontrol.SessionControlAck
			if err := json.Unmarshal([]byte(msg.Payload), &ack); err != nil {
				return edgecontrol.SessionControlAck{}, fmt.Errorf("edge redis bus decode session control ack: %w", err)
			}
			if ack.CommandID != cmd.CommandID {
				continue
			}
			return ack, nil
		}
	}
}

func (b *Bus) SubscribeSessionControls(ctx context.Context, instanceID string, handle edgecontrol.SessionControlCommandHandler) error {
	if b == nil || b.c == nil {
		return ErrNilClient
	}
	if strings.TrimSpace(instanceID) == "" {
		return fmt.Errorf("edge redis bus: instance id is required")
	}
	if handle == nil {
		return fmt.Errorf("edge redis bus: session control handler is required")
	}
	pubsub := b.c.Subscribe(ctx, b.sessionControlCommandChannel(instanceID))
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("edge redis bus subscribe session control commands: %w", err)
	}
	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return fmt.Errorf("edge redis bus session control command subscription closed")
			}
			var cmd edgecontrol.SessionControlCommand
			if err := json.Unmarshal([]byte(msg.Payload), &cmd); err != nil {
				continue
			}
			go b.handleSessionControlCommand(ctx, cmd, handle)
		}
	}
}

func (b *Bus) handleOutboxCommand(ctx context.Context, cmd edgecontrol.OutboxPushCommand, handle edgecontrol.OutboxPushCommandHandler) {
	handleCtx := ctx
	cancel := func() {}
	timeout := cmd.DeliveryTimeout
	if timeout <= 0 {
		timeout = defaultSubscriberCommandTime
	}
	if timeout > 0 {
		handleCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	ack := handle(handleCtx, cmd)
	if ack.CommandID == "" {
		ack.CommandID = cmd.CommandID
	}
	if ack.SourceInstanceID == "" {
		ack.SourceInstanceID = cmd.SourceInstanceID
	}
	if ack.TargetInstanceID == "" {
		ack.TargetInstanceID = cmd.TargetInstanceID
	}
	if ack.DeliveryRef.Empty() {
		ack.DeliveryRef = cmd.DeliveryRef
	}
	raw, err := json.Marshal(ack)
	if err != nil {
		return
	}
	_ = b.c.Publish(ctx, b.outboxAckChannel(cmd.CommandID), raw).Err()
}

func (b *Bus) handleSessionControlCommand(ctx context.Context, cmd edgecontrol.SessionControlCommand, handle edgecontrol.SessionControlCommandHandler) {
	handleCtx := ctx
	cancel := func() {}
	timeout := cmd.DeliveryTimeout
	if timeout <= 0 {
		timeout = defaultSubscriberCommandTime
	}
	if timeout > 0 {
		handleCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	ack := handle(handleCtx, cmd)
	if ack.CommandID == "" {
		ack.CommandID = cmd.CommandID
	}
	if ack.SourceInstanceID == "" {
		ack.SourceInstanceID = cmd.SourceInstanceID
	}
	if ack.TargetInstanceID == "" {
		ack.TargetInstanceID = cmd.TargetInstanceID
	}
	raw, err := json.Marshal(ack)
	if err != nil {
		return
	}
	_ = b.c.Publish(ctx, b.sessionControlAckChannel(cmd.CommandID), raw).Err()
}

func (b *Bus) outboxCommandChannel(instanceID string) string {
	return fmt.Sprintf("%s:outbox:push:%s", b.prefix, instanceID)
}

func (b *Bus) outboxAckChannel(commandID string) string {
	return fmt.Sprintf("%s:outbox:ack:%s", b.prefix, commandID)
}

func (b *Bus) sessionControlCommandChannel(instanceID string) string {
	return fmt.Sprintf("%s:session:control:%s", b.prefix, instanceID)
}

func (b *Bus) sessionControlAckChannel(commandID string) string {
	return fmt.Sprintf("%s:session:ack:%s", b.prefix, commandID)
}
