package edgecontrol

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

const (
	defaultOutboxCommandTimeout = 2 * time.Second
	minOutboxCommandTimeout     = 500 * time.Millisecond
	outboxPushSubscribeRetry    = time.Second
)

var outboxCommandSeq atomic.Uint64

type OutboxFabricConfig struct {
	InstanceID     string
	Registry       LocationRegistry
	Bus            OutboxPushCommandBus
	CommandTimeout time.Duration
}

type OutboxFabric struct {
	instanceID     string
	registry       LocationRegistry
	bus            OutboxPushCommandBus
	commandTimeout time.Duration
}

func NewOutboxFabric(cfg OutboxFabricConfig) *OutboxFabric {
	return &OutboxFabric{
		instanceID:     cfg.InstanceID,
		registry:       cfg.Registry,
		bus:            cfg.Bus,
		commandTimeout: cfg.CommandTimeout,
	}
}

func (f *OutboxFabric) PushOutboxUpdate(ctx context.Context, req OutboxPushRequest) (OutboxPushResult, error) {
	if f == nil {
		return OutboxPushResult{Status: OutboxDeliveryIndeterminate}, ErrOutboxDeliveryIndeterminate
	}
	ref, err := validateOutboxDeliveryRef(req.TargetUserID, req.DeliveryRef)
	if err != nil {
		return OutboxPushResult{Status: OutboxDeliveryIndeterminate}, err
	}
	req.DeliveryRef = ref
	if len(req.UpdateBytes) == 0 {
		return OutboxPushResult{Status: OutboxDeliveryIndeterminate}, fmt.Errorf("empty outbox update bytes")
	}
	sent := 0
	if f.registry == nil || f.bus == nil {
		return OutboxPushResult{Status: OutboxDeliveryIndeterminate}, ErrOutboxDeliveryIndeterminate
	}
	records, err := f.registry.ListUser(ctx, req.TargetUserID)
	if err != nil {
		return OutboxPushResult{Sent: sent, Status: OutboxDeliveryIndeterminate}, err
	}
	targets := remoteOutboxTargets(f.instanceID, req, records)
	if len(targets) == 0 {
		return confirmedOutboxResult(sent), nil
	}
	updateBytes := append([]byte(nil), req.UpdateBytes...)
	indeterminate := false
	for _, target := range targets {
		cmd := OutboxPushCommand{
			CommandID:        nextOutboxCommandID(f.instanceID),
			SourceInstanceID: f.instanceID,
			TargetInstanceID: target,
			TargetUserID:     req.TargetUserID,
			DeliveryRef:      req.DeliveryRef,
			ExcludeAuthKeyID: req.ExcludeAuthKeyID,
			ExcludeSessionID: req.ExcludeSessionID,
			MessageType:      req.MessageType,
			UpdateBytes:      updateBytes,
			DeliveryTimeout:  req.DeliveryTimeout,
		}
		ack, err := f.sendOutboxCommand(ctx, req, target, cmd)
		if err != nil {
			if errors.Is(err, ErrOutboxTargetUnavailable) {
				continue
			}
			indeterminate = true
			continue
		}
		if err := validateOutboxAckRef(cmd, ack); err != nil {
			indeterminate = true
			continue
		}
		sent += ack.Sent
		if ack.Error != "" {
			indeterminate = true
			continue
		}
		switch ack.Status {
		case OutboxDeliveryDelivered, OutboxDeliveryNoKnownOnlineTargets:
		case OutboxDeliveryIndeterminate:
			indeterminate = true
		default:
			if ack.Sent == 0 {
				indeterminate = true
			}
		}
	}
	if indeterminate {
		return OutboxPushResult{Sent: sent, Status: OutboxDeliveryIndeterminate}, ErrOutboxDeliveryIndeterminate
	}
	return confirmedOutboxResult(sent), nil
}

func (f *OutboxFabric) sendOutboxCommand(ctx context.Context, req OutboxPushRequest, target string, cmd OutboxPushCommand) (OutboxPushAck, error) {
	timeout := f.commandTimeout
	if timeout <= 0 {
		timeout = req.DeliveryTimeout
	}
	if timeout <= 0 {
		timeout = defaultOutboxCommandTimeout
	}
	if timeout < minOutboxCommandTimeout {
		timeout = minOutboxCommandTimeout
	}
	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return f.bus.SendOutboxPush(sendCtx, target, cmd)
}

func confirmedOutboxResult(sent int) OutboxPushResult {
	if sent > 0 {
		return OutboxPushResult{Sent: sent, Status: OutboxDeliveryDelivered}
	}
	return OutboxPushResult{Status: OutboxDeliveryNoKnownOnlineTargets}
}

func remoteOutboxTargets(localInstanceID string, req OutboxPushRequest, records []LocationRecord) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(records))
	for _, record := range records {
		if record.UserID != req.TargetUserID || !record.ReceivesUpdates || record.InstanceID == "" || record.InstanceID == localInstanceID {
			continue
		}
		if req.ExcludeSessionID != 0 && record.RawAuthKeyID == req.ExcludeAuthKeyID && record.SessionID == req.ExcludeSessionID {
			continue
		}
		if _, ok := seen[record.InstanceID]; ok {
			continue
		}
		seen[record.InstanceID] = struct{}{}
		out = append(out, record.InstanceID)
	}
	return out
}

func EncodeOutboxUpdate(update tg.UpdatesClass) ([]byte, error) {
	if update == nil {
		return nil, fmt.Errorf("nil outbox update")
	}
	var b bin.Buffer
	if err := update.Encode(&b); err != nil {
		return nil, err
	}
	return b.Copy(), nil
}

func DecodeOutboxUpdate(raw []byte) (tg.UpdatesClass, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty outbox update")
	}
	return tg.DecodeUpdates(&bin.Buffer{Buf: append([]byte(nil), raw...)})
}

func HandleOutboxPushCommand(ctx context.Context, local OutboxDeliverer, cmd OutboxPushCommand) OutboxPushAck {
	ref, refErr := validateOutboxDeliveryRef(cmd.TargetUserID, cmd.DeliveryRef)
	ack := OutboxPushAck{
		CommandID:        cmd.CommandID,
		SourceInstanceID: cmd.SourceInstanceID,
		TargetInstanceID: cmd.TargetInstanceID,
		DeliveryRef:      ref,
		Status:           OutboxDeliveryIndeterminate,
	}
	if refErr != nil {
		ack.Error = refErr.Error()
		return ack
	}
	if local == nil {
		ack.Error = "local edge deliverer is not configured"
		return ack
	}
	result, err := local.PushOutboxUpdate(ctx, OutboxPushRequest{
		TargetUserID:     cmd.TargetUserID,
		DeliveryRef:      ref,
		ExcludeAuthKeyID: cmd.ExcludeAuthKeyID,
		ExcludeSessionID: cmd.ExcludeSessionID,
		MessageType:      cmd.MessageType,
		UpdateBytes:      append([]byte(nil), cmd.UpdateBytes...),
		DeliveryTimeout:  cmd.DeliveryTimeout,
	})
	ack.Sent = result.Sent
	ack.Status = result.Status
	if err != nil {
		ack.Error = err.Error()
		if ack.Status == OutboxDeliveryUnknown {
			ack.Status = OutboxDeliveryIndeterminate
		}
	}
	return ack
}

func RunOutboxPushSubscriber(ctx context.Context, bus OutboxPushCommandBus, instanceID string, local OutboxDeliverer) {
	if bus == nil || instanceID == "" || local == nil {
		return
	}
	for {
		err := bus.SubscribeOutboxPushes(ctx, instanceID, func(ctx context.Context, cmd OutboxPushCommand) OutboxPushAck {
			return HandleOutboxPushCommand(ctx, local, cmd)
		})
		if ctx.Err() != nil {
			return
		}
		_ = err
		select {
		case <-ctx.Done():
			return
		case <-time.After(outboxPushSubscribeRetry):
		}
	}
}

func nextOutboxCommandID(instanceID string) string {
	if instanceID == "" {
		instanceID = "edge"
	}
	return fmt.Sprintf("%s:%d:%d", instanceID, time.Now().UnixNano(), outboxCommandSeq.Add(1))
}

func validateOutboxDeliveryRef(targetUserID int64, ref OutboxDeliveryRef) (OutboxDeliveryRef, error) {
	if targetUserID <= 0 {
		return ref, fmt.Errorf("outbox target user id is required")
	}
	if ref.TargetUserID == 0 {
		ref.TargetUserID = targetUserID
	}
	if ref.TargetUserID != targetUserID {
		return ref, fmt.Errorf("outbox delivery ref target_user_id mismatch")
	}
	if ref.OutboxID <= 0 {
		return ref, fmt.Errorf("outbox delivery ref outbox_id is required")
	}
	return ref, nil
}

func validateOutboxAckRef(cmd OutboxPushCommand, ack OutboxPushAck) error {
	if cmd.DeliveryRef.Empty() {
		return fmt.Errorf("outbox command delivery ref is required")
	}
	if ack.DeliveryRef.Empty() {
		return fmt.Errorf("outbox ack delivery ref is required")
	}
	if ack.DeliveryRef != cmd.DeliveryRef {
		return fmt.Errorf("outbox ack delivery ref mismatch")
	}
	return nil
}
