package edgecontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
)

type fabricRegistry struct {
	records []LocationRecord
	err     error
}

func (r *fabricRegistry) Heartbeat(context.Context, LocationRecord, time.Duration) error { return nil }
func (r *fabricRegistry) Remove(context.Context, LocationRecord) error                   { return nil }
func (r *fabricRegistry) ListUser(context.Context, int64) ([]LocationRecord, error) {
	return append([]LocationRecord(nil), r.records...), r.err
}
func (r *fabricRegistry) ListBusinessAuthKey(context.Context, [8]byte) ([]LocationRecord, error) {
	return nil, nil
}
func (r *fabricRegistry) ListInstance(context.Context, string) ([]LocationRecord, error) {
	return nil, nil
}

type fabricBus struct {
	ack         OutboxPushAck
	err         error
	ackByTarget map[string]OutboxPushAck
	errByTarget map[string]error
	commands    []OutboxPushCommand
}

func (b *fabricBus) SendOutboxPush(_ context.Context, targetInstanceID string, cmd OutboxPushCommand) (OutboxPushAck, error) {
	cmd.TargetInstanceID = targetInstanceID
	b.commands = append(b.commands, cmd)
	ack := b.ack
	if b.ackByTarget != nil {
		if targetAck, ok := b.ackByTarget[targetInstanceID]; ok {
			ack = targetAck
		}
	}
	err := b.err
	if b.errByTarget != nil {
		if targetErr, ok := b.errByTarget[targetInstanceID]; ok {
			err = targetErr
		}
	}
	if ack.CommandID == "" {
		ack.CommandID = cmd.CommandID
	}
	if ack.DeliveryRef.Empty() {
		ack.DeliveryRef = cmd.DeliveryRef
	}
	return ack, err
}

func (b *fabricBus) SubscribeOutboxPushes(context.Context, string, OutboxPushCommandHandler) error {
	return nil
}

type fabricLocalDeliverer struct {
	result   OutboxPushResult
	err      error
	requests []OutboxPushRequest
}

func (d *fabricLocalDeliverer) PushOutboxUpdate(_ context.Context, req OutboxPushRequest) (OutboxPushResult, error) {
	d.requests = append(d.requests, req)
	return d.result, d.err
}

func fabricOutboxRef(outboxID int64, targetUserID int64, pts int, attempt int) OutboxDeliveryRef {
	return OutboxDeliveryRef{
		OutboxID:     outboxID,
		TargetUserID: targetUserID,
		Pts:          pts,
		Attempt:      attempt,
	}
}

func fabricUpdateBytes(t *testing.T, date int) []byte {
	t.Helper()
	raw, err := EncodeOutboxUpdate(&tg.Updates{Date: date})
	if err != nil {
		t.Fatalf("EncodeOutboxUpdate: %v", err)
	}
	return raw
}

func TestOutboxFabricRemoteDeliveredAfterAck(t *testing.T) {
	ref := fabricOutboxRef(7001, 42, 11, 2)
	registry := &fabricRegistry{records: []LocationRecord{{
		InstanceID:      "edge-b",
		UserID:          42,
		RawAuthKeyID:    [8]byte{2},
		SessionID:       22,
		ReceivesUpdates: true,
	}}}
	bus := &fabricBus{ack: OutboxPushAck{Sent: 1, Status: OutboxDeliveryDelivered}}
	fabric := NewOutboxFabric(OutboxFabricConfig{
		InstanceID: "edge-a",
		Registry:   registry,
		Bus:        bus,
	})

	result, err := fabric.PushOutboxUpdate(context.Background(), OutboxPushRequest{
		TargetUserID: 42,
		DeliveryRef:  ref,
		MessageType:  proto.MessageFromServer,
		UpdateBytes:  fabricUpdateBytes(t, 123),
	})
	if err != nil {
		t.Fatalf("PushOutboxUpdate: %v", err)
	}
	if result.Status != OutboxDeliveryDelivered || result.Sent != 1 {
		t.Fatalf("result = %+v, want delivered sent=1", result)
	}
	if len(bus.commands) != 1 || bus.commands[0].TargetInstanceID != "edge-b" {
		t.Fatalf("commands = %+v, want one command to edge-b", bus.commands)
	}
	if bus.commands[0].DeliveryRef != ref {
		t.Fatalf("command delivery ref = %+v, want %+v", bus.commands[0].DeliveryRef, ref)
	}
	if decoded, err := DecodeOutboxUpdate(bus.commands[0].UpdateBytes); err != nil {
		t.Fatalf("DecodeOutboxUpdate: %v", err)
	} else if _, ok := decoded.(*tg.Updates); !ok {
		t.Fatalf("decoded update = %T, want *tg.Updates", decoded)
	}
}

func TestOutboxFabricRemoteIndeterminateKeepsOutboxLeased(t *testing.T) {
	ref := fabricOutboxRef(7002, 42, 12, 1)
	registry := &fabricRegistry{records: []LocationRecord{{
		InstanceID:      "edge-b",
		UserID:          42,
		RawAuthKeyID:    [8]byte{2},
		SessionID:       22,
		ReceivesUpdates: true,
	}}}
	bus := &fabricBus{err: context.DeadlineExceeded}
	fabric := NewOutboxFabric(OutboxFabricConfig{
		InstanceID: "edge-a",
		Registry:   registry,
		Bus:        bus,
	})

	result, err := fabric.PushOutboxUpdate(context.Background(), OutboxPushRequest{
		TargetUserID: 42,
		DeliveryRef:  ref,
		MessageType:  proto.MessageFromServer,
		UpdateBytes:  fabricUpdateBytes(t, 123),
	})
	if !errors.Is(err, ErrOutboxDeliveryIndeterminate) {
		t.Fatalf("err = %v, want ErrOutboxDeliveryIndeterminate", err)
	}
	if result.Status != OutboxDeliveryIndeterminate {
		t.Fatalf("result = %+v, want indeterminate", result)
	}
}

func TestOutboxFabricUnavailableTargetIsNoKnownOnlineTarget(t *testing.T) {
	ref := fabricOutboxRef(7010, 42, 20, 1)
	registry := &fabricRegistry{records: []LocationRecord{{
		InstanceID:      "edge-b",
		UserID:          42,
		RawAuthKeyID:    [8]byte{2},
		SessionID:       22,
		ReceivesUpdates: true,
	}}}
	bus := &fabricBus{err: ErrOutboxTargetUnavailable}
	fabric := NewOutboxFabric(OutboxFabricConfig{
		InstanceID: "edge-a",
		Registry:   registry,
		Bus:        bus,
	})

	result, err := fabric.PushOutboxUpdate(context.Background(), OutboxPushRequest{
		TargetUserID: 42,
		DeliveryRef:  ref,
		MessageType:  proto.MessageFromServer,
		UpdateBytes:  fabricUpdateBytes(t, 123),
	})
	if err != nil {
		t.Fatalf("PushOutboxUpdate: %v", err)
	}
	if result.Status != OutboxDeliveryNoKnownOnlineTargets || result.Sent != 0 {
		t.Fatalf("result = %+v, want no known online targets", result)
	}
}

func TestOutboxFabricAckErrorIsIndeterminateEvenWithSentCount(t *testing.T) {
	ref := fabricOutboxRef(7003, 42, 13, 1)
	registry := &fabricRegistry{records: []LocationRecord{{
		InstanceID:      "edge-b",
		UserID:          42,
		RawAuthKeyID:    [8]byte{2},
		SessionID:       22,
		ReceivesUpdates: true,
	}}}
	bus := &fabricBus{ack: OutboxPushAck{Sent: 1, Status: OutboxDeliveryDelivered, Error: "remote failed after partial write"}}
	fabric := NewOutboxFabric(OutboxFabricConfig{
		InstanceID: "edge-a",
		Registry:   registry,
		Bus:        bus,
	})

	result, err := fabric.PushOutboxUpdate(context.Background(), OutboxPushRequest{
		TargetUserID: 42,
		DeliveryRef:  ref,
		MessageType:  proto.MessageFromServer,
		UpdateBytes:  fabricUpdateBytes(t, 123),
	})
	if !errors.Is(err, ErrOutboxDeliveryIndeterminate) {
		t.Fatalf("err = %v, want ErrOutboxDeliveryIndeterminate", err)
	}
	if result.Status != OutboxDeliveryIndeterminate || result.Sent != 1 {
		t.Fatalf("result = %+v, want indeterminate with sent=1", result)
	}
}

func TestOutboxFabricRegistryErrorIsIndeterminate(t *testing.T) {
	ref := fabricOutboxRef(7004, 42, 14, 1)
	registryErr := errors.New("redis unavailable")
	fabric := NewOutboxFabric(OutboxFabricConfig{
		InstanceID: "edge-a",
		Registry:   &fabricRegistry{err: registryErr},
		Bus:        &fabricBus{},
	})

	result, err := fabric.PushOutboxUpdate(context.Background(), OutboxPushRequest{
		TargetUserID: 42,
		DeliveryRef:  ref,
		MessageType:  proto.MessageFromServer,
		UpdateBytes:  fabricUpdateBytes(t, 123),
	})
	if !errors.Is(err, registryErr) {
		t.Fatalf("err = %v, want registry error", err)
	}
	if result.Status != OutboxDeliveryIndeterminate || result.Sent != 0 {
		t.Fatalf("result = %+v, want indeterminate without local delivery", result)
	}
}

func TestOutboxFabricMissingFabricFailsClosed(t *testing.T) {
	ref := fabricOutboxRef(7008, 42, 18, 1)
	tests := []struct {
		name   string
		config OutboxFabricConfig
	}{
		{name: "missing registry", config: OutboxFabricConfig{InstanceID: "egress-a", Bus: &fabricBus{}}},
		{name: "missing bus", config: OutboxFabricConfig{InstanceID: "egress-a", Registry: &fabricRegistry{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewOutboxFabric(tt.config).PushOutboxUpdate(context.Background(), OutboxPushRequest{
				TargetUserID: 42,
				DeliveryRef:  ref,
				MessageType:  proto.MessageFromServer,
				UpdateBytes:  fabricUpdateBytes(t, 123),
			})
			if !errors.Is(err, ErrOutboxDeliveryIndeterminate) {
				t.Fatalf("err = %v, want ErrOutboxDeliveryIndeterminate", err)
			}
			if result.Status != OutboxDeliveryIndeterminate || result.Sent != 0 {
				t.Fatalf("result = %+v, want indeterminate with no sent", result)
			}
		})
	}
}

func TestOutboxFabricAnyRemoteFailureKeepsOutboxLeased(t *testing.T) {
	ref := fabricOutboxRef(7005, 42, 15, 1)
	registry := &fabricRegistry{records: []LocationRecord{
		{
			InstanceID:      "edge-b",
			UserID:          42,
			RawAuthKeyID:    [8]byte{2},
			SessionID:       22,
			ReceivesUpdates: true,
		},
		{
			InstanceID:      "edge-c",
			UserID:          42,
			RawAuthKeyID:    [8]byte{3},
			SessionID:       33,
			ReceivesUpdates: true,
		},
	}}
	bus := &fabricBus{
		ackByTarget: map[string]OutboxPushAck{
			"edge-b": {Sent: 1, Status: OutboxDeliveryDelivered},
		},
		errByTarget: map[string]error{
			"edge-c": context.DeadlineExceeded,
		},
	}
	fabric := NewOutboxFabric(OutboxFabricConfig{
		InstanceID: "edge-a",
		Registry:   registry,
		Bus:        bus,
	})

	result, err := fabric.PushOutboxUpdate(context.Background(), OutboxPushRequest{
		TargetUserID: 42,
		DeliveryRef:  ref,
		MessageType:  proto.MessageFromServer,
		UpdateBytes:  fabricUpdateBytes(t, 123),
	})
	if !errors.Is(err, ErrOutboxDeliveryIndeterminate) {
		t.Fatalf("err = %v, want ErrOutboxDeliveryIndeterminate", err)
	}
	if result.Status != OutboxDeliveryIndeterminate || result.Sent != 1 {
		t.Fatalf("result = %+v, want indeterminate preserving delivered sent count", result)
	}
	if len(bus.commands) != 2 {
		t.Fatalf("commands = %d, want both remote targets attempted", len(bus.commands))
	}
}

func TestOutboxFabricRejectsMismatchedAckDeliveryRef(t *testing.T) {
	ref := fabricOutboxRef(7006, 42, 16, 1)
	registry := &fabricRegistry{records: []LocationRecord{{
		InstanceID:      "edge-b",
		UserID:          42,
		RawAuthKeyID:    [8]byte{2},
		SessionID:       22,
		ReceivesUpdates: true,
	}}}
	bus := &fabricBus{ack: OutboxPushAck{
		Sent:        1,
		Status:      OutboxDeliveryDelivered,
		DeliveryRef: fabricOutboxRef(9999, 42, 16, 1),
	}}
	fabric := NewOutboxFabric(OutboxFabricConfig{
		InstanceID: "edge-a",
		Registry:   registry,
		Bus:        bus,
	})

	result, err := fabric.PushOutboxUpdate(context.Background(), OutboxPushRequest{
		TargetUserID: 42,
		DeliveryRef:  ref,
		MessageType:  proto.MessageFromServer,
		UpdateBytes:  fabricUpdateBytes(t, 123),
	})
	if !errors.Is(err, ErrOutboxDeliveryIndeterminate) {
		t.Fatalf("err = %v, want ErrOutboxDeliveryIndeterminate", err)
	}
	if result.Status != OutboxDeliveryIndeterminate || result.Sent != 0 {
		t.Fatalf("result = %+v, want indeterminate without confirmed sent", result)
	}
}

func TestHandleOutboxPushCommandDecodesAndDeliversLocal(t *testing.T) {
	updateBytes, err := EncodeOutboxUpdate(&tg.Updates{Date: 456})
	if err != nil {
		t.Fatalf("EncodeOutboxUpdate: %v", err)
	}
	ref := fabricOutboxRef(7007, 42, 17, 3)
	local := &fabricLocalDeliverer{result: OutboxPushResult{Sent: 2, Status: OutboxDeliveryDelivered}}

	ack := HandleOutboxPushCommand(context.Background(), local, OutboxPushCommand{
		CommandID:        "cmd-1",
		SourceInstanceID: "edge-a",
		TargetInstanceID: "edge-b",
		TargetUserID:     42,
		DeliveryRef:      ref,
		ExcludeAuthKeyID: [8]byte{7},
		ExcludeSessionID: 77,
		MessageType:      proto.MessageFromServer,
		UpdateBytes:      updateBytes,
		DeliveryTimeout:  time.Second,
	})
	if ack.Status != OutboxDeliveryDelivered || ack.Sent != 2 || ack.Error != "" {
		t.Fatalf("ack = %+v, want delivered sent=2", ack)
	}
	if ack.DeliveryRef != ref {
		t.Fatalf("ack delivery ref = %+v, want %+v", ack.DeliveryRef, ref)
	}
	if len(local.requests) != 1 {
		t.Fatalf("local requests = %d, want 1", len(local.requests))
	}
	req := local.requests[0]
	if req.TargetUserID != 42 || req.ExcludeAuthKeyID != [8]byte{7} || req.ExcludeSessionID != 77 || req.MessageType != proto.MessageFromServer {
		t.Fatalf("local request = %+v", req)
	}
	if req.DeliveryRef != ref {
		t.Fatalf("local delivery ref = %+v, want %+v", req.DeliveryRef, ref)
	}
	if decoded, err := DecodeOutboxUpdate(req.UpdateBytes); err != nil {
		t.Fatalf("DecodeOutboxUpdate: %v", err)
	} else if _, ok := decoded.(*tg.Updates); !ok {
		t.Fatalf("local update bytes decode = %T, want *tg.Updates", decoded)
	}
}
