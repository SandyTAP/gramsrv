package redisbus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/edgecontrol"
)

func TestBusRequiresRedisClient(t *testing.T) {
	bus := New(nil)
	if _, err := bus.SendOutboxPush(context.Background(), "edge-b", edgecontrol.OutboxPushCommand{CommandID: "cmd"}); !errors.Is(err, ErrNilClient) {
		t.Fatalf("SendOutboxPush err = %v, want ErrNilClient", err)
	}
	if err := bus.SubscribeOutboxPushes(context.Background(), "edge-b", func(context.Context, edgecontrol.OutboxPushCommand) edgecontrol.OutboxPushAck {
		return edgecontrol.OutboxPushAck{}
	}); !errors.Is(err, ErrNilClient) {
		t.Fatalf("SubscribeOutboxPushes err = %v, want ErrNilClient", err)
	}
	if _, err := bus.SendSessionControl(context.Background(), "edge-b", edgecontrol.SessionControlCommand{CommandID: "cmd"}); !errors.Is(err, ErrNilClient) {
		t.Fatalf("SendSessionControl err = %v, want ErrNilClient", err)
	}
	if err := bus.SubscribeSessionControls(context.Background(), "edge-b", func(context.Context, edgecontrol.SessionControlCommand) edgecontrol.SessionControlAck {
		return edgecontrol.SessionControlAck{}
	}); !errors.Is(err, ErrNilClient) {
		t.Fatalf("SubscribeSessionControls err = %v, want ErrNilClient", err)
	}
}

func TestBusOutboxCommandRoundTripRedis(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis bus integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	prefix := fmt.Sprintf("test:edge:control:%d", time.Now().UnixNano())
	bus := New(client, WithPrefix(prefix))
	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()
	errCh := make(chan error, 1)
	go func() {
		errCh <- bus.SubscribeOutboxPushes(subCtx, "edge-b", func(_ context.Context, cmd edgecontrol.OutboxPushCommand) edgecontrol.OutboxPushAck {
			return edgecontrol.OutboxPushAck{
				CommandID:        cmd.CommandID,
				SourceInstanceID: cmd.SourceInstanceID,
				TargetInstanceID: cmd.TargetInstanceID,
				Sent:             3,
				Status:           edgecontrol.OutboxDeliveryDelivered,
			}
		})
	}()
	time.Sleep(100 * time.Millisecond)

	sendCtx, cancelSend := context.WithTimeout(ctx, 2*time.Second)
	defer cancelSend()
	ack, err := bus.SendOutboxPush(sendCtx, "edge-b", edgecontrol.OutboxPushCommand{
		CommandID:        "cmd-1",
		SourceInstanceID: "edge-a",
		TargetUserID:     42,
		DeliveryRef: edgecontrol.OutboxDeliveryRef{
			OutboxID:     8801,
			TargetUserID: 42,
			Pts:          31,
			Attempt:      4,
		},
	})
	if err != nil {
		t.Fatalf("SendOutboxPush: %v", err)
	}
	if ack.CommandID != "cmd-1" || ack.Sent != 3 || ack.Status != edgecontrol.OutboxDeliveryDelivered {
		t.Fatalf("ack = %+v, want delivered cmd-1 sent=3", ack)
	}
	if got, want := ack.DeliveryRef, (edgecontrol.OutboxDeliveryRef{OutboxID: 8801, TargetUserID: 42, Pts: 31, Attempt: 4}); got != want {
		t.Fatalf("ack delivery ref = %+v, want %+v", got, want)
	}
	cancelSub()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("subscriber err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not stop")
	}
}

func TestBusOutboxNoSubscriberReturnsUnavailableRedis(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis bus integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	prefix := fmt.Sprintf("test:edge:control:%d", time.Now().UnixNano())
	bus := New(client, WithPrefix(prefix))

	sendCtx, cancelSend := context.WithTimeout(ctx, 2*time.Second)
	defer cancelSend()
	_, err := bus.SendOutboxPush(sendCtx, "edge-missing", edgecontrol.OutboxPushCommand{
		CommandID:        "cmd-missing",
		SourceInstanceID: "edge-a",
		TargetUserID:     42,
		DeliveryRef: edgecontrol.OutboxDeliveryRef{
			OutboxID:     8802,
			TargetUserID: 42,
			Pts:          32,
			Attempt:      1,
		},
	})
	if !errors.Is(err, edgecontrol.ErrOutboxTargetUnavailable) {
		t.Fatalf("SendOutboxPush err = %v, want ErrOutboxTargetUnavailable", err)
	}
}

func TestBusSessionControlRoundTripRedis(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis bus integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	prefix := fmt.Sprintf("test:edge:control:%d", time.Now().UnixNano())
	bus := New(client, WithPrefix(prefix))
	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()
	errCh := make(chan error, 1)
	go func() {
		errCh <- bus.SubscribeSessionControls(subCtx, "edge-b", func(_ context.Context, cmd edgecontrol.SessionControlCommand) edgecontrol.SessionControlAck {
			return edgecontrol.SessionControlAck{
				CommandID:        cmd.CommandID,
				SourceInstanceID: cmd.SourceInstanceID,
				TargetInstanceID: cmd.TargetInstanceID,
				Affected:         5,
			}
		})
	}()
	time.Sleep(100 * time.Millisecond)

	sendCtx, cancelSend := context.WithTimeout(ctx, 2*time.Second)
	defer cancelSend()
	ack, err := bus.SendSessionControl(sendCtx, "edge-b", edgecontrol.SessionControlCommand{
		CommandID:        "cmd-2",
		SourceInstanceID: "edge-a",
		Kind:             edgecontrol.SessionControlCloseBusinessAuthKey,
		AuthKeyID:        [8]byte{9},
	})
	if err != nil {
		t.Fatalf("SendSessionControl: %v", err)
	}
	if ack.CommandID != "cmd-2" || ack.Affected != 5 {
		t.Fatalf("ack = %+v, want cmd-2 affected=5", ack)
	}
	cancelSub()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("subscriber err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not stop")
	}
}
