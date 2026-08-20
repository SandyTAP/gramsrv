package redisbus_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/redis/go-redis/v9"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/edgecontrol/redisbus"
	"telesrv/internal/edgecontrol/redisregistry"
)

func TestRedisFabricRoutesOutboxAndSessionControlAcrossEdges(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis fabric integration test")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	prefix := fmt.Sprintf("test:edge:fabric:%d", time.Now().UnixNano())
	defer cleanupRedisPrefix(t, client, prefix)

	registry := redisregistry.New(client, redisregistry.WithPrefix(prefix+":location"))
	bus := redisbus.New(client, redisbus.WithPrefix(prefix+":control"))
	rawAuthKeyID := [8]byte{2}
	businessAuthKeyID := [8]byte{8}
	record := edgecontrol.LocationRecord{
		InstanceID:        "edge-b",
		UserID:            42,
		RawAuthKeyID:      rawAuthKeyID,
		BusinessAuthKeyID: businessAuthKeyID,
		SessionID:         22,
		ReceivesUpdates:   true,
		Layer:             228,
	}
	const locationLeaseID = "edge-b-test-lease"
	if err := registry.AcquireInstanceLease(ctx, record.InstanceID, locationLeaseID, time.Minute); err != nil {
		t.Fatalf("acquire edge-b location lease: %v", err)
	}
	if err := registry.ApplyLocationMutations(ctx, record.InstanceID, locationLeaseID, []edgecontrol.LocationMutation{{Record: record}}); err != nil {
		t.Fatalf("publish edge-b location: %v", err)
	}

	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()
	errCh := make(chan error, 2)
	edgeBOutbox := &redisFabricOutboxDeliverer{
		result: edgecontrol.OutboxPushResult{Sent: 1, Status: edgecontrol.OutboxDeliveryDelivered},
	}
	go func() {
		errCh <- bus.SubscribeOutboxPushes(subCtx, "edge-b", func(ctx context.Context, cmd edgecontrol.OutboxPushCommand) edgecontrol.OutboxPushAck {
			return edgecontrol.HandleOutboxPushCommand(ctx, edgeBOutbox, cmd)
		})
	}()
	edgeBControl := &redisFabricFullController{seedRawAffected: 1}
	go func() {
		errCh <- bus.SubscribeSessionControls(subCtx, "edge-b", func(_ context.Context, cmd edgecontrol.SessionControlCommand) edgecontrol.SessionControlAck {
			return edgecontrol.HandleSessionControlCommand(edgeBControl, cmd)
		})
	}()
	time.Sleep(100 * time.Millisecond)

	outbox := edgecontrol.NewOutboxFabric(edgecontrol.OutboxFabricConfig{
		InstanceID:     "edge-a",
		Registry:       registry,
		Bus:            bus,
		CommandTimeout: 2 * time.Second,
	})
	updateBytes, err := edgecontrol.EncodeOutboxUpdate(&tg.Updates{Date: 123})
	if err != nil {
		t.Fatalf("EncodeOutboxUpdate: %v", err)
	}
	result, err := outbox.PushOutboxUpdate(ctx, edgecontrol.OutboxPushRequest{
		TargetUserID: 42,
		DeliveryRef: edgecontrol.OutboxDeliveryRef{
			OutboxID:     9901,
			TargetUserID: 42,
			Pts:          19,
			Attempt:      2,
		},
		MessageType: proto.MessageFromServer,
		UpdateBytes: updateBytes,
	})
	if err != nil {
		t.Fatalf("PushOutboxUpdate: %v", err)
	}
	if result.Status != edgecontrol.OutboxDeliveryDelivered || result.Sent != 1 {
		t.Fatalf("outbox result = %+v, want delivered sent=1", result)
	}
	if got := edgeBOutbox.requestCount(); got != 1 {
		t.Fatalf("edge-b outbox requests = %d, want 1", got)
	}
	if got, want := edgeBOutbox.lastDeliveryRef(), (edgecontrol.OutboxDeliveryRef{OutboxID: 9901, TargetUserID: 42, Pts: 19, Attempt: 2}); got != want {
		t.Fatalf("edge-b delivery ref = %+v, want %+v", got, want)
	}

	control := edgecontrol.NewSessionControlFabric(edgecontrol.SessionControlFabricConfig{
		InstanceID:     "edge-a",
		Registry:       registry,
		Bus:            bus,
		CommandTimeout: 2 * time.Second,
	})
	control.BindUserForAuthKey(rawAuthKeyID, 22, 42)
	if edgeBControl.boundUserRaw != rawAuthKeyID || edgeBControl.boundUserSession != 22 || edgeBControl.boundUserID != 42 {
		t.Fatalf("edge-b bound user = raw:%v session:%d user:%d", edgeBControl.boundUserRaw, edgeBControl.boundUserSession, edgeBControl.boundUserID)
	}
	affected := control.SeedInheritedLayerForRawAuthKey(rawAuthKeyID, 228)
	if affected != 1 {
		t.Fatalf("seed raw layer affected = %d, want 1 remote edge", affected)
	}
	if edgeBControl.seedRawKey != rawAuthKeyID || edgeBControl.seedRawLayer != 228 {
		t.Fatalf("edge-b seeded layer = raw:%v layer:%d", edgeBControl.seedRawKey, edgeBControl.seedRawLayer)
	}
	reconnectRawAuthKeyID := [8]byte{3}
	staleRecord := edgecontrol.LocationRecord{InstanceID: "edge-stale", UserID: record.UserID, RawAuthKeyID: reconnectRawAuthKeyID, SessionID: 23, Layer: 228}
	currentRecord := staleRecord
	currentRecord.InstanceID = record.InstanceID
	const staleLocationLeaseID = "edge-stale-test-lease"
	if err := registry.AcquireInstanceLease(ctx, staleRecord.InstanceID, staleLocationLeaseID, time.Minute); err != nil {
		t.Fatalf("acquire stale edge location lease: %v", err)
	}
	if err := registry.ApplyLocationMutations(ctx, staleRecord.InstanceID, staleLocationLeaseID, []edgecontrol.LocationMutation{{Record: staleRecord}}); err != nil {
		t.Fatalf("publish stale edge location: %v", err)
	}
	if err := registry.ApplyLocationMutations(ctx, currentRecord.InstanceID, locationLeaseID, []edgecontrol.LocationMutation{{Record: currentRecord}}); err != nil {
		t.Fatalf("publish current reconnect location: %v", err)
	}
	syncID, disposition, err := control.BeginSessionChannelMembershipSync(ctx, reconnectRawAuthKeyID, currentRecord.SessionID, currentRecord.UserID)
	if err != nil || syncID != 31 || disposition != edgecontrol.ChannelMembershipSyncAcquired {
		t.Fatalf("begin membership sync id=%d disposition=%q err=%v, want newest edge id=31/acquired", syncID, disposition, err)
	}
	if edgeBControl.membershipBeginCount != 1 {
		t.Fatalf("edge-b membership begin count = %d, want 1", edgeBControl.membershipBeginCount)
	}

	cancelSub()
	waitRedisFabricSubscribers(t, errCh, 2)
}

func cleanupRedisPrefix(t *testing.T, client *redis.Client, prefix string) {
	t.Helper()
	keys, err := client.Keys(context.Background(), prefix+"*").Result()
	if err != nil {
		t.Logf("list redis cleanup keys: %v", err)
		return
	}
	if len(keys) > 0 {
		if err := client.Del(context.Background(), keys...).Err(); err != nil {
			t.Logf("cleanup redis keys: %v", err)
		}
	}
}

func waitRedisFabricSubscribers(t *testing.T, errCh <-chan error, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < want; i++ {
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				t.Fatalf("subscriber err = %v", err)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for redis fabric subscriber %d/%d", i+1, want)
		}
	}
}

type redisFabricOutboxDeliverer struct {
	mu       sync.Mutex
	result   edgecontrol.OutboxPushResult
	requests []edgecontrol.OutboxPushRequest
}

func (d *redisFabricOutboxDeliverer) PushOutboxUpdate(_ context.Context, req edgecontrol.OutboxPushRequest) (edgecontrol.OutboxPushResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.requests = append(d.requests, req)
	return d.result, nil
}

func (d *redisFabricOutboxDeliverer) requestCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.requests)
}

func (d *redisFabricOutboxDeliverer) lastDeliveryRef() edgecontrol.OutboxDeliveryRef {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.requests) == 0 {
		return edgecontrol.OutboxDeliveryRef{}
	}
	return d.requests[len(d.requests)-1].DeliveryRef
}

type redisFabricFullController struct {
	edgecontrol.FullController

	boundUserRaw     [8]byte
	boundUserSession int64
	boundUserID      int64

	seedRawKey      [8]byte
	seedRawLayer    int
	seedRawAffected int

	membershipBeginCount int
}

func (c *redisFabricFullController) BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64) {
	c.boundUserRaw = rawAuthKeyID
	c.boundUserSession = sessionID
	c.boundUserID = userID
}

func (c *redisFabricFullController) SeedInheritedLayerForRawAuthKey(rawAuthKeyID [8]byte, layer int) int {
	c.seedRawKey = rawAuthKeyID
	c.seedRawLayer = layer
	return c.seedRawAffected
}

func (c *redisFabricFullController) BeginSessionChannelMembershipSync(context.Context, [8]byte, int64, int64) (int64, edgecontrol.ChannelMembershipSyncDisposition, error) {
	c.membershipBeginCount++
	return 31, edgecontrol.ChannelMembershipSyncAcquired, nil
}
