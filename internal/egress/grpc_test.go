package egress

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"telesrv/internal/egress/egresspb"
	"telesrv/internal/store"
)

func TestGRPCAckServerPersistsFencedClientAck(t *testing.T) {
	authKeyID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	ackedAt := time.Unix(0, 1700000000123456789)
	ackStore := &captureClientAckStore{}
	srv := newEgressAckGRPCServer(ackStore, nil, "egress-a")

	res, err := srv.AckOutboxDelivery(context.Background(), &egresspb.OutboxClientAckRequest{
		OutboxId:        42,
		TargetUserId:    1001,
		Pts:             77,
		Attempt:         3,
		AuthKeyId:       authKeyID[:],
		SessionId:       -55,
		ServerMsgId:     9001,
		AckedAtUnixNano: ackedAt.UnixNano(),
	})
	if err != nil {
		t.Fatalf("AckOutboxDelivery err = %v", err)
	}
	if res.GetError() != "" {
		t.Fatalf("AckOutboxDelivery response error = %q", res.GetError())
	}
	want := store.DispatchOutboxClientAck{
		OutboxID:     42,
		TargetUserID: 1001,
		Pts:          77,
		Attempt:      3,
		AuthKeyID:    authKeyID,
		SessionID:    -55,
		ServerMsgID:  9001,
		AckedAt:      ackedAt,
	}
	if ackStore.ack != want {
		t.Fatalf("stored ack = %+v, want %+v", ackStore.ack, want)
	}
}

func TestGRPCAckServerPersistsNonPTSDeliveryAck(t *testing.T) {
	authKeyID := [8]byte{8, 6, 4, 2, 1, 3, 5, 7}
	ackedAt := time.Unix(0, 1700000000222000000)
	deliveryStore := &captureDeliveryAckStore{}
	srv := newEgressAckGRPCServer(nil, deliveryStore, "egress-a")

	res, err := srv.AckOutboxDelivery(context.Background(), &egresspb.OutboxClientAckRequest{
		OutboxId:        43,
		TargetUserId:    1002,
		Pts:             0,
		Attempt:         4,
		AuthKeyId:       authKeyID[:],
		SessionId:       56,
		ServerMsgId:     9002,
		AckedAtUnixNano: ackedAt.UnixNano(),
	})
	if err != nil {
		t.Fatalf("AckOutboxDelivery err = %v", err)
	}
	if res.GetError() != "" {
		t.Fatalf("AckOutboxDelivery response error = %q", res.GetError())
	}
	want := store.DeliveryOutboxClientAck{
		OutboxID:     43,
		TargetUserID: 1002,
		Attempt:      4,
		AuthKeyID:    authKeyID,
		SessionID:    56,
		ServerMsgID:  9002,
		AckedAt:      ackedAt,
	}
	if deliveryStore.ack != want {
		t.Fatalf("stored delivery ack = %+v, want %+v", deliveryStore.ack, want)
	}
}

func TestGRPCAckServerRejectsMalformedFence(t *testing.T) {
	ackStore := &captureClientAckStore{}
	srv := newEgressAckGRPCServer(ackStore, nil, "egress-a")

	_, err := srv.AckOutboxDelivery(context.Background(), &egresspb.OutboxClientAckRequest{
		OutboxId:        42,
		TargetUserId:    1001,
		Pts:             77,
		Attempt:         3,
		AuthKeyId:       []byte{1, 2, 3},
		SessionId:       55,
		ServerMsgId:     9001,
		AckedAtUnixNano: time.Now().UnixNano(),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AckOutboxDelivery status = %s (%v), want InvalidArgument", status.Code(err), err)
	}
	if ackStore.called {
		t.Fatal("malformed ack reached store")
	}
}

func TestGRPCAckRemoteReportsDelivery(t *testing.T) {
	authKeyID := [8]byte{8, 7, 6, 5, 4, 3, 2, 1}
	ackedAt := time.Unix(0, 1700000000999000000)
	ackStore := &captureClientAckStore{}
	lis := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcAckBearerUnaryServerInterceptor("secret")))
	egresspb.RegisterEgressAckServiceServer(grpcServer, newEgressAckGRPCServer(ackStore, nil, "egress-a"))
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus(egresspb.EgressAckService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCAckRemote(ctx, GRPCAckClientConfig{
		Targets: []string{"127.0.0.1:2450"},
		Token:   "secret",
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
		},
	})
	if err != nil {
		t.Fatalf("DialGRPCAckRemote: %v", err)
	}
	defer func() { _ = conn.Close() }()

	err = remote.AckOutboxDelivery(ctx, DeliveryAck{
		OutboxID:     84,
		TargetUserID: 2002,
		Pts:          88,
		Attempt:      2,
		AuthKeyID:    authKeyID,
		SessionID:    66,
		ServerMsgID:  9901,
		AckedAt:      ackedAt,
	})
	if err != nil {
		t.Fatalf("AckOutboxDelivery: %v", err)
	}
	if !ackStore.called {
		t.Fatal("remote ack did not reach store")
	}
	if ackStore.ack.OutboxID != 84 || ackStore.ack.TargetUserID != 2002 || ackStore.ack.AuthKeyID != authKeyID || !ackStore.ack.AckedAt.Equal(ackedAt) {
		t.Fatalf("stored ack = %+v", ackStore.ack)
	}
}

func TestGRPCAckRequiresConfiguredBearerToken(t *testing.T) {
	ctx := context.Background()
	store := &captureClientAckStore{}
	if _, err := StartGRPCAck(ctx, GRPCAckServerConfig{Addr: "127.0.0.1:0", Store: store}); !errors.Is(err, ErrGRPCAckTokenMissing) {
		t.Fatalf("StartGRPCAck missing token err = %v, want ErrGRPCAckTokenMissing", err)
	}
	if _, _, err := DialGRPCAckRemote(ctx, GRPCAckClientConfig{Targets: []string{"127.0.0.1:2510"}}); !errors.Is(err, ErrGRPCAckTokenMissing) {
		t.Fatalf("DialGRPCAckRemote missing token err = %v, want ErrGRPCAckTokenMissing", err)
	}
}

type captureClientAckStore struct {
	called bool
	ack    store.DispatchOutboxClientAck
	err    error
}

func (s *captureClientAckStore) MarkClientAcked(_ context.Context, ack store.DispatchOutboxClientAck) error {
	s.called = true
	s.ack = ack
	return s.err
}

type captureDeliveryAckStore struct {
	called bool
	ack    store.DeliveryOutboxClientAck
	err    error
}

func (s *captureDeliveryAckStore) MarkClientAcked(_ context.Context, ack store.DeliveryOutboxClientAck) error {
	s.called = true
	s.ack = ack
	return s.err
}
