package egress

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	"telesrv/internal/egress/egresspb"
	"telesrv/internal/store"
)

const (
	defaultGRPCAckClientTimeout  = 5 * time.Second
	defaultMaxGRPCAckMessageSize = 1 << 20
	grpcAckProtocolVersion       = 1
	grpcAckMinSupportedVersion   = 1
	grpcAckResolverStatic        = "static"
	grpcAckResolverDNS           = "dns"
	staticGRPCAckResolverScheme  = "egressack-static"
	staticGRPCAckResolverTarget  = staticGRPCAckResolverScheme + ":///egressack"
	dnsGRPCAckResolverScheme     = "dns"
)

var (
	ErrGRPCAckAddrMissing      = errors.New("egress ack grpc: addr is empty")
	ErrGRPCAckTargetsMissing   = errors.New("egress ack grpc: targets are empty")
	ErrGRPCAckTokenMissing     = errors.New("egress ack grpc: bearer token is required")
	ErrGRPCAckUnavailable      = errors.New("egress ack grpc: unavailable")
	ErrGRPCAckProtocolMismatch = errors.New("egress ack grpc: protocol version mismatch")
	ErrGRPCAckInvalidDelivery  = errors.New("egress ack grpc: invalid delivery ack")
	ErrAckLeaseLost            = store.ErrDispatchLeaseLost
	grpcAckCapabilities        = []string{"durable-outbox-client-ack", "fenced-delivery-ref"}
)

// DeliveryAck is the Edge-observed client msg_ack for one fenced durable outbox
// delivery attempt.
type DeliveryAck struct {
	OutboxID     int64
	TargetUserID int64
	Pts          int
	Attempt      int
	AuthKeyID    [8]byte
	SessionID    int64
	ServerMsgID  int64
	AckedAt      time.Time
}

// AckSink is the Edge-facing boundary for reporting client ACKs to Durable Egress.
type AckSink interface {
	AckOutboxDelivery(context.Context, DeliveryAck) error
}

type GRPCAckServerConfig struct {
	Addr            string
	InstanceID      string
	Token           string
	TLSCertFile     string
	TLSKeyFile      string
	TLSClientCAFile string
	Store           store.DispatchOutboxClientAckStore
	DeliveryStore   store.DeliveryOutboxClientAckStore
	Logger          *zap.Logger
	MaxRecvMsgBytes int
	MaxSendMsgBytes int
}

type GRPCAckClientConfig struct {
	Targets         []string
	Resolver        GRPCAckResolverProvider
	ResolverKind    string
	Token           string
	Logger          *zap.Logger
	RequestTimeout  time.Duration
	TLSCAFile       string
	TLSServerName   string
	TLSCertFile     string
	TLSKeyFile      string
	MaxRecvMsgBytes int
	MaxSendMsgBytes int
	DialOptions     []grpc.DialOption
}

type GRPCAckRemote struct {
	client         egresspb.EgressAckServiceClient
	health         healthpb.HealthClient
	requestTimeout time.Duration
	log            *zap.Logger
}

type egressAckGRPCServer struct {
	egresspb.UnimplementedEgressAckServiceServer

	store         store.DispatchOutboxClientAckStore
	deliveryStore store.DeliveryOutboxClientAckStore
	instanceID    string
}

func StartGRPCAck(ctx context.Context, cfg GRPCAckServerConfig) (*grpc.Server, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, ErrGRPCAckAddrMissing
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, ErrGRPCAckTokenMissing
	}
	if cfg.Store == nil || cfg.DeliveryStore == nil {
		return nil, ErrMissingDependency
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	transportCreds, tlsEnabled, err := grpcAckServerTransportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen egress ack grpc %s: %w", cfg.Addr, err)
	}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(grpcAckBearerUnaryServerInterceptor(cfg.Token)),
		grpc.MaxRecvMsgSize(grpcAckMessageSize(cfg.MaxRecvMsgBytes)),
		grpc.MaxSendMsgSize(grpcAckMessageSize(cfg.MaxSendMsgBytes)),
	}
	if tlsEnabled {
		opts = append(opts, grpc.Creds(transportCreds))
	}
	srv := grpc.NewServer(opts...)
	egresspb.RegisterEgressAckServiceServer(srv, newEgressAckGRPCServer(cfg.Store, cfg.DeliveryStore, cfg.InstanceID))
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus(egresspb.EgressAckService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)
	go func() {
		<-ctx.Done()
		healthSrv.Shutdown()
		stopped := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			srv.Stop()
		}
	}()
	go func() {
		cfg.Logger.Info("egress ack grpc listening", zap.String("addr", ln.Addr().String()))
		if err := srv.Serve(ln); err != nil {
			cfg.Logger.Warn("egress ack grpc exited", zap.Error(err))
		}
	}()
	return srv, nil
}

func DialGRPCAckRemote(ctx context.Context, cfg GRPCAckClientConfig) (*GRPCAckRemote, *grpc.ClientConn, error) {
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, nil, ErrGRPCAckTokenMissing
	}
	resolverProvider, err := grpcAckResolverProvider(cfg)
	if err != nil {
		return nil, nil, err
	}
	target := strings.TrimSpace(resolverProvider.Target())
	if target == "" {
		return nil, nil, ErrGRPCAckTargetsMissing
	}
	opts, err := grpcAckClientDialOptions(cfg, resolverProvider)
	if err != nil {
		return nil, nil, err
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("egress ack grpc client: %w", err)
	}
	remote := &GRPCAckRemote{
		client:         egresspb.NewEgressAckServiceClient(conn),
		health:         healthpb.NewHealthClient(conn),
		requestTimeout: grpcAckClientRequestTimeout(cfg.RequestTimeout),
		log:            cfg.Logger,
	}
	if remote.log == nil {
		remote.log = zap.NewNop()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := remote.Check(ctx); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return remote, conn, nil
}

func (r *GRPCAckRemote) Check(ctx context.Context) error {
	if r == nil || r.client == nil {
		return ErrGRPCAckUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	if r.health != nil {
		res, err := r.health.Check(callCtx, &healthpb.HealthCheckRequest{
			Service: egresspb.EgressAckService_ServiceDesc.ServiceName,
		})
		if err != nil {
			return fmt.Errorf("egress ack grpc health: %w", err)
		}
		if res.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("egress ack grpc health: status %s", res.GetStatus().String())
		}
	}
	info, err := r.client.GetInfo(callCtx, &egresspb.EgressInfoRequest{
		ProtocolVersion:             grpcAckProtocolVersion,
		MinSupportedProtocolVersion: grpcAckMinSupportedVersion,
		Capabilities:                append([]string(nil), grpcAckCapabilities...),
	})
	if err != nil {
		return fmt.Errorf("egress ack grpc info: %w", err)
	}
	if info.GetError() != "" {
		return fmt.Errorf("%w: %s", ErrGRPCAckProtocolMismatch, info.GetError())
	}
	if !grpcAckProtocolRangesOverlap(
		grpcAckMinSupportedVersion,
		grpcAckProtocolVersion,
		info.GetMinSupportedProtocolVersion(),
		info.GetProtocolVersion(),
	) {
		return fmt.Errorf("%w: edge=%d..%d egress=%d..%d",
			ErrGRPCAckProtocolMismatch,
			grpcAckMinSupportedVersion,
			grpcAckProtocolVersion,
			info.GetMinSupportedProtocolVersion(),
			info.GetProtocolVersion(),
		)
	}
	return nil
}

func (r *GRPCAckRemote) AckOutboxDelivery(ctx context.Context, ack DeliveryAck) error {
	if r == nil || r.client == nil {
		return ErrGRPCAckUnavailable
	}
	req, err := grpcAckRequest(ack)
	if err != nil {
		return err
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.AckOutboxDelivery(callCtx, req)
	if err != nil {
		return fmt.Errorf("egress ack grpc ack_outbox_delivery: %w", err)
	}
	return grpcAckResponseError(res.GetError())
}

func newEgressAckGRPCServer(dispatchStore store.DispatchOutboxClientAckStore, deliveryStore store.DeliveryOutboxClientAckStore, instanceID string) *egressAckGRPCServer {
	return &egressAckGRPCServer{
		store:         dispatchStore,
		deliveryStore: deliveryStore,
		instanceID:    strings.TrimSpace(instanceID),
	}
}

func (s *egressAckGRPCServer) GetInfo(_ context.Context, req *egresspb.EgressInfoRequest) (*egresspb.EgressInfoResponse, error) {
	res := &egresspb.EgressInfoResponse{
		ProtocolVersion:             grpcAckProtocolVersion,
		MinSupportedProtocolVersion: grpcAckMinSupportedVersion,
		Capabilities:                append([]string(nil), grpcAckCapabilities...),
		Implementation:              "telesrv-egress-ack-grpc",
		InstanceId:                  s.instanceID,
	}
	if !grpcAckProtocolRangesOverlap(
		req.GetMinSupportedProtocolVersion(),
		req.GetProtocolVersion(),
		grpcAckMinSupportedVersion,
		grpcAckProtocolVersion,
	) {
		res.Error = fmt.Sprintf("incompatible protocol edge=%d..%d egress=%d..%d",
			req.GetMinSupportedProtocolVersion(),
			req.GetProtocolVersion(),
			grpcAckMinSupportedVersion,
			grpcAckProtocolVersion,
		)
	}
	return res, nil
}

func (s *egressAckGRPCServer) AckOutboxDelivery(ctx context.Context, req *egresspb.OutboxClientAckRequest) (*egresspb.ErrorResponse, error) {
	if s == nil || (s.store == nil && s.deliveryStore == nil) {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	ack, err := deliveryAckFromPB(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if ack.Pts == 0 {
		if s.deliveryStore == nil {
			return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
		}
		if err := s.deliveryStore.MarkClientAcked(ctx, store.DeliveryOutboxClientAck{
			OutboxID:     ack.OutboxID,
			TargetUserID: ack.TargetUserID,
			Attempt:      ack.Attempt,
			AuthKeyID:    ack.AuthKeyID,
			SessionID:    ack.SessionID,
			ServerMsgID:  ack.ServerMsgID,
			AckedAt:      ack.AckedAt,
		}); err != nil {
			return &egresspb.ErrorResponse{Error: err.Error()}, nil
		}
		return &egresspb.ErrorResponse{}, nil
	}
	if s.store == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	if err := s.store.MarkClientAcked(ctx, store.DispatchOutboxClientAck(ack)); err != nil {
		return &egresspb.ErrorResponse{Error: err.Error()}, nil
	}
	return &egresspb.ErrorResponse{}, nil
}

func grpcAckRequest(ack DeliveryAck) (*egresspb.OutboxClientAckRequest, error) {
	if ack.OutboxID <= 0 || ack.TargetUserID <= 0 || ack.Pts < 0 || ack.Attempt <= 0 ||
		ack.SessionID == 0 || ack.ServerMsgID <= 0 || ack.AckedAt.IsZero() {
		return nil, ErrGRPCAckInvalidDelivery
	}
	return &egresspb.OutboxClientAckRequest{
		OutboxId:        ack.OutboxID,
		TargetUserId:    ack.TargetUserID,
		Pts:             int32(ack.Pts),
		Attempt:         int32(ack.Attempt),
		AuthKeyId:       append([]byte(nil), ack.AuthKeyID[:]...),
		SessionId:       ack.SessionID,
		ServerMsgId:     ack.ServerMsgID,
		AckedAtUnixNano: ack.AckedAt.UnixNano(),
	}, nil
}

func dispatchOutboxClientAckFromPB(req *egresspb.OutboxClientAckRequest) (store.DispatchOutboxClientAck, error) {
	ack, err := deliveryAckFromPB(req)
	if err != nil {
		return store.DispatchOutboxClientAck{}, err
	}
	if ack.Pts <= 0 {
		return store.DispatchOutboxClientAck{}, ErrGRPCAckInvalidDelivery
	}
	return store.DispatchOutboxClientAck(ack), nil
}

func deliveryAckFromPB(req *egresspb.OutboxClientAckRequest) (store.DispatchOutboxClientAck, error) {
	if req == nil {
		return store.DispatchOutboxClientAck{}, ErrGRPCAckInvalidDelivery
	}
	authKeyID, err := grpcAckAuthKeyID(req.GetAuthKeyId())
	if err != nil {
		return store.DispatchOutboxClientAck{}, err
	}
	if req.GetOutboxId() <= 0 || req.GetTargetUserId() <= 0 || req.GetPts() < 0 || req.GetAttempt() <= 0 ||
		req.GetSessionId() == 0 || req.GetServerMsgId() <= 0 || req.GetAckedAtUnixNano() <= 0 {
		return store.DispatchOutboxClientAck{}, ErrGRPCAckInvalidDelivery
	}
	return store.DispatchOutboxClientAck{
		OutboxID:     req.GetOutboxId(),
		TargetUserID: req.GetTargetUserId(),
		Pts:          int(req.GetPts()),
		Attempt:      int(req.GetAttempt()),
		AuthKeyID:    authKeyID,
		SessionID:    req.GetSessionId(),
		ServerMsgID:  req.GetServerMsgId(),
		AckedAt:      time.Unix(0, req.GetAckedAtUnixNano()),
	}, nil
}

func grpcAckAuthKeyID(raw []byte) ([8]byte, error) {
	var out [8]byte
	if len(raw) != len(out) {
		return out, fmt.Errorf("%w: auth_key_id must be 8 bytes", ErrGRPCAckInvalidDelivery)
	}
	copy(out[:], raw)
	return out, nil
}

func grpcAckResponseError(message string) error {
	message = strings.TrimSpace(message)
	if message == "" {
		return nil
	}
	if message == store.ErrDispatchLeaseLost.Error() {
		return store.ErrDispatchLeaseLost
	}
	return errors.New(message)
}

func grpcAckProtocolRangesOverlap(aMin, aMax, bMin, bMax uint32) bool {
	if aMin == 0 || aMax == 0 || bMin == 0 || bMax == 0 {
		return false
	}
	return aMin <= bMax && bMin <= aMax
}

func grpcAckClientRequestTimeout(value time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return defaultGRPCAckClientTimeout
}

func (r *GRPCAckRemote) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	timeout := defaultGRPCAckClientTimeout
	if r != nil && r.requestTimeout > 0 {
		timeout = r.requestTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

func grpcAckMessageSize(configured int) int {
	if configured > 0 {
		return configured
	}
	return defaultMaxGRPCAckMessageSize
}

func grpcAckBearerUnaryServerInterceptor(token string) grpc.UnaryServerInterceptor {
	token = strings.TrimSpace(token)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token == "" {
			return nil, status.Error(codes.Unauthenticated, "bearer token is not configured")
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing authorization")
		}
		values := md.Get("authorization")
		expected := "Bearer " + token
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(expected)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid authorization")
		}
		return handler(ctx, req)
	}
}

func grpcAckBearerUnaryClientInterceptor(token string) grpc.UnaryClientInterceptor {
	token = strings.TrimSpace(token)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func grpcAckClientDialOptions(cfg GRPCAckClientConfig, resolverProvider GRPCAckResolverProvider) ([]grpc.DialOption, error) {
	transportCreds, err := grpcAckClientTransportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithChainUnaryInterceptor(grpcAckBearerUnaryClientInterceptor(cfg.Token)),
		grpc.WithDisableRetry(),
		grpc.WithDefaultServiceConfig(grpcAckDefaultServiceConfig()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(grpcAckMessageSize(cfg.MaxRecvMsgBytes)),
			grpc.MaxCallSendMsgSize(grpcAckMessageSize(cfg.MaxSendMsgBytes)),
		),
	}
	if resolverProvider != nil {
		opts = append(opts, resolverProvider.DialOptions()...)
	}
	opts = append(opts, cfg.DialOptions...)
	return opts, nil
}

func grpcAckDefaultServiceConfig() string {
	return fmt.Sprintf(`{"loadBalancingConfig":[{"round_robin":{}}],"healthCheckConfig":{"serviceName":%q}}`,
		egresspb.EgressAckService_ServiceDesc.ServiceName)
}

func grpcAckServerTransportCredentials(cfg GRPCAckServerConfig) (credentials.TransportCredentials, bool, error) {
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	clientCAFile := strings.TrimSpace(cfg.TLSClientCAFile)
	if certFile == "" && keyFile == "" && clientCAFile == "" {
		return nil, false, nil
	}
	if (certFile == "") != (keyFile == "") {
		return nil, false, fmt.Errorf("egress ack grpc tls: server cert and key must be configured together")
	}
	if certFile == "" {
		return nil, false, fmt.Errorf("egress ack grpc tls: server cert and key are required when client CA is configured")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, false, fmt.Errorf("egress ack grpc tls: load server certificate: %w", err)
	}
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if clientCAFile != "" {
		pool, err := grpcAckLoadCertPool(clientCAFile)
		if err != nil {
			return nil, false, fmt.Errorf("egress ack grpc tls: load client CA: %w", err)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsCfg), true, nil
}

func grpcAckClientTransportCredentials(cfg GRPCAckClientConfig) (credentials.TransportCredentials, error) {
	caFile := strings.TrimSpace(cfg.TLSCAFile)
	serverName := strings.TrimSpace(cfg.TLSServerName)
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	if caFile == "" && serverName == "" && certFile == "" && keyFile == "" {
		return insecure.NewCredentials(), nil
	}
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("egress ack grpc tls: client cert and key must be configured together")
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
	}
	if caFile != "" {
		pool, err := grpcAckLoadCertPool(caFile)
		if err != nil {
			return nil, fmt.Errorf("egress ack grpc tls: load root CA: %w", err)
		}
		tlsCfg.RootCAs = pool
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("egress ack grpc tls: load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsCfg), nil
}

func grpcAckLoadCertPool(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("no PEM certificates found in %s", path)
	}
	return pool, nil
}

type GRPCAckResolverProvider interface {
	Target() string
	DialOptions() []grpc.DialOption
}

type StaticGRPCAckResolverProvider struct {
	mu        sync.RWMutex
	targets   []string
	resolvers map[*manual.Resolver]struct{}
}

type DNSGRPCAckResolverProvider struct {
	target string
}

func ParseGRPCAckTargetsForResolver(raw, resolverKind string) ([]string, error) {
	switch strings.ToLower(strings.TrimSpace(resolverKind)) {
	case "", grpcAckResolverStatic:
		return parseGRPCAckTargets(raw)
	case grpcAckResolverDNS:
		return parseDNSGRPCAckTargets(raw)
	default:
		return nil, fmt.Errorf("egress ack grpc: unsupported resolver provider %q", resolverKind)
	}
}

func parseGRPCAckTargets(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, ErrGRPCAckTargetsMissing
	}
	parts := strings.Split(raw, ",")
	targets := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		target := strings.TrimSpace(part)
		if target == "" {
			continue
		}
		if err := validateGRPCAckTarget(target); err != nil {
			return nil, err
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return nil, ErrGRPCAckTargetsMissing
	}
	return targets, nil
}

func parseDNSGRPCAckTargets(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, ErrGRPCAckTargetsMissing
	}
	parts := strings.Split(raw, ",")
	targets := make([]string, 0, len(parts))
	for _, part := range parts {
		target := strings.TrimSpace(part)
		if target != "" {
			targets = append(targets, target)
		}
	}
	if len(targets) == 0 {
		return nil, ErrGRPCAckTargetsMissing
	}
	if len(targets) != 1 {
		return nil, fmt.Errorf("egress ack grpc dns resolver requires exactly one target, got %d", len(targets))
	}
	if err := validateGRPCAckDNSTarget(targets[0]); err != nil {
		return nil, err
	}
	return targets, nil
}

func NewStaticGRPCAckResolverProvider(targets []string) (*StaticGRPCAckResolverProvider, error) {
	targets, err := normalizeGRPCAckTargets(targets)
	if err != nil {
		return nil, err
	}
	return &StaticGRPCAckResolverProvider{
		targets:   targets,
		resolvers: make(map[*manual.Resolver]struct{}),
	}, nil
}

func (p *StaticGRPCAckResolverProvider) Target() string {
	if p == nil {
		return ""
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.targets) == 0 {
		return ""
	}
	return staticGRPCAckResolverTarget
}

func (p *StaticGRPCAckResolverProvider) DialOptions() []grpc.DialOption {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.targets) == 0 {
		return nil
	}
	r := manual.NewBuilderWithScheme(staticGRPCAckResolverScheme)
	r.InitialState(grpcAckResolverState(p.targets))
	r.CloseCallback = func() {
		p.removeResolver(r)
	}
	p.resolvers[r] = struct{}{}
	return []grpc.DialOption{grpc.WithResolvers(r)}
}

func (p *StaticGRPCAckResolverProvider) removeResolver(r *manual.Resolver) {
	if p == nil || r == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.resolvers, r)
}

func NewDNSGRPCAckResolverProviderFromTargets(targets []string) (*DNSGRPCAckResolverProvider, error) {
	targets, err := parseDNSGRPCAckTargets(strings.Join(targets, ","))
	if err != nil {
		return nil, err
	}
	return &DNSGRPCAckResolverProvider{target: targets[0]}, nil
}

func (p *DNSGRPCAckResolverProvider) Target() string {
	if p == nil || p.target == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(p.target)), dnsGRPCAckResolverScheme+"://") {
		return p.target
	}
	return dnsGRPCAckResolverScheme + ":///" + p.target
}

func (p *DNSGRPCAckResolverProvider) DialOptions() []grpc.DialOption {
	return nil
}

func grpcAckResolverProvider(cfg GRPCAckClientConfig) (GRPCAckResolverProvider, error) {
	if cfg.Resolver != nil {
		return cfg.Resolver, nil
	}
	switch strings.ToLower(strings.TrimSpace(cfg.ResolverKind)) {
	case "", grpcAckResolverStatic:
		return NewStaticGRPCAckResolverProvider(cfg.Targets)
	case grpcAckResolverDNS:
		return NewDNSGRPCAckResolverProviderFromTargets(cfg.Targets)
	default:
		return nil, fmt.Errorf("egress ack grpc: unsupported resolver provider %q", cfg.ResolverKind)
	}
}

func normalizeGRPCAckTargets(targets []string) ([]string, error) {
	if len(targets) == 0 {
		return nil, ErrGRPCAckTargetsMissing
	}
	out := make([]string, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, raw := range targets {
		target := strings.TrimSpace(raw)
		if target == "" {
			continue
		}
		if err := validateGRPCAckTarget(target); err != nil {
			return nil, err
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	if len(out) == 0 {
		return nil, ErrGRPCAckTargetsMissing
	}
	return out, nil
}

func validateGRPCAckTarget(target string) error {
	if target == "" {
		return fmt.Errorf("empty egress ack target")
	}
	if err := validateGRPCAckHostPort(target); err != nil {
		return fmt.Errorf("egress ack grpc target must be host:port %q: %w", target, err)
	}
	return nil
}

func validateGRPCAckDNSTarget(target string) error {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(target)), dnsGRPCAckResolverScheme+"://") {
		return validateGRPCAckDNSTargetURI(target)
	}
	return validateGRPCAckTarget(target)
}

func validateGRPCAckDNSTargetURI(target string) error {
	u, err := url.Parse(target)
	if err != nil {
		return fmt.Errorf("invalid egress ack grpc dns target URI %q: %w", target, err)
	}
	if !strings.EqualFold(u.Scheme, dnsGRPCAckResolverScheme) {
		return fmt.Errorf("egress ack grpc dns target URI must use dns scheme %q", target)
	}
	if u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("egress ack grpc dns target URI must not include userinfo, opaque data, query, or fragment %q", target)
	}
	if strings.ContainsAny(target, " \t\r\n") {
		return fmt.Errorf("invalid whitespace in egress ack grpc dns target URI %q", target)
	}
	if strings.TrimSpace(u.Host) != "" {
		if err := validateGRPCAckHostPort(u.Host); err != nil {
			return fmt.Errorf("egress ack grpc dns target authority must be host:port %q: %w", target, err)
		}
	}
	endpoint := strings.TrimPrefix(u.Path, "/")
	if endpoint == "" || strings.Contains(endpoint, "/") {
		return fmt.Errorf("egress ack grpc dns target URI must contain exactly one endpoint path %q", target)
	}
	if err := validateGRPCAckTarget(endpoint); err != nil {
		return fmt.Errorf("egress ack grpc dns target endpoint invalid %q: %w", target, err)
	}
	return nil
}

func validateGRPCAckHostPort(target string) error {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	if strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return fmt.Errorf("invalid host:port target %q", target)
	}
	if strings.ContainsAny(host+port, " \t\r\n") {
		return fmt.Errorf("invalid whitespace in host:port target %q", target)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("invalid numeric port in target %q: %w", target, err)
	}
	if portNumber <= 0 || portNumber > 65535 {
		return fmt.Errorf("port out of range in target %q", target)
	}
	return nil
}

func grpcAckResolverState(targets []string) resolver.State {
	addresses := make([]resolver.Address, 0, len(targets))
	for _, target := range targets {
		addresses = append(addresses, resolver.Address{Addr: target})
	}
	return resolver.State{Addresses: addresses}
}
