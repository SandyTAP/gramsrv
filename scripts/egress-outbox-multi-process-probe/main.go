package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iamxvbaba/td/tg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/edgecontrol/redisregistry"
	"telesrv/internal/egress"
	"telesrv/internal/store/postgres"
	"telesrv/internal/store/redisstore"
)

const (
	defaultPostgresDSN      = "postgres://telesrv:telesrv@127.0.0.1:5432/telesrv?sslmode=disable"
	defaultRedisAddr        = "127.0.0.1:6399"
	defaultRedisBusPrefix   = "edge:control"
	defaultProbeUserCount   = 16
	defaultEventsPerUser    = 2
	defaultProbeTimeout     = 30 * time.Second
	defaultLocationTTL      = 30 * time.Second
	defaultClientAckDelay   = 25 * time.Millisecond
	defaultGRPCRequestDelay = 2 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run() error {
	postgresDSN := flag.String("postgres-dsn", defaultProbePostgresDSN(), "PostgreSQL DSN; defaults to TELESRV_TEST_POSTGRES_DSN, TELESRV_POSTGRES_DSN, then the local development DSN")
	redisAddr := flag.String("redis-addr", defaultProbeRedisAddr(), "Redis address; defaults to TELESRV_REDIS_ADDR then the local development Redis")
	redisPassword := flag.String("redis-password", os.Getenv("TELESRV_REDIS_PASSWORD"), "Redis password; defaults to TELESRV_REDIS_PASSWORD")
	redisDB := flag.Int("redis-db", envInt("TELESRV_REDIS_DB", 0), "Redis DB; defaults to TELESRV_REDIS_DB")
	redisBusPrefix := flag.String("redis-bus-prefix", defaultRedisBusPrefix, "Redis outbox command bus prefix")
	egressAckTargetsRaw := flag.String("egress-ack-targets", defaultEgressAckTargets(), "Egress ACK gRPC targets")
	egressAckResolver := flag.String("egress-ack-resolver", envOr("TELESRV_EGRESS_ACK_GRPC_RESOLVER", "static"), "Egress ACK gRPC resolver: static or dns")
	egressAckToken := flag.String("egress-ack-token", os.Getenv("TELESRV_EGRESS_ACK_TOKEN"), "Egress ACK bearer token")
	users := flag.Int("users", defaultProbeUserCount, "probe users to create; each user owns an independent durable outbox lane")
	eventsPerUser := flag.Int("events-per-user", defaultEventsPerUser, "durable update events appended per probe user")
	expectSourceInstances := flag.Int("expect-source-instances", 1, "minimum distinct Egress SourceInstanceID values expected in Redis outbox push commands")
	timeout := flag.Duration("timeout", defaultProbeTimeout, "overall probe timeout")
	locationTTL := flag.Duration("location-ttl", defaultLocationTTL, "fake Edge location TTL")
	commandAckDelay := flag.Duration("command-ack-delay", 0, "delay before the fake Edge confirms Redis outbox command enqueue")
	clientAckDelay := flag.Duration("client-ack-delay", defaultClientAckDelay, "delay after Redis enqueue ACK before reporting late client ACK over Egress ACK gRPC")
	crashRecovery := flag.Bool("crash-recovery", false, "run one-event indeterminate dispatching crash/reclaim flow; first delivery is not service-ACKed until the smoke script kills the source Egress")
	crashSignalFile := flag.String("crash-signal-file", "", "coordination JSON path written after first delivery stays dispatching; required with -crash-recovery")
	skipMigrate := flag.Bool("skip-migrate", false, "skip embedded PostgreSQL migrations before probing")
	tlsCAFile := flag.String("tls-ca-file", os.Getenv("TELESRV_EGRESS_ACK_GRPC_TLS_CA_FILE"), "Egress ACK gRPC root CA file")
	tlsServerName := flag.String("tls-server-name", os.Getenv("TELESRV_EGRESS_ACK_GRPC_TLS_SERVER_NAME"), "Egress ACK gRPC TLS server name")
	tlsClientCertFile := flag.String("tls-client-cert-file", os.Getenv("TELESRV_EGRESS_ACK_GRPC_TLS_CLIENT_CERT_FILE"), "Egress ACK gRPC client certificate file")
	tlsClientKeyFile := flag.String("tls-client-key-file", os.Getenv("TELESRV_EGRESS_ACK_GRPC_TLS_CLIENT_KEY_FILE"), "Egress ACK gRPC client key file")
	flag.Parse()

	if strings.TrimSpace(*postgresDSN) == "" {
		return fmt.Errorf("-postgres-dsn is required")
	}
	if strings.TrimSpace(*redisAddr) == "" {
		return fmt.Errorf("-redis-addr is required")
	}
	if strings.TrimSpace(*egressAckTargetsRaw) == "" {
		return fmt.Errorf("-egress-ack-targets is required")
	}
	if strings.TrimSpace(*egressAckToken) == "" {
		return fmt.Errorf("-egress-ack-token is required")
	}
	if *users <= 0 {
		return fmt.Errorf("-users must be positive")
	}
	if *eventsPerUser <= 0 {
		return fmt.Errorf("-events-per-user must be positive")
	}
	if *expectSourceInstances <= 0 {
		return fmt.Errorf("-expect-source-instances must be positive")
	}
	if !*crashRecovery && *expectSourceInstances > *users {
		return fmt.Errorf("-expect-source-instances cannot exceed -users")
	}
	if *timeout <= 0 {
		return fmt.Errorf("-timeout must be positive")
	}
	if *locationTTL <= 0 {
		return fmt.Errorf("-location-ttl must be positive")
	}
	if *commandAckDelay < 0 {
		return fmt.Errorf("-command-ack-delay must not be negative")
	}
	if *clientAckDelay < 0 {
		return fmt.Errorf("-client-ack-delay must not be negative")
	}
	if *crashRecovery {
		if strings.TrimSpace(*crashSignalFile) == "" {
			return fmt.Errorf("-crash-signal-file is required with -crash-recovery")
		}
		if *users != 1 || *eventsPerUser != 1 {
			return fmt.Errorf("-crash-recovery requires -users 1 and -events-per-user 1")
		}
		if *expectSourceInstances < 2 {
			return fmt.Errorf("-crash-recovery requires -expect-source-instances >= 2")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	migration := "skipped"
	if !*skipMigrate {
		status, err := postgres.MigrateAndStatus(*postgresDSN)
		if err != nil {
			return fmt.Errorf("migrate postgres: %w", err)
		}
		migration = fmt.Sprintf("version=%d dirty=%t empty=%t", status.Version, status.Dirty, status.Empty)
	}

	pool, err := postgres.Open(ctx, *postgresDSN, postgres.WithMaxConns(8), postgres.WithMinConns(1))
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	rdb, err := redisstore.Open(ctx, *redisAddr, *redisPassword, *redisDB)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	ackTargets, err := egress.ParseGRPCAckTargetsForResolver(*egressAckTargetsRaw, *egressAckResolver)
	if err != nil {
		return fmt.Errorf("parse egress ack targets: %w", err)
	}
	ackRemote, ackConn, err := egress.DialGRPCAckRemote(ctx, egress.GRPCAckClientConfig{
		Targets:        ackTargets,
		ResolverKind:   strings.TrimSpace(*egressAckResolver),
		Token:          *egressAckToken,
		RequestTimeout: defaultGRPCRequestDelay,
		TLSCAFile:      *tlsCAFile,
		TLSServerName:  *tlsServerName,
		TLSCertFile:    *tlsClientCertFile,
		TLSKeyFile:     *tlsClientKeyFile,
	})
	if err != nil {
		return fmt.Errorf("dial egress ack grpc: %w", err)
	}
	defer func() { _ = ackConn.Close() }()

	edgeInstanceID := fmt.Sprintf("probe-edge-%d-%d", os.Getpid(), time.Now().UnixNano())
	probeUsers, err := createProbeUsers(ctx, pool, *users)
	if err != nil {
		return err
	}
	userIDs := userIDs(probeUsers)
	defer cleanupProbeUsers(pool, userIDs)

	registry := redisregistry.New(rdb)
	locationLeaseID := fmt.Sprintf("probe-lease-%d-%d", os.Getpid(), time.Now().UnixNano())
	if err := registry.AcquireInstanceLease(ctx, edgeInstanceID, locationLeaseID, *locationTTL); err != nil {
		return fmt.Errorf("acquire fake edge location lease: %w", err)
	}
	locationByUser := make(map[int64]edgecontrol.LocationRecord, len(probeUsers))
	mutations := make([]edgecontrol.LocationMutation, 0, len(probeUsers))
	for i, user := range probeUsers {
		record := probeLocation(edgeInstanceID, user.ID, i)
		locationByUser[user.ID] = record
		mutations = append(mutations, edgecontrol.LocationMutation{Record: record})
	}
	if err := registry.ApplyLocationMutations(ctx, edgeInstanceID, locationLeaseID, mutations); err != nil {
		return fmt.Errorf("publish fake edge locations: %w", err)
	}
	defer cleanupProbeLocations(registry, edgeInstanceID, locationLeaseID)

	var crash *crashCoordinator
	if *crashRecovery {
		crash = newCrashCoordinator(pool, *crashSignalFile, *clientAckDelay)
	}
	recorder := newCommandRecorder()
	subCtx, stopSubscriber := context.WithCancel(ctx)
	defer stopSubscriber()
	subscriberReady := make(chan error, 1)
	subscriberErr := make(chan error, 1)
	go func() {
		subscriberErr <- runFakeEdgeSubscriber(subCtx, rdb, *redisBusPrefix, edgeInstanceID, locationByUser, recorder, ackRemote, *commandAckDelay, *clientAckDelay, crash, subscriberReady)
	}()
	if err := waitSubscriberReady(ctx, subscriberReady); err != nil {
		return err
	}

	expectedEvents, err := appendProbeEvents(ctx, pool, probeUsers, *eventsPerUser)
	if err != nil {
		return err
	}
	expectedCount := len(expectedEvents)
	if expectedCount == 0 {
		return fmt.Errorf("no probe events appended")
	}

	if *crashRecovery {
		if err := waitForCrashRecoveryCommands(ctx, recorder, subscriberErr); err != nil {
			return err
		}
		records := recorder.snapshot()
		if err := validateCrashRecoveryRecords(records, expectedEvents[0], *expectSourceInstances); err != nil {
			return err
		}
		if err := waitOutboxEmpty(ctx, pool, userIDs); err != nil {
			return err
		}
		records = recorder.snapshot()
		if err := validateCrashRecoveryRecords(records, expectedEvents[0], *expectSourceInstances); err != nil {
			return err
		}
		fmt.Printf("egress outbox process crash recovery probe ok: user_id=%d pts=%d outbox_id=%d first_source=%s recovery_source=%s fake_edge=%s ack_targets=%s migration=%s\n",
			expectedEvents[0].userID,
			expectedEvents[0].pts,
			records[0].cmd.DeliveryRef.OutboxID,
			records[0].cmd.SourceInstanceID,
			records[1].cmd.SourceInstanceID,
			edgeInstanceID,
			strings.Join(ackTargets, ","),
			migration,
		)
		return nil
	}

	if err := waitForRecordedCommands(ctx, recorder, subscriberErr, expectedCount, *expectSourceInstances); err != nil {
		return err
	}
	records := recorder.snapshot()
	if err := validateRecordedCommands(records, expectedEvents, *expectSourceInstances); err != nil {
		return err
	}
	if err := waitOutboxEmpty(ctx, pool, userIDs); err != nil {
		return err
	}

	records = recorder.snapshot()
	if err := validateRecordedCommands(records, expectedEvents, *expectSourceInstances); err != nil {
		return err
	}
	sources := sortedSources(records)
	fmt.Printf("egress outbox multi-process probe ok: users=%d events=%d source_instances=%s fake_edge=%s ack_targets=%s migration=%s\n",
		len(probeUsers),
		expectedCount,
		strings.Join(sources, ","),
		edgeInstanceID,
		strings.Join(ackTargets, ","),
		migration,
	)
	return nil
}

type probeEvent struct {
	userID int64
	pts    int
}

func defaultProbePostgresDSN() string {
	if dsn := strings.TrimSpace(os.Getenv("TELESRV_TEST_POSTGRES_DSN")); dsn != "" {
		return dsn
	}
	if dsn := strings.TrimSpace(os.Getenv("TELESRV_POSTGRES_DSN")); dsn != "" {
		return dsn
	}
	return defaultPostgresDSN
}

func defaultProbeRedisAddr() string {
	if addr := strings.TrimSpace(os.Getenv("TELESRV_REDIS_ADDR")); addr != "" {
		return addr
	}
	return defaultRedisAddr
}

func defaultEgressAckTargets() string {
	if targets := strings.TrimSpace(os.Getenv("TELESRV_EGRESS_ACK_GRPC_TARGETS")); targets != "" {
		return targets
	}
	return strings.TrimSpace(os.Getenv("TELESRV_EGRESS_ACK_GRPC_ADDR"))
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func createProbeUsers(ctx context.Context, pool *pgxpool.Pool, count int) ([]domain.User, error) {
	users := postgres.NewUserStore(pool)
	out := make([]domain.User, 0, count)
	stamp := time.Now().UnixNano() % 1_000_000_000_000
	for i := 0; i < count; i++ {
		user, err := users.Create(ctx, domain.User{
			AccessHash: int64(stamp*100 + int64(i) + 1),
			Phone:      fmt.Sprintf("+190092%012d%02d", stamp, i),
			FirstName:  "EgressMulti",
		})
		if err != nil {
			return nil, fmt.Errorf("create probe user %d: %w", i, err)
		}
		out = append(out, user)
	}
	return out, nil
}

func userIDs(users []domain.User) []int64 {
	ids := make([]int64, len(users))
	for i, user := range users {
		ids[i] = user.ID
	}
	return ids
}

func cleanupProbeUsers(pool *pgxpool.Pool, ids []int64) {
	if len(ids) == 0 || pool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = ANY($1::bigint[])`, ids)
}

func cleanupProbeLocations(registry *redisregistry.Registry, instanceID, leaseID string) {
	if registry == nil || instanceID == "" || leaseID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = registry.ReleaseInstanceLease(ctx, instanceID, leaseID)
}

func probeLocation(instanceID string, userID int64, index int) edgecontrol.LocationRecord {
	return edgecontrol.LocationRecord{
		InstanceID:      instanceID,
		UserID:          userID,
		RawAuthKeyID:    probeAuthKeyID(userID, index),
		SessionID:       900_000_000 + int64(index+1),
		ReceivesUpdates: true,
		Layer:           tg.Layer,
	}
}

func probeAuthKeyID(userID int64, index int) [8]byte {
	var out [8]byte
	value := uint64(userID)<<16 | uint64(index+1)
	for i := 7; i >= 0; i-- {
		out[i] = byte(value)
		value >>= 8
	}
	if out == ([8]byte{}) {
		out[7] = byte(index + 1)
	}
	return out
}

func appendProbeEvents(ctx context.Context, pool *pgxpool.Pool, users []domain.User, eventsPerUser int) ([]probeEvent, error) {
	events := postgres.NewUpdateEventStore(pool)
	out := make([]probeEvent, 0, len(users)*eventsPerUser)
	now := int(time.Now().Unix())
	for _, user := range users {
		for eventIndex := 0; eventIndex < eventsPerUser; eventIndex++ {
			event, err := events.AppendAllocatedWithDispatch(ctx, user.ID, domain.UpdateEvent{
				Type:             domain.UpdateEventReadHistoryInbox,
				PtsCount:         1,
				Date:             now,
				Peer:             domain.Peer{Type: domain.PeerTypeUser, ID: user.ID},
				MaxID:            eventIndex + 1,
				StillUnreadCount: 0,
			}, [8]byte{}, 0)
			if err != nil {
				return nil, fmt.Errorf("append probe event user_id=%d event=%d: %w", user.ID, eventIndex, err)
			}
			out = append(out, probeEvent{userID: user.ID, pts: event.Pts})
		}
	}
	return out, nil
}

type recordedCommand struct {
	cmd        edgecontrol.OutboxPushCommand
	receivedAt time.Time
	err        string
}

type commandRecorder struct {
	mu      sync.Mutex
	records []recordedCommand
}

func newCommandRecorder() *commandRecorder {
	return &commandRecorder{}
}

func (r *commandRecorder) add(cmd edgecontrol.OutboxPushCommand, err error) {
	entry := recordedCommand{cmd: cmd, receivedAt: time.Now()}
	if err != nil {
		entry.err = err.Error()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, entry)
}

func (r *commandRecorder) snapshot() []recordedCommand {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedCommand, len(r.records))
	copy(out, r.records)
	return out
}

func runFakeEdgeSubscriber(
	ctx context.Context,
	rdb *redis.Client,
	prefix string,
	instanceID string,
	locationByUser map[int64]edgecontrol.LocationRecord,
	recorder *commandRecorder,
	ackRemote egress.AckSink,
	commandAckDelay time.Duration,
	clientAckDelay time.Duration,
	crash *crashCoordinator,
	ready chan<- error,
) error {
	if rdb == nil {
		return fmt.Errorf("redis client is nil")
	}
	prefix = strings.TrimRight(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		prefix = defaultRedisBusPrefix
	}
	pubsub := rdb.Subscribe(ctx, outboxCommandChannel(prefix, instanceID))
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil {
		select {
		case ready <- fmt.Errorf("subscribe fake edge outbox channel: %w", err):
		default:
		}
		return err
	}
	select {
	case ready <- nil:
	default:
	}
	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return fmt.Errorf("fake edge outbox subscription closed")
			}
			var cmd edgecontrol.OutboxPushCommand
			if err := json.Unmarshal([]byte(msg.Payload), &cmd); err != nil {
				continue
			}
			err := handleFakeEdgeCommand(ctx, rdb, prefix, instanceID, locationByUser, ackRemote, commandAckDelay, clientAckDelay, crash, cmd)
			recorder.add(cmd, err)
		}
	}
}

func handleFakeEdgeCommand(
	ctx context.Context,
	rdb *redis.Client,
	prefix string,
	instanceID string,
	locationByUser map[int64]edgecontrol.LocationRecord,
	ackRemote egress.AckSink,
	commandAckDelay time.Duration,
	clientAckDelay time.Duration,
	crash *crashCoordinator,
	cmd edgecontrol.OutboxPushCommand,
) error {
	if cmd.TargetInstanceID != instanceID {
		return fmt.Errorf("command target instance=%q, want %q", cmd.TargetInstanceID, instanceID)
	}
	if strings.TrimSpace(cmd.SourceInstanceID) == "" {
		return fmt.Errorf("command source instance is empty")
	}
	if strings.TrimSpace(cmd.CommandID) == "" {
		return fmt.Errorf("command id is empty")
	}
	if cmd.DeliveryRef.Empty() {
		return fmt.Errorf("command delivery ref is empty")
	}
	if cmd.DeliveryRef.TargetUserID != cmd.TargetUserID {
		return fmt.Errorf("command delivery ref target_user_id=%d, want %d", cmd.DeliveryRef.TargetUserID, cmd.TargetUserID)
	}
	if cmd.DeliveryRef.Attempt <= 0 || cmd.DeliveryRef.Pts <= 0 || cmd.DeliveryRef.OutboxID <= 0 {
		return fmt.Errorf("command delivery ref is incomplete: %+v", cmd.DeliveryRef)
	}
	record, ok := locationByUser[cmd.TargetUserID]
	if !ok {
		return fmt.Errorf("command targets unregistered probe user_id=%d", cmd.TargetUserID)
	}
	if _, err := edgecontrol.DecodeOutboxUpdate(cmd.UpdateBytes); err != nil {
		return fmt.Errorf("decode command update bytes: %w", err)
	}
	if err := sleepContext(ctx, commandAckDelay); err != nil {
		return err
	}
	if crash != nil {
		return crash.handle(ctx, rdb, prefix, cmd, record, ackRemote)
	}
	if err := publishFabricAck(ctx, rdb, prefix, cmd, edgecontrol.OutboxPushAck{
		CommandID:        cmd.CommandID,
		SourceInstanceID: cmd.SourceInstanceID,
		TargetInstanceID: cmd.TargetInstanceID,
		DeliveryRef:      cmd.DeliveryRef,
		Sent:             1,
		Status:           edgecontrol.OutboxDeliveryDelivered,
	}); err != nil {
		return err
	}
	if err := sleepContext(ctx, clientAckDelay); err != nil {
		return err
	}
	if ackRemote == nil {
		return fmt.Errorf("egress ack sink is nil")
	}
	return ackRemote.AckOutboxDelivery(ctx, egress.DeliveryAck{
		OutboxID:     cmd.DeliveryRef.OutboxID,
		TargetUserID: cmd.DeliveryRef.TargetUserID,
		Pts:          cmd.DeliveryRef.Pts,
		Attempt:      cmd.DeliveryRef.Attempt,
		AuthKeyID:    record.RawAuthKeyID,
		SessionID:    record.SessionID,
		ServerMsgID:  cmd.DeliveryRef.OutboxID*10_000 + int64(cmd.DeliveryRef.Attempt),
		AckedAt:      time.Now(),
	})
}

type crashCoordinator struct {
	pool           *pgxpool.Pool
	signalFile     string
	clientAckDelay time.Duration

	mu          sync.Mutex
	firstSource string
	firstRef    edgecontrol.OutboxDeliveryRef
	signaled    bool
}

type crashSignal struct {
	Status           string `json:"status"`
	SourceInstanceID string `json:"source_instance_id"`
	TargetUserID     int64  `json:"target_user_id"`
	OutboxID         int64  `json:"outbox_id"`
	Pts              int    `json:"pts"`
	Attempt          int    `json:"attempt"`
	WrittenAtUnixNS  int64  `json:"written_at_unix_ns"`
}

func newCrashCoordinator(pool *pgxpool.Pool, signalFile string, clientAckDelay time.Duration) *crashCoordinator {
	signalFile = strings.TrimSpace(signalFile)
	if signalFile != "" {
		_ = os.Remove(signalFile)
	}
	return &crashCoordinator{
		pool:           pool,
		signalFile:     signalFile,
		clientAckDelay: clientAckDelay,
	}
}

func (c *crashCoordinator) handle(ctx context.Context, rdb *redis.Client, prefix string, cmd edgecontrol.OutboxPushCommand, record edgecontrol.LocationRecord, ackRemote egress.AckSink) error {
	switch cmd.DeliveryRef.Attempt {
	case 1:
		return c.handleFirstAttempt(ctx, rdb, prefix, cmd)
	case 2:
		return c.handleRecoveryAttempt(ctx, rdb, prefix, cmd, record, ackRemote)
	default:
		return fmt.Errorf("crash recovery expected attempts 1 and 2, got %d for ref %+v", cmd.DeliveryRef.Attempt, cmd.DeliveryRef)
	}
}

func (c *crashCoordinator) handleFirstAttempt(ctx context.Context, rdb *redis.Client, prefix string, cmd edgecontrol.OutboxPushCommand) error {
	c.mu.Lock()
	if c.signaled {
		c.mu.Unlock()
		return fmt.Errorf("duplicate first crash attempt after signal: source=%s ref=%+v", cmd.SourceInstanceID, cmd.DeliveryRef)
	}
	c.firstSource = cmd.SourceInstanceID
	c.firstRef = cmd.DeliveryRef
	c.signaled = true
	c.mu.Unlock()

	if _, err := waitOutboxState(ctx, c.pool, cmd.DeliveryRef.TargetUserID, cmd.DeliveryRef.Pts, "dispatching", 1); err != nil {
		return err
	}
	return c.writeSignal(crashSignal{
		Status:           "dispatching",
		SourceInstanceID: cmd.SourceInstanceID,
		TargetUserID:     cmd.DeliveryRef.TargetUserID,
		OutboxID:         cmd.DeliveryRef.OutboxID,
		Pts:              cmd.DeliveryRef.Pts,
		Attempt:          cmd.DeliveryRef.Attempt,
		WrittenAtUnixNS:  time.Now().UnixNano(),
	})
}

func (c *crashCoordinator) handleRecoveryAttempt(ctx context.Context, rdb *redis.Client, prefix string, cmd edgecontrol.OutboxPushCommand, record edgecontrol.LocationRecord, ackRemote egress.AckSink) error {
	c.mu.Lock()
	firstSource := c.firstSource
	firstRef := c.firstRef
	signaled := c.signaled
	c.mu.Unlock()
	if !signaled {
		return fmt.Errorf("recovery attempt arrived before first dispatching signal")
	}
	if firstSource == cmd.SourceInstanceID {
		return fmt.Errorf("recovery attempt came from crashed source %s", cmd.SourceInstanceID)
	}
	if firstRef.OutboxID != cmd.DeliveryRef.OutboxID ||
		firstRef.TargetUserID != cmd.DeliveryRef.TargetUserID ||
		firstRef.Pts != cmd.DeliveryRef.Pts ||
		cmd.DeliveryRef.Attempt != firstRef.Attempt+1 {
		return fmt.Errorf("recovery ref=%+v, want same outbox/target/pts and next attempt after %+v", cmd.DeliveryRef, firstRef)
	}
	if err := publishFabricAck(ctx, rdb, prefix, cmd, edgecontrol.OutboxPushAck{
		CommandID:        cmd.CommandID,
		SourceInstanceID: cmd.SourceInstanceID,
		TargetInstanceID: cmd.TargetInstanceID,
		DeliveryRef:      cmd.DeliveryRef,
		Sent:             1,
		Status:           edgecontrol.OutboxDeliveryDelivered,
	}); err != nil {
		return err
	}
	if err := sleepContext(ctx, c.clientAckDelay); err != nil {
		return err
	}
	return ackOutboxDeliveryWithRetry(ctx, ackRemote, egress.DeliveryAck{
		OutboxID:     cmd.DeliveryRef.OutboxID,
		TargetUserID: cmd.DeliveryRef.TargetUserID,
		Pts:          cmd.DeliveryRef.Pts,
		Attempt:      cmd.DeliveryRef.Attempt,
		AuthKeyID:    record.RawAuthKeyID,
		SessionID:    record.SessionID,
		ServerMsgID:  cmd.DeliveryRef.OutboxID*10_000 + int64(cmd.DeliveryRef.Attempt),
		AckedAt:      time.Now(),
	})
}

func (c *crashCoordinator) writeSignal(signal crashSignal) error {
	if c == nil || strings.TrimSpace(c.signalFile) == "" {
		return fmt.Errorf("crash signal file is not configured")
	}
	dir := filepath.Dir(c.signalFile)
	if dir != "." && strings.TrimSpace(dir) != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create crash signal dir: %w", err)
		}
	}
	raw, err := json.MarshalIndent(signal, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal crash signal: %w", err)
	}
	tmp := c.signalFile + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o644); err != nil {
		return fmt.Errorf("write crash signal temp file: %w", err)
	}
	if err := os.Rename(tmp, c.signalFile); err != nil {
		return fmt.Errorf("publish crash signal file: %w", err)
	}
	return nil
}

func ackOutboxDeliveryWithRetry(ctx context.Context, ackRemote egress.AckSink, ack egress.DeliveryAck) error {
	if ackRemote == nil {
		return fmt.Errorf("egress ack sink is nil")
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		if err := ackRemote.AckOutboxDelivery(ctx, ack); err != nil {
			lastErr = err
		} else {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ack outbox delivery after recovery: %w: last error: %v", ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func publishFabricAck(ctx context.Context, rdb *redis.Client, prefix string, cmd edgecontrol.OutboxPushCommand, ack edgecontrol.OutboxPushAck) error {
	raw, err := json.Marshal(ack)
	if err != nil {
		return fmt.Errorf("marshal fake edge outbox ack: %w", err)
	}
	if err := rdb.Publish(ctx, outboxAckChannel(prefix, cmd.CommandID), raw).Err(); err != nil {
		return fmt.Errorf("publish fake edge outbox ack: %w", err)
	}
	return nil
}

func waitSubscriberReady(ctx context.Context, ready <-chan error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ready:
		if err != nil {
			return err
		}
		return nil
	}
}

func waitForRecordedCommands(ctx context.Context, recorder *commandRecorder, subscriberErr <-chan error, expectedCount, expectSourceInstances int) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		records := recorder.snapshot()
		if err := firstRecordError(records); err != nil {
			return err
		}
		if len(records) >= expectedCount && len(sourceSet(records)) >= expectSourceInstances {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for outbox commands: got records=%d/%d source_instances=%d/%d: %w",
				len(records),
				expectedCount,
				len(sourceSet(records)),
				expectSourceInstances,
				ctx.Err(),
			)
		case err := <-subscriberErr:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("fake edge subscriber exited before probe completed: %w", err)
		case <-ticker.C:
		}
	}
}

func waitForCrashRecoveryCommands(ctx context.Context, recorder *commandRecorder, subscriberErr <-chan error) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		records := recorder.snapshot()
		if err := firstRecordError(records); err != nil {
			return err
		}
		if len(records) >= 2 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for crash recovery redelivery: got records=%d: %w", len(records), ctx.Err())
		case err := <-subscriberErr:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("fake edge subscriber exited before crash recovery completed: %w", err)
		case <-ticker.C:
		}
	}
}

func waitOutboxEmpty(ctx context.Context, pool *pgxpool.Pool, userIDs []int64) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		count, statusSummary, err := outboxCount(ctx, pool, userIDs)
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for dispatch_outbox drain: remaining=%d status=%s: %w", count, statusSummary, ctx.Err())
		case <-ticker.C:
		}
	}
}

func outboxCount(ctx context.Context, pool *pgxpool.Pool, userIDs []int64) (int64, string, error) {
	rows, err := pool.Query(ctx, `
SELECT status, count(*)
FROM dispatch_outbox
WHERE target_user_id = ANY($1::bigint[])
GROUP BY status
ORDER BY status`, userIDs)
	if err != nil {
		return 0, "", fmt.Errorf("query dispatch_outbox status: %w", err)
	}
	defer rows.Close()
	var total int64
	parts := make([]string, 0)
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return 0, "", err
		}
		total += count
		parts = append(parts, fmt.Sprintf("%s=%d", status, count))
	}
	if err := rows.Err(); err != nil {
		return 0, "", err
	}
	return total, strings.Join(parts, ","), nil
}

type outboxState struct {
	id       int64
	status   string
	attempts int
}

func waitOutboxState(ctx context.Context, pool *pgxpool.Pool, userID int64, pts int, status string, attempts int) (outboxState, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := loadOutboxState(ctx, pool, userID, pts)
		if err != nil {
			return outboxState{}, err
		}
		if state.status == status && state.attempts == attempts {
			return state, nil
		}
		select {
		case <-ctx.Done():
			return outboxState{}, fmt.Errorf("timed out waiting for dispatch_outbox state user_id=%d pts=%d: got status=%s attempts=%d want status=%s attempts=%d: %w",
				userID,
				pts,
				state.status,
				state.attempts,
				status,
				attempts,
				ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func loadOutboxState(ctx context.Context, pool *pgxpool.Pool, userID int64, pts int) (outboxState, error) {
	var state outboxState
	err := pool.QueryRow(ctx, `
SELECT id, status, attempts
FROM dispatch_outbox
WHERE target_user_id = $1 AND pts = $2`,
		userID,
		int32(pts),
	).Scan(&state.id, &state.status, &state.attempts)
	if err != nil {
		return outboxState{}, fmt.Errorf("load dispatch_outbox state user_id=%d pts=%d: %w", userID, pts, err)
	}
	return state, nil
}

func validateRecordedCommands(records []recordedCommand, expected []probeEvent, expectSourceInstances int) error {
	if err := firstRecordError(records); err != nil {
		return err
	}
	expectedByUser := make(map[int64][]int, len(expected))
	for _, event := range expected {
		expectedByUser[event.userID] = append(expectedByUser[event.userID], event.pts)
	}
	for userID := range expectedByUser {
		sort.Ints(expectedByUser[userID])
	}
	if len(records) != len(expected) {
		return fmt.Errorf("recorded outbox commands=%d, want exactly %d", len(records), len(expected))
	}
	seenRefs := make(map[edgecontrol.OutboxDeliveryRef]struct{}, len(records))
	gotByUser := make(map[int64][]int, len(expectedByUser))
	for _, record := range records {
		cmd := record.cmd
		ref := cmd.DeliveryRef
		if _, duplicate := seenRefs[ref]; duplicate {
			return fmt.Errorf("duplicate delivery ref observed: %+v", ref)
		}
		seenRefs[ref] = struct{}{}
		if ref.Attempt != 1 {
			return fmt.Errorf("delivery ref attempt=%d for user_id=%d pts=%d, want 1", ref.Attempt, ref.TargetUserID, ref.Pts)
		}
		if _, ok := expectedByUser[ref.TargetUserID]; !ok {
			return fmt.Errorf("unexpected target user_id in outbox command: %d", ref.TargetUserID)
		}
		gotByUser[ref.TargetUserID] = append(gotByUser[ref.TargetUserID], ref.Pts)
	}
	for userID, want := range expectedByUser {
		got := gotByUser[userID]
		if len(got) != len(want) {
			return fmt.Errorf("user_id=%d command count=%d, want %d", userID, len(got), len(want))
		}
		if !intsEqual(got, want) {
			return fmt.Errorf("user_id=%d command pts order=%v, want %v", userID, got, want)
		}
	}
	if sources := len(sourceSet(records)); sources < expectSourceInstances {
		return fmt.Errorf("source instances=%d, want at least %d", sources, expectSourceInstances)
	}
	return nil
}

func validateCrashRecoveryRecords(records []recordedCommand, expected probeEvent, expectSourceInstances int) error {
	if err := firstRecordError(records); err != nil {
		return err
	}
	if len(records) != 2 {
		return fmt.Errorf("crash recovery recorded commands=%d, want exactly 2", len(records))
	}
	first := records[0].cmd
	second := records[1].cmd
	if first.DeliveryRef.TargetUserID != expected.userID || second.DeliveryRef.TargetUserID != expected.userID {
		return fmt.Errorf("crash recovery target user mismatch: first=%d second=%d want=%d", first.DeliveryRef.TargetUserID, second.DeliveryRef.TargetUserID, expected.userID)
	}
	if first.DeliveryRef.Pts != expected.pts || second.DeliveryRef.Pts != expected.pts {
		return fmt.Errorf("crash recovery pts mismatch: first=%d second=%d want=%d", first.DeliveryRef.Pts, second.DeliveryRef.Pts, expected.pts)
	}
	if first.DeliveryRef.OutboxID == 0 || second.DeliveryRef.OutboxID != first.DeliveryRef.OutboxID {
		return fmt.Errorf("crash recovery outbox id mismatch: first=%d second=%d", first.DeliveryRef.OutboxID, second.DeliveryRef.OutboxID)
	}
	if first.DeliveryRef.Attempt != 1 || second.DeliveryRef.Attempt != 2 {
		return fmt.Errorf("crash recovery attempts=%d,%d want 1,2", first.DeliveryRef.Attempt, second.DeliveryRef.Attempt)
	}
	if strings.TrimSpace(first.SourceInstanceID) == "" || strings.TrimSpace(second.SourceInstanceID) == "" {
		return fmt.Errorf("crash recovery source instances must be non-empty")
	}
	if first.SourceInstanceID == second.SourceInstanceID {
		return fmt.Errorf("crash recovery second attempt reused crashed source %s", first.SourceInstanceID)
	}
	if sources := len(sourceSet(records)); sources < expectSourceInstances {
		return fmt.Errorf("crash recovery source instances=%d, want at least %d", sources, expectSourceInstances)
	}
	return nil
}

func firstRecordError(records []recordedCommand) error {
	for _, record := range records {
		if record.err != "" {
			return fmt.Errorf("fake edge command failed command_id=%s source=%s target_user_id=%d: %s",
				record.cmd.CommandID,
				record.cmd.SourceInstanceID,
				record.cmd.TargetUserID,
				record.err,
			)
		}
	}
	return nil
}

func sourceSet(records []recordedCommand) map[string]struct{} {
	out := make(map[string]struct{})
	for _, record := range records {
		source := strings.TrimSpace(record.cmd.SourceInstanceID)
		if source != "" {
			out[source] = struct{}{}
		}
	}
	return out
}

func sortedSources(records []recordedCommand) []string {
	set := sourceSet(records)
	out := make([]string, 0, len(set))
	for source := range set {
		out = append(out, source)
	}
	sort.Strings(out)
	return out
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func outboxCommandChannel(prefix, instanceID string) string {
	return fmt.Sprintf("%s:outbox:push:%s", prefix, instanceID)
}

func outboxAckChannel(prefix, commandID string) string {
	return fmt.Sprintf("%s:outbox:ack:%s", prefix, commandID)
}
