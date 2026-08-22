// Package egress wires the dedicated durable Egress role.
package egress

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap"

	"telesrv/internal/config"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/edgecontrol/redisbus"
	"telesrv/internal/edgecontrol/redisregistry"
	egresssvc "telesrv/internal/egress"
	"telesrv/internal/node/common"
	obsmetrics "telesrv/internal/observability/metrics"
	"telesrv/internal/store/postgres"
	"telesrv/internal/store/redisstore"
)

// Run starts the dedicated Durable Egress outbox service.
func Run(logger *zap.Logger, buildMeta common.BuildMetadata) error {
	cfg, err := config.LoadEgress()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	return runWithConfig(logger, cfg, buildMeta)
}

func runWithConfig(logger *zap.Logger, cfg config.EgressConfig, buildMeta common.BuildMetadata) error {
	if err := validateEgressConfig(cfg); err != nil {
		return err
	}
	instanceID := config.ResolveInstanceID(cfg.InstanceID)
	if err := common.ConfigureProcessGlobals(cfg); err != nil {
		return err
	}
	logger.Info("telesrv egress starting",
		zap.Int("dc", cfg.DC),
		zap.Int("tl_layer", tg.Layer),
		zap.String("git_commit", buildMeta.Commit),
		zap.String("git_branch", buildMeta.Branch),
		zap.String("git_tree_state", buildMeta.TreeState),
		zap.String("build_time", buildMeta.BuildTime),
		zap.String("go_version", buildMeta.GoVersion),
		zap.String("instance_id", instanceID),
		zap.Int("outbox_workers", cfg.OutboxWorkers),
		zap.Int("outbox_batch", cfg.OutboxBatch),
		zap.Duration("outbox_lease_timeout", cfg.OutboxLeaseTimeout),
		zap.Duration("outbound_push_timeout", cfg.OutboundPushTimeout),
		zap.String("egress_ack_grpc_addr", cfg.EgressAckGRPCAddr),
	)

	ctx, stop, metricRegistry := common.StartRuntimeSupport(cfg, logger)
	defer stop()
	migrationStatus, err := postgres.MigrateAndStatus(cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("postgres migrate: %w", err)
	}
	pool, err := postgres.Open(ctx, cfg.PostgresDSN,
		postgres.WithMaxConns(cfg.PostgresMaxConns),
		postgres.WithMinConns(cfg.PostgresMinConns),
	)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	metricRegistry.AddGaugeProvider(func() []obsmetrics.GaugeSample {
		stat := pool.Stat()
		return []obsmetrics.GaugeSample{
			{Name: "telesrv_postgres_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "total"}}, Value: float64(stat.TotalConns())},
			{Name: "telesrv_postgres_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "acquired"}}, Value: float64(stat.AcquiredConns())},
			{Name: "telesrv_postgres_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "idle"}}, Value: float64(stat.IdleConns())},
			{Name: "telesrv_postgres_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "constructing"}}, Value: float64(stat.ConstructingConns())},
			{Name: "telesrv_postgres_pool_max_connections", Value: float64(stat.MaxConns())},
			{Name: "telesrv_postgres_pool_acquire_count", Value: float64(stat.AcquireCount())},
			{Name: "telesrv_postgres_pool_acquire_wait_seconds", Value: stat.AcquireDuration().Seconds()},
			{Name: "telesrv_postgres_pool_empty_acquire_count", Value: float64(stat.EmptyAcquireCount())},
			{Name: "telesrv_postgres_pool_canceled_acquire_count", Value: float64(stat.CanceledAcquireCount())},
		}
	})
	rdb, err := redisstore.Open(ctx, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()
	metricRegistry.AddGaugeProvider(func() []obsmetrics.GaugeSample {
		stat := rdb.PoolStats()
		return []obsmetrics.GaugeSample{
			{Name: "telesrv_redis_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "total"}}, Value: float64(stat.TotalConns)},
			{Name: "telesrv_redis_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "idle"}}, Value: float64(stat.IdleConns)},
			{Name: "telesrv_redis_pool_pending_requests", Value: float64(stat.PendingRequests)},
			{Name: "telesrv_redis_pool_hits", Value: float64(stat.Hits)},
			{Name: "telesrv_redis_pool_misses", Value: float64(stat.Misses)},
			{Name: "telesrv_redis_pool_timeouts", Value: float64(stat.Timeouts)},
			{Name: "telesrv_redis_pool_wait_count", Value: float64(stat.WaitCount)},
			{Name: "telesrv_redis_pool_wait_seconds", Value: time.Duration(stat.WaitDurationNs).Seconds()},
		}
	})

	updateEventStore := postgres.NewUpdateEventStore(pool, postgres.WithUpdateEventLogger(logger.Named("store").Named("updates")))
	dispatchOutboxStore := postgres.NewDispatchOutboxStore(pool, postgres.WithLeaseTimeout(cfg.OutboxLeaseTimeout))
	deliveryOutboxStore := postgres.NewDeliveryOutboxStore(pool, postgres.WithDeliveryLeaseTimeout(cfg.OutboxLeaseTimeout))
	welcomeMessageStore := postgres.NewWelcomeMessageStore(pool)
	if _, err := egresssvc.StartGRPCAck(ctx, egresssvc.GRPCAckServerConfig{
		Addr:            cfg.EgressAckGRPCAddr,
		InstanceID:      instanceID,
		Token:           cfg.EgressAckToken,
		TLSCertFile:     cfg.EgressAckGRPCTLSCertFile,
		TLSKeyFile:      cfg.EgressAckGRPCTLSKeyFile,
		TLSClientCAFile: cfg.EgressAckGRPCTLSClientCAFile,
		Store:           dispatchOutboxStore,
		DeliveryStore:   deliveryOutboxStore,
		Logger:          logger.Named("egress").Named("ack").Named("grpc"),
	}); err != nil {
		return fmt.Errorf("start egress ack grpc: %w", err)
	}
	deliverer := edgecontrol.NewOutboxFabric(edgecontrol.OutboxFabricConfig{
		InstanceID:     instanceID,
		Registry:       redisregistry.New(rdb),
		Bus:            redisbus.New(rdb),
		CommandTimeout: cfg.OutboundPushTimeout,
	})
	welcomeEdgeControl, err := edgecontrol.NewControlFabricController(
		edgecontrol.NewNoLocalController(),
		edgecontrol.NewSessionControlFabric(edgecontrol.SessionControlFabricConfig{
			InstanceID: instanceID, Registry: redisregistry.New(rdb), Bus: redisbus.New(rdb),
			CommandTimeout: cfg.OutboundPushTimeout,
		}),
	)
	if err != nil {
		return fmt.Errorf("init welcome Edge control fabric: %w", err)
	}
	projection := newEgressProjectionRuntime(pool, rdb, cfg, instanceID, logger.Named("egress").Named("projection"))
	welcomeDispatcher := projection.projector.NewWelcomeDeliveryDispatcher(
		welcomeEdgeControl, welcomeMessageStore, logger.Named("egress").Named("welcome-delivery"),
	)
	readModelListener := postgres.NewReadModelChangeListener(cfg.PostgresDSN, projection.caches, logger.Named("egress").Named("read-model-listener"))
	go readModelListener.Run(ctx)
	wakes := newEgressWakeLanes()
	outboxReadyListener := postgres.NewDispatchOutboxReadyListener(cfg.PostgresDSN, logger.Named("egress").Named("outbox-ready-listener"))
	go outboxReadyListener.Run(ctx, wakes.wakeDispatchOutbox)
	deliveryReadyListener := postgres.NewEdgeDeliveryOutboxReadyListener(cfg.PostgresDSN, logger.Named("egress").Named("delivery-ready-listener"))
	go deliveryReadyListener.Run(ctx, wakes.wakeEdgeDeliveryOutbox)
	service, err := egresssvc.NewService(updateEventStore, dispatchOutboxStore, deliverer, projection.projector.BuildOutboxUpdateBytes, metricRegistry, logger.Named("egress"), egresssvc.Config{
		Workers:     cfg.OutboxWorkers,
		Batch:       cfg.OutboxBatch,
		PushTimeout: cfg.OutboundPushTimeout,
	})
	if err != nil {
		return fmt.Errorf("init egress service: %w", err)
	}
	deliveryService, err := egresssvc.NewDeliveryService(deliveryOutboxStore, deliverer, metricRegistry, logger.Named("egress").Named("delivery"), egresssvc.Config{
		Workers:     cfg.OutboxWorkers,
		Batch:       cfg.OutboxBatch,
		PushTimeout: cfg.OutboundPushTimeout,
	})
	if err != nil {
		return fmt.Errorf("init edge delivery service: %w", err)
	}
	logger.Info("telesrv egress ready",
		zap.String("instance_id", instanceID),
		zap.Int("pid", os.Getpid()),
		zap.String("git_commit", buildMeta.Commit),
		zap.Uint("schema_version", migrationStatus.Version),
		zap.String("egress_ack_grpc_addr", cfg.EgressAckGRPCAddr),
	)
	go deliveryService.RunWithWake(ctx, wakes.edgeDeliveryOutbox)
	go welcomeDispatcher.Run(ctx)
	service.RunWithWake(ctx, wakes.dispatchOutbox)
	return nil
}

func validateEgressConfig(cfg config.EgressConfig) error {
	if strings.TrimSpace(cfg.EgressAckGRPCAddr) == "" {
		return fmt.Errorf("TELESRV_EGRESS_ACK_GRPC_ADDR is required by cmd/telesrv-egress")
	}
	if strings.TrimSpace(cfg.EgressAckToken) == "" {
		return fmt.Errorf("TELESRV_EGRESS_ACK_TOKEN is required by cmd/telesrv-edge and cmd/telesrv-egress")
	}
	return nil
}

type egressWakeLanes struct {
	dispatchOutbox         <-chan struct{}
	edgeDeliveryOutbox     <-chan struct{}
	wakeDispatchOutbox     func()
	wakeEdgeDeliveryOutbox func()
}

func newEgressWakeLanes() egressWakeLanes {
	dispatchOutbox, wakeDispatchOutbox := newWakeLane()
	edgeDeliveryOutbox, wakeEdgeDeliveryOutbox := newWakeLane()
	return egressWakeLanes{
		dispatchOutbox:         dispatchOutbox,
		edgeDeliveryOutbox:     edgeDeliveryOutbox,
		wakeDispatchOutbox:     wakeDispatchOutbox,
		wakeEdgeDeliveryOutbox: wakeEdgeDeliveryOutbox,
	}
}

func newWakeLane() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	wake := func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return ch, wake
}
