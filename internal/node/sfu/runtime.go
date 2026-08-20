// Package sfu wires the standalone telesrv-sfu media role.
package sfu

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/config"
	"telesrv/internal/node/common"
	mediasfu "telesrv/internal/sfu"
	"telesrv/internal/store/redisstore"
)

// Run starts the standalone SFU media process.
func Run(logger *zap.Logger) error {
	cfg, err := config.LoadSFU()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := validateConfig(&cfg); err != nil {
		return err
	}
	ctx, stop, _ := common.StartRuntimeSupport(cfg, logger)
	defer stop()
	return runWithConfig(ctx, cfg, logger)
}

func runWithConfig(ctx context.Context, cfg config.SFUConfig, logger *zap.Logger) error {
	instanceID := config.ResolveInstanceID(cfg.InstanceID)
	sfuControlURL := resolveControlURL(cfg)
	sfuAdvertise := cfg.SFUAdvertiseIP
	if sfuAdvertise == "" {
		sfuAdvertise = cfg.AdvertiseIP
	}
	reporter, err := mediasfu.NewHTTPGroupCallTouchReporter(nil, cfg.GroupCallControlURL, cfg.GroupCallControlToken)
	if err != nil {
		return fmt.Errorf("init groupcall liveness reporter: %w", err)
	}
	rdb, err := redisstore.Open(ctx, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	pionSFU, err := mediasfu.NewPion(mediasfu.PionConfig{
		UDPPort:     cfg.SFUUDPPort,
		AdvertiseIP: sfuAdvertise,
		Logger:      logger.Named("sfu"),
		Touch: func(callID, userID int64) {
			touchCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := reporter.Touch(touchCtx, callID, userID); err != nil {
				logger.Debug("groupcall liveness touch", zap.Int64("call_id", callID), zap.Int64("user_id", userID), zap.Error(err))
			}
		},
	})
	if err != nil {
		return fmt.Errorf("init sfu: %w", err)
	}
	registry := mediasfu.NewRedisInstanceRegistry(rdb)
	activeProvider, _ := pionSFU.(mediasfu.ActiveCallProvider)
	heartbeatStatus := mediasfu.NewInstanceHeartbeatStatus(cfg.SFUInstanceTTL)
	if err := startControl(ctx, cfg, instanceID, pionSFU, heartbeatStatus.Ready, logger.Named("sfu").Named("control")); err != nil {
		return fmt.Errorf("start sfu control: %w", err)
	}
	instanceRecord := mediasfu.InstanceRecord{
		InstanceID:  instanceID,
		ControlAddr: sfuControlURL,
		AdvertiseIP: sfuAdvertise,
		UDPPort:     cfg.SFUUDPPort,
		MaxCalls:    cfg.SFUInstanceMaxActiveCalls,
	}
	if activeProvider != nil {
		instanceRecord.ActiveCalls = len(activeProvider.ActiveCallIDs())
	}
	if err := registry.Register(ctx, instanceRecord, cfg.SFUInstanceTTL); err != nil {
		return fmt.Errorf("register sfu instance heartbeat: %w", err)
	}
	heartbeatStatus.ObserveInstanceHeartbeat(nil)
	go mediasfu.RunInstanceHeartbeatWithActiveCallsAndObserver(ctx, registry, instanceRecord, cfg.SFUInstanceTTL, cfg.SFUInstanceHeartbeatInterval, activeProvider, heartbeatStatus)
	logger.Info("telesrv-sfu started",
		zap.String("instance_id", instanceID),
		zap.String("control_grpc_addr", cfg.SFUControlGRPCAddr),
		zap.String("control_url", sfuControlURL),
		zap.String("core_groupcall_control_url", cfg.GroupCallControlURL),
		zap.Int("udp_port", cfg.SFUUDPPort),
		zap.String("advertise_ip", sfuAdvertise),
	)
	<-ctx.Done()
	return nil
}

func resolveControlURL(cfg config.SFUConfig) string {
	if strings.TrimSpace(cfg.SFUControlGRPCURL) != "" {
		return strings.TrimSpace(cfg.SFUControlGRPCURL)
	}
	return strings.TrimSpace(cfg.SFUControlGRPCAddr)
}

func startControl(ctx context.Context, cfg config.SFUConfig, instanceID string, service mediasfu.Service, health func(context.Context) error, logger *zap.Logger) error {
	_, err := mediasfu.StartGRPCControl(ctx, mediasfu.GRPCControlServerConfig{
		Addr:            cfg.SFUControlGRPCAddr,
		InstanceID:      instanceID,
		Token:           cfg.SFUControlToken,
		TLSCertFile:     cfg.SFUControlGRPCTLSCertFile,
		TLSKeyFile:      cfg.SFUControlGRPCTLSKeyFile,
		TLSClientCAFile: cfg.SFUControlGRPCTLSClientCAFile,
		Service:         service,
		Health:          health,
		Logger:          logger,
		MaxRecvMsgBytes: 0,
		MaxSendMsgBytes: 0,
	})
	return err
}

func validateConfig(cfg *config.SFUConfig) error {
	if cfg == nil {
		return fmt.Errorf("nil config")
	}
	if strings.TrimSpace(cfg.SFUControlGRPCAddr) == "" {
		return fmt.Errorf("TELESRV_SFU_CONTROL_GRPC_ADDR is required for telesrv-sfu")
	}
	if strings.TrimSpace(cfg.SFUControlGRPCURL) != "" && strings.TrimSpace(cfg.SFUControlGRPCAddr) == "" {
		return fmt.Errorf("TELESRV_SFU_CONTROL_GRPC_ADDR is required when TELESRV_SFU_CONTROL_GRPC_URL is set")
	}
	if strings.TrimSpace(cfg.SFUControlToken) == "" {
		return fmt.Errorf("TELESRV_SFU_CONTROL_TOKEN is required for telesrv-sfu")
	}
	if strings.TrimSpace(cfg.GroupCallControlURL) == "" {
		return fmt.Errorf("TELESRV_GROUPCALL_CONTROL_URL is required for telesrv-sfu")
	}
	if strings.TrimSpace(cfg.GroupCallControlToken) == "" {
		return fmt.Errorf("TELESRV_GROUPCALL_CONTROL_TOKEN is required for telesrv-sfu")
	}
	return nil
}
