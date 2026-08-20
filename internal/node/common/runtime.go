// Package common contains shared process startup primitives for telesrv roles.
package common

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"telesrv/internal/branding"
	"telesrv/internal/config"
	"telesrv/internal/coreexec"
	"telesrv/internal/domain"
	"telesrv/internal/mtprotoedge"
	obsmetrics "telesrv/internal/observability/metrics"
)

// BuildMetadata is injected by cmd entrypoints. Keep ldflags variables in each
// main package so existing -X main.* release commands continue to work.
type BuildMetadata struct {
	Commit    string
	Branch    string
	TreeState string
	BuildTime string
	GoVersion string
}

func BuildMetadataFromValues(commit, branch, treeState, buildTime string) BuildMetadata {
	meta := BuildMetadata{
		Commit:    valueOrUnknown(commit),
		Branch:    valueOrUnknown(branch),
		TreeState: valueOrUnknown(treeState),
		BuildTime: valueOrUnknown(buildTime),
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		meta.GoVersion = info.GoVersion
		settings := map[string]string{}
		for _, setting := range info.Settings {
			settings[setting.Key] = setting.Value
		}
		if meta.Commit == "unknown" {
			meta.Commit = valueOrUnknown(settings["vcs.revision"])
		}
		if meta.TreeState == "unknown" {
			switch settings["vcs.modified"] {
			case "true":
				meta.TreeState = "dirty"
			case "false":
				meta.TreeState = "clean"
			}
		}
	}
	meta.GoVersion = valueOrUnknown(meta.GoVersion)
	return meta
}

func valueOrUnknown(v string) string {
	if v == "" {
		return "unknown"
	}
	return v
}

// NewLogger builds the production process logger shared by all telesrv roles.
func NewLogger() (*zap.Logger, error) {
	level := zapcore.InfoLevel
	if v := strings.TrimSpace(os.Getenv("TELESRV_LOG_LEVEL")); v != "" {
		if err := level.UnmarshalText([]byte(strings.ToLower(v))); err != nil {
			return nil, fmt.Errorf("parse TELESRV_LOG_LEVEL %q: %w", v, err)
		}
	}
	ws := &zapcore.BufferedWriteSyncer{
		WS:            zapcore.AddSync(os.Stderr),
		FlushInterval: 500 * time.Millisecond,
	}
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()), ws, level)
	return zap.New(core,
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
		zap.ErrorOutput(zapcore.AddSync(os.Stderr)),
	), nil
}

func Main(role string, run func(*zap.Logger) error) {
	logger, err := NewLogger()
	if err != nil {
		fmt.Fprintln(os.Stderr, "init logger:", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	if err := run(logger); err != nil {
		logger.Error(role+" stopped", zap.Error(err))
		_ = logger.Sync()
		os.Exit(1)
	}
}

func MainWithMetadata(role string, meta BuildMetadata, run func(*zap.Logger, BuildMetadata) error) {
	Main(role, func(logger *zap.Logger) error {
		return run(logger, meta)
	})
}

func startDebugServer(ctx context.Context, addr string, metricsHandler http.Handler, logger *zap.Logger) {
	if addr == "" {
		return
	}
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(10000)

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	if metricsHandler != nil {
		mux.Handle("/metrics", metricsHandler)
	}

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("pprof 调试端点已启用", zap.String("addr", addr),
			zap.String("hint", "go tool pprof http://"+addr+"/debug/pprof/profile?seconds=30"))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Warn("pprof 端点退出", zap.Error(err))
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
}

func goRuntimeGaugeSamples() []obsmetrics.GaugeSample {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return []obsmetrics.GaugeSample{
		{Name: "telesrv_go_goroutines", Value: float64(runtime.NumGoroutine())},
		{Name: "telesrv_go_heap_alloc_bytes", Value: float64(mem.HeapAlloc)},
		{Name: "telesrv_go_heap_inuse_bytes", Value: float64(mem.HeapInuse)},
		{Name: "telesrv_go_heap_objects", Value: float64(mem.HeapObjects)},
		{Name: "telesrv_go_stack_inuse_bytes", Value: float64(mem.StackInuse)},
		{Name: "telesrv_go_sys_bytes", Value: float64(mem.Sys)},
		{Name: "telesrv_go_gc_cycles", Value: float64(mem.NumGC)},
		{Name: "telesrv_go_gc_pause_seconds", Value: time.Duration(mem.PauseTotalNs).Seconds()},
	}
}

func MTProtoRuntimeGaugeSamples(snapshot mtprotoedge.RuntimeSnapshot) []obsmetrics.GaugeSample {
	return []obsmetrics.GaugeSample{
		{Name: "telesrv_mtproto_raw_connections", Value: float64(snapshot.RawConnections)},
		{Name: "telesrv_mtproto_raw_connection_limit", Value: float64(snapshot.RawConnectionLimit)},
		{Name: "telesrv_mtproto_handshakes_active", Value: float64(snapshot.Handshakes)},
		{Name: "telesrv_mtproto_handshake_limit", Value: float64(snapshot.HandshakeLimit)},
		{Name: "telesrv_mtproto_sessions", Labels: []obsmetrics.Label{{Name: "state", Value: "active"}}, Value: float64(snapshot.ActiveSessions)},
		{Name: "telesrv_mtproto_sessions", Labels: []obsmetrics.Label{{Name: "state", Value: "provisional"}}, Value: float64(snapshot.ProvisionalSessions)},
		{Name: "telesrv_mtproto_logical_sessions", Labels: []obsmetrics.Label{{Name: "state", Value: "retained"}}, Value: float64(snapshot.LogicalSessions)},
		{Name: "telesrv_mtproto_logical_sessions", Labels: []obsmetrics.Label{{Name: "state", Value: "offline"}}, Value: float64(snapshot.OfflineLogicalSessions)},
		{Name: "telesrv_mtproto_logical_outbox_frames", Value: float64(snapshot.LogicalOutboxFrames)},
		{Name: "telesrv_mtproto_logical_outbox_bytes", Value: float64(snapshot.LogicalOutboxBytes)},
		{Name: "telesrv_mtproto_pending_push_bytes", Value: float64(snapshot.PendingPushBytes)},
		{Name: "telesrv_mtproto_inbound_rpc_tasks", Value: float64(snapshot.InboundRPCTasks)},
		{Name: "telesrv_mtproto_inbound_rpc_bytes", Value: float64(snapshot.InboundRPCBytes)},
		{Name: "telesrv_mtproto_inbound_rpc_ready_connections", Value: float64(snapshot.InboundRPCReadyConnections)},
		{Name: "telesrv_mtproto_inbound_rpc_task_limit", Value: float64(snapshot.InboundRPCMaxTasks)},
		{Name: "telesrv_mtproto_inbound_rpc_byte_limit", Value: float64(snapshot.InboundRPCMaxBytes)},
		{Name: "telesrv_mtproto_inbound_frame_bytes", Value: float64(snapshot.InboundFrameBytes)},
		{Name: "telesrv_mtproto_inbound_frame_byte_limit", Value: float64(snapshot.InboundFrameMaxBytes)},
		{Name: "telesrv_mtproto_outbound_tracked_bytes", Labels: []obsmetrics.Label{{Name: "kind", Value: "body"}}, Value: float64(snapshot.OutboundTrackedBytes)},
		{Name: "telesrv_mtproto_outbound_tracked_bytes", Labels: []obsmetrics.Label{{Name: "kind", Value: "control"}}, Value: float64(snapshot.OutboundControlBytes)},
		{Name: "telesrv_mtproto_outbound_tracked_byte_limit", Labels: []obsmetrics.Label{{Name: "kind", Value: "body"}}, Value: float64(snapshot.OutboundTrackedMaxBytes)},
		{Name: "telesrv_mtproto_outbound_tracked_byte_limit", Labels: []obsmetrics.Label{{Name: "kind", Value: "control"}}, Value: float64(snapshot.OutboundControlMaxBytes)},
		{Name: "telesrv_mtproto_outbound_write_bytes", Value: float64(snapshot.OutboundWriteBytes)},
		{Name: "telesrv_mtproto_outbound_write_byte_limit", Value: float64(snapshot.OutboundWriteMaxBytes)},
		{Name: "telesrv_mtproto_rpc_execution_owners", Value: float64(snapshot.RPCExecutionOwners)},
		{Name: "telesrv_mtproto_rpc_execution_reserved_entries", Value: float64(snapshot.RPCExecutionReservedEntries)},
		{Name: "telesrv_mtproto_rpc_execution_receipts", Value: float64(snapshot.RPCExecutionReceipts)},
		{Name: "telesrv_mtproto_rpc_execution_receipt_budget_bytes", Value: float64(snapshot.RPCExecutionReceiptBudgetBytes)},
		{Name: "telesrv_mtproto_rpc_execution_subscribers", Value: float64(snapshot.RPCExecutionSubscribers)},
	}
}

type coreExecPendingAdmissionStatser interface {
	PendingAdmissionStats() coreexec.PendingAdmissionStats
}

func RegisterCoreExecPendingAdmissionMetrics(registry *obsmetrics.Registry, transport string, source coreExecPendingAdmissionStatser) {
	if registry == nil || source == nil {
		return
	}
	registry.AddGaugeProvider(func() []obsmetrics.GaugeSample {
		return CoreExecPendingAdmissionGaugeSamples(transport, source.PendingAdmissionStats())
	})
}

func CoreExecPendingAdmissionGaugeSamples(transport string, stats coreexec.PendingAdmissionStats) []obsmetrics.GaugeSample {
	labels := []obsmetrics.Label{{Name: "transport", Value: transport}}
	return []obsmetrics.GaugeSample{
		{Name: "telesrv_coreexec_pending_admissions", Labels: labels, Value: float64(stats.Count)},
		{Name: "telesrv_coreexec_pending_admission_capacity", Labels: labels, Value: float64(stats.Capacity)},
		{Name: "telesrv_coreexec_pending_admission_oldest_age_seconds", Labels: labels, Value: stats.OldestAge.Seconds()},
		{Name: "telesrv_coreexec_pending_admission_ttl_seconds", Labels: labels, Value: stats.TTL.Seconds()},
	}
}

type ProcessGlobalsProvider interface {
	ProcessGlobals() config.ProcessGlobals
}

type RuntimeSupportConfig interface {
	DebugAddress() string
}

func ConfigureProcessGlobals(cfg ProcessGlobalsProvider) error {
	globals := cfg.ProcessGlobals()
	if err := branding.Configure(globals.Branding); err != nil {
		return fmt.Errorf("configure branding: %w", err)
	}
	if !domain.ConfigurePremiumBotUserID(globals.PremiumBotUserID) {
		return fmt.Errorf("configure Premium bot user id %d", globals.PremiumBotUserID)
	}
	if !domain.ConfigurePremiumBotUsername(globals.PremiumBotUsername) {
		return fmt.Errorf("configure Premium bot username %q", globals.PremiumBotUsername)
	}
	return nil
}

func StartRuntimeSupport(cfg RuntimeSupportConfig, logger *zap.Logger) (context.Context, context.CancelFunc, *obsmetrics.Registry) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	metricRegistry := obsmetrics.New()
	metricRegistry.AddGaugeProvider(goRuntimeGaugeSamples)
	startDebugServer(ctx, cfg.DebugAddress(), metricRegistry, logger)
	return ctx, stop, metricRegistry
}
