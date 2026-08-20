package edge

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"

	"telesrv/internal/config"
	"telesrv/internal/edgecontrol"
)

func TestServeUntilLocationRegistryFatalCancelsServer(t *testing.T) {
	fatal := make(chan error, 1)
	fatal <- edgecontrol.ErrLocationLeaseLost

	err := serveUntilLocationRegistryFatal(context.Background(), fatal, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, edgecontrol.ErrLocationLeaseLost) {
		t.Fatalf("serve error = %v, want ErrLocationLeaseLost", err)
	}
}

func TestServeUntilLocationRegistryFatalPreservesServerError(t *testing.T) {
	want := errors.New("listen failed")
	err := serveUntilLocationRegistryFatal(context.Background(), make(chan error), func(context.Context) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("serve error = %v, want %v", err, want)
	}
}

func TestValidateEdgeConfigDoesNotRequireCoreSFUControlAuth(t *testing.T) {
	cfg := config.EdgeConfig{
		CoreExecGRPCTargets:   "127.0.0.1:2440",
		CoreExecGRPCResolver:  "static",
		CoreExecToken:         "coreexec-secret",
		FileGRPCTargets:       "127.0.0.1:2460",
		FileGRPCResolver:      "static",
		FileToken:             "file-secret",
		EgressAckGRPCTargets:  "127.0.0.1:2450",
		EgressAckGRPCResolver: "static",
		EgressAckToken:        "egress-secret",
	}
	if err := validateEdgeConfig(cfg); err != nil {
		t.Fatalf("edge config rejected: %v", err)
	}
}

func TestEdgeRuntimeDoesNotDependOnPostgresStore(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "telesrv/internal/node/edge")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list edge deps: %v", err)
	}
	for _, dep := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(dep) == "telesrv/internal/store/postgres" {
			t.Fatal("edge runtime must not depend on internal/store/postgres")
		}
	}
}

func TestValidateEdgeConfigRequiresEgressAckBoundary(t *testing.T) {
	base := config.EdgeConfig{
		CoreExecGRPCTargets:   "127.0.0.1:2440",
		CoreExecGRPCResolver:  "static",
		CoreExecToken:         "coreexec-secret",
		FileGRPCTargets:       "127.0.0.1:2460",
		FileGRPCResolver:      "static",
		FileToken:             "file-secret",
		EgressAckGRPCTargets:  "127.0.0.1:2450",
		EgressAckGRPCResolver: "static",
		EgressAckToken:        "egress-secret",
	}
	if err := validateEdgeConfig(base); err != nil {
		t.Fatalf("base config rejected: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*config.EdgeConfig)
		wantErr string
	}{
		{name: "coreexec token", mutate: func(cfg *config.EdgeConfig) { cfg.CoreExecToken = " " }, wantErr: "TELESRV_CORE_EXEC_TOKEN"},
		{name: "file targets", mutate: func(cfg *config.EdgeConfig) { cfg.FileGRPCTargets = " " }, wantErr: "TELESRV_FILE_GRPC_TARGETS"},
		{name: "file token", mutate: func(cfg *config.EdgeConfig) { cfg.FileToken = " " }, wantErr: "TELESRV_FILE_TOKEN"},
		{name: "targets", mutate: func(cfg *config.EdgeConfig) { cfg.EgressAckGRPCTargets = " " }, wantErr: "TELESRV_EGRESS_ACK_GRPC_TARGETS"},
		{name: "token", mutate: func(cfg *config.EdgeConfig) { cfg.EgressAckToken = " " }, wantErr: "TELESRV_EGRESS_ACK_TOKEN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			err := validateEdgeConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateEdgeConfig err = %v, want %s", err, tt.wantErr)
			}
		})
	}
}
