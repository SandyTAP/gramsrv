package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadEdgeReadsRoleYAMLWithoutProcessEnvOverride(t *testing.T) {
	path := writeRoleYAML(t, "edge.yaml", `
version: 1
instance:
  id: edge-test
public:
  dc: 2
  advertise:
    ip: 10.1.2.3
    port: 2398
redis:
  addr: 127.0.0.1:6399
edge:
  listen: 0.0.0.0:2398
  rsa_key: data/test-rsa.pem
  websocket:
    enabled: false
    allowed_origins:
      - http://localhost:1234
  location:
    ttl: 2m
    heartbeat_interval: 30s
core_exec:
  resolver: static
  targets:
    - 127.0.0.1:2440
    - 127.0.0.1:2441
  token: ${TEST_CORE_TOKEN}
file_data:
  resolver: dns
  targets:
    - telesrv-file.default.svc.cluster.local:2520
  token: ${TEST_FILE_TOKEN}
  timeout: 11s
egress:
  ack:
    resolver: static
    targets:
      - 127.0.0.1:2510
    token: ${TEST_EGRESS_TOKEN}
`)
	t.Setenv("TELESRV_CONFIG", path)
	t.Setenv("TEST_CORE_TOKEN", "core-secret")
	t.Setenv("TEST_FILE_TOKEN", "file-secret")
	t.Setenv("TEST_EGRESS_TOKEN", "egress-secret")
	t.Setenv("TELESRV_CORE_EXEC_TOKEN", "must-not-win")

	cfg, err := LoadEdge()
	if err != nil {
		t.Fatalf("LoadEdge: %v", err)
	}
	if cfg.InstanceID != "edge-test" || cfg.ListenAddr != "0.0.0.0:2398" {
		t.Fatalf("unexpected edge identity/listen: instance=%q listen=%q", cfg.InstanceID, cfg.ListenAddr)
	}
	if cfg.CoreExecToken != "core-secret" {
		t.Fatalf("CoreExecToken = %q, want YAML-expanded secret", cfg.CoreExecToken)
	}
	if cfg.FileGRPCResolver != "dns" || cfg.FileGRPCTargets != "telesrv-file.default.svc.cluster.local:2520" {
		t.Fatalf("file grpc resolver/targets = %q/%q", cfg.FileGRPCResolver, cfg.FileGRPCTargets)
	}
	if cfg.WebSocketEnable {
		t.Fatalf("WebSocketEnable = true, want false from YAML")
	}
}

func TestLoadRoleYAMLRejectsUnknownFields(t *testing.T) {
	path := writeRoleYAML(t, "edge-bad.yaml", `
version: 1
edge:
  listen: 0.0.0.0:2398
  typo_field: true
`)
	t.Setenv("TELESRV_CONFIG", path)
	if _, err := LoadEdge(); err == nil || !strings.Contains(err.Error(), "typo_field") {
		t.Fatalf("LoadEdge err = %v, want unknown field rejection", err)
	}
}

func TestLoadFileReadsStorageAndUploadYAML(t *testing.T) {
	path := writeRoleYAML(t, "file.yaml", `
version: 1
public:
  dc: 2
postgres:
  dsn: postgres://file:file@127.0.0.1:5432/file?sslmode=disable
file_data:
  addr: 127.0.0.1:2520
  token: ${TEST_FILE_TOKEN}
storage:
  blob_backend: localfs
  blob_dir: data/test-blobs
  low_space_guard_enabled: false
  usage_refresh_interval: 2m
upload:
  part_ttl: 24h
  part_gc_interval: 5m
  part_gc_batch: 50
  in_flight_max_bytes: 1048576
media:
  external_media_enable: false
  web_page_preview_enable: false
`)
	t.Setenv("TELESRV_CONFIG", path)
	t.Setenv("TEST_FILE_TOKEN", "file-secret")

	cfg, err := LoadFile()
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.FileGRPCAddr != "127.0.0.1:2520" || cfg.FileToken != "file-secret" {
		t.Fatalf("file grpc = %q token=%q", cfg.FileGRPCAddr, cfg.FileToken)
	}
	if cfg.BlobDir != "data/test-blobs" || cfg.StorageLowSpaceGuardEnable {
		t.Fatalf("storage blob_dir=%q guard=%v", cfg.BlobDir, cfg.StorageLowSpaceGuardEnable)
	}
	if cfg.UploadPartGCBatch != 50 || cfg.UploadInFlightMaxBytes != 1048576 {
		t.Fatalf("upload batch=%d bytes=%d", cfg.UploadPartGCBatch, cfg.UploadInFlightMaxBytes)
	}
}

func TestLoadExampleRoleYAMLFiles(t *testing.T) {
	t.Setenv("TELESRV_CORE_EXEC_TOKEN", "core-secret")
	t.Setenv("TELESRV_FILE_TOKEN", "file-secret")
	t.Setenv("TELESRV_EGRESS_ACK_TOKEN", "egress-secret")
	t.Setenv("TELESRV_GROUPCALL_CONTROL_TOKEN", "groupcall-secret")
	t.Setenv("TELESRV_SFU_CONTROL_TOKEN", "sfu-secret")
	t.Setenv("TELESRV_POSTGRES_DSN", "postgres://telesrv:telesrv@127.0.0.1:5432/telesrv?sslmode=disable")
	t.Setenv("TELESRV_ADMIN_UI_PASSWORD", "admin-password")
	t.Setenv("TELESRV_ADMIN_UI_TOKEN", "")
	t.Setenv("TELESRV_ADMIN_SESSION_KEY", "admin-session-secret")
	t.Setenv("TELESRV_ADMIN_API_TOKEN", "admin-api-secret")

	exampleDir := filepath.Join("..", "..", "configs", "examples")
	tests := []struct {
		name string
		file string
		load func() error
	}{
		{name: "edge", file: "edge.local.yaml", load: func() error { _, err := LoadEdge(); return err }},
		{name: "core", file: "core.local.yaml", load: func() error { _, err := LoadCore(); return err }},
		{name: "egress", file: "egress.local.yaml", load: func() error { _, err := LoadEgress(); return err }},
		{name: "file", file: "file.local.yaml", load: func() error { _, err := LoadFile(); return err }},
		{name: "sfu", file: "sfu.local.yaml", load: func() error { _, err := LoadSFU(); return err }},
		{name: "admin", file: "admin.local.yaml", load: func() error { _, err := LoadAdmin(); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TELESRV_CONFIG", filepath.Join(exampleDir, tt.file))
			if err := tt.load(); err != nil {
				t.Fatalf("load %s: %v", tt.file, err)
			}
		})
	}
}

func writeRoleYAML(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)+"\n"), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	return path
}
