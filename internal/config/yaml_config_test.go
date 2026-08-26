package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
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
  delivery:
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

func TestLoadCoreReadsCounterRecoveryPoolSizeFromYAML(t *testing.T) {
	path := writeRoleYAML(t, "core-postgres.yaml", `
version: 1
postgres:
  dsn: postgres://core-test
  max_conns: 11
  min_conns: 3
  counter_recovery_max_conns: 2
`)
	t.Setenv("TELESRV_CONFIG", path)
	t.Setenv("TELESRV_POSTGRES_COUNTER_RECOVERY_MAX_CONNS", "99")

	cfg, err := LoadCore()
	if err != nil {
		t.Fatalf("LoadCore: %v", err)
	}
	if cfg.PostgresDSN != "postgres://core-test" || cfg.PostgresMaxConns != 11 || cfg.PostgresMinConns != 3 || cfg.PostgresCounterRecoveryMaxConns != 2 {
		t.Fatalf("postgres config = dsn:%q max:%d min:%d recovery:%d", cfg.PostgresDSN, cfg.PostgresMaxConns, cfg.PostgresMinConns, cfg.PostgresCounterRecoveryMaxConns)
	}
}

func TestLoadCoreReadsStarGiftsYAMLWithoutProcessEnvOverride(t *testing.T) {
	path := writeRoleYAML(t, "core.yaml", `
version: 1
star_gifts:
  ton_starting_grant: 42
  sweep_interval: 17s
  sweep_batch: 23
  transfer_stars: 31
  drop_original_details_stars: 37
  offer_min_stars: 41
  stars_proceeds_permille: 950
  ton_proceeds_permille: 925
  export_delay: 1m
  transfer_delay: 2m
  resell_delay: 3m
  craft_delay: 4m
  craft_chance_permille: 275
  export:
    mode: disabled
`)
	t.Setenv("TELESRV_CONFIG", path)
	t.Setenv("TELESRV_STARGIFT_EXPORT_MODE", StarGiftExportModeTON)
	t.Setenv("TELESRV_STARGIFT_SWEEP_BATCH", "9999")

	cfg, err := LoadCore()
	if err != nil {
		t.Fatalf("LoadCore: %v", err)
	}
	if cfg.StarGiftExportMode != StarGiftExportModeDisabled || cfg.StarGiftTONStartingGrant != 42 ||
		cfg.StarGiftSweepInterval != 17*time.Second || cfg.StarGiftSweepBatch != 23 {
		t.Fatalf("star gift mode/grant/sweep = %q/%d/%s/%d", cfg.StarGiftExportMode, cfg.StarGiftTONStartingGrant, cfg.StarGiftSweepInterval, cfg.StarGiftSweepBatch)
	}
	if cfg.StarGiftTransferStars != 31 || cfg.StarGiftDropOriginalDetailsStars != 37 || cfg.StarGiftOfferMinStars != 41 ||
		cfg.StarGiftStarsProceedsPermille != 950 || cfg.StarGiftTONProceedsPermille != 925 {
		t.Fatalf("star gift fees = transfer:%d drop:%d offer:%d stars:%d ton:%d", cfg.StarGiftTransferStars, cfg.StarGiftDropOriginalDetailsStars, cfg.StarGiftOfferMinStars, cfg.StarGiftStarsProceedsPermille, cfg.StarGiftTONProceedsPermille)
	}
	if cfg.StarGiftExportDelay != time.Minute || cfg.StarGiftTransferDelay != 2*time.Minute ||
		cfg.StarGiftResellDelay != 3*time.Minute || cfg.StarGiftCraftDelay != 4*time.Minute || cfg.StarGiftCraftChancePermille != 275 {
		t.Fatalf("star gift lifecycle = export:%s transfer:%s resell:%s craft:%s chance:%d", cfg.StarGiftExportDelay, cfg.StarGiftTransferDelay, cfg.StarGiftResellDelay, cfg.StarGiftCraftDelay, cfg.StarGiftCraftChancePermille)
	}
}

func TestLoadCoreDefaultsStarGiftExportToLocalAndRejectsInvalidMode(t *testing.T) {
	path := writeRoleYAML(t, "core-default.yaml", "version: 1")
	t.Setenv("TELESRV_CONFIG", path)
	cfg, err := LoadCore()
	if err != nil {
		t.Fatalf("LoadCore default: %v", err)
	}
	if cfg.StarGiftExportMode != StarGiftExportModeLocal {
		t.Fatalf("StarGiftExportMode = %q, want local", cfg.StarGiftExportMode)
	}

	path = writeRoleYAML(t, "core-invalid.yaml", `
version: 1
star_gifts:
  export:
    mode: fragment
`)
	t.Setenv("TELESRV_CONFIG", path)
	if _, err := LoadCore(); err == nil || !strings.Contains(err.Error(), "must be local, ton or disabled") {
		t.Fatalf("LoadCore invalid mode err = %v", err)
	}
}

func TestLoadCoreRequiresTONExportAdmissionOnlyInTONMode(t *testing.T) {
	path := writeRoleYAML(t, "core-ton.yaml", `
version: 1
star_gifts:
  export:
    mode: ton
    ton:
      network: mainnet
      collection_address: 0:collection
      collection_code_hash: collection-code-hash
      mint_abi: basic-collection-v1
      initial_item_index: "17"
      proof_domain: gifts.example
      capability_secret_file: secrets/ton-capability.key
      allow_user_ids: [29, 31]
      ttl: 45m
      challenge_ttl: 3m
      finalizer:
        batch: 7
        poll_interval: 4s
        lease_timeout: 40s
        request_timeout: 12s
        retry_delay: 6s
      claim:
        enabled: true
        bot_token_file: secrets/ton-claim-bot.token
        init_data_ttl: 20m
`)
	t.Setenv("TELESRV_CONFIG", path)
	cfg, err := LoadCore()
	if err != nil {
		t.Fatalf("LoadCore TON: %v", err)
	}
	if cfg.StarGiftTONNetwork != TONNetworkMainnet || cfg.StarGiftTONCollectionCodeHash != "collection-code-hash" ||
		cfg.StarGiftTONMintABI != "basic-collection-v1" || cfg.StarGiftTONInitialItemIndex != "17" ||
		cfg.StarGiftTONExportTTL != 45*time.Minute || cfg.StarGiftTONChallengeTTL != 3*time.Minute ||
		cfg.StarGiftTONFinalizerBatch != 7 || cfg.StarGiftTONFinalizerPollInterval != 4*time.Second ||
		cfg.StarGiftTONFinalizerLeaseTimeout != 40*time.Second || cfg.StarGiftTONFinalizerRequestTimeout != 12*time.Second ||
		cfg.StarGiftTONFinalizerRetryDelay != 6*time.Second || !cfg.StarGiftTONClaimEnabled ||
		cfg.StarGiftTONClaimBotTokenFile != "secrets/ton-claim-bot.token" || cfg.StarGiftTONClaimInitDataTTL != 20*time.Minute ||
		!reflect.DeepEqual(cfg.StarGiftTONAllowUserIDs, []int64{29, 31}) {
		t.Fatalf("TON export config = network:%q index:%q ttl:%s/%s", cfg.StarGiftTONNetwork,
			cfg.StarGiftTONInitialItemIndex, cfg.StarGiftTONExportTTL, cfg.StarGiftTONChallengeTTL)
	}

	path = writeRoleYAML(t, "core-ton-missing.yaml", `
version: 1
star_gifts:
  export:
    mode: ton
`)
	t.Setenv("TELESRV_CONFIG", path)
	if _, err := LoadCore(); err == nil || !strings.Contains(err.Error(), "capability secret") {
		t.Fatalf("LoadCore incomplete TON err = %v", err)
	}

	path = writeRoleYAML(t, "core-ton-invalid-allowlist.yaml", `
version: 1
star_gifts:
  export:
    ton:
      allow_user_ids: [7, 0]
`)
	t.Setenv("TELESRV_CONFIG", path)
	if _, err := LoadCore(); err == nil || !strings.Contains(err.Error(), "positive user IDs") {
		t.Fatalf("LoadCore invalid TON allowlist err = %v", err)
	}
}

func TestLoadTONReadsStrictYAMLAndDefaultsMintOff(t *testing.T) {
	path := writeRoleYAML(t, "ton.yaml", `
version: 1
instance:
  id: ton-test-a
postgres:
  dsn: ${TEST_TON_POSTGRES_DSN}
ton:
  network: mainnet
  lite_servers:
    - address: 1.2.3.4:19949
      public_key: lite-server-key
  trusted_block:
    workchain: -1
    shard: -9223372036854775808
    seqno: 100
    root_hash: root-hash
    file_hash: file-hash
  collection:
    address: EQCollection
    code_hash: collection-code-hash
    mint_abi: basic-collection-v1
    initial_item_index: "17"
  relayer:
    wallet_version: v4r2
  worker:
    poll_interval: 3s
`)
	t.Setenv("TELESRV_CONFIG", path)
	t.Setenv("TEST_TON_POSTGRES_DSN", "postgres://ton")
	t.Setenv("TELESRV_POSTGRES_DSN", "must-not-win")

	cfg, err := LoadTON()
	if err != nil {
		t.Fatalf("LoadTON: %v", err)
	}
	if cfg.PostgresDSN != "postgres://ton" || cfg.InstanceID != "ton-test-a" || cfg.Network != TONNetworkMainnet ||
		cfg.MintABI != "basic-collection-v1" || cfg.InitialItemIndex != "17" {
		t.Fatalf("unexpected TON identity: dsn=%q instance=%q network=%q", cfg.PostgresDSN, cfg.InstanceID, cfg.Network)
	}
	if cfg.MintEnabled || cfg.RelayerMnemonicFile != "" {
		t.Fatalf("mint defaults = enabled:%v mnemonic:%q, want reconciliation-only", cfg.MintEnabled, cfg.RelayerMnemonicFile)
	}
	if cfg.WorkerBatch != 10 || cfg.PollInterval != 3*time.Second || cfg.LeaseTimeout != 2*time.Minute || cfg.RequestTimeout != 90*time.Second {
		t.Fatalf("worker defaults = batch:%d poll:%s lease:%s request:%s", cfg.WorkerBatch, cfg.PollInterval, cfg.LeaseTimeout, cfg.RequestTimeout)
	}
}

func TestLoadTONRejectsUnknownAndUnsafeConfig(t *testing.T) {
	base := `
version: 1
instance:
  id: ton-test-a
postgres:
  dsn: postgres://ton
ton:
  network: mainnet
  lite_servers:
    - address: 1.2.3.4:19949
      public_key: lite-server-key
  trusted_block:
    workchain: -1
    shard: -9223372036854775808
    seqno: 100
    root_hash: root-hash
    file_hash: file-hash
  collection:
    address: EQCollection
    code_hash: collection-code-hash
    mint_abi: basic-collection-v1
    initial_item_index: "17"
  relayer:
    wallet_version: v4r2
`

	path := writeRoleYAML(t, "ton-unknown.yaml", base+"  typo_field: true\n")
	t.Setenv("TELESRV_CONFIG", path)
	if _, err := LoadTON(); err == nil || !strings.Contains(err.Error(), "typo_field") {
		t.Fatalf("LoadTON unknown field err = %v", err)
	}

	path = writeRoleYAML(t, "ton-unsupported-abi.yaml", strings.Replace(base,
		"mint_abi: basic-collection-v1", "mint_abi: custom-v1", 1))
	t.Setenv("TELESRV_CONFIG", path)
	if _, err := LoadTON(); err == nil || !strings.Contains(err.Error(), "basic-collection-v1") {
		t.Fatalf("LoadTON unsupported mint ABI err = %v", err)
	}

	path = writeRoleYAML(t, "ton-enabled.yaml", base+`  mint:
    enabled: true
`)
	t.Setenv("TELESRV_CONFIG", path)
	if _, err := LoadTON(); err == nil || !strings.Contains(err.Error(), "mnemonic_file") {
		t.Fatalf("LoadTON enabled without mnemonic err = %v", err)
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
	t.Setenv("TELESRV_EGRESS_DELIVERY_TOKEN", "egress-secret")
	t.Setenv("TELESRV_GROUPCALL_CONTROL_TOKEN", "groupcall-secret")
	t.Setenv("TELESRV_SFU_CONTROL_TOKEN", "sfu-secret")
	t.Setenv("TELESRV_POSTGRES_DSN", "postgres://telesrv:telesrv@127.0.0.1:5432/telesrv?sslmode=disable")
	t.Setenv("TELESRV_ADMIN_UI_PASSWORD", "admin-password")
	t.Setenv("TELESRV_ADMIN_UI_TOKEN", "")
	t.Setenv("TELESRV_ADMIN_SESSION_KEY", "admin-session-secret")
	t.Setenv("TELESRV_ADMIN_API_TOKEN", "admin-api-secret")
	t.Setenv("TELESRV_TON_LITESERVER_PUBLIC_KEY", "test-lite-server-key")
	t.Setenv("TELESRV_TON_COLLECTION_ADDRESS", "EQTestCollection")
	t.Setenv("TELESRV_TON_COLLECTION_CODE_HASH", "test-collection-code-hash")
	t.Setenv("TELESRV_TON_TRUSTED_BLOCK_SEQNO", "1")
	t.Setenv("TELESRV_TON_TRUSTED_BLOCK_ROOT_HASH", strings.Repeat("00", 32))
	t.Setenv("TELESRV_TON_TRUSTED_BLOCK_FILE_HASH", strings.Repeat("11", 32))
	t.Setenv("TELESRV_PUBLIC_IP", "203.0.113.10")
	t.Setenv("TELESRV_PUBLIC_HOST", "gifts.example")
	t.Setenv("TELESRV_WEB_HOST", "web.gifts.example")
	t.Setenv("TELESRV_REDIS_ADDR", "127.0.0.1:6379")
	t.Setenv("TELESRV_REDIS_PASSWORD", "redis-secret")
	t.Setenv("TELESRV_TON_INITIAL_ITEM_INDEX", "1000")
	t.Setenv("TELESRV_TON_CAPABILITY_SECRET_FILE", "secrets/ton-capability.key")
	t.Setenv("TELESRV_TON_CLAIM_BOT_TOKEN_FILE", "secrets/ton-claim-bot.token")
	t.Setenv("TELESRV_TON_LITESERVER_1_ADDRESS", "1.2.3.4:19949")
	t.Setenv("TELESRV_TON_LITESERVER_1_PUBLIC_KEY", "mainnet-lite-key-1")
	t.Setenv("TELESRV_TON_LITESERVER_2_ADDRESS", "5.6.7.8:19949")
	t.Setenv("TELESRV_TON_LITESERVER_2_PUBLIC_KEY", "mainnet-lite-key-2")
	t.Setenv("TELESRV_TON_MNEMONIC_FILE", "secrets/ton-relayer.mnemonic")

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
		{name: "ton", file: "ton.local.yaml", load: func() error { _, err := LoadTON(); return err }},
		{name: "core-ton-mainnet", file: "core.ton-mainnet.yaml", load: func() error { _, err := LoadCore(); return err }},
		{name: "ton-mainnet", file: "ton.mainnet.yaml", load: func() error { _, err := LoadTON(); return err }},
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

func TestLoadDockerRoleYAMLFiles(t *testing.T) {
	t.Setenv("TELESRV_INSTANCE_ID", "compose-test-1")
	t.Setenv("TELESRV_POSTGRES_DSN", "postgres://telesrv:secret@postgres:5432/telesrv_v2?sslmode=disable")
	t.Setenv("TELESRV_REDIS_PASSWORD", "redis-secret")
	t.Setenv("TELESRV_CORE_EXEC_TOKEN", "core-secret")
	t.Setenv("TELESRV_FILE_TOKEN", "file-secret")
	t.Setenv("TELESRV_EGRESS_DELIVERY_TOKEN", "egress-secret")
	t.Setenv("TELESRV_GROUPCALL_CONTROL_TOKEN", "group-call-secret")
	t.Setenv("TELESRV_SFU_CONTROL_TOKEN", "sfu-secret")
	t.Setenv("TELESRV_DEV_AUTH_CODE", "918274")
	t.Setenv("TELESRV_PHONE_CODE_DELIVERY_PROVIDER", "development")
	t.Setenv("TELESRV_OTP_WEBHOOK_URL", "https://otp.example.test/v1/deliveries")
	t.Setenv("TELESRV_OTP_WEBHOOK_SECRET", "otp-secret")
	t.Setenv("TELESRV_OTP_WEBHOOK_TIMEOUT", "4s")
	t.Setenv("TELESRV_ADVERTISE_IP", "203.0.113.10")
	t.Setenv("TELESRV_ADVERTISE_PORT", "2398")
	t.Setenv("TELESRV_SFU_UDP_PORT", "12399")
	t.Setenv("TELESRV_DEFAULT_COUNTRY_CODE", "CN")
	t.Setenv("TELESRV_PUBLIC_BASE_URL", "https://example.test")
	t.Setenv("TELESRV_PUBLIC_APP_SCHEME", "telesrv")
	t.Setenv("TELESRV_PUBLIC_WEB_BASE_URL", "https://web.example.test")
	t.Setenv("TELESRV_BLOB_BACKEND", "localfs")
	t.Setenv("TELESRV_EXTERNAL_MEDIA_ENABLE", "true")
	t.Setenv("TELESRV_WEBPAGE_PREVIEW_ENABLE", "true")
	t.Setenv("TELESRV_S3_USE_SSL", "true")
	t.Setenv("TELESRV_S3_PATH_STYLE", "false")
	t.Setenv("TELESRV_S3_CREATE_BUCKET", "false")

	dockerConfigDir := filepath.Join("..", "..", "deploy", "docker", "config")
	tests := []struct {
		name string
		file string
		load func() error
	}{
		{name: "edge", file: "edge.yaml", load: func() error {
			cfg, err := LoadEdge()
			if err == nil && (cfg.InstanceID != "compose-test-1" || cfg.CoreExecGRPCTargets != "core:2440" || cfg.CoreExecGRPCResolver != "dns" || cfg.FileGRPCTargets != "file:2520" || cfg.FileGRPCResolver != "dns" || cfg.EgressDeliveryGRPCTargets != "egress:2510" || cfg.EgressDeliveryGRPCResolver != "dns") {
				return fmt.Errorf("unexpected Docker edge identity or service targets")
			}
			return err
		}},
		{name: "core", file: "core.yaml", load: func() error {
			cfg, err := LoadCore()
			if err == nil && (cfg.InstanceID != "compose-test-1" || cfg.CoreExecGRPCAddr != "0.0.0.0:2440" || cfg.FileGRPCTargets != "file:2520" || cfg.FileGRPCResolver != "dns" || cfg.LangPackSeedDir != "/usr/share/telesrv/langpack" || cfg.OTPWebhookURL != "https://otp.example.test/v1/deliveries" || cfg.OTPWebhookSecret != "otp-secret" || cfg.OTPWebhookTimeout != 4*time.Second) {
				return fmt.Errorf("unexpected Docker core identity, listeners, or seed path")
			}
			return err
		}},
		{name: "egress", file: "egress.yaml", load: func() error {
			cfg, err := LoadEgress()
			if err == nil && (cfg.InstanceID != "compose-test-1" || cfg.EgressDeliveryGRPCAddr != "0.0.0.0:2510" || cfg.DefaultCountryCode != "CN" || cfg.PublicBaseURL != "https://example.test" || cfg.PublicAppScheme != "telesrv" || cfg.DeliveryAttemptTimeout != 2*time.Second || cfg.DeliveryClockSkewAllowance != time.Second) {
				return fmt.Errorf("unexpected Docker egress identity or listener")
			}
			return err
		}},
		{name: "file", file: "file.yaml", load: func() error {
			cfg, err := LoadFile()
			if err == nil && (cfg.InstanceID != "compose-test-1" || cfg.FileGRPCAddr != "0.0.0.0:2520" || cfg.BlobDir != "/var/lib/telesrv-file/blobs") {
				return fmt.Errorf("unexpected Docker file identity, listener, or blob path")
			}
			return err
		}},
		{name: "sfu", file: "sfu.yaml", load: func() error {
			cfg, err := LoadSFU()
			if err == nil && (cfg.InstanceID != "compose-test-1" || cfg.SFUControlGRPCAddr != "0.0.0.0:2450" || cfg.SFUControlGRPCURL != "grpc://compose-test-1:2450" || cfg.GroupCallControlURL != "http://core:2420") {
				return fmt.Errorf("unexpected Docker SFU identity or control endpoint")
			}
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TELESRV_CONFIG", filepath.Join(dockerConfigDir, tt.file))
			if err := tt.load(); err != nil {
				t.Fatalf("load Docker %s: %v", tt.file, err)
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
