package postgres

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"telesrv/deploy"
)

// headMigrationVersion 从嵌入的迁移脚本推导期望版本号。硬编码版本号每加一条迁移
// 就会失效（本用例曾长期停留在 185），因此以 deploy.Migrations 为唯一事实来源。
func headMigrationVersion(t *testing.T) uint {
	t.Helper()
	entries, err := deploy.Migrations.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read embedded migrations: %v", err)
	}
	var head uint
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		prefix, _, ok := strings.Cut(name, "_")
		if !ok {
			continue
		}
		version, err := strconv.ParseUint(prefix, 10, 32)
		if err != nil {
			continue
		}
		if uint(version) > head {
			head = uint(version)
		}
	}
	if head == 0 {
		t.Fatal("no embedded up migrations found")
	}
	return head
}

func TestStarGiftLifecycleMigrationsApply(t *testing.T) {
	dsn := os.Getenv("TELESRV_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TELESRV_TEST_POSTGRES_DSN to run postgres integration test")
	}
	head := headMigrationVersion(t)
	status, err := MigrateAndStatus(dsn)
	if err != nil {
		t.Fatalf("migrate star gift lifecycle schema: %v", err)
	}
	if status.Dirty || status.Empty || status.Version != head {
		t.Fatalf("migration status = %+v, want clean version %d", status, head)
	}
}
