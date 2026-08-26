package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestRPCInnerHandledLogLevelGate(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	router := New(Config{DC: 2, IP: "127.0.0.1", Port: 2398}, Deps{}, zap.New(core), clock.System)

	var success bin.Buffer
	if err := (&tg.HelpGetConfigRequest{}).Encode(&success); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Dispatch(context.Background(), [8]byte{1}, 2, &success); err != nil {
		t.Fatal(err)
	}
	if got := logs.FilterMessage("RPC inner handled").Len(); got != 0 {
		t.Fatalf("fast successful request emitted %d Info logs, want none with Debug disabled", got)
	}

	var failed bin.Buffer
	if err := (&tg.UploadGetWebFileRequest{
		Location: &tg.InputWebFileLocation{URL: "https://example.invalid/file", AccessHash: 1},
		Offset:   0,
		Limit:    1,
	}).Encode(&failed); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Dispatch(WithUserID(context.Background(), 42), [8]byte{1}, 2, &failed); err == nil {
		t.Fatal("upload.getWebFile without file service unexpectedly succeeded")
	}
	entries := logs.FilterMessage("RPC inner handled").All()
	if len(entries) != 1 {
		t.Fatalf("failed request emitted %d inner-handler logs, want one", len(entries))
	}
	if entries[0].Level != zapcore.InfoLevel {
		t.Fatalf("failed request level = %s, want Info", entries[0].Level)
	}
	fields := entries[0].ContextMap()
	if fields["method"] != "upload.getWebFile" || fields["error"] == nil {
		t.Fatalf("failed request observability fields = %v", fields)
	}
}
