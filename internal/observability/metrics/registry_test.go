package metrics

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"telesrv/internal/coreexec"
	"telesrv/internal/mtprotoedge"
	"telesrv/internal/rpc"
)

var (
	_ mtprotoedge.Metrics                 = (*Registry)(nil)
	_ mtprotoedge.RPCResultMetrics        = (*Registry)(nil)
	_ mtprotoedge.LogicalOutboxMetrics    = (*Registry)(nil)
	_ mtprotoedge.ConnectionIntakeMetrics = (*Registry)(nil)
	_ coreexec.Metrics                    = (*Registry)(nil)
	_ rpc.Metrics                         = (*Registry)(nil)
)

func TestRegistryExportsBoundedAggregateMetrics(t *testing.T) {
	registry := New()
	registry.maxSeries = 2
	registry.RPCHandled("help.getConfig", 5*time.Millisecond, nil)
	registry.RPCHandled("users.getUsers", time.Second, errors.New("secret auth_key_id=deadbeef session=123"))

	recorder := httptest.NewRecorder()
	registry.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	if recorder.Code != 200 {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if !strings.Contains(body, "telesrv_mtproto_rpc_handled_total") || !strings.Contains(body, "telesrv_mtproto_rpc_duration_seconds_bucket") {
		t.Fatalf("expected RPC counter and histogram, got:\n%s", body)
	}
	if strings.Contains(body, "deadbeef") || strings.Contains(body, "session=123") {
		t.Fatalf("raw error identity leaked into metrics:\n%s", body)
	}
	if got := registry.series.Load(); got != registry.maxSeries {
		t.Fatalf("resident dynamic series = %d, want cap %d", got, registry.maxSeries)
	}
	if got := registry.dropped.Load(); got == 0 {
		t.Fatal("series overflow was not reported")
	}
	if !strings.Contains(body, "telesrv_metrics_dropped_observations_total 2") {
		t.Fatalf("overflow counter missing from:\n%s", body)
	}
}

func TestRegistryExportsCoreExecGRPCMetrics(t *testing.T) {
	registry := New()
	registry.CoreExecGRPCCall("client", "dispatch_admitted", "ok", 5*time.Millisecond)
	registry.CoreExecGRPCCall("server", "get_info", "version_mismatch", 7*time.Millisecond)

	recorder := httptest.NewRecorder()
	registry.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, `telesrv_coreexec_grpc_calls_total{side="client",operation="dispatch_admitted",outcome="ok"} 1`) {
		t.Fatalf("client CoreExec metric missing:\n%s", body)
	}
	if !strings.Contains(body, `telesrv_coreexec_grpc_call_seconds_bucket{side="server",operation="get_info",outcome="version_mismatch"`) {
		t.Fatalf("server CoreExec histogram missing:\n%s", body)
	}
}

func TestRegistryExportsCoreExecPendingAdmissionMetrics(t *testing.T) {
	registry := New()
	registry.CoreExecPendingAdmissionRejected("grpc", "capacity")
	registry.AddGaugeProvider(func() []GaugeSample {
		return []GaugeSample{
			{Name: "telesrv_coreexec_pending_admissions", Labels: []Label{{Name: "transport", Value: "grpc"}}, Value: 7},
			{Name: "telesrv_coreexec_pending_admission_capacity", Labels: []Label{{Name: "transport", Value: "grpc"}}, Value: 8192},
			{Name: "telesrv_coreexec_pending_admission_oldest_age_seconds", Labels: []Label{{Name: "transport", Value: "grpc"}}, Value: 1.5},
			{Name: "telesrv_coreexec_pending_admission_ttl_seconds", Labels: []Label{{Name: "transport", Value: "grpc"}}, Value: 120},
		}
	})

	recorder := httptest.NewRecorder()
	registry.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, `telesrv_coreexec_pending_admission_rejected_total{transport="grpc",reason="capacity"} 1`) {
		t.Fatalf("pending admission reject counter missing:\n%s", body)
	}
	if !strings.Contains(body, `telesrv_coreexec_pending_admissions{transport="grpc"} 7`) {
		t.Fatalf("pending admission gauge missing:\n%s", body)
	}
	if !strings.Contains(body, `telesrv_coreexec_pending_admission_capacity{transport="grpc"} 8192`) {
		t.Fatalf("pending admission capacity gauge missing:\n%s", body)
	}
	if !strings.Contains(body, `telesrv_coreexec_pending_admission_oldest_age_seconds{transport="grpc"} 1.5`) {
		t.Fatalf("pending admission oldest-age gauge missing:\n%s", body)
	}
	if !strings.Contains(body, `telesrv_coreexec_pending_admission_ttl_seconds{transport="grpc"} 120`) {
		t.Fatalf("pending admission ttl gauge missing:\n%s", body)
	}
}

func TestRegistrySanitizesAndBoundsProviderSamples(t *testing.T) {
	registry := New()
	registry.AddGaugeProvider(func() []GaugeSample {
		return []GaugeSample{{
			Name:   "9 invalid metric",
			Labels: []Label{{Name: "bad:label", Value: "quoted\"\nvalue"}},
			Value:  3,
		}}
	})
	recorder := httptest.NewRecorder()
	registry.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, `__invalid_metric{bad_label="quoted\"\nvalue"} 3`) {
		t.Fatalf("provider sample was not safely sanitized:\n%s", body)
	}
}

func TestErrorOutcomeHasFixedCardinality(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{nil, "ok"},
		{errors.New("FLOOD_WAIT_1 for phone 123"), "flood_wait"},
		{errors.New("global capacity exceeded for auth key"), "edge_overload"},
		{errors.New("arbitrary user-controlled failure"), "error"},
	}
	for _, test := range tests {
		if got := errorOutcome(test.err); got != test.want {
			t.Errorf("errorOutcome(%v) = %q, want %q", test.err, got, test.want)
		}
	}
}
