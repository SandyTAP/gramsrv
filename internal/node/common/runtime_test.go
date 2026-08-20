package common

import (
	"testing"
	"time"

	"telesrv/internal/coreexec"
)

func TestCoreExecPendingAdmissionGaugeSamples(t *testing.T) {
	samples := CoreExecPendingAdmissionGaugeSamples("grpc", coreexec.PendingAdmissionStats{
		Count:     3,
		Capacity:  8192,
		TTL:       2 * time.Minute,
		OldestAge: 1500 * time.Millisecond,
	})
	got := map[string]float64{}
	for _, sample := range samples {
		if len(sample.Labels) != 1 || sample.Labels[0].Name != "transport" || sample.Labels[0].Value != "grpc" {
			t.Fatalf("sample %s labels = %+v, want transport=grpc", sample.Name, sample.Labels)
		}
		got[sample.Name] = sample.Value
	}
	want := map[string]float64{
		"telesrv_coreexec_pending_admissions":                   3,
		"telesrv_coreexec_pending_admission_capacity":           8192,
		"telesrv_coreexec_pending_admission_oldest_age_seconds": 1.5,
		"telesrv_coreexec_pending_admission_ttl_seconds":        120,
	}
	for name, value := range want {
		if got[name] != value {
			t.Fatalf("%s = %v, want %v; samples=%+v", name, got[name], value, samples)
		}
	}
}
