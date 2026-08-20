package coreexec

import "time"

// Metrics observes the CoreExec gRPC boundary with fixed-cardinality labels.
// Callers must not put addresses, auth keys, session IDs, request IDs, or user
// supplied error text into side/operation/outcome.
type Metrics interface {
	CoreExecGRPCCall(side, operation, outcome string, d time.Duration)
	CoreExecPendingAdmissionRejected(transport, reason string)
}

// NopMetrics is the zero-cost CoreExec metrics implementation.
type NopMetrics struct{}

func (NopMetrics) CoreExecGRPCCall(string, string, string, time.Duration) {}
func (NopMetrics) CoreExecPendingAdmissionRejected(string, string)        {}

func coreExecMetrics(metrics Metrics) Metrics {
	if metrics == nil {
		return NopMetrics{}
	}
	return metrics
}

func observeCoreExecGRPCCall(metrics Metrics, side, operation string, start time.Time, outcome string) {
	if metrics == nil {
		return
	}
	if outcome == "" {
		outcome = "unknown"
	}
	metrics.CoreExecGRPCCall(side, operation, outcome, time.Since(start))
}

func observeCoreExecPendingAdmissionRejected(metrics Metrics, transport, reason string) {
	if metrics == nil {
		return
	}
	if transport == "" {
		transport = "unknown"
	}
	if reason == "" {
		reason = "unknown"
	}
	metrics.CoreExecPendingAdmissionRejected(transport, reason)
}
