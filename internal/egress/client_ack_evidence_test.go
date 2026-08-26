package egress

import (
	"context"
	"strings"
	"testing"
	"time"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

type clientAckObservationStore struct {
	store.DurableOutboxStateStore
	recorded      []store.OutboxAttemptEvidence
	finalizeCalls int
}

func (s *clientAckObservationStore) RecordAttemptEvidenceBatch(
	_ context.Context,
	evidence []store.OutboxAttemptEvidence,
) ([]store.OutboxEvidenceResult, error) {
	s.recorded = append(s.recorded, evidence...)
	results := make([]store.OutboxEvidenceResult, len(evidence))
	for i := range evidence {
		results[i] = store.OutboxEvidenceResult{Ref: evidence[i].Ref, Outcome: store.OutboxEvidenceRecorded}
	}
	return results, nil
}

func (s *clientAckObservationStore) FinalizeAttempts(
	context.Context,
	[]store.OutboxFinalizeRequest,
) (store.OutboxFinalizeBatch, error) {
	s.finalizeCalls++
	return store.OutboxFinalizeBatch{}, nil
}

func TestClientAckObservationNeverFinalizesDelivery(t *testing.T) {
	state := &clientAckObservationStore{}
	var batch edgecontrol.BatchID
	var command edgecontrol.CommandID
	batch[0], command[0] = 1, 2
	observation := edgecontrol.ClientAckObservation{
		Tracking: edgecontrol.DeliveryTracking{
			BatchID: batch, CommandID: command, SourceInstanceID: "egress-a",
			Ref: edgecontrol.DeliveryRef{
				Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 41},
				OutboxID: 51, TargetUserID: 41, PTS: 61, LeaseFence: 71, Attempt: 1,
			},
		},
		TargetInstanceID: "edge-a",
		AuthKeyID:        [8]byte{1},
		SessionID:        81,
		ServerMsgID:      91,
		ObservedAt:       time.Now().UTC(),
	}

	results := applyClientAckBatch(context.Background(), deliveryEvidenceStores{dispatch: state}, []edgecontrol.ClientAckObservation{observation})
	if len(results) != 1 || results[0].Outcome != edgecontrol.ClientAckObservationApplied {
		t.Fatalf("results = %+v, want one applied observation", results)
	}
	if len(state.recorded) != 1 || state.recorded[0].Kind != store.OutboxEvidenceClientAck {
		t.Fatalf("recorded evidence = %+v, want one client ACK observation", state.recorded)
	}
	if state.finalizeCalls != 0 {
		t.Fatalf("FinalizeAttempts calls = %d, client ACK must not advance completion", state.finalizeCalls)
	}
}

func TestPhysicalReceiptRejectsInstanceIdentityBeyondWireBound(t *testing.T) {
	receipt := edgecontrol.PhysicalReceipt{
		BatchID: edgecontrol.BatchID{1}, CommandID: edgecontrol.CommandID{2},
		SourceInstanceID: "egress-a", TargetInstanceID: "edge-a",
		Ref: edgecontrol.DeliveryRef{
			Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 41},
			OutboxID: 51, TargetUserID: 41, PTS: 61, LeaseFence: 71, Attempt: 1,
		},
		Outcome: edgecontrol.PhysicalWritten, EligibleSessions: 1, WrittenSessions: 1,
		FirstServerMsgID: 91, ObservedAt: time.Now().UTC(),
	}
	if err := validatePhysicalReceipt(receipt); err != nil {
		t.Fatalf("valid physical receipt rejected: %v", err)
	}
	receipt.SourceInstanceID = strings.Repeat("s", edgecontrol.MaxDeliveryInstanceIDBytes+1)
	if err := validatePhysicalReceipt(receipt); err == nil {
		t.Fatal("oversized physical receipt source instance identity was accepted")
	}
	receipt.SourceInstanceID = "egress-a "
	if err := validatePhysicalReceipt(receipt); err == nil {
		t.Fatal("non-canonical physical receipt source instance identity was accepted")
	}
	receipt.SourceInstanceID = "egress-a"
	receipt.Outcome = edgecontrol.PhysicalIndeterminate
	receipt.WrittenSessions = 0
	if err := validatePhysicalReceipt(receipt); err == nil {
		t.Fatal("indeterminate physical receipt with a server message id was accepted")
	}
}
