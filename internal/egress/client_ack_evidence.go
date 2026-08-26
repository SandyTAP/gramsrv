package egress

import (
	"context"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

func applyClientAckBatch(
	ctx context.Context,
	stores deliveryEvidenceStores,
	observations []edgecontrol.ClientAckObservation,
) []edgecontrol.ClientAckObservationResult {
	results := make([]edgecontrol.ClientAckObservationResult, len(observations))
	type queueBatch struct {
		store   store.DurableOutboxStateStore
		indices []int
		items   []store.OutboxAttemptEvidence
	}
	groups := make(map[store.OutboxQueueKind]*queueBatch, 3)
	for i, observation := range observations {
		stateStore, queueKind, ok := stores.forEdgeKind(observation.Tracking.Ref.Domain.Kind)
		ref, refOK := storeAttemptRef(observation.Tracking.Ref)
		if !ok || !refOK || !observation.Valid() {
			results[i] = edgecontrol.ClientAckObservationResult{
				Outcome: edgecontrol.ClientAckObservationRejected, Detail: edgecontrol.DetailInvalidIdentity,
			}
			continue
		}
		group := groups[queueKind]
		if group == nil {
			group = &queueBatch{store: stateStore}
			groups[queueKind] = group
		}
		group.indices = append(group.indices, i)
		group.items = append(group.items, store.OutboxAttemptEvidence{
			Ref: ref, Kind: store.OutboxEvidenceClientAck,
			SourceInstanceID: observation.Tracking.SourceInstanceID,
			TargetInstanceID: observation.TargetInstanceID,
			TargetUserID:     observation.Tracking.Ref.TargetUserID,
			BatchID:          [16]byte(observation.Tracking.BatchID), CommandID: [16]byte(observation.Tracking.CommandID),
			AuthKeyID: observation.AuthKeyID, SessionID: observation.SessionID,
			ServerMsgID: observation.ServerMsgID, ObservedAt: observation.ObservedAt,
		})
	}
	for _, kind := range [...]store.OutboxQueueKind{
		store.OutboxQueueDispatchPTS, store.OutboxQueueAbsoluteDelivery, store.OutboxQueueChannelPTS,
	} {
		group := groups[kind]
		if group == nil {
			continue
		}
		applyClientAckEvidenceGroup(ctx, group.store, group.indices, group.items, results)
	}
	return results
}

func applyClientAckEvidenceGroup(
	ctx context.Context,
	stateStore store.DurableOutboxStateStore,
	indices []int,
	evidence []store.OutboxAttemptEvidence,
	results []edgecontrol.ClientAckObservationResult,
) {
	if stateStore == nil {
		markClientAckResultsRetryable(indices, results)
		return
	}
	recorded, err := stateStore.RecordAttemptEvidenceBatch(ctx, evidence)
	if err != nil || len(recorded) != len(evidence) {
		markClientAckResultsRetryable(indices, results)
		return
	}
	for i, result := range recorded {
		index := indices[i]
		switch result.Outcome {
		case store.OutboxEvidenceRecorded, store.OutboxEvidenceDuplicate:
			results[index] = edgecontrol.ClientAckObservationResult{Outcome: edgecontrol.ClientAckObservationApplied}
		case store.OutboxEvidenceAlreadyFinalized:
			results[index] = edgecontrol.ClientAckObservationResult{Outcome: edgecontrol.ClientAckObservationApplied}
		case store.OutboxEvidenceFenced:
			results[index] = edgecontrol.ClientAckObservationResult{
				Outcome: edgecontrol.ClientAckObservationStale, Detail: edgecontrol.DetailAttemptConflict,
			}
		case store.OutboxEvidenceRejected:
			results[index] = edgecontrol.ClientAckObservationResult{
				Outcome: edgecontrol.ClientAckObservationRejected, Detail: edgecontrol.DetailAttemptConflict,
			}
		default:
			results[index] = edgecontrol.ClientAckObservationResult{Outcome: edgecontrol.ClientAckObservationRetryable}
		}
	}
	// Client ACK is diagnostic corroboration only. Physical writer evidence is
	// the sole completion gate, so an ACK must never invoke finalization.
}

func markClientAckResultsRetryable(indices []int, results []edgecontrol.ClientAckObservationResult) {
	for _, index := range indices {
		results[index] = edgecontrol.ClientAckObservationResult{Outcome: edgecontrol.ClientAckObservationRetryable}
	}
}
