package coreexec

import (
	"context"
	"time"

	"telesrv/internal/coreexec/coreexecpb"
)

func (r *GRPCRemote) ResolveNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) (layer int, msgID int64, found bool, err error) {
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.ResolveNegotiatedSessionLayer(callCtx, &coreexecpb.AuthKeyRequest{
		RawAuthKeyId: append([]byte(nil), rawAuthKeyID[:]...),
		SessionId:    sessionID,
	})
	if err != nil {
		callErr := grpcRemoteCallError("resolve_negotiated_session_layer", err)
		observeCoreExecGRPCCall(r.metrics, "client", "resolve_negotiated_session_layer", start, grpcRemoteCallOutcome(callErr))
		return 0, 0, false, callErr
	}
	if err := errorFromResponse(res.GetError()); err != nil {
		observeCoreExecGRPCCall(r.metrics, "client", "resolve_negotiated_session_layer", start, grpcRemoteCallOutcome(err))
		return 0, 0, false, err
	}
	observeCoreExecGRPCCall(r.metrics, "client", "resolve_negotiated_session_layer", start, "ok")
	return int(res.GetLayer()), res.GetMsgId(), res.GetFound(), nil
}

func (r *GRPCRemote) ResolveInheritedAuthKeyLayer(ctx context.Context, rawAuthKeyID [8]byte) (layer int, found bool, err error) {
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.ResolveInheritedAuthKeyLayer(callCtx, &coreexecpb.AuthKeyRequest{
		RawAuthKeyId: append([]byte(nil), rawAuthKeyID[:]...),
	})
	if err != nil {
		callErr := grpcRemoteCallError("resolve_inherited_auth_key_layer", err)
		observeCoreExecGRPCCall(r.metrics, "client", "resolve_inherited_auth_key_layer", start, grpcRemoteCallOutcome(callErr))
		return 0, false, callErr
	}
	if err := errorFromResponse(res.GetError()); err != nil {
		observeCoreExecGRPCCall(r.metrics, "client", "resolve_inherited_auth_key_layer", start, grpcRemoteCallOutcome(err))
		return 0, false, err
	}
	observeCoreExecGRPCCall(r.metrics, "client", "resolve_inherited_auth_key_layer", start, "ok")
	return int(res.GetLayer()), res.GetFound(), nil
}

func (r *GRPCRemote) AdvanceNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, layer int, msgID int64) (currentLayer int, currentMsgID int64, publishShared bool, err error) {
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.AdvanceNegotiatedSessionLayer(callCtx, &coreexecpb.AuthKeyRequest{
		RawAuthKeyId: append([]byte(nil), rawAuthKeyID[:]...),
		SessionId:    sessionID,
		Layer:        int32(layer),
		MsgId:        msgID,
	})
	if err != nil {
		callErr := grpcRemoteCallError("advance_negotiated_session_layer", err)
		observeCoreExecGRPCCall(r.metrics, "client", "advance_negotiated_session_layer", start, grpcRemoteCallOutcome(callErr))
		return 0, 0, false, callErr
	}
	if err := errorFromResponse(res.GetError()); err != nil {
		observeCoreExecGRPCCall(r.metrics, "client", "advance_negotiated_session_layer", start, grpcRemoteCallOutcome(err))
		return 0, 0, false, err
	}
	observeCoreExecGRPCCall(r.metrics, "client", "advance_negotiated_session_layer", start, "ok")
	return int(res.GetCurrentLayer()), res.GetCurrentMsgId(), res.GetPublishShared(), nil
}

func (r *GRPCRemote) DeleteNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) (deleted bool, err error) {
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.DeleteNegotiatedSessionLayer(callCtx, &coreexecpb.AuthKeyRequest{
		RawAuthKeyId: append([]byte(nil), rawAuthKeyID[:]...),
		SessionId:    sessionID,
	})
	if err != nil {
		callErr := grpcRemoteCallError("delete_negotiated_session_layer", err)
		observeCoreExecGRPCCall(r.metrics, "client", "delete_negotiated_session_layer", start, grpcRemoteCallOutcome(callErr))
		return false, callErr
	}
	if err := errorFromResponse(res.GetError()); err != nil {
		observeCoreExecGRPCCall(r.metrics, "client", "delete_negotiated_session_layer", start, grpcRemoteCallOutcome(err))
		return false, err
	}
	observeCoreExecGRPCCall(r.metrics, "client", "delete_negotiated_session_layer", start, "ok")
	return res.GetDeleted(), nil
}

func (r *GRPCRemote) PublishAdmittedLayerProfileEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, safeFloor uint64, layer int) error {
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.PublishLayerEvidence(callCtx, &coreexecpb.PublishLayerEvidenceRequest{
		RawAuthKeyId: append([]byte(nil), rawAuthKeyID[:]...),
		SessionId:    sessionID,
		MsgId:        msgID,
		AdmissionSeq: admissionSeq,
		SafeFloor:    safeFloor,
		Layer:        int32(layer),
	})
	if err != nil {
		callErr := grpcRemoteCallError("publish_layer_evidence", err)
		observeCoreExecGRPCCall(r.metrics, "client", "publish_layer_evidence", start, grpcRemoteCallOutcome(callErr))
		return callErr
	}
	callErr := errorFromResponse(res.GetError())
	observeCoreExecGRPCCall(r.metrics, "client", "publish_layer_evidence", start, grpcRemoteCallOutcome(callErr))
	return callErr
}

func (r *GRPCRemote) ObserveInitConnection(ctx context.Context, authKeyID [8]byte, sessionID int64, layer, apiID int, deviceModel, systemVersion, appVersion, systemLangCode, langPack, langCode string) error {
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.ObserveInitConnection(callCtx, &coreexecpb.ObserveInitConnectionRequest{
		AuthKeyId:      append([]byte(nil), authKeyID[:]...),
		SessionId:      sessionID,
		Layer:          int32(layer),
		ApiId:          int32(apiID),
		DeviceModel:    deviceModel,
		SystemVersion:  systemVersion,
		AppVersion:     appVersion,
		SystemLangCode: systemLangCode,
		LangPack:       langPack,
		LangCode:       langCode,
	})
	if err != nil {
		callErr := grpcRemoteCallError("observe_init_connection", err)
		observeCoreExecGRPCCall(r.metrics, "client", "observe_init_connection", start, grpcRemoteCallOutcome(callErr))
		return callErr
	}
	callErr := errorFromResponse(res.GetError())
	observeCoreExecGRPCCall(r.metrics, "client", "observe_init_connection", start, grpcRemoteCallOutcome(callErr))
	return callErr
}
