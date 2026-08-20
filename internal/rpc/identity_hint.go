package rpc

import (
	"context"

	"telesrv/internal/rpcidentity"
)

type layerRPCIdentityHintKey struct{}

func (r *Router) WithLayerRPCIdentityHint(ctx context.Context, hint rpcidentity.LayerRPCIdentityHint) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, layerRPCIdentityHintKey{}, hint)
}

func layerRPCIdentityHintFromContext(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) (rpcidentity.LayerRPCIdentityHint, bool) {
	if ctx == nil {
		return rpcidentity.LayerRPCIdentityHint{}, false
	}
	hint, ok := ctx.Value(layerRPCIdentityHintKey{}).(rpcidentity.LayerRPCIdentityHint)
	if !ok || hint.RawAuthKeyID != rawAuthKeyID || hint.SessionID != sessionID {
		return rpcidentity.LayerRPCIdentityHint{}, false
	}
	return hint, true
}

func (r *Router) permanentAuthKeyIDFromIdentityHint(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) ([8]byte, bool) {
	hint, ok := layerRPCIdentityHintFromContext(ctx, rawAuthKeyID, sessionID)
	if !ok ||
		!hint.BusinessAuthKeyResolved ||
		hint.BusinessAuthKeyID != rawAuthKeyID ||
		!hint.RawAuthKeyExpiresAtResolved ||
		hint.RawAuthKeyExpiresAt != 0 {
		return [8]byte{}, false
	}
	return rawAuthKeyID, true
}

func (r *Router) cachedTempAuthKeyIDFromIdentityHint(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) ([8]byte, bool) {
	if r == nil || r.cfg.TempKeyResolveCacheTTL <= 0 {
		return [8]byte{}, false
	}
	hint, ok := layerRPCIdentityHintFromContext(ctx, rawAuthKeyID, sessionID)
	if !ok ||
		!hint.BusinessAuthKeyResolved ||
		hint.BusinessAuthKeyID == rawAuthKeyID ||
		hint.BusinessAuthKeyID == ([8]byte{}) {
		return [8]byte{}, false
	}
	return r.tempKeyResolveCache.Get(rawAuthKeyID, hint.BusinessAuthKeyID, r.clock.Now())
}

func (r *Router) userIDFromIdentityHint(ctx context.Context, rawAuthKeyID, authKeyID [8]byte, sessionID int64) (int64, bool) {
	hint, ok := layerRPCIdentityHintFromContext(ctx, rawAuthKeyID, sessionID)
	if !ok ||
		!hint.BusinessAuthKeyResolved ||
		hint.BusinessAuthKeyID != authKeyID ||
		!hint.UserIDResolved ||
		hint.UserID == 0 {
		return 0, false
	}
	cachedUserID, ok := r.positiveCachedAuthUser(authKeyID)
	if !ok || cachedUserID != hint.UserID {
		return 0, false
	}
	return hint.UserID, true
}
