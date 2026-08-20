package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/iamxvbaba/td/clock"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/rpcidentity"
)

func TestIdentityHintPermanentAuthKeySkipsDurableResolve(t *testing.T) {
	authKeyID := [8]byte{0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21}
	const sessionID int64 = 2101
	auth := &captureAuthService{}
	r := New(Config{}, Deps{Auth: auth}, zaptest.NewLogger(t), clock.System)
	ctx := r.WithLayerRPCIdentityHint(context.Background(), rpcidentity.LayerRPCIdentityHint{
		RawAuthKeyID:                authKeyID,
		SessionID:                   sessionID,
		BusinessAuthKeyID:           authKeyID,
		BusinessAuthKeyResolved:     true,
		RawAuthKeyExpiresAt:         0,
		RawAuthKeyExpiresAtResolved: true,
	})

	got, err := r.effectiveAuthKeyID(ctx, authKeyID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != authKeyID {
		t.Fatalf("effective auth key = %x, want raw %x", got, authKeyID)
	}
	if auth.resolveCount != 0 {
		t.Fatalf("ResolveAuthKey calls = %d, want 0", auth.resolveCount)
	}
}

func TestIdentityHintTemporaryRawKeyStillUsesDurableResolveWithoutCoreCache(t *testing.T) {
	tempAuthKeyID := [8]byte{0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22}
	permAuthKeyID := [8]byte{0x23, 0x23, 0x23, 0x23, 0x23, 0x23, 0x23, 0x23}
	const sessionID int64 = 2102
	auth := &captureAuthService{resolvedAuthKeyID: permAuthKeyID, hasResolved: true}
	r := New(Config{TempKeyResolveCacheTTL: time.Minute}, Deps{Auth: auth}, zaptest.NewLogger(t), clock.System)
	ctx := r.WithLayerRPCIdentityHint(context.Background(), rpcidentity.LayerRPCIdentityHint{
		RawAuthKeyID:                tempAuthKeyID,
		SessionID:                   sessionID,
		BusinessAuthKeyID:           permAuthKeyID,
		BusinessAuthKeyResolved:     true,
		RawAuthKeyExpiresAt:         int(time.Now().Add(time.Hour).Unix()),
		RawAuthKeyExpiresAtResolved: true,
	})

	got, err := r.effectiveAuthKeyID(ctx, tempAuthKeyID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != permAuthKeyID {
		t.Fatalf("effective auth key = %x, want perm %x", got, permAuthKeyID)
	}
	if auth.resolveCount != 1 {
		t.Fatalf("ResolveAuthKey calls = %d, want durable resolve", auth.resolveCount)
	}
}

func TestIdentityHintTemporaryRawKeyCanUseCoreResolveCache(t *testing.T) {
	tempAuthKeyID := [8]byte{0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24, 0x24}
	permAuthKeyID := [8]byte{0x25, 0x25, 0x25, 0x25, 0x25, 0x25, 0x25, 0x25}
	const sessionID int64 = 2103
	now := time.Unix(1700002103, 0)
	auth := &captureAuthService{}
	r := New(Config{TempKeyResolveCacheTTL: time.Minute}, Deps{Auth: auth}, zaptest.NewLogger(t), fixedClock{now: now})
	r.tempKeyResolveCache.Store(tempAuthKeyID, permAuthKeyID, now.Add(time.Minute), now)
	ctx := r.WithLayerRPCIdentityHint(context.Background(), rpcidentity.LayerRPCIdentityHint{
		RawAuthKeyID:                tempAuthKeyID,
		SessionID:                   sessionID,
		BusinessAuthKeyID:           permAuthKeyID,
		BusinessAuthKeyResolved:     true,
		RawAuthKeyExpiresAt:         int(now.Add(time.Hour).Unix()),
		RawAuthKeyExpiresAtResolved: true,
	})

	got, err := r.effectiveAuthKeyID(ctx, tempAuthKeyID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != permAuthKeyID {
		t.Fatalf("effective auth key = %x, want cached perm %x", got, permAuthKeyID)
	}
	if auth.resolveCount != 0 {
		t.Fatalf("ResolveAuthKey calls = %d, want 0 from Core temp cache", auth.resolveCount)
	}
}

func TestIdentityHintUserRequiresPositiveCoreAuthCache(t *testing.T) {
	authKeyID := [8]byte{0x26, 0x26, 0x26, 0x26, 0x26, 0x26, 0x26, 0x26}
	const (
		sessionID = int64(2104)
		userID    = int64(1000002104)
	)
	hint := rpcidentity.LayerRPCIdentityHint{
		RawAuthKeyID:                authKeyID,
		SessionID:                   sessionID,
		BusinessAuthKeyID:           authKeyID,
		BusinessAuthKeyResolved:     true,
		UserID:                      userID,
		UserIDResolved:              true,
		RawAuthKeyExpiresAt:         0,
		RawAuthKeyExpiresAtResolved: true,
	}

	t.Run("without core cache rechecks durable authorization", func(t *testing.T) {
		auth := &captureAuthService{userID: userID}
		r := New(Config{}, Deps{Auth: auth}, zaptest.NewLogger(t), clock.System)
		ctx := r.WithLayerRPCIdentityHint(context.Background(), hint)
		gotUserID, found, err := r.effectiveUserID(ctx, authKeyID, authKeyID, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		if gotUserID != userID || !found {
			t.Fatalf("effective user = %d/%v, want %d/true", gotUserID, found, userID)
		}
		if auth.userIDCount != 1 {
			t.Fatalf("Auth.UserID calls = %d, want durable lookup", auth.userIDCount)
		}
	})

	t.Run("with core cache skips durable authorization", func(t *testing.T) {
		auth := &captureAuthService{}
		r := New(Config{}, Deps{Auth: auth}, zaptest.NewLogger(t), clock.System)
		r.setAuthUserCache(authKeyID, userID, true)
		ctx := r.WithLayerRPCIdentityHint(context.Background(), hint)
		gotUserID, found, err := r.effectiveUserID(ctx, authKeyID, authKeyID, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		if gotUserID != userID || !found {
			t.Fatalf("effective user = %d/%v, want %d/true", gotUserID, found, userID)
		}
		if auth.userIDCount != 0 {
			t.Fatalf("Auth.UserID calls = %d, want 0 from positive Core cache", auth.userIDCount)
		}
	})
}
