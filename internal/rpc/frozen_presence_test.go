package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/domain"
)

const frozenPresenceTestUserID = int64(1009)

// TestFrozenPresenceUpdateStatusNeverGoesOnline locks the invariant that a
// frozen account cannot re-enter the online watermark: account.updateStatus
// online and reconnect renewals must be forced back to offline, and the
// account's own sessions must never receive an online updateUserStatus.
func TestFrozenPresenceUpdateStatusNeverGoesOnline(t *testing.T) {
	userID := frozenPresenceTestUserID
	provider := &frozenGateFreezeProvider{items: map[int64]domain.AccountFreeze{
		userID: frozenGateActiveState(userID),
	}}
	sessions := &captureSessions{}
	r := New(Config{}, Deps{AccountFreeze: provider, Sessions: sessions}, zaptest.NewLogger(t), clock.System)
	ctx := WithSessionID(WithUserID(context.Background(), userID), 22)

	if err := r.NotifyAccountFreezeChanged(ctx, domain.AccountFreeze{UserID: userID, Frozen: true}); err != nil {
		t.Fatalf("NotifyAccountFreezeChanged: %v", err)
	}
	// 冻结前可能残留的在线水位必须被清掉，且显式 updateStatus(online) 不得复活它。
	if ok, err := r.onAccountUpdateStatus(ctx, false); err != nil || !ok {
		t.Fatalf("updateStatus = %v, %v", ok, err)
	}
	if st, ok := r.presence.statusFor(userID, int(time.Now().Unix())); ok && st.Kind == domain.UserStatusOnline {
		t.Fatalf("frozen account tracked online: %+v", st)
	}
	if st := r.userPresenceStatus(userID); st.Kind == domain.UserStatusOnline {
		t.Fatalf("userPresenceStatus(frozen) = %+v, want non-online", st)
	}
	if msg := sessions.lastUserPush(); msg != nil {
		update := pushedUserStatus(t, msg)
		if update.UserID != userID {
			t.Fatalf("pushed status user = %d, want %d", update.UserID, userID)
		}
		if _, ok := update.Status.(*tg.UserStatusOnline); ok {
			t.Fatalf("frozen account pushed online: %+v", update)
		}
	}
}

// TestFrozenPresenceUserStatusNeverOnlineFromProvider locks the read path: even
// when the edge session tracker reports the frozen account online, the presence
// overlay must refuse to render online toward any peer.
func TestFrozenPresenceUserStatusNeverOnlineFromProvider(t *testing.T) {
	userID := frozenPresenceTestUserID
	sessions := &captureSessions{onlineUserIDs: []int64{userID}}
	r := New(Config{}, Deps{Sessions: sessions}, zaptest.NewLogger(t), clock.System)
	r.applyFrozenPresence(userID, true)
	if !sessions.IsUserOnline(userID) {
		t.Fatal("test precondition: provider must report the account online")
	}
	if st := r.userPresenceStatus(userID); st.Kind == domain.UserStatusOnline {
		t.Fatalf("userPresenceStatus(frozen) = %+v, provider overlay leaked online", st)
	}
}

// TestFrozenPresenceNotifyDropsTrackerAndBroadcastsOffline locks the freeze
// transition: pending online presence entries are evicted and the relevant
// peers receive an immediate offline updateUserStatus.
func TestFrozenPresenceNotifyDropsTrackerAndBroadcastsOffline(t *testing.T) {
	userID := frozenPresenceTestUserID
	sessions := &captureSessions{}
	r := New(Config{}, Deps{Sessions: sessions}, zaptest.NewLogger(t), clock.System)
	now := time.Now()
	key := presenceSessionKey{sessionID: 1}
	r.presence.setSessionStatus(key, userID, domain.UserStatus{
		Kind:      domain.UserStatusOnline,
		Expires:   int(now.Add(userOnlineTTL).Unix()),
		WasOnline: int(now.Unix()),
	})
	if err := r.NotifyAccountFreezeChanged(context.Background(), domain.AccountFreeze{
		UserID: userID,
		Frozen: true,
		Since:  now.UTC(),
		Until:  now.Add(24 * time.Hour).UTC(),
	}); err != nil {
		t.Fatalf("NotifyAccountFreezeChanged: %v", err)
	}
	if st, ok := r.presence.statusFor(userID, int(now.Unix())); ok {
		t.Fatalf("frozen presence entry survived freeze: %+v", st)
	}
	if !r.isFrozenPresenceUser(userID) {
		t.Fatal("freeze mask not set")
	}
	msg := sessions.lastUserPush()
	if msg == nil {
		t.Fatal("no offline status broadcast on freeze")
	}
	update := pushedUserStatus(t, msg)
	if update.UserID != userID {
		t.Fatalf("pushed status user = %d, want %d", update.UserID, userID)
	}
	if _, ok := update.Status.(*tg.UserStatusOnline); ok {
		t.Fatalf("freeze broadcast pushed online: %+v", update)
	}
	if _, ok := update.Status.(*tg.UserStatusOffline); !ok {
		t.Fatalf("freeze broadcast status = %T, want offline", update.Status)
	}
}

// TestFrozenPresenceSeedFromDurableFact locks the restart recovery: after a
// process restart the durable notifications have already been consumed, so the
// first session bind must backfill the freeze mask from the store.
func TestFrozenPresenceSeedFromDurableFact(t *testing.T) {
	userID := frozenPresenceTestUserID
	provider := &frozenGateFreezeProvider{items: map[int64]domain.AccountFreeze{
		userID: frozenGateActiveState(userID),
	}}
	r := New(Config{}, Deps{AccountFreeze: provider}, zaptest.NewLogger(t), clock.System)
	r.seedFrozenPresence(context.Background(), userID)
	if !r.isFrozenPresenceUser(userID) {
		t.Fatal("freeze mask not seeded from durable fact")
	}
	// non-frozen account must stay out of the mask
	r.seedFrozenPresence(context.Background(), userID+9)
	if r.isFrozenPresenceUser(userID + 9) {
		t.Fatal("non-frozen account seeded into freeze mask")
	}
}

// TestFrozenPresenceUnfreezeReenablesPresence locks the unfreeze transition:
// the mask is cleared so the account can announce online again.
func TestFrozenPresenceUnfreezeReenablesPresence(t *testing.T) {
	userID := frozenPresenceTestUserID
	r := New(Config{}, Deps{}, zaptest.NewLogger(t), clock.System)
	r.frozenPresence.Store(userID, struct{}{})
	r.applyFrozenPresence(userID, false)
	if r.isFrozenPresenceUser(userID) {
		t.Fatal("account still frozen after unfreeze")
	}
}
