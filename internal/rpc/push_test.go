package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"
)

type ordinaryOnlySessions struct {
	rawAuthKeyID [8]byte
	sessionID    int64
	authKeyID    [8]byte
	authFound    bool
	userID       int64
	userFound    bool
	message      tg.UpdatesClass
	pushUserIDs  []int64
}

type boundedTrapSessions struct {
	ordinaryOnlySessions
	boundedCalls int
}

func (s *boundedTrapSessions) PushToUserExceptAuthKeySessionBounded(context.Context, int64, [8]byte, int64, proto.MessageType, tg.UpdatesClass, time.Duration) (int, error) {
	s.boundedCalls++
	return 99, nil
}

func TestPushUserMessageDoesNotUseBoundedSessionPusher(t *testing.T) {
	sessions := &boundedTrapSessions{}
	router := &Router{
		cfg:  Config{OutboundPushTimeout: time.Second},
		log:  zaptest.NewLogger(t),
		deps: Deps{Sessions: sessions},
	}
	ctx := WithAuthKeyID(WithSessionID(context.Background(), 77), [8]byte{1, 2, 3})

	sent := router.pushUserMessage(ctx, 1000000001, "push ordinary test", &tg.Updates{Date: 1})
	if sent != 1 {
		t.Fatalf("sent = %d, want ordinary push result 1", sent)
	}
	if sessions.boundedCalls != 0 {
		t.Fatalf("bounded calls = %d, want 0 for ordinary push", sessions.boundedCalls)
	}
	if ids := sessions.pushUserIDs; len(ids) != 1 || ids[0] != 1000000001 {
		t.Fatalf("ordinary push target IDs = %v, want [1000000001]", ids)
	}
}

func (s *ordinaryOnlySessions) BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte) {
	s.rawAuthKeyID, s.sessionID, s.authKeyID, s.authFound = rawAuthKeyID, sessionID, authKeyID, true
}

func (s *ordinaryOnlySessions) AuthKeyIDForSession([8]byte, int64) ([8]byte, bool) {
	return s.authKeyID, s.authFound
}

func (s *ordinaryOnlySessions) BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64) {
	s.rawAuthKeyID, s.sessionID, s.userID, s.userFound = rawAuthKeyID, sessionID, userID, true
}

func (s *ordinaryOnlySessions) UserIDResolvedForAuthKey([8]byte, int64) (int64, bool) {
	return s.userID, s.userFound
}

func (s *ordinaryOnlySessions) UnbindAuthKey(authKeyID [8]byte) int {
	if s.authKeyID != authKeyID {
		return 0
	}
	s.userID, s.userFound = 0, true
	return 1
}

func (s *ordinaryOnlySessions) SetReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64, _ bool) {
	s.rawAuthKeyID, s.sessionID = rawAuthKeyID, sessionID
}

func (s *ordinaryOnlySessions) PushToSessionForAuthKey(_ context.Context, rawAuthKeyID [8]byte, sessionID int64, _ proto.MessageType, msg tg.UpdatesClass) error {
	s.rawAuthKeyID, s.sessionID, s.message = rawAuthKeyID, sessionID, msg
	return nil
}

func (s *ordinaryOnlySessions) PushToUserExceptAuthKeySession(_ context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, _ proto.MessageType, msg tg.UpdatesClass) (int, error) {
	s.userID, s.rawAuthKeyID, s.sessionID, s.message = userID, excludeAuthKeyID, excludeSessionID, msg
	s.pushUserIDs = append(s.pushUserIDs, userID)
	return 1, nil
}

func TestPushUserMessageTransientWithoutCapabilityDoesNotFallbackToOrdinaryPush(t *testing.T) {
	sessions := &ordinaryOnlySessions{}
	router := &Router{
		log:  zaptest.NewLogger(t),
		deps: Deps{Sessions: sessions},
	}
	ctx := WithAuthKeyID(WithSessionID(context.Background(), 77), [8]byte{1, 2, 3})

	sent := router.pushUserMessageTransient(ctx, 1000000001, "push transient test", &tg.UpdateShort{
		Update: &tg.UpdateUserTyping{UserID: 1000000002, Action: &tg.SendMessageTypingAction{}},
		Date:   1,
	})
	if sent != 0 {
		t.Fatalf("sent = %d, want 0 without TransientSessionPusher", sent)
	}
	if got := sessions.message; got != nil {
		t.Fatalf("ordinary push fallback captured %#v, want nil", got)
	}
	if ids := sessions.pushUserIDs; len(ids) != 0 {
		t.Fatalf("ordinary push fallback target IDs = %v, want none", ids)
	}
}
