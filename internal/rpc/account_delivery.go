package rpc

import (
	"context"

	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

func (r *Router) requireProfileDelivery(userID int64, method string) error {
	return r.requireAccountDelivery(userID, method)
}

func (r *Router) requireAccountDelivery(userID int64, method string) error {
	if r.deps.DeliveryOutbox != nil {
		return nil
	}
	r.log.Error("account mutation requires durable delivery outbox",
		zap.String("method", method),
		zap.Int64("user_id", userID),
	)
	return internalErr()
}

func deliveryExclusionFromContext(ctx context.Context) ([8]byte, int64) {
	sessionID, _ := SessionIDFrom(ctx)
	authKeyID := rawAuthKeyIDForOrigin(ctx)
	if sessionID == 0 || authKeyID == ([8]byte{}) {
		return [8]byte{}, 0
	}
	return authKeyID, sessionID
}

func (r *Router) usernameDeliveryPayloadBuilder(ctx context.Context) store.UserDeliveryPayloadBuilder {
	return func(snapshot store.UserDeliverySnapshot) ([]byte, error) {
		u := snapshot.User
		pushedSelf := r.tgSelfUserFromDeliverySnapshot(ctx, snapshot)
		usernames := tgUsernames(u.Username)
		if vector, ok := pushedSelf.GetUsernames(); ok && len(vector) > 0 {
			usernames = vector
		}
		return edgecontrol.EncodeOutboxUpdate(&tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateUserName{
				UserID:    u.ID,
				FirstName: u.FirstName,
				LastName:  u.LastName,
				Usernames: usernames,
			}},
			Users: []tg.UserClass{pushedSelf},
			Date:  int(r.clock.Now().Unix()),
		})
	}
}

func (r *Router) selfUserRefreshDeliveryPayloadBuilder(ctx context.Context) store.UserDeliveryPayloadBuilder {
	return func(snapshot store.UserDeliverySnapshot) ([]byte, error) {
		u := snapshot.User
		return edgecontrol.EncodeOutboxUpdate(&tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateUser{UserID: u.ID}},
			Users:   []tg.UserClass{r.tgSelfUserFromDeliverySnapshot(ctx, snapshot)},
			Date:    int(r.clock.Now().Unix()),
		})
	}
}

func (r *Router) privacyDeliveryPayloadBuilder() store.PrivacyDeliveryPayloadBuilder {
	return func(rules domain.PrivacyRules) ([]byte, error) {
		return edgecontrol.EncodeOutboxUpdate(&tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdatePrivacy{
				Key:   tgPrivacyKey(rules.Key),
				Rules: tgPrivacyRules(rules.Rules),
			}},
			Users: []tg.UserClass{},
			Chats: []tg.ChatClass{},
			Date:  int(r.clock.Now().Unix()),
			Seq:   0,
		})
	}
}

func (r *Router) tgSelfUserFromDeliverySnapshot(ctx context.Context, snapshot store.UserDeliverySnapshot) *tg.User {
	if !snapshot.UsernamesLoaded {
		return r.tgSelfUserWithUsernames(ctx, snapshot.User)
	}
	self := r.tgSelfUser(snapshot.User)
	applyUsernamesFromRegistry(
		[]tg.UserClass{self},
		nil,
		map[domain.Peer][]domain.Username{
			{Type: domain.PeerTypeUser, ID: snapshot.User.ID}: snapshot.Usernames,
		},
	)
	return self
}
