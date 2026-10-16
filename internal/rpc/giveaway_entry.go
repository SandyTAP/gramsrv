package rpc

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

// giveawayEntryForSend charges a user for answering a giveaway post.
//
// Layer 229 has no joinGiveaway, so the trigger has to be something a client
// genuinely sends: an ordinary message that replies to the giveaway's launch
// post. Anything else in the channel is free.
//
// The fee is charged BEFORE the message is sent, and refunded if the send then
// fails. That order is the one that keeps the economics honest:
//
//   - Charging first means a user who cannot afford the entry never gets an
//     uncosted message posted, which would make the draw free for everyone who
//     is short on stars.
//   - Refunding on failure means a send rejected by a rate limit or a
//     permissions check does not leave a participant who posted nothing.
//
// Returns a refund function that is a no-op when nothing was charged.
func (r *Router) giveawayEntryForSend(ctx context.Context, userID int64, peer domain.Peer, replyTo *domain.MessageReply) (func(sendFailed bool), error) {
	noop := func(bool) {}
	if r.deps.Giveaways == nil || userID == 0 {
		return noop, nil
	}
	if peer.Type != domain.PeerTypeChannel || replyTo == nil || replyTo.MessageID <= 0 {
		return noop, nil
	}

	now := int(r.clock.Now().Unix())
	run, found, err := r.deps.Giveaways.ActiveGiveawayByMessage(ctx, peer.ID, replyTo.MessageID, now)
	if err != nil {
		// A giveaway lookup failure must not block an ordinary message.
		r.log.Warn("stars.giveaway.lookup_failed",
			zap.Int64("channel_id", peer.ID), zap.Int("reply_to_msg_id", replyTo.MessageID), zap.Error(err))
		return noop, nil
	}
	if !found {
		return noop, nil
	}

	if _, err := r.deps.Giveaways.JoinPaid(ctx, run.ID, userID, 0); err != nil {
		if errors.Is(err, domain.ErrStarsInsufficient) {
			return noop, balanceTooLowErr()
		}
		if errors.Is(err, domain.ErrGiveawaySelfEntry) {
			return noop, nil
		}
		if errors.Is(err, domain.ErrGiveawayNotActive) || errors.Is(err, domain.ErrGiveawayEnded) {
			return noop, nil
		}
		r.log.Warn("stars.giveaway.entry_failed",
			zap.Int64("channel_id", peer.ID), zap.Int64("giveaway_id", run.ID),
			zap.Int64("user_id", userID), zap.Error(err))
		return noop, nil
	}

	charged := true
	return func(sendFailed bool) {
		if !charged || !sendFailed {
			return
		}
		charged = false
		if err := r.deps.Giveaways.RefundEntry(ctx, run.ID, userID, domain.StarsGiveawayEntryFeeStars); err != nil {
			// The user keeps a free entry and the holder keeps the fee. Logged
			// loudly because it is a real, if small, accounting loss.
			r.log.Error("stars.giveaway.entry_refund_failed",
				zap.Int64("channel_id", peer.ID), zap.Int64("giveaway_id", run.ID),
				zap.Int64("user_id", userID), zap.Error(err))
		}
		charged = false
	}, nil
}
