package rpc

import (
	"context"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

// ensureChannelReadableForViewer refuses a channel that is under moderator
// review, unless the viewer runs the channel.
//
// The refusal is what produces the plaque the owner asked for: clients render
// their own standard "this channel can't be displayed" banner for this error, so
// no bespoke markup has to travel over the wire and every client shows it the
// way it already knows how.
//
// Administrators and moderators are exempt on purpose. An operator who cannot
// read what they are moderating cannot moderate it, and an owner who suddenly
// loses access to their own channel will file a bug rather than a moderation
// request. A review that has passed its deadline is already inactive by the
// time this runs, so a temporary review lifts itself with no extra machinery.
func (r *Router) ensureChannelReadableForViewer(ctx context.Context, userID, channelID int64) error {
	if r.deps.Channels == nil {
		return nil
	}
	review, active, err := r.deps.Channels.ChannelUnderReview(ctx, channelID, int(r.clock.Now().Unix()))
	if err != nil {
		// A failed review lookup must not block ordinary reading. Treating a
		// moderation check as a gate on every history read would turn a
		// database hiccup into a platform-wide outage for channels.
		return nil
	}
	if !active {
		return nil
	}
	if r.channelViewerIsStaff(ctx, userID, channelID) {
		return nil
	}
	r.log.Debug("channel hidden by review",
		zap.Int64("channel_id", channelID), zap.Int64("user_id", userID),
		zap.Int("until", review.Until))
	return tgerr400("CHANNEL_RESTRICTED")
}

// channelViewerIsStaff reports whether the viewer administers the channel.
func (r *Router) channelViewerIsStaff(ctx context.Context, userID, channelID int64) bool {
	view, err := r.deps.Channels.GetChannel(ctx, userID, channelID)
	if err != nil {
		return false
	}
	switch view.Self.Role {
	case domain.ChannelRoleCreator, domain.ChannelRoleAdmin:
		return true
	}
	return view.Self.AdminRights.ChangeInfo || view.Self.AdminRights.PostMessages
}
