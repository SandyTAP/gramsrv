package domain

import (
	"errors"
	"strings"
)

// Channel review: a channel is put under moderator review, its posts stop being
// readable to ordinary viewers, and the channel explains why with a service
// message. The profile, member list and boost counters stay visible, so the
// channel does not simply vanish -- a viewer who already had it in their chats
// is told what happened instead of finding it gone.
const (
	// MaxChannelReviewReasonRunes bounds the text shown to viewers. It is read
	// off the hot path of every history render, so it is capped rather than
	// trusted.
	MaxChannelReviewReasonRunes = 200
	// MaxChannelReviewDays bounds an explicit deadline. A review that lasts
	// years is not a review.
	MaxChannelReviewDays = 30
)

var (
	// ErrChannelReviewReasonRequired reports a review with nothing to show the
	// viewer. Without a reason the channel would be blocked with no explanation,
	// which is indistinguishable from a bug.
	ErrChannelReviewReasonRequired = errors.New("channel review reason is required")
	// ErrChannelReviewUntilInvalid reports a deadline that is not after the
	// moment the review starts.
	ErrChannelReviewUntilInvalid = errors.New("channel review deadline is invalid")
)

// ChannelReview is the moderation state of one channel.
type ChannelReview struct {
	ChannelID int64
	// Reason is what the viewer is shown on the channel's service message.
	Reason string
	// Until is an absolute unix timestamp; zero means no deadline and the
	// channel stays under review until an operator clears it.
	Until int
	// StartedAt is when the review began.
	StartedAt int
}

// Active reports whether the review is in force at now.
//
// A deadline is compared rather than swept, so expiry costs nothing and works
// even if the server was down when it expired: the very next read sees a
// channel that is no longer blocked.
func (r ChannelReview) Active(now int) bool {
	if r.StartedAt <= 0 || r.Reason == "" {
		return false
	}
	return r.Until == 0 || r.Until > now
}

// ValidateChannelReview checks an operator's request before it is written.
func ValidateChannelReview(reason string, until, now int) error {
	if now <= 0 {
		now = 1
	}
	// Empty is rejected here as well as in the store: the table's CHECK also
	// refuses it, and a validator that let a blank reason through would only
	// surface the failure later as a constraint violation.
	if strings.TrimSpace(reason) == "" {
		return ErrChannelReviewReasonRequired
	}
	if len([]rune(reason)) > MaxChannelReviewReasonRunes {
		return ErrChannelReviewReasonRequired
	}
	// A deadline in the past would mark the channel blocked and unblock it in
	// the same comparison, which reads as a no-op with a confusing audit entry.
	if until != 0 && until <= now {
		return ErrChannelReviewUntilInvalid
	}
	if until > 0 {
		if maxUntil := now + MaxChannelReviewDays*24*60*60; until > maxUntil {
			return ErrChannelReviewUntilInvalid
		}
	}
	return nil
}
