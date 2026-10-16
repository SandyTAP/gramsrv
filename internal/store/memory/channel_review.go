package memory

import (
	"context"
	"strings"

	"telesrv/internal/domain"
)

// SetChannelUnderReview records a channel's review state in memory.
//
// The in-memory store backs tests and single-process development, so expiry is
// evaluated on read rather than by a sweeper, exactly as the SQL store does.
// That keeps a test that writes a deadline and advances the clock honest.
func (s *ChannelStore) SetChannelUnderReview(_ context.Context, channelID int64, reason string, until, now int) (domain.ChannelReview, error) {
	if channelID <= 0 {
		return domain.ChannelReview{}, domain.ErrChannelInvalid
	}
	reason = strings.TrimSpace(reason)

	s.reviewMu.Lock()
	defer s.reviewMu.Unlock()
	if reason == "" {
		delete(s.reviews, channelID)
		return domain.ChannelReview{ChannelID: channelID}, nil
	}
	if err := domain.ValidateChannelReview(reason, until, now); err != nil {
		return domain.ChannelReview{}, err
	}
	review := domain.ChannelReview{ChannelID: channelID, Reason: reason, Until: until, StartedAt: now}
	s.reviews[channelID] = review
	return review, nil
}

// ChannelUnderReview reports the review state and whether it is in force at now.
func (s *ChannelStore) ChannelUnderReview(_ context.Context, channelID int64, now int) (domain.ChannelReview, bool, error) {
	if channelID <= 0 {
		return domain.ChannelReview{}, false, nil
	}
	s.reviewMu.RLock()
	review, ok := s.reviews[channelID]
	s.reviewMu.RUnlock()
	if !ok || !review.Active(now) {
		return domain.ChannelReview{}, false, nil
	}
	return review, true, nil
}
