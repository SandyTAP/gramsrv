package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
)

// SetChannelUnderReview puts a channel into moderator review, or clears it.
//
// until is an absolute unix timestamp; 0 means "no deadline", which is what an
// operator who omits the field gets. The write is a single UPDATE guarded on the
// current state so two operators clicking at once cannot half-apply.
//
// The reason is the text viewers will be shown, so it is required and length
// checked before it reaches the database: a channel blocked with no explanation
// is indistinguishable from a bug, and that complaint costs more than the check.
func (s *ChannelStore) SetChannelUnderReview(ctx context.Context, channelID int64, reason string, until, now int) (domain.ChannelReview, error) {
	if channelID <= 0 {
		return domain.ChannelReview{}, domain.ErrChannelInvalid
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return domain.ChannelReview{}, domain.ErrChannelReviewReasonRequired
	}
	if err := domain.ValidateChannelReview(reason, until, now); err != nil {
		return domain.ChannelReview{}, err
	}

	if until == 0 {
		// Clearing: until==0 with no reason clears, otherwise it would store a
		// review the CHECK forbids.
		if _, err := s.db.Exec(ctx, `
            UPDATE channels
            SET under_review_started_at = 0,
                under_review_until     = 0,
                under_review_reason    = ''
            WHERE id = $1 AND under_review_started_at > 0`, channelID); err != nil {
			return domain.ChannelReview{}, fmt.Errorf("clear channel review: %w", err)
		}
		return domain.ChannelReview{ChannelID: channelID}, nil
	}

	review := domain.ChannelReview{ChannelID: channelID, Reason: reason, Until: until, StartedAt: now}
	if _, err := s.db.Exec(ctx, `
        UPDATE channels
        SET under_review_started_at = $2,
            under_review_until     = $3,
            under_review_reason    = $4
        WHERE id = $1`,
		channelID, now, until, reason); err != nil {
		return domain.ChannelReview{}, fmt.Errorf("set channel review: %w", err)
	}
	return review, nil
}

// ClearChannelUnderReview lifts a review so the channel's posts become readable
// again.
func (s *ChannelStore) ClearChannelUnderReview(ctx context.Context, channelID int64, now int) (domain.ChannelReview, error) {
	return s.SetChannelUnderReview(ctx, channelID, "", 0, now)
}

// ChannelUnderReview reports the review state of one channel.
//
// Returns found=false for a channel that exists but is not under review, which
// is the common case and must stay cheap: this sits on the hot path of every
// history read.
func (s *ChannelStore) ChannelUnderReview(ctx context.Context, channelID int64, now int) (domain.ChannelReview, bool, error) {
	if channelID <= 0 {
		return domain.ChannelReview{}, false, nil
	}
	var started, until int
	var reason string
	err := s.db.QueryRow(ctx, `
        SELECT under_review_started_at, under_review_until, under_review_reason
        FROM channels
        WHERE id = $1`, channelID).Scan(&started, &until, &reason)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ChannelReview{}, false, nil
		}
		return domain.ChannelReview{}, false, fmt.Errorf("read channel review: %w", err)
	}
	review := domain.ChannelReview{ChannelID: channelID, Reason: reason, Until: until, StartedAt: started}
	if !review.Active(now) {
		return domain.ChannelReview{}, false, nil
	}
	return review, true, nil
}
