package stargiveaways

import (
	"context"
	"time"

	"telesrv/internal/domain"
)

// EntryStore is the persistence needed to record participation.
type EntryStore interface {
	StarsGiveawayForChannel(ctx context.Context, channelID int64, now int) (domain.StarsGiveawayRun, bool, error)
	EnterStarsGiveaway(ctx context.Context, giveawayID, userID int64, now int) (domain.StarsGiveawayEntry, bool, error)
	StarsGiveawayByID(ctx context.Context, giveawayID int64) (domain.StarsGiveawayRun, error)
	PayStarsGiveawayEntry(ctx context.Context, charge domain.ChargeGiveawayEntry) (domain.StarsGiveawayEntry, bool, error)
	ActiveStarsGiveawayByMessage(ctx context.Context, channelID int64, messageID int, now int) (domain.StarsGiveawayRun, bool, error)
	RefundStarsGiveawayEntry(ctx context.Context, giveawayID, participantUserID, holderUserID, feeStars int64, now int) error
}

// EnterService adapts the draw store to the rpc layer's narrow attribution
// contract.
type EnterService struct {
	store EntryStore
	now   func() time.Time
}

// NewEnterService builds the adapter.
func NewEnterService(store EntryStore, now func() time.Time) *EnterService {
	if now == nil {
		now = time.Now
	}
	return &EnterService{store: store, now: now}
}

// EnterForBoost records userID in the channel's running giveaway.
//
// Returns nil in every case where the boost simply did not participate: no
// giveaway on the channel, the giveaway already ended, or the user already
// entered. None of those are errors from the caller's point of view -- a boost
// that does not enter a giveaway is a perfectly good boost, and the only
// visible outcome is that the user is absent from the draw.
func (s *EnterService) EnterForBoost(ctx context.Context, userID, channelID int64, now int) error {
	if s == nil || s.store == nil || userID <= 0 || channelID <= 0 {
		return nil
	}
	if now <= 0 {
		now = int(s.now().UTC().Unix())
	}

	run, found, err := s.store.StarsGiveawayForChannel(ctx, channelID, now)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if _, _, err := s.store.EnterStarsGiveaway(ctx, run.ID, userID, now); err != nil {
		return err
	}
	return nil
}

// JoinPaid charges userID the flat entry fee and records the entry.
//
// This is the participation path clients actually use. It deliberately does not
// require a boost: layer 229 has no joinGiveaway and premium.applyBoost cannot
// carry a giveaway reference, so "boost the channel" was never a mechanism the
// client could be made to drive. A flat fee is also the honest pricing for a
// draw that someone else is funding.
//
// feeStars <= 0 uses the configured default.
func (s *EnterService) JoinPaid(ctx context.Context, giveawayID, userID int64, feeStars int64) (domain.StarsGiveawayEntry, error) {
	if s == nil || s.store == nil {
		return domain.StarsGiveawayEntry{}, domain.ErrGiveawayNotFound
	}
	run, err := s.store.StarsGiveawayByID(ctx, giveawayID)
	if err != nil {
		return domain.StarsGiveawayEntry{}, err
	}
	if run.State != domain.StarsGiveawayActive {
		return domain.StarsGiveawayEntry{}, domain.ErrGiveawayNotActive
	}
	if feeStars <= 0 {
		feeStars = domain.StarsGiveawayEntryFeeStars
	}

	entry, _, err := s.store.PayStarsGiveawayEntry(ctx, domain.ChargeGiveawayEntry{
		ParticipantUserID: userID,
		// The holder is the giveaway's buyer: the account that paid the prize
		// pool at launch and therefore receives the entry fee.
		HolderUserID: run.BuyerUserID,
		GiveawayID:   giveawayID,
		ChannelID:    run.ChannelID,
		FeeStars:     feeStars,
		Date:         int(s.now().UTC().Unix()),
	})
	if err != nil {
		return domain.StarsGiveawayEntry{}, err
	}
	return entry, nil
}

// ActiveGiveawayByMessage reports the running giveaway launched by a channel
// message, which is how the rpc layer decides whether a reply is an entry.
func (s *EnterService) ActiveGiveawayByMessage(ctx context.Context, channelID int64, messageID int, now int) (domain.StarsGiveawayRun, bool, error) {
	if s == nil || s.store == nil || channelID <= 0 || messageID <= 0 {
		return domain.StarsGiveawayRun{}, false, nil
	}
	if now <= 0 {
		now = int(s.now().UTC().Unix())
	}
	return s.store.ActiveStarsGiveawayByMessage(ctx, channelID, messageID, now)
}

// RefundEntry reverses a charged entry whose message never made it out.
//
// It returns the fee to the participant and takes it back from the holder, so
// the ledger stays conserved exactly as it was for a charge that never
// happened. The entry row is deleted rather than marked refunded, because the
// user never participated: leaving the row would put a non-payer into the draw.
func (s *EnterService) RefundEntry(ctx context.Context, giveawayID, participantUserID, feeStars int64) error {
	if s == nil || s.store == nil {
		return nil
	}
	if feeStars <= 0 {
		feeStars = domain.StarsGiveawayEntryFeeStars
	}
	run, err := s.store.StarsGiveawayByID(ctx, giveawayID)
	if err != nil {
		return err
	}
	return s.store.RefundStarsGiveawayEntry(ctx, giveawayID, participantUserID, run.BuyerUserID, feeStars, int(s.now().UTC().Unix()))
}
