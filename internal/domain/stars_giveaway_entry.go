package domain

import "errors"

// StarsGiveawayEntryFeeStars is what one entry costs the participant.
//
// Participation here is paid rather than boost-based: layer 229 has no
// joinGiveaway and premium.applyBoost carries no giveaway reference, so a
// boost is not a usable participation mechanism. The participant pays a flat
// fee and the channel holder's prize pool is funded separately at launch.
const StarsGiveawayEntryFeeStars int64 = 10

var (
	// ErrGiveawayEntryFee reports a failed entry charge.
	ErrGiveawayEntryFee = errors.New("giveaway entry fee failed")
	// ErrGiveawaySelfEntry reports a participant trying to enter their own
	// giveaway. The holder paid the prize and must not also win it back.
	ErrGiveawaySelfEntry = errors.New("giveaway holder cannot enter their own giveaway")
)

// ChargeGiveawayEntry is the money side of one entry.
type ChargeGiveawayEntry struct {
	ParticipantUserID int64
	HolderUserID      int64
	GiveawayID        int64
	ChannelID         int64
	FeeStars          int64
	Date              int
}

// ValidateChargeGiveawayEntry rejects an entry that must not be charged.
//
// The self-entry check lives here rather than in the SQL so that a caller which
// forgets it cannot debit the holder's own prize back to the holder, and so the
// rule is pinned by unit tests instead of by whoever remembers the WHERE clause.
func ValidateGiveawayEntryCharge(ch ChargeGiveawayEntry) error {
	if ch.ParticipantUserID <= 0 || ch.GiveawayID <= 0 || ch.ChannelID <= 0 {
		return ErrGiveawayWinnersInvalid
	}
	if ch.FeeStars <= 0 {
		return ErrGiveawayWinnersInvalid
	}
	if ch.ParticipantUserID == ch.HolderUserID {
		return ErrGiveawaySelfEntry
	}
	return nil
}
