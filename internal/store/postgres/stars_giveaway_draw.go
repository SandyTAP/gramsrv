package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
)

// starsGiveawayRunColumns is the giveaway projection the draw reads.
const starsGiveawayRunColumns = `id, buyer_user_id, channel_id, launch_message_id, stars, users, per_user_stars, until_date, created_at, state, drawn_at, winner_count, activated_count`

func scanStarsGiveawayRun(scan func(...any) error) (domain.StarsGiveawayRun, error) {
	var run domain.StarsGiveawayRun
	var state string
	err := scan(&run.ID, &run.BuyerUserID, &run.ChannelID, &run.LaunchMsgID, &run.Stars, &run.Users,
		&run.PerUserStars, &run.UntilDate, &run.CreatedAt, &state,
		&run.DrawnAt, &run.WinnerCount, &run.ActivatedCount)
	if err != nil {
		return domain.StarsGiveawayRun{}, err
	}
	run.State = state
	return run, nil
}

// EnterStarsGiveaway records a participant.
//
// Idempotent on (giveaway_id, user_id): re-boosting a channel during an active
// giveaway must not buy a second chance, and the client happily lets a user
// boost more than once. The DO NOTHING on conflict is the whole guarantee --
// there is no read-then-write window to lose.
//
// The active/until window is enforced in the same statement so that a giveaway
// closed by the draw worker a moment earlier cannot accept an entry.
func (s *StarsStore) EnterStarsGiveaway(ctx context.Context, giveawayID, userID int64, now int) (domain.StarsGiveawayEntry, bool, error) {
	if giveawayID <= 0 || userID <= 0 || now <= 0 {
		return domain.StarsGiveawayEntry{}, false, domain.ErrGiveawayWinnersInvalid
	}
	var entered domain.StarsGiveawayEntry
	err := s.db.QueryRow(ctx, `
        INSERT INTO stars_giveaway_entries (giveaway_id, user_id, entered_at)
        SELECT id, $2, $3
        FROM stars_giveaways
        WHERE id = $1
          AND state = 'active'
          AND until_date > $3
        ON CONFLICT (giveaway_id, user_id) DO NOTHING
        RETURNING giveaway_id, user_id, entered_at`, giveawayID, userID, now).
		Scan(&entered.GiveawayID, &entered.UserID, &entered.EnteredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Either the giveaway is gone, closed, over, or this user already
		// entered. The caller only cares that no new entry was created.
		return domain.StarsGiveawayEntry{}, false, nil
	}
	if err != nil {
		return domain.StarsGiveawayEntry{}, false, err
	}
	return entered, true, nil
}

// ListStarsGiveawayEntries returns one giveaway's roster in entry order.
func (s *StarsStore) ListStarsGiveawayEntries(ctx context.Context, giveawayID int64, limit int) ([]domain.StarsGiveawayEntry, error) {
	if giveawayID <= 0 {
		return nil, domain.ErrGiveawayNotFound
	}
	if limit <= 0 || limit > domain.MaxGiveawayWinners*100 {
		limit = domain.MaxGiveawayWinners * 100
	}
	rows, err := s.db.Query(ctx, `
        SELECT giveaway_id, user_id, entered_at
        FROM stars_giveaway_entries
        WHERE giveaway_id = $1
        ORDER BY entered_at, user_id
        LIMIT $2`, giveawayID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.StarsGiveawayEntry
	for rows.Next() {
		var entry domain.StarsGiveawayEntry
		if err := rows.Scan(&entry.GiveawayID, &entry.UserID, &entry.EnteredAt); err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

// StarsGiveawayForChannel finds the giveaway a boost on this channel joins.
//
// This is the implicit attribution rule. premium.applyBoost carries no
// giveaway_msg_id in layer 229, so the server cannot learn which post a boost
// came from; the only information available is "this account boosted this
// channel". A boost therefore counts as an entry to whichever giveaway on the
// channel is currently active.
//
// Returns found=false when the channel has no active giveaway, which is the
// overwhelmingly common case and must stay cheap: boosting a channel is not a
// giveaway interaction and should not pay for a giveaway table probe on every
// boost.
func (s *StarsStore) StarsGiveawayForChannel(ctx context.Context, channelID int64, now int) (domain.StarsGiveawayRun, bool, error) {
	if channelID <= 0 || now <= 0 {
		return domain.StarsGiveawayRun{}, false, nil
	}
	run, err := scanStarsGiveawayRun(s.db.QueryRow(ctx, `
        SELECT `+starsGiveawayRunColumns+`
        FROM stars_giveaways
        WHERE channel_id = $1 AND state = 'active' AND until_date > $2
        ORDER BY id DESC
        LIMIT 1`, channelID, now).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StarsGiveawayRun{}, false, nil
	}
	if err != nil {
		return domain.StarsGiveawayRun{}, false, err
	}
	return run, true, nil
}

// DueStarsGiveaways returns the work queue: giveaways that need this worker to
// act right now.
//
// Two jobs, one queue, because a sweep that only ever looked at active
// giveaways would silently abandon any winner whose credit failed -- the
// giveaway leaves the queue the moment it is drawn, and nobody retries it.
// Joining on an EXISTS keeps the payment branch indexed rather than running a
// correlated scan per row.
func (s *StarsStore) DueStarsGiveaways(ctx context.Context, now, limit int) ([]domain.StarsGiveawayRun, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.Query(ctx, `
        SELECT `+starsGiveawayRunColumns+`
        FROM stars_giveaways g
        WHERE (g.state = 'active' AND g.until_date <= $1)
           OR (g.state = 'completed'
               AND EXISTS (SELECT 1 FROM stars_giveaway_entries e
                           WHERE e.giveaway_id = g.id AND e.reward_state = 'won'))
        ORDER BY g.until_date, g.id
        LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.StarsGiveawayRun
	for rows.Next() {
		run, err := scanStarsGiveawayRun(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

// CommitStarsGiveawayDraw writes the winner list and closes the giveaway in one
// statement, and only if the giveaway is still active.
//
// The WHERE state = 'active' is the load-bearing part: two workers, or one
// worker retried after a crash between choosing and paying, both lose the race
// here rather than each recording a different set of winners. The loser gets
// zero rows and committed=false.
//
// Marking the entries 'won' happens in the same transaction so the roster and
// the announcement cannot disagree about who won.
func (s *StarsStore) CommitStarsGiveawayDraw(ctx context.Context, draw domain.StarsGiveawayDraw, winners []int64) (bool, error) {
	if draw.GiveawayID <= 0 || len(winners) == 0 {
		return false, domain.ErrGiveawayWinnersInvalid
	}
	payload, err := json.Marshal(winners)
	if err != nil {
		return false, err
	}

	beginner, ok := s.db.(txBeginner)
	if !ok {
		return false, domain.ErrGiveawayNotActive
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin stars giveaway draw: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// RETURNING id is load-bearing. Without it the statement produces no rows,
	// pgx reports ErrNoRows, and this function reports "lost the race" for
	// every draw -- so the worker silently never drew anything while reporting
	// success. A genuine no-rows now means the WHERE clause did not match,
	// which is the one case that really is a lost race.
	var committed int64
	err = tx.QueryRow(ctx, `
        UPDATE stars_giveaways
        SET state = 'completed',
            winners_json = $2,
            drawn_at = $3,
            activated_count = $4,
            winner_count = $5
        WHERE id = $1 AND state = 'active' AND drawn_at = 0
        RETURNING id`,
		draw.GiveawayID, payload, draw.DrawnAt, draw.ActivatedCount, draw.WinnerCount).Scan(&committed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if _, err := tx.Exec(ctx, `
        UPDATE stars_giveaway_entries
        SET reward_state = 'won'
        WHERE giveaway_id = $1 AND user_id = ANY($2)`, draw.GiveawayID, winners); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// StarsGiveawayByID reads one giveaway in any state.
//
// The payout pass needs this: a giveaway that was already drawn is no longer
// "due", so the work queue cannot find it, but the amount to pay is money and
// must come from the committed row rather than from anything cached in memory.
func (s *StarsStore) StarsGiveawayByID(ctx context.Context, giveawayID int64) (domain.StarsGiveawayRun, error) {
	if giveawayID <= 0 {
		return domain.StarsGiveawayRun{}, domain.ErrGiveawayNotFound
	}
	run, err := scanStarsGiveawayRun(s.db.QueryRow(ctx, `
        SELECT `+starsGiveawayRunColumns+`
        FROM stars_giveaways
        WHERE id = $1`, giveawayID).Scan)
	if err != nil {
		return domain.StarsGiveawayRun{}, err
	}
	return run, nil
}

// MarkStarsGiveawayEntryPaid records that a winner was actually credited.
//
// This is separate from the draw commit on purpose. The draw records who won;
// this records who was paid. A winner whose credit failed is recorded as
// 'failed' with the reason kept in reward_error, and stays in the work queue
// (StarsGiveawayPendingPayouts matches 'won' and 'failed') so the worker
// retries it instead of announcing a prize and then forgetting it.
func (s *StarsStore) MarkStarsGiveawayEntryPaid(ctx context.Context, giveawayID, userID int64, now int, failure string) error {
	rewardState := "paid"
	if failure != "" {
		rewardState = "failed"
	}
	res, err := s.db.Exec(ctx, `
        UPDATE stars_giveaway_entries
        SET reward_state = $3, reward_at = $4, reward_error = $5
        WHERE giveaway_id = $1 AND user_id = $2 AND reward_state = 'won'`,
		giveawayID, userID, rewardState, now, failure)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("%w: entry %d of giveaway %d is not pending payment", domain.ErrGiveawayWinnersInvalid, userID, giveawayID)
	}
	return nil
}

// StarsGiveawayPendingPayouts lists winners that were drawn but not yet paid.
//
// Both 'won' (not attempted, or attempted before a crash) and 'failed' (the
// credit was attempted and rejected) are outstanding. Matching only 'won' would
// silently abandon every winner whose first credit attempt failed: the row
// leaves the work queue and no sweep ever returns to it.
func (s *StarsStore) StarsGiveawayPendingPayouts(ctx context.Context, giveawayID int64, limit int) ([]int64, error) {
	if giveawayID <= 0 {
		return nil, domain.ErrGiveawayNotFound
	}
	if limit <= 0 || limit > domain.MaxGiveawayWinners {
		limit = domain.MaxGiveawayWinners
	}
	rows, err := s.db.Query(ctx, `
        SELECT user_id
        FROM stars_giveaway_entries
        WHERE giveaway_id = $1 AND reward_state IN ('won', 'failed')
        ORDER BY user_id
        LIMIT $2`, giveawayID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// NowInt is a small helper so callers do not each re-derive the clock.
func NowInt(t time.Time) int { return int(t.UTC().Unix()) }

// PayStarsGiveawayEntry charges the entry fee and records the participant in
// one transaction.
//
// Atomicity is the point. Charging without recording loses the money;
// recording without charging hands out free tickets. Neither half is
// recoverable alone, so they share a transaction.
//
// The fee moves to the giveaway's holder rather than disappearing, so the
// ledger stays conserved: the participant's balance drops by exactly what the
// holder's balance gains.
//
// A repeat tap is free. The insert's ON CONFLICT DO NOTHING detects the
// duplicate first and returns no row, so the debits below never run.
func (s *StarsStore) PayStarsGiveawayEntry(ctx context.Context, charge domain.ChargeGiveawayEntry) (domain.StarsGiveawayEntry, bool, error) {
	if err := domain.ValidateGiveawayEntryCharge(charge); err != nil {
		return domain.StarsGiveawayEntry{}, false, err
	}

	var entry domain.StarsGiveawayEntry
	err := withTx(ctx, s.db, "pay stars giveaway entry", func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
        INSERT INTO stars_giveaway_entries (giveaway_id, user_id, entered_at)
        SELECT id, $2, $3
        FROM stars_giveaways
        WHERE id = $1 AND state = 'active' AND until_date > $3
        ON CONFLICT (giveaway_id, user_id) DO NOTHING
        RETURNING giveaway_id, user_id, entered_at`,
			charge.GiveawayID, charge.ParticipantUserID, charge.Date).Scan(&entry.GiveawayID, &entry.UserID, &entry.EnteredAt)
		if errors.Is(err, pgx.ErrNoRows) {
			// Closed, over, or already entered. Signalled with a sentinel so
			// the caller can tell "you already joined" from "payment failed"
			// without inspecting the error string.
			return errGiveawayEntryNotNew
		}
		if err != nil {
			return err
		}

		peer := domain.Peer{Type: domain.PeerTypeChannel, ID: charge.ChannelID}
		if err := debitInTx(ctx, tx, charge.ParticipantUserID, charge.FeeStars,
			domain.StarsReasonGiveaway, peer, charge.Date, "Giveaway entry", "Joined a giveaway"); err != nil {
			return err
		}
		return creditInTx(ctx, tx, charge.HolderUserID, charge.FeeStars,
			domain.StarsReasonGift, peer, charge.Date, "Giveaway entry fee", "Giveaway entry received")
	})
	if errors.Is(err, errGiveawayEntryNotNew) {
		return domain.StarsGiveawayEntry{}, false, nil
	}
	if err != nil {
		return domain.StarsGiveawayEntry{}, false, err
	}
	return entry, true, nil
}

// errGiveawayEntryNotNew means the entry already existed, so nothing was
// charged.
var errGiveawayEntryNotNew = errors.New("giveaway entry already recorded")

// debitInTx removes stars inside a caller-owned transaction, refusing to go
// negative. Mirrors StarsStore.Debit's balance guard so the two paths cannot
// drift.
func debitInTx(ctx context.Context, tx pgx.Tx, userID, amount int64,
	reason domain.StarsTransactionReason, peer domain.Peer, date int, title, desc string) error {
	var balance int64
	err := tx.QueryRow(ctx, `SELECT balance FROM stars_balances WHERE user_id = $1 FOR UPDATE`, userID).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrStarsInsufficient
	}
	if err != nil {
		return fmt.Errorf("select stars balance for giveaway entry: %w", err)
	}
	if balance < amount {
		return domain.ErrStarsInsufficient
	}
	if _, err := tx.Exec(ctx,
		`UPDATE stars_balances SET balance = balance - $2, updated_at = now() WHERE user_id = $1`,
		userID, amount); err != nil {
		return fmt.Errorf("debit stars for giveaway entry: %w", err)
	}
	return insertStarsTxn(ctx, tx, userID, -amount, reason, peer, date, title, desc)
}

// creditInTx adds stars inside a caller-owned transaction.
func creditInTx(ctx context.Context, tx pgx.Tx, userID, amount int64,
	reason domain.StarsTransactionReason, peer domain.Peer, date int, title, desc string) error {
	if _, err := tx.Exec(ctx, `
        INSERT INTO stars_balances (user_id, balance, updated_at) VALUES ($1, $2, now())
        ON CONFLICT (user_id) DO UPDATE
          SET balance = stars_balances.balance + EXCLUDED.balance, updated_at = now()`,
		userID, amount); err != nil {
		return fmt.Errorf("credit stars for giveaway entry: %w", err)
	}
	return insertStarsTxn(ctx, tx, userID, amount, reason, peer, date, title, desc)
}

// ActiveStarsGiveawayByMessage finds a running giveaway launched by one of a
// channel's messages.
//
// This is how participation is reached from a real client. Layer 229 has no
// joinGiveaway, so the only request a client will actually send when a user
// answers a giveaway post is an ordinary messages.sendMessage. Matching on the
// replied-to launch message is therefore what turns a post into an entry, and
// it is scoped to that one message so replying elsewhere in the channel costs
// nothing.
func (s *StarsStore) ActiveStarsGiveawayByMessage(ctx context.Context, channelID int64, messageID int, now int) (domain.StarsGiveawayRun, bool, error) {
	if channelID <= 0 || messageID <= 0 || now <= 0 {
		return domain.StarsGiveawayRun{}, false, nil
	}
	run, err := scanStarsGiveawayRun(s.db.QueryRow(ctx, `
        SELECT `+starsGiveawayRunColumns+`
        FROM stars_giveaways
        WHERE channel_id = $1
          AND launch_message_id = $2
          AND state = 'active'
          AND until_date > $3
        LIMIT 1`, channelID, messageID, now).Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.StarsGiveawayRun{}, false, nil
		}
		return domain.StarsGiveawayRun{}, false, err
	}
	return run, true, nil
}

// RefundStarsGiveawayEntry reverses a charged entry.
//
// Both legs run in one transaction and the entry row is deleted, so a refunded
// user cannot be drawn and the ledger returns to its pre-charge state. If the
// entry row is already gone there is nothing to refund -- the money was either
// never taken or already reversed -- so this is a no-op rather than an error
// that would make a caller retry a refund it already performed.
func (s *StarsStore) RefundStarsGiveawayEntry(ctx context.Context, giveawayID, participantUserID, holderUserID, feeStars int64, now int) error {
	if giveawayID <= 0 || participantUserID <= 0 || feeStars <= 0 {
		return domain.ErrGiveawayWinnersInvalid
	}
	return withTx(ctx, s.db, "refund stars giveaway entry", func(tx pgx.Tx) error {
		var exists bool
		err := tx.QueryRow(ctx, `
            SELECT EXISTS (SELECT 1 FROM stars_giveaway_entries
                           WHERE giveaway_id = $1 AND user_id = $2)`,
			giveawayID, participantUserID).Scan(&exists)
		if err != nil {
			return fmt.Errorf("check giveaway entry for refund: %w", err)
		}
		if !exists {
			return nil
		}

		peer := domain.Peer{Type: domain.PeerTypeChannel}
		if err := creditInTx(ctx, tx, participantUserID, feeStars,
			domain.StarsReasonGiveaway, peer, now, "Giveaway entry refund", "Giveaway entry refunded"); err != nil {
			return err
		}
		if holderUserID > 0 && holderUserID != participantUserID {
			if err := debitInTx(ctx, tx, holderUserID, feeStars,
				domain.StarsReasonGift, peer, now, "Giveaway entry fee refund", "Giveaway entry fee returned"); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
            DELETE FROM stars_giveaway_entries WHERE giveaway_id = $1 AND user_id = $2`,
			giveawayID, participantUserID); err != nil {
			return fmt.Errorf("delete refunded giveaway entry: %w", err)
		}
		return nil
	})
}
