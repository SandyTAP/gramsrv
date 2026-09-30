package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
)

// Bot XTR invoices.
//
// payments.sendPaymentForm has no purpose field in this layer, so the price is
// resolved from the stored invoice: the client reports only the message id.
// The row is therefore the settlement authority, and settlement is a single
// atomic flag flip so a retried payment can never charge twice.

func scanBotInvoice(row interface {
	Scan(dest ...any) error
}) (domain.BotInvoice, error) {
	var (
		invoice domain.BotInvoice
		date    int32
		paidAt  int32
	)
	err := row.Scan(
		&invoice.ID, &invoice.BotUserID, &invoice.ChatID, &invoice.MessageID,
		&invoice.Title, &invoice.Description, &invoice.Amount, &invoice.Currency,
		&invoice.Payload, &invoice.ChargeID, &invoice.PayerID, &invoice.Paid,
		&invoice.Refunded, &paidAt, &date,
	)
	if err != nil {
		return domain.BotInvoice{}, err
	}
	invoice.PaidAt = int(paidAt)
	invoice.Date = int(date)
	return invoice, nil
}

const botInvoiceColumns = `id,bot_user_id,chat_id,message_id,title,description,
	amount,currency,payload,charge_id,payer_user_id,paid,refunded,paid_at,date`

// CreateBotInvoice stores a freshly sent invoice message. The message id is
// unique per bot chat, so a resend after a delete cannot orphan a paid invoice:
// the unique index rejects it rather than silently replacing history.
func (s *BotStore) CreateBotInvoice(ctx context.Context, invoice domain.BotInvoice) (domain.BotInvoice, error) {
	if !invoice.Valid() {
		return domain.BotInvoice{}, domain.ErrBotInvoiceInvalid
	}
	row := s.db.QueryRow(ctx, `INSERT INTO bot_invoices
		(bot_user_id,chat_id,message_id,title,description,amount,currency,payload,date)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING `+botInvoiceColumns,
		invoice.BotUserID, invoice.ChatID, invoice.MessageID, invoice.Title,
		invoice.Description, invoice.Amount, invoice.Currency, invoice.Payload, invoice.Date)
	stored, err := scanBotInvoice(row)
	if err != nil {
		return domain.BotInvoice{}, err
	}
	return stored, nil
}

// BotInvoiceByMessage resolves the invoice a client referenced through
// inputInvoiceMessage.
func (s *BotStore) BotInvoiceByMessage(ctx context.Context, botUserID, chatID int64, messageID int) (domain.BotInvoice, bool, error) {
	if botUserID <= 0 || chatID == 0 || messageID <= 0 {
		return domain.BotInvoice{}, false, domain.ErrBotInvoiceInvalid
	}
	row := s.db.QueryRow(ctx, `SELECT `+botInvoiceColumns+`
FROM bot_invoices WHERE bot_user_id=$1 AND chat_id=$2 AND message_id=$3`,
		botUserID, chatID, messageID)
	invoice, err := scanBotInvoice(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.BotInvoice{}, false, nil
		}
		return domain.BotInvoice{}, false, err
	}
	return invoice, true, nil
}

// SettleBotInvoice marks the invoice paid and records the charge id. It reports
// settled=false when the invoice was already paid, so the caller can answer a
// duplicate sendPaymentForm from the stored receipt instead of charging again.
func (s *BotStore) SettleBotInvoice(ctx context.Context, botUserID, chatID int64, messageID int, payerUserID int64, chargeID string, date int) (domain.BotInvoice, bool, error) {
	if botUserID <= 0 || chatID == 0 || messageID <= 0 || payerUserID <= 0 || chargeID == "" || date <= 0 {
		return domain.BotInvoice{}, false, domain.ErrBotInvoiceInvalid
	}
	// The paid=false predicate makes the update a compare-and-set: a concurrent
	// second settle matches no row and falls through to the read below.
	row := s.db.QueryRow(ctx, `UPDATE bot_invoices
		SET paid=true, charge_id=$4, payer_user_id=$5, paid_at=$6
		WHERE bot_user_id=$1 AND chat_id=$2 AND message_id=$3 AND paid=false AND refunded=false
		RETURNING `+botInvoiceColumns,
		botUserID, chatID, messageID, chargeID, payerUserID, date)
	invoice, err := scanBotInvoice(row)
	if err == nil {
		return invoice, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.BotInvoice{}, false, err
	}
	stored, found, err := s.BotInvoiceByMessage(ctx, botUserID, chatID, messageID)
	if err != nil || !found {
		return domain.BotInvoice{}, false, err
	}
	return stored, false, nil
}

// BotInvoiceByCharge resolves an invoice by the charge id the client reported,
// leaving it untouched so the caller can check ownership before flagging it.
func (s *BotStore) BotInvoiceByCharge(ctx context.Context, chargeID string) (domain.BotInvoice, bool, error) {
	if chargeID == "" {
		return domain.BotInvoice{}, false, domain.ErrBotInvoiceInvalid
	}
	row := s.db.QueryRow(ctx, `SELECT `+botInvoiceColumns+` FROM bot_invoices WHERE charge_id=$1`, chargeID)
	invoice, err := scanBotInvoice(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.BotInvoice{}, false, domain.ErrBotInvoiceNotFound
	}
	if err != nil {
		return domain.BotInvoice{}, false, err
	}
	return invoice, true, nil
}

// MarkBotInvoiceRefunded flags the charge refunded and reports whether this call
// is the one that did it, so a repeated refundStarPayment is a no-op.
func (s *BotStore) MarkBotInvoiceRefunded(ctx context.Context, chargeID string) (bool, error) {
	if chargeID == "" {
		return false, domain.ErrBotInvoiceInvalid
	}
	tag, err := s.db.Exec(ctx, `UPDATE bot_invoices SET refunded=true
		WHERE charge_id=$1 AND refunded=false`, chargeID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() > 0 {
		return true, nil
	}
	// Nothing to flag: either the charge is unknown or it was already refunded.
	if _, _, err := s.BotInvoiceByCharge(ctx, chargeID); err != nil {
		return false, err
	}
	return false, nil
}

// ReleaseBotInvoiceRefund clears the flag so a refund whose money movement
// failed can be retried instead of being permanently swallowed as a replay.
func (s *BotStore) ReleaseBotInvoiceRefund(ctx context.Context, chargeID string) error {
	if chargeID == "" {
		return domain.ErrBotInvoiceInvalid
	}
	tag, err := s.db.Exec(ctx, `UPDATE bot_invoices SET refunded=false
		WHERE charge_id=$1`, chargeID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrBotInvoiceNotFound
	}
	return nil
}

var _ interface {
	CreateBotInvoice(context.Context, domain.BotInvoice) (domain.BotInvoice, error)
	BotInvoiceByMessage(context.Context, int64, int64, int) (domain.BotInvoice, bool, error)
	SettleBotInvoice(context.Context, int64, int64, int, int64, string, int) (domain.BotInvoice, bool, error)
	BotInvoiceByCharge(context.Context, string) (domain.BotInvoice, bool, error)
	MarkBotInvoiceRefunded(context.Context, string) (bool, error)
	ReleaseBotInvoiceRefund(context.Context, string) error
} = (*BotStore)(nil)
