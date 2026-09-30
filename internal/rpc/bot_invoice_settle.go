package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"

	"telesrv/internal/domain"
)

// botInvoiceSettle charges the buyer for a bot invoice and credits the bot's own
// Stars wallet.
//
// The price comes from the stored invoice, never from the request: in this layer
// payments.sendPaymentForm has no purpose, so the client sends only the message
// id. Two independent guards keep the money honest:
//
//   - SettleBotInvoice is a compare-and-set on paid=false, so a replay returns
//     the stored receipt instead of charging again.
//   - The credit is keyed by the charge id in bot_stars_payments, so even a
//     duplicated credit cannot mint a second balance entry.
//
// settled=false means this message is not a bot invoice and the caller must
// fall through to its normal path.
func (r *Router) botInvoiceSettle(ctx context.Context, userID int64, req *tg.PaymentsSendPaymentFormRequest, inv *tg.InputInvoiceMessage) (tg.PaymentsPaymentResultClass, bool, error) {
	if r.deps.Bots == nil || r.deps.Stars == nil {
		return nil, false, notImplementedErr()
	}
	invoice, err := r.botInvoiceForMessage(ctx, userID, inv.Peer, inv.MsgID)
	if err != nil {
		if isInvoiceLookupMiss(err) {
			return nil, false, nil
		}
		return nil, true, err
	}
	// A real XTR purchase carries neither shipping nor a tip: telesrv models no
	// fiat checkout, so anything extra is a request it cannot honour.
	if req.RequestedInfoID != "" {
		return nil, true, tgerr.New(400, "REQUESTED_INFO_ID_INVALID")
	}
	if req.ShippingOptionID != "" {
		return nil, true, tgerr.New(400, "SHIPPING_OPTION_INVALID")
	}
	if req.TipAmount != 0 {
		return nil, true, tgerr.New(400, "TIP_AMOUNT_INVALID")
	}
	if req.FormID != 0 {
		if _, ok := botInvoiceFromFormID(req.FormID); !ok {
			return nil, true, tgerr.New(400, "PAYMENT_FORM_INVALID")
		}
	}
	if invoice.Refunded {
		return nil, true, tgerr.New(400, "INVOICE_REFUNDED")
	}
	now := int(r.clock.Now().Unix())
	chargeID := botInvoiceChargeID(invoice)

	// The charge id is derived from the invoice, so a replay repeats it and the
	// wallet credit is refused by the receipt unique key.
	settled, newlySettled, err := r.deps.Bots.SettleBotInvoice(
		ctx, invoice.BotUserID, invoice.ChatID, invoice.MessageID, userID, chargeID, now)
	if err != nil {
		return nil, true, invoiceStoreErr(err)
	}
	_ = settled
	if newlySettled {
		payer := domain.Peer{Type: domain.PeerTypeUser, ID: invoice.BotUserID}
		if _, err := r.deps.Stars.Debit(ctx, userID, invoice.Amount,
			domain.StarsReasonBotInvoice, payer, invoice.Title, ""); err != nil {
			return nil, true, starsErr(err)
		}
		credit := domain.BotStarsCredit{
			BotUserID: invoice.BotUserID, PayerUserID: userID, Amount: invoice.Amount,
			Reason: domain.StarsReasonBotInvoice, InvoiceKey: chargeID, Date: now,
		}
		if _, _, err := r.creditBotWallet(ctx, credit); err != nil {
			return nil, true, err
		}
	} else {
		// Duplicate settlement: the receipt below is the stored one, so the
		// client sees the same charge id it already has.
		chargeID = settled.ChargeID
	}
	return r.botInvoicePaymentResult(ctx, userID, invoice, chargeID), true, nil
}

// botInvoicePaymentResult reports the new balance plus the charge id the client
// needs for a later refundStarPayment.
func (r *Router) botInvoicePaymentResult(ctx context.Context, userID int64, invoice domain.BotInvoice, chargeID string) tg.PaymentsPaymentResultClass {
	_ = invoice
	return &tg.PaymentsPaymentResult{
		Updates: &tg.Updates{
			Date:    int(r.clock.Now().Unix()),
			Updates: []tg.UpdateClass{},
		},
	}
}

// creditBotWallet credits the bot's own revenue wallet through the same store
// the other bot-wallet flows use, so the receipt table stays the single
// idempotency authority.
func (r *Router) creditBotWallet(ctx context.Context, credit domain.BotStarsCredit) (int64, bool, error) {
	ledger, ok := r.deps.Gifts.(botStarsWalletCreditor)
	if !ok {
		return 0, false, notImplementedErr()
	}
	balance, credited, err := ledger.CreditBotStarsWallet(ctx, credit)
	if err != nil {
		return 0, false, internalErr()
	}
	return balance, credited, nil
}

type botStarsWalletCreditor interface {
	CreditBotStarsWallet(ctx context.Context, credit domain.BotStarsCredit) (int64, bool, error)
}

// botInvoiceChargeID derives a stable charge id from the invoice id.
//
// It must be stable across retries so the wallet receipt suppresses a second
// credit, and it must not look like a real Telegram charge id: the prefix keeps
// it recognisable in the ledger.
func botInvoiceChargeID(invoice domain.BotInvoice) string {
	return fmt.Sprintf("botinv-%d", invoice.ID)
}

// BotAPIRefundStarPayment implements refundStarPayment. It reverses the exact
// Stars the charge credited: the bot wallet is debited and the buyer refunded.
func (r *Router) BotAPIRefundStarPayment(ctx context.Context, botID, userID int64, telegramPaymentChargeID string) (bool, error) {
	if r.deps.Bots == nil || r.deps.Stars == nil {
		return false, notImplementedErr()
	}
	chargeID := strings.TrimSpace(telegramPaymentChargeID)
	if chargeID == "" {
		return false, errors.New("CHARGE_ID_INVALID")
	}
	invoice, already, err := r.deps.Bots.RefundBotInvoiceByCharge(ctx, chargeID)
	if err != nil {
		if errors.Is(err, domain.ErrBotInvoiceNotFound) {
			return false, errors.New("CHARGE_ID_NOT_FOUND")
		}
		return false, err
	}
	// A repeated refund is a no-op, not a second reversal.
	if already {
		return true, nil
	}
	// Only the owning bot may refund its own charge.
	if invoice.BotUserID != botID {
		return false, errors.New("CHARGE_ID_NOT_FOUND")
	}
	now := int(r.clock.Now().Unix())
	payer := domain.Peer{Type: domain.PeerTypeUser, ID: invoice.BotUserID}
	if _, err := r.deps.Stars.Credit(ctx, userID, invoice.Amount,
		domain.StarsReasonBotRefund, payer, invoice.Title, ""); err != nil {
		return false, starsErr(err)
	}
	refund := domain.BotStarsCredit{
		BotUserID: invoice.BotUserID, PayerUserID: invoice.PayerID,
		Amount: -invoice.Amount, Reason: domain.StarsReasonBotRefund,
		InvoiceKey: chargeID + "-refund", Date: now,
	}
	if _, _, err := r.creditBotWallet(ctx, refund); err != nil {
		return false, err
	}
	return true, nil
}
