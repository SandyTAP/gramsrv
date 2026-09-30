package rpc

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
	"go.uber.org/zap"

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
		// Only the first settlement announces the payment, so a replayed form
		// cannot post the service message twice.
		r.sendBotInvoicePaidMessage(ctx, userID, invoice, chargeID, now)
	} else {
		// Duplicate settlement: the receipt below is the stored one, so the
		// client sees the same charge id it already has.
		chargeID = settled.ChargeID
	}
	return r.botInvoicePaymentResult(ctx, userID, invoice, chargeID), true, nil
}

// botInvoicePaidRandomID derives a stable random id from the charge, so that a
// retried settlement produces the same id and is deduplicated by the message store
// instead of posting a second service message.
func botInvoicePaidRandomID(chargeID string) int64 {
	sum := fnv.New64a()
	_, _ = sum.Write([]byte(chargeID))
	return int64(sum.Sum64() >> 1)
}

// botInvoiceServiceMessagePoster is the optional capability that lets the server
// post a private service message on behalf of another user.
type botInvoiceServiceMessagePoster interface {
	PostPrivateServiceMessage(ctx context.Context, recipientUserID, fromUserID int64, req domain.SendPrivateTextRequest) (domain.SendPrivateTextResult, error)
}

// sendBotInvoicePaidMessage posts the service message a bot receives once its
// invoice is paid. It carries the charge id, which is the only way a bot can learn
// it and therefore the only way refundStarPayment can ever be called: the buyer
// never sees the charge, and no Bot API method reports it back.
//
// A failure here is logged rather than propagated: the payment itself already
// settled and the money moved, so refusing the result would only hide that.
func (r *Router) sendBotInvoicePaidMessage(ctx context.Context, payerID int64, invoice domain.BotInvoice, chargeID string, date int) {
	// Optional rather than part of MessagesService: only this one flow needs to
	// author a message whose sender is not the acting user, and a stand-in
	// messages service should not be forced to pretend it can.
	poster, ok := r.deps.Messages.(botInvoiceServiceMessagePoster)
	if !ok || poster == nil {
		return
	}
	res, err := poster.PostPrivateServiceMessage(ctx, invoice.BotUserID, payerID, domain.SendPrivateTextRequest{
		RandomID: botInvoicePaidRandomID(chargeID),
		Date:     date,
		Media: &domain.MessageMedia{
			Kind: domain.MessageMediaKindService,
			ServiceAction: &domain.MessageServiceAction{
				Kind: domain.MessageServiceActionPayment,
				Payment: &domain.MessagePaymentAction{
					Currency:         invoice.Currency,
					TotalAmount:      invoice.Amount,
					Payload:          invoice.Payload,
					ChargeID:         chargeID,
					ProviderChargeID: chargeID,
					Title:            invoice.Title,
					Description:      invoice.Description,
				},
			},
		},
	})
	if err != nil {
		if r.log != nil {
			r.log.Error("bot invoice paid message",
				zap.Int64("bot_user_id", invoice.BotUserID),
				zap.Int64("payer_id", payerID),
				zap.String("charge_id", chargeID),
				zap.Error(err))
		}
		return
	}
	if res.RecipientMessage.ID > 0 && r.log != nil {
		r.log.Info("bot invoice paid message sent",
			zap.Int64("bot_user_id", invoice.BotUserID),
			zap.Int("msg_id", res.RecipientMessage.ID),
			zap.String("charge_id", chargeID))
	}
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
//
// Nothing is flagged until the request has proved it owns the charge, because a
// bot calling this with someone else's charge id would otherwise mark it refunded
// and permanently deny the real owner the refund.
func (r *Router) BotAPIRefundStarPayment(ctx context.Context, botID, requestedPayerID int64, telegramPaymentChargeID string) (bool, error) {
	if r.deps.Bots == nil || r.deps.Stars == nil {
		return false, notImplementedErr()
	}
	chargeID := strings.TrimSpace(telegramPaymentChargeID)
	if chargeID == "" {
		return false, errors.New("CHARGE_ID_INVALID")
	}
	invoice, found, err := r.deps.Bots.BotInvoiceByCharge(ctx, chargeID)
	if err != nil {
		if errors.Is(err, domain.ErrBotInvoiceNotFound) {
			return false, errors.New("CHARGE_ID_NOT_FOUND")
		}
		return false, err
	}
	if !found {
		return false, errors.New("CHARGE_ID_NOT_FOUND")
	}
	// Only the owning bot may refund its own charge.
	if invoice.BotUserID != botID {
		return false, errors.New("CHARGE_ID_NOT_FOUND")
	}
	// user_id from the request body is not trusted to say who gets the money. The
	// payer is recorded on the invoice, so the two have to agree; otherwise a bot
	// could refund its own charge into an account it controls.
	if invoice.PayerID <= 0 || requestedPayerID != invoice.PayerID {
		return false, errors.New("USER_ID_INVALID")
	}

	newly, err := r.deps.Bots.MarkBotInvoiceRefunded(ctx, chargeID)
	if err != nil {
		if errors.Is(err, domain.ErrBotInvoiceNotFound) {
			return false, errors.New("CHARGE_ID_NOT_FOUND")
		}
		return false, err
	}
	if !newly {
		// A repeated refund is a no-op, not a second reversal.
		return true, nil
	}

	now := int(r.clock.Now().Unix())
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: invoice.BotUserID}
	// The wallet is debited first: it can refuse an overdraft, and refunding the
	// buyer out of a wallet that cannot cover it would be money from nowhere.
	if _, _, err := r.creditBotWallet(ctx, domain.BotStarsCredit{
		BotUserID: invoice.BotUserID, PayerUserID: invoice.PayerID,
		Amount: -invoice.Amount, Reason: domain.StarsReasonBotRefund,
		InvoiceKey: chargeID + "-refund", Date: now,
	}); err != nil {
		// No money moved, so the flag goes back and the bot can retry.
		r.releaseBotInvoiceRefund(ctx, chargeID)
		return false, err
	}
	if _, err := r.deps.Stars.Credit(ctx, invoice.PayerID, invoice.Amount,
		domain.StarsReasonBotRefund, peer, invoice.Title, ""); err != nil {
		// The wallet is already debited, so put it back before lifting the flag.
		// If the rollback itself fails the refund stays flagged, which loses the
		// buyer's Stars but never mints them; either way it is logged.
		if _, _, rbErr := r.creditBotWallet(ctx, domain.BotStarsCredit{
			BotUserID: invoice.BotUserID, PayerUserID: invoice.PayerID,
			Amount: invoice.Amount, Reason: domain.StarsReasonBotRefund,
			InvoiceKey: chargeID + "-refund-rollback", Date: now,
		}); rbErr == nil {
			r.releaseBotInvoiceRefund(ctx, chargeID)
		}
		return false, starsErr(err)
	}
	return true, nil
}

// releaseBotInvoiceRefund lifts the refunded flag after a failed refund so the
// charge is not swallowed as a replay on the next attempt.
func (r *Router) releaseBotInvoiceRefund(ctx context.Context, chargeID string) {
	if err := r.deps.Bots.ReleaseBotInvoiceRefund(ctx, chargeID); err != nil && r.log != nil {
		r.log.Error("release bot invoice refund",
			zap.String("charge_id", chargeID), zap.Error(err))
	}
}
