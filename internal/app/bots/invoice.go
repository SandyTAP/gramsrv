package bots

import (
	"context"

	"telesrv/internal/domain"
)

// Bot XTR invoices.
//
// payments.sendPaymentForm has no purpose in this layer, so the price of a bot
// invoice is resolved from the stored row rather than from the request. The
// service is a thin pass-through; the settlement rules live in the store.

// CreateBotInvoice records the price the server advertised for an invoice
// message.
func (s *Service) CreateBotInvoice(ctx context.Context, invoice domain.BotInvoice) (domain.BotInvoice, error) {
	if s == nil || s.bots == nil {
		return domain.BotInvoice{}, domain.ErrBotInvoiceInvalid
	}
	return s.bots.CreateBotInvoice(ctx, invoice)
}

// BotInvoiceByMessage resolves the invoice behind an inputInvoiceMessage.
func (s *Service) BotInvoiceByMessage(ctx context.Context, botUserID, chatID int64, messageID int) (domain.BotInvoice, bool, error) {
	if s == nil || s.bots == nil {
		return domain.BotInvoice{}, false, nil
	}
	return s.bots.BotInvoiceByMessage(ctx, botUserID, chatID, messageID)
}

// SettleBotInvoice marks the invoice paid. settled=false means it was already
// paid, which keeps a retried sendPaymentForm from charging the buyer twice.
func (s *Service) SettleBotInvoice(ctx context.Context, botUserID, chatID int64, messageID int, payerUserID int64, chargeID string, date int) (domain.BotInvoice, bool, error) {
	if s == nil || s.bots == nil {
		return domain.BotInvoice{}, false, domain.ErrBotInvoiceInvalid
	}
	return s.bots.SettleBotInvoice(ctx, botUserID, chatID, messageID, payerUserID, chargeID, date)
}

// RefundBotInvoiceByCharge resolves an invoice by telegram_payment_charge_id
// and flags it refunded. already=true means this charge was refunded before.
func (s *Service) RefundBotInvoiceByCharge(ctx context.Context, chargeID string) (domain.BotInvoice, bool, error) {
	if s == nil || s.bots == nil {
		return domain.BotInvoice{}, false, domain.ErrBotInvoiceInvalid
	}
	return s.bots.RefundBotInvoiceByCharge(ctx, chargeID)
}
