package memory

import (
	"context"
	"sync"

	"telesrv/internal/domain"
)

// In-memory bot invoices. Mirrors the postgres semantics that matter to callers:
// a message resolves to one invoice, settlement is a one-shot compare-and-set, and
// a charge id refunds at most once.

type botInvoiceKey struct {
	botUserID int64
	chatID    int64
	messageID int
}

type botInvoiceLedger struct {
	mu       sync.Mutex
	nextID   int64
	byMsg    map[botInvoiceKey]domain.BotInvoice
	byCharge map[string]botInvoiceKey
}

func newBotInvoiceLedger() *botInvoiceLedger {
	return &botInvoiceLedger{
		byMsg:    make(map[botInvoiceKey]domain.BotInvoice),
		byCharge: make(map[string]botInvoiceKey),
	}
}

func (l *botInvoiceLedger) create(invoice domain.BotInvoice) (domain.BotInvoice, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := botInvoiceKey{invoice.BotUserID, invoice.ChatID, invoice.MessageID}
	if _, exists := l.byMsg[key]; exists {
		return domain.BotInvoice{}, domain.ErrBotInvoiceSettled
	}
	l.nextID++
	invoice.ID = l.nextID
	l.byMsg[key] = invoice
	return invoice, nil
}

func (l *botInvoiceLedger) byMessage(botUserID, chatID int64, messageID int) (domain.BotInvoice, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	invoice, found := l.byMsg[botInvoiceKey{botUserID, chatID, messageID}]
	return invoice, found
}

// settle is a compare-and-set: only the first caller flips paid, so a replayed
// sendPaymentForm reads the stored receipt instead of charging again.
func (l *botInvoiceLedger) settle(botUserID, chatID int64, messageID int, payerUserID int64, chargeID string, date int) (domain.BotInvoice, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := botInvoiceKey{botUserID, chatID, messageID}
	invoice, found := l.byMsg[key]
	if !found {
		return domain.BotInvoice{}, false, nil
	}
	if invoice.Paid || invoice.Refunded {
		return invoice, false, nil
	}
	invoice.Paid = true
	invoice.ChargeID = chargeID
	invoice.PayerID = payerUserID
	invoice.PaidAt = date
	l.byMsg[key] = invoice
	l.byCharge[chargeID] = key
	return invoice, true, nil
}

func (l *botInvoiceLedger) refundByCharge(chargeID string) (domain.BotInvoice, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key, found := l.byCharge[chargeID]
	if !found {
		return domain.BotInvoice{}, false, domain.ErrBotInvoiceNotFound
	}
	invoice := l.byMsg[key]
	if invoice.Refunded {
		return invoice, true, nil
	}
	invoice.Refunded = true
	l.byMsg[key] = invoice
	return invoice, false, nil
}

func (s *BotStore) CreateBotInvoice(_ context.Context, invoice domain.BotInvoice) (domain.BotInvoice, error) {
	if !invoice.Valid() {
		return domain.BotInvoice{}, domain.ErrBotInvoiceInvalid
	}
	return s.invoices.create(invoice)
}

func (s *BotStore) BotInvoiceByMessage(_ context.Context, botUserID, chatID int64, messageID int) (domain.BotInvoice, bool, error) {
	if botUserID <= 0 || chatID == 0 || messageID <= 0 {
		return domain.BotInvoice{}, false, domain.ErrBotInvoiceInvalid
	}
	invoice, found := s.invoices.byMessage(botUserID, chatID, messageID)
	return invoice, found, nil
}

func (s *BotStore) SettleBotInvoice(_ context.Context, botUserID, chatID int64, messageID int, payerUserID int64, chargeID string, date int) (domain.BotInvoice, bool, error) {
	if botUserID <= 0 || chatID == 0 || messageID <= 0 || payerUserID <= 0 || chargeID == "" || date <= 0 {
		return domain.BotInvoice{}, false, domain.ErrBotInvoiceInvalid
	}
	return s.invoices.settle(botUserID, chatID, messageID, payerUserID, chargeID, date)
}

func (s *BotStore) RefundBotInvoiceByCharge(_ context.Context, chargeID string) (domain.BotInvoice, bool, error) {
	if chargeID == "" {
		return domain.BotInvoice{}, false, domain.ErrBotInvoiceInvalid
	}
	return s.invoices.refundByCharge(chargeID)
}
