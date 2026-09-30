package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// The invoice row is the settlement authority: payments.sendPaymentForm has no
// purpose field, so the price comes from here, and a replayed settle must not
// charge the payer twice.

func TestBotInvoiceSettlementIsOneShotAndRefundIsReplaySafe(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	now := int(time.Now().Unix())
	users := NewUserStore(pool)
	payer := createTestUser(t, ctx, users, "+1896"+suffix+"01", "InvoicePayer", "")
	bot := createTestUser(t, ctx, users, "+1896"+suffix+"02", "InvoiceBot", "")

	store := NewBotStore(pool)
	invoice, err := store.CreateBotInvoice(ctx, domain.BotInvoice{
		BotUserID: bot.ID, ChatID: payer.ID, MessageID: 42,
		Title: "Pro month", Description: "one month", Amount: 750,
		Currency: domain.PremiumCurrencyStars, Payload: "sku-1", Date: now,
	})
	if err != nil {
		t.Fatalf("create invoice: %v", err)
	}
	if invoice.ID <= 0 || invoice.Paid {
		t.Fatalf("stored invoice = %+v, want a fresh unpaid row", invoice)
	}

	// The form path resolves the price from the message alone.
	found, ok, err := store.BotInvoiceByMessage(ctx, bot.ID, payer.ID, 42)
	if err != nil || !ok {
		t.Fatalf("by message: %v, %v, want found", err, ok)
	}
	if found.Amount != 750 || found.Currency != domain.PremiumCurrencyStars || found.Payload != "sku-1" {
		t.Fatalf("resolved invoice = %+v, want amount 750 XTR payload sku-1", found)
	}

	settled, ok, err := store.SettleBotInvoice(ctx, bot.ID, payer.ID, 42, payer.ID, "charge-abc", now)
	if err != nil || !ok || !settled.Paid {
		t.Fatalf("settle = %+v, %v, %v, want a settled invoice", settled, ok, err)
	}
	if settled.ChargeID != "charge-abc" || settled.PayerID != payer.ID || settled.PaidAt != now {
		t.Fatalf("settled receipt = %+v, want charge-abc from the payer", settled)
	}

	// A retried sendPaymentForm must not charge again: the second settle
	// reports settled=false and returns the stored receipt.
	again, ok, err := store.SettleBotInvoice(ctx, bot.ID, payer.ID, 42, payer.ID, "charge-abc", now)
	if err != nil || ok {
		t.Fatalf("second settle = %+v, %v, %v, want a no-op", again, ok, err)
	}
	if again.ChargeID != "charge-abc" {
		t.Fatalf("second settle receipt = %q, want the original charge id", again.ChargeID)
	}

	// Looking a charge up changes nothing, so ownership can be checked first.
	found, foundOK, err := store.BotInvoiceByCharge(ctx, "charge-abc")
	if err != nil || !foundOK || found.Refunded {
		t.Fatalf("lookup = %+v, %v, %v, want the unflagged invoice", found, foundOK, err)
	}
	// Flagging twice is a no-op rather than a second reversal.
	newly, err := store.MarkBotInvoiceRefunded(ctx, "charge-abc")
	if err != nil || !newly {
		t.Fatalf("mark refunded = %v, %v, want the first flag to win", newly, err)
	}
	newly, err = store.MarkBotInvoiceRefunded(ctx, "charge-abc")
	if err != nil || newly {
		t.Fatalf("second mark refunded = %v, %v, want a no-op", newly, err)
	}
	// A failed refund releases the flag so the next attempt is not swallowed.
	if err := store.ReleaseBotInvoiceRefund(ctx, "charge-abc"); err != nil {
		t.Fatalf("release: %v", err)
	}
	found, foundOK, err = store.BotInvoiceByCharge(ctx, "charge-abc")
	if err != nil || !foundOK || found.Refunded {
		t.Fatalf("after release = %+v, %v, %v, want the flag cleared", found, ok, err)
	}
	// A refunded invoice is terminal: it must not be settleable again.
	// A refunded invoice is terminal: it must not be settleable again.
	if newly, err = store.MarkBotInvoiceRefunded(ctx, "charge-abc"); err != nil || !newly {
		t.Fatalf("mark before settle-after-refund = %v, %v", newly, err)
	}
	_, ok, err = store.SettleBotInvoice(ctx, bot.ID, payer.ID, 42, payer.ID, "charge-xyz", now)
	if err != nil || ok {
		t.Fatalf("settle after refund = %v, %v, want a no-op", ok, err)
	}

	// An unknown charge id is not found rather than silently succeeding.
	if _, _, err := store.BotInvoiceByCharge(ctx, "charge-unknown"); !errors.Is(err, domain.ErrBotInvoiceNotFound) {
		t.Fatalf("unknown charge err = %v, want ErrBotInvoiceNotFound", err)
	}
}

func TestBotInvoiceRejectsInvalidShape(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	now := int(time.Now().Unix())
	users := NewUserStore(pool)
	payer := createTestUser(t, ctx, users, "+1897"+suffix+"01", "BadInvoicePayer", "")

	store := NewBotStore(pool)
	base := domain.BotInvoice{
		BotUserID: payer.ID, ChatID: payer.ID, MessageID: 1,
		Title: "ok", Description: "ok", Amount: 10,
		Currency: domain.PremiumCurrencyStars, Date: now,
	}
	for name, mutate := range map[string]func(*domain.BotInvoice){
		"no bot":        func(i *domain.BotInvoice) { i.BotUserID = 0 },
		"no message":    func(i *domain.BotInvoice) { i.MessageID = 0 },
		"no amount":     func(i *domain.BotInvoice) { i.Amount = 0 },
		"fiat currency": func(i *domain.BotInvoice) { i.Currency = "USD" },
		"long title":    func(i *domain.BotInvoice) { i.Title = string(make([]byte, 40)) },
		"long payload":  func(i *domain.BotInvoice) { i.Payload = string(make([]byte, 80)) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := base
			bad.MessageID += len(name) // keep the unique index happy
			mutate(&bad)
			if _, err := store.CreateBotInvoice(ctx, bad); !errors.Is(err, domain.ErrBotInvoiceInvalid) {
				t.Fatalf("err = %v, want ErrBotInvoiceInvalid", err)
			}
		})
	}
}
