package rpc

import (
	"context"
	"errors"
	"testing"

	"github.com/iamxvbaba/td/tg"

	"telesrv/internal/domain"
)

// refundFailGifts refuses the debit leg, standing in for an overdraft.
type refundFailGifts struct {
	GiftsService
	err   error
	debit bool
}

func (g *refundFailGifts) CreditBotStarsWallet(_ context.Context, credit domain.BotStarsCredit) (int64, bool, error) {
	if credit.Amount < 0 {
		g.debit = true
		if g.err != nil {
			return 0, false, g.err
		}
	}
	return 0, true, nil
}

func TestBotInvoiceRefundReleasesFlagWhenWalletRefuses(t *testing.T) {
	chain := newInvoiceChain(t)
	gifts := &refundFailGifts{err: errors.New("wallet overdrawn")}
	chain.router.deps.Gifts = gifts

	if _, err := chain.router.BotAPISendInvoice(context.Background(),
		chain.botID, chain.buyerID, "Товар", "Оплата", "refund-fail", 100); err != nil {
		t.Fatalf("sendInvoice: %v", err)
	}
	payerID := chain.newestInvoiceID(t)
	invoice := &tg.InputInvoiceMessage{Peer: &tg.InputPeerUser{UserID: chain.botID, AccessHash: 1}, MsgID: payerID}
	stored, _, err := chain.router.deps.Bots.BotInvoiceByMessage(context.Background(), chain.botID, chain.botID, payerID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	chain.pay(t, stored.ID, invoice)
	chargeID := botInvoiceChargeID(stored)

	if _, err := chain.router.BotAPIRefundStarPayment(context.Background(), chain.botID, chain.buyerID, chargeID); err == nil {
		t.Fatal("refund succeeded despite the wallet refusing the debit")
	}
	if !gifts.debit {
		t.Fatal("the refund never reached the wallet")
	}
	// The charge must not stay flagged, or the retry would be swallowed as a replay.
	got, found, err := chain.router.deps.Bots.BotInvoiceByCharge(context.Background(), chargeID)
	if err != nil || !found {
		t.Fatalf("charge lookup after failed refund: %v found=%v", err, found)
	}
	if got.Refunded {
		t.Fatal("the refund flag survived a failed refund, so the retry is dead")
	}
}
