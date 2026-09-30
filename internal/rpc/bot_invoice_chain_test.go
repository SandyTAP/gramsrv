package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"

	appbots "telesrv/internal/app/bots"
	appmessages "telesrv/internal/app/messages"
	appstars "telesrv/internal/app/stars"
	appusers "telesrv/internal/app/users"
	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

// invoiceGifts credits the bot wallet like the production service does.
type invoiceGifts struct {
	GiftsService
	balance int64
	credits []domain.BotStarsCredit
}

func (g *invoiceGifts) CreditBotStarsWallet(_ context.Context, credit domain.BotStarsCredit) (int64, bool, error) {
	g.credits = append(g.credits, credit)
	g.balance += credit.Amount
	return g.balance, true, nil
}

type invoiceChain struct {
	router  *Router
	store   *memory.MessageStore
	stars   *appstars.Service
	gifts   *invoiceGifts
	botID   int64
	buyerID int64
}

func newInvoiceChain(t *testing.T) *invoiceChain {
	t.Helper()
	ctx := context.Background()
	users := memory.NewUserStore()
	botStore := memory.NewBotStore(users)
	dialogs := memory.NewDialogStore()
	msgStore := memory.NewMessageStore(dialogs)
	bots := appbots.NewService(users, botStore, msgStore)

	buyer, err := users.Create(ctx, domain.User{AccessHash: 5502, Phone: "15550005502", FirstName: "Buyer"})
	if err != nil {
		t.Fatalf("create buyer: %v", err)
	}
	bot, _, err := bots.CreateBot(ctx, buyer.ID, "Chain Bot", "chain_bot")
	if err != nil {
		t.Fatalf("create bot: %v", err)
	}

	// The real messages service, because the store keeps a separate message id
	// counter per participant box and that difference is the whole point here.
	messages := appmessages.NewService(msgStore, dialogs)
	stars := appstars.NewService(memory.NewStarsStore(), appstars.WithStartingGrant(5000))
	gifts := &invoiceGifts{}
	r := New(Config{}, Deps{
		Users:    appusers.NewService(users),
		Bots:     bots,
		Messages: messages,
		Stars:    stars,
		Gifts:    gifts,
	}, zaptest.NewLogger(t), clock.System)

	// Box ids are allocated per user, not per chat, so a user who has been
	// active elsewhere sits at a different counter from a fresh bot. That skew is
	// ordinary on a real server and it is what makes the two copies of the
	// invoice carry different ids, so the fixture has to reproduce it: Create
	// allocates in the buyer's box alone.
	if _, err := msgStore.Create(ctx, domain.Message{
		OwnerUserID: buyer.ID, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 900001},
		From: domain.Peer{Type: domain.PeerTypeUser, ID: 900001},
		Out:  true, Body: "unrelated", Date: 1700000001,
	}); err != nil {
		t.Fatalf("skew buyer box: %v", err)
	}
	if _, err := messages.SendPrivateText(ctx, buyer.ID, domain.SendPrivateTextRequest{
		SenderUserID: buyer.ID, RecipientUserID: bot.ID, RandomID: 4242,
		Message: "привет", Date: 1700000000,
	}); err != nil {
		t.Fatalf("buyer message: %v", err)
	}
	return &invoiceChain{r, msgStore, stars, gifts, bot.ID, buyer.ID}
}

// buyerInvoiceID finds the invoice as the buyer sees it, by scanning their own
// box. This is the id the client puts in inputInvoiceMessage.
func (c *invoiceChain) buyerInvoiceID(t *testing.T) int {
	t.Helper()
	list, err := c.store.GetByIDs(context.Background(), c.buyerID, []int{1, 2, 3, 4, 5, 6, 7, 8})
	if err != nil {
		t.Fatalf("scan buyer box: %v", err)
	}
	for _, m := range list.Messages {
		if m.Media != nil && m.Media.Kind == domain.MessageMediaKindInvoice {
			return m.ID
		}
	}
	t.Fatalf("no invoice in the buyer box (count=%d)", list.Count)
	return 0
}

func (c *invoiceChain) dispatch(t *testing.T, userID int64, req bin.Encoder) any {
	t.Helper()
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := c.router.Dispatch(WithUserID(context.Background(), userID), [8]byte{}, 0, &buf)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	return got
}

func (c *invoiceChain) payerBalance(t *testing.T) int64 {
	t.Helper()
	bal, err := c.stars.GetBalance(context.Background(), c.buyerID)
	if err != nil {
		t.Fatalf("payer balance: %v", err)
	}
	return bal.Balance
}

// A buyer must be able to pay the invoice sendInvoice advertised. Each side
// numbers its own box, so the invoice has to be recorded under the id the payer
// will reference, not the bot's.
func TestBotInvoicePaymentChainFromSendToCredit(t *testing.T) {
	chain := newInvoiceChain(t)

	msg, err := chain.router.BotAPISendInvoice(context.Background(),
		chain.botID, chain.buyerID, "Тестовый товар", "Оплата 100 звёздами", "test-item-1", 100)
	if err != nil {
		t.Fatalf("sendInvoice: %v", err)
	}
	payerID := chain.buyerInvoiceID(t)
	if payerID == msg.ID {
		t.Fatalf("boxes should differ: bot id %d == payer id %d, test cannot catch the bug", msg.ID, payerID)
	}

	stored, found, err := chain.router.deps.Bots.BotInvoiceByMessage(context.Background(),
		chain.botID, chain.botID, payerID)
	if err != nil || !found {
		t.Fatalf("invoice under the payer id %d: %v found=%v, want found", payerID, err, found)
	}
	if stored.MessageID != payerID {
		t.Fatalf("stored message id = %d, want %d", stored.MessageID, payerID)
	}
	if stored.Amount != 100 || stored.Currency != domain.PremiumCurrencyStars {
		t.Fatalf("stored invoice = %+v, want 100 XTR", stored)
	}

	invoice := &tg.InputInvoiceMessage{Peer: &tg.InputPeerUser{UserID: chain.botID, AccessHash: 1}, MsgID: payerID}
	form := chain.dispatch(t, chain.buyerID, &tg.PaymentsGetPaymentFormRequest{Invoice: invoice})
	starsForm, ok := form.(*tg.PaymentsPaymentFormStars)
	if !ok {
		t.Fatalf("form type = %T, want *tg.PaymentsPaymentFormStars", form)
	}
	if len(starsForm.Invoice.Prices) != 1 || starsForm.Invoice.Prices[0].Amount != 100 {
		t.Fatalf("form = %+v, want one price of 100", starsForm.Invoice)
	}

	// Paying debits the payer, credits the bot, and a repeat moves no money.
	before := chain.payerBalance(t)
	chain.pay(t, stored.ID, invoice)

	after := chain.payerBalance(t)
	if after != before-100 {
		t.Fatalf("payer balance = %d, want %d", after, before-100)
	}
	if chain.gifts.balance != 100 {
		t.Fatalf("bot wallet = %d, want 100", chain.gifts.balance)
	}

	chain.pay(t, stored.ID, invoice)
	if chain.gifts.balance != 100 {
		t.Fatalf("bot wallet after repeat = %d, want 100 (idempotent)", chain.gifts.balance)
	}

	if got := chain.payerBalance(t); got != after {
		t.Fatalf("payer balance after repeat = %d, want %d", got, after)
	}

	// TDesktop pays a stars invoice over sendStarsForm rather than
	// sendPaymentForm, so that path has to settle the same way. A second invoice
	// keeps the money movement unambiguous instead of relying on a repeat.
	chain.payViaStarsForm(t, 100)
}

// payViaStarsForm sells one more invoice and settles it the way TDesktop does.
func (c *invoiceChain) payViaStarsForm(t *testing.T, amount int64) {
	t.Helper()
	if _, err := c.router.BotAPISendInvoice(context.Background(),
		c.botID, c.buyerID, "Второй товар", "Оплата звёздами", "test-item-2", amount); err != nil {
		t.Fatalf("second sendInvoice: %v", err)
	}
	payerID := c.newestInvoiceID(t)
	inv := &tg.InputInvoiceMessage{Peer: &tg.InputPeerUser{UserID: c.botID, AccessHash: 1}, MsgID: payerID}
	stored, found, err := c.router.deps.Bots.BotInvoiceByMessage(context.Background(), c.botID, c.botID, payerID)
	if err != nil || !found {
		t.Fatalf("second invoice lookup: %v found=%v", err, found)
	}

	before := c.payerBalance(t)
	wallet := c.gifts.balance
	res := c.dispatch(t, c.buyerID, &tg.PaymentsSendStarsFormRequest{
		FormID:  stored.ID + botInvoiceFormIDBase,
		Invoice: inv,
	})
	if _, ok := res.(*tg.PaymentsPaymentResult); !ok {
		t.Fatalf("sendStarsForm result = %T, want *tg.PaymentsPaymentResult", res)
	}
	if got := c.payerBalance(t); got != before-amount {
		t.Fatalf("payer balance after stars form = %d, want %d", got, before-amount)
	}
	if c.gifts.balance != wallet+amount {
		t.Fatalf("bot wallet after stars form = %d, want %d", c.gifts.balance, wallet+amount)
	}
}

// newestInvoiceID returns the id of the most recent invoice in the buyer's box.
func (c *invoiceChain) newestInvoiceID(t *testing.T) int {
	t.Helper()
	list, err := c.store.GetByIDs(context.Background(), c.buyerID, []int{1, 2, 3, 4, 5, 6, 7, 8})
	if err != nil {
		t.Fatalf("scan buyer box: %v", err)
	}
	newest := 0
	for _, m := range list.Messages {
		if m.Media != nil && m.Media.Kind == domain.MessageMediaKindInvoice && m.ID > newest {
			newest = m.ID
		}
	}
	if newest == 0 {
		t.Fatalf("no invoice in the buyer box")
	}
	return newest
}

// pay settles through the real dispatch, carrying the same form id both times so
// the repeat exercises idempotency rather than a second purchase.
func (c *invoiceChain) pay(t *testing.T, invoiceID int64, invoice *tg.InputInvoiceMessage) {
	t.Helper()
	res := c.dispatch(t, c.buyerID, &tg.PaymentsSendPaymentFormRequest{
		// An XTR purchase carries no card, matching what a real client sends.
		Credentials: &tg.InputPaymentCredentials{},
		FormID:      invoiceID + botInvoiceFormIDBase,
		Invoice:     invoice,
	})
	if _, ok := res.(*tg.PaymentsPaymentResult); !ok {
		t.Fatalf("payment result = %T, want *tg.PaymentsPaymentResult", res)
	}
}

// A refund has to give the buyer's Stars back, take them out of the bot wallet,
// survive a repeat without reversing twice, and refuse a bot that does not own
// the charge - without that refusal flagging the charge and denying the real
// owner its money.
func TestBotInvoiceRefundChain(t *testing.T) {
	chain := newInvoiceChain(t)

	if _, err := chain.router.BotAPISendInvoice(context.Background(),
		chain.botID, chain.buyerID, "Товар", "Оплата 100 звёздами", "refund-1", 100); err != nil {
		t.Fatalf("sendInvoice: %v", err)
	}
	payerID := chain.newestInvoiceID(t)
	invoice := &tg.InputInvoiceMessage{Peer: &tg.InputPeerUser{UserID: chain.botID, AccessHash: 1}, MsgID: payerID}
	stored, found, err := chain.router.deps.Bots.BotInvoiceByMessage(context.Background(), chain.botID, chain.botID, payerID)
	if err != nil || !found {
		t.Fatalf("invoice lookup: %v found=%v", err, found)
	}
	chain.pay(t, stored.ID, invoice)

	start := chain.payerBalance(t)
	wallet := chain.gifts.balance
	chargeID := botInvoiceChargeID(stored)

	// A bot that does not own the charge is refused, and must not consume the
	// refund: the real owner still has to be able to take it.
	if _, err := chain.router.BotAPIRefundStarPayment(context.Background(), chain.botID+1, chain.buyerID, chargeID); err == nil {
		t.Fatal("a foreign bot refunded a charge it does not own")
	}
	// The payer named in the request has to be the payer on the invoice.
	if _, err := chain.router.BotAPIRefundStarPayment(context.Background(), chain.botID, chain.buyerID+1, chargeID); err == nil {
		t.Fatal("a refund redirected to another user_id was accepted")
	}
	if chain.gifts.balance != wallet {
		t.Fatalf("wallet moved on a refused refund: %d, want %d", chain.gifts.balance, wallet)
	}

	if _, err := chain.router.BotAPIRefundStarPayment(context.Background(), chain.botID, chain.buyerID, chargeID); err != nil {
		t.Fatalf("refund: %v", err)
	}
	if got := chain.payerBalance(t); got != start+100 {
		t.Fatalf("buyer balance after refund = %d, want %d", got, start+100)
	}
	if chain.gifts.balance != wallet-100 {
		t.Fatalf("bot wallet after refund = %d, want %d", chain.gifts.balance, wallet-100)
	}

	// A repeat is a no-op rather than a second reversal.
	if _, err := chain.router.BotAPIRefundStarPayment(context.Background(), chain.botID, chain.buyerID, chargeID); err != nil {
		t.Fatalf("repeat refund: %v", err)
	}
	if got := chain.payerBalance(t); got != start+100 {
		t.Fatalf("buyer balance after repeat = %d, want %d", got, start+100)
	}
	if chain.gifts.balance != wallet-100 {
		t.Fatalf("bot wallet after repeat = %d, want %d", chain.gifts.balance, wallet-100)
	}
}

// A settled invoice must reach the bot as a successful_payment carrying the
// charge id. That object is the only place a bot can learn the charge, so without
// it refundStarPayment is unreachable no matter how correct it is.
func TestBotInvoicePaidNotifiesBotWithChargeID(t *testing.T) {
	chain := newInvoiceChain(t)

	if _, err := chain.router.BotAPISendInvoice(context.Background(),
		chain.botID, chain.buyerID, "Товар", "Оплата 100 звёздами", "notify-1", 100); err != nil {
		t.Fatalf("sendInvoice: %v", err)
	}
	payerID := chain.newestInvoiceID(t)
	invoice := &tg.InputInvoiceMessage{Peer: &tg.InputPeerUser{UserID: chain.botID, AccessHash: 1}, MsgID: payerID}
	stored, _, err := chain.router.deps.Bots.BotInvoiceByMessage(context.Background(), chain.botID, chain.botID, payerID)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	chain.pay(t, stored.ID, invoice)

	// The message the bot receives is addressed to the bot and reads from the payer.
	list, err := chain.store.GetByIDs(context.Background(), chain.botID, []int{1, 2, 3, 4, 5, 6, 7, 8})
	if err != nil {
		t.Fatalf("scan bot box: %v", err)
	}
	var found bool
	for _, m := range list.Messages {
		if m.Media == nil || m.Media.Kind != domain.MessageMediaKindService || m.Media.ServiceAction == nil {
			continue
		}
		action := m.Media.ServiceAction
		if action.Kind != domain.MessageServiceActionPayment {
			continue
		}
		found = true
		payment := action.Payment
		if payment == nil || payment.ChargeID != botInvoiceChargeID(stored) {
			t.Fatalf("charge id = %+v, want %q", payment, botInvoiceChargeID(stored))
		}
		if payment.Currency != domain.PremiumCurrencyStars || payment.TotalAmount != 100 {
			t.Fatalf("payment = %+v, want 100 XTR", payment)
		}
		if payment.Payload != "notify-1" {
			t.Fatalf("payload = %q, want the invoice payload", payment.Payload)
		}
		if m.From.ID != chain.buyerID {
			t.Fatalf("service message from %d, want the payer %d", m.From.ID, chain.buyerID)
		}
	}
	if !found {
		t.Fatal("the bot received no payment service message")
	}

	// Replaying the payment must not announce it twice.
	before := countPaymentMessages(t, chain)
	chain.pay(t, stored.ID, invoice)
	if after := countPaymentMessages(t, chain); after != before {
		t.Fatalf("payment messages = %d after a replay, want %d", after, before)
	}

	// And the charge id from that message is exactly what a refund needs.
	if _, err := chain.router.BotAPIRefundStarPayment(context.Background(),
		chain.botID, chain.buyerID, botInvoiceChargeID(stored)); err != nil {
		t.Fatalf("refund with the announced charge id: %v", err)
	}
}

func countPaymentMessages(t *testing.T, chain *invoiceChain) int {
	t.Helper()
	list, err := chain.store.GetByIDs(context.Background(), chain.botID, []int{1, 2, 3, 4, 5, 6, 7, 8})
	if err != nil {
		t.Fatalf("scan bot box: %v", err)
	}
	count := 0
	for _, m := range list.Messages {
		if m.Media != nil && m.Media.ServiceAction != nil &&
			m.Media.ServiceAction.Kind == domain.MessageServiceActionPayment {
			count++
		}
	}
	return count
}
