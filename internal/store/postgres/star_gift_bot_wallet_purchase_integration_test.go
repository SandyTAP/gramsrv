package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// A bot's spendable Stars are the revenue it earned from settled invoices, which
// lands in bot_stars_balances. The bot user identity has no stars_balances row at
// all, so charging a gift purchase against the personal ledger refused every bot
// with ErrStarsInsufficient — a BALANCE_TOO_LOW no matter how full the wallet was.
// These tests pin the wallet routing so that regression cannot come back.

// TestBotGiftPurchaseDebitsBotWalletPostgres proves a bot-initiated sendGift is
// charged to bot_stars_balances and leaves the personal ledger untouched.
func TestBotGiftPurchaseDebitsBotWalletPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	now := int(time.Now().Unix())
	users := NewUserStore(pool)
	payer := createTestUser(t, ctx, users, "+1896"+suffix+"01", "BotGiftPayer", "")
	bot := createTestUser(t, ctx, users, "+1896"+suffix+"02", "BotGiftBot", "")
	recipient := createTestUser(t, ctx, users, "+1896"+suffix+"03", "BotGiftRecipient", "")

	lifecycle := NewStarGiftLifecycleStore(pool, NewMessageStore(pool), 1_000_000)
	if _, _, err := lifecycle.CreditBotStarsWallet(ctx, domain.BotStarsCredit{
		BotUserID: bot.ID, PayerUserID: payer.ID, Amount: 300,
		Reason: domain.StarsReasonBotInvoice, InvoiceKey: "botgift-" + suffix, Date: now,
	}); err != nil {
		t.Fatalf("credit bot wallet: %v", err)
	}

	stars := NewStarsStore(pool)
	// A personal balance on the bot identity must not exist, otherwise the test
	// would pass for the wrong reason.
	if balance, err := stars.GetBalance(ctx, bot.ID); err == nil && balance.Balance != 0 {
		t.Fatalf("bot personal balance = %d, want 0", balance.Balance)
	}

	// The origin pair must be a zero pair. Bot API has no MTProto session, so a
	// lone OriginAuthKeyID made enqueueDispatch reject the whole transaction with
	// errInvalidDispatchOutboxExclusionPair — a 500 no matter what the wallet held.
	// This exercises the real message-send path, which is the only place the guard
	// fires, so a regression cannot hide behind the wallet assertions below.

	gifts := NewStarGiftStore(pool)
	baseDocumentID := time.Now().UnixNano() & 0x7ffffffffffff000
	entry, err := gifts.CreateCatalogRevision(ctx, domain.StarGiftCatalogWrite{
		Title: "Bot Wallet Gift " + suffix, Stars: 150, ConvertStars: 20, Enabled: true,
		Document: collectibleTestDocument(baseDocumentID, "bot-wallet.tgs"),
		Blob:     collectibleTestBlob(baseDocumentID, "bot-wallet"), Animation: collectibleTestAnimation("bot-wallet.tgs"),
		Actor: "integration", CommandID: "bot-wallet-catalog-" + suffix,
	})
	if err != nil {
		t.Fatalf("create bot wallet catalog: %v", err)
	}
	// Priced above what is left in the wallet after the first gift, so spending it
	// is a real overdraft rather than an exact-balance spend.
	pricey, err := gifts.CreateCatalogRevision(ctx, domain.StarGiftCatalogWrite{
		Title: "Bot Wallet Pricey " + suffix, Stars: 400, ConvertStars: 20, Enabled: true,
		Document: collectibleTestDocument(baseDocumentID+8, "bot-wallet-pricey.tgs"),
		Blob:     collectibleTestBlob(baseDocumentID+8, "bot-wallet-pricey"), Animation: collectibleTestAnimation("bot-wallet-pricey.tgs"),
		Actor: "integration", CommandID: "bot-wallet-pricey-catalog-" + suffix,
	})
	if err != nil {
		t.Fatalf("create pricey catalog: %v", err)
	}

	base := domain.StarGiftPurchaseRequest{
		BuyerUserID: bot.ID, To: domain.Peer{Type: domain.PeerTypeUser, ID: recipient.ID},
		GiftID: entry.Gift.ID, CommandKey: "botapi-stargift:" + suffix + ":send", Date: now,
		BuyerIsBot: true, Message: "from the bot",
	}

	// A half origin pair must be refused outright, and must not touch the wallet:
	// this is the guard that made sendGift fail for every gift regardless of funds.
	halfPair := base
	halfPair.OriginAuthKeyID = [8]byte{'B', 'O', 'T', 'A', 'P', 'I', 0, 1}
	halfPair.CommandKey = "botapi-stargift:" + suffix + ":halfpair"
	if _, err := lifecycle.PurchaseStarGift(ctx, issueLifecyclePurchaseForm(t, ctx, lifecycle, halfPair)); !errors.Is(err, errInvalidDispatchOutboxExclusionPair) {
		t.Fatalf("half origin pair err = %v, want errInvalidDispatchOutboxExclusionPair", err)
	}
	if balance, err := lifecycle.BotStarsBalance(ctx, bot.ID); err != nil || balance != 300 {
		t.Fatalf("wallet after rejected half pair = %d err %v, want 300", balance, err)
	}

	purchased, err := lifecycle.PurchaseStarGift(ctx, issueLifecyclePurchaseForm(t, ctx, lifecycle, base))
	if err != nil {
		t.Fatalf("bot purchase: %v", err)
	}
	if purchased.Saved.ID <= 0 || purchased.Saved.FromUserID != bot.ID {
		t.Fatalf("purchased = %+v", purchased.Saved)
	}

	// The wallet paid for it, and the reported balance is the wallet balance.
	if balance, err := lifecycle.BotStarsBalance(ctx, bot.ID); err != nil || balance != 150 {
		t.Fatalf("wallet after purchase = %d err %v, want 150", balance, err)
	}
	if purchased.Balance.UserID != bot.ID || purchased.Balance.Balance != 150 {
		t.Fatalf("purchase balance = %+v, want user %d balance 150", purchased.Balance, bot.ID)
	}
	// The personal ledger must not have been created or touched.
	if balance, err := stars.GetBalance(ctx, bot.ID); err == nil && balance.Balance != 0 {
		t.Fatalf("bot personal balance after purchase = %d, want 0", balance.Balance)
	}

	// The spend is journaled in the wallet so the operator's revenue screen shows
	// it, and no personal stars_transactions row was invented for the bot.
	var reason string
	var amount int64
	if err := pool.QueryRow(ctx, `SELECT reason,amount FROM bot_stars_transactions
WHERE bot_user_id=$1 AND amount<0 ORDER BY id DESC LIMIT 1`, bot.ID).Scan(&reason, &amount); err != nil {
		t.Fatalf("read wallet spend: %v", err)
	}
	if reason != string(domain.StarsReasonBotSpend) || amount != -150 {
		t.Fatalf("wallet spend = %s/%d, want bot_spend/-150", reason, amount)
	}
	var personalRows int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM stars_transactions WHERE user_id=$1`, bot.ID).
		Scan(&personalRows); err != nil || personalRows != 0 {
		t.Fatalf("personal transactions = %d err %v, want 0", personalRows, err)
	}

	// A replay under the same command key must not spend the wallet twice.
	retry := issueLifecyclePurchaseForm(t, ctx, lifecycle, base)
	if _, found, err := lifecycle.SettledStarGiftPurchase(ctx, retry); err != nil || !found {
		t.Fatalf("settled replay = found %v err %v", found, err)
	}
	if balance, err := lifecycle.BotStarsBalance(ctx, bot.ID); err != nil || balance != 150 {
		t.Fatalf("wallet after replay = %d err %v, want 150", balance, err)
	}

	// Spending past the wallet balance is still refused, and leaves it untouched.
	over := base
	over.GiftID = pricey.Gift.ID
	over.CommandKey = "botapi-stargift:" + suffix + ":over"
	over.Date = now + 1
	if _, err := lifecycle.PurchaseStarGift(ctx, issueLifecyclePurchaseForm(t, ctx, lifecycle, over)); !errors.Is(err, domain.ErrStarsInsufficient) {
		t.Fatalf("overdraft err = %v, want ErrStarsInsufficient", err)
	}
	if balance, err := lifecycle.BotStarsBalance(ctx, bot.ID); err != nil || balance != 150 {
		t.Fatalf("wallet after rejected overdraft = %d err %v, want 150", balance, err)
	}
}

// TestGiftPurchaseForUnfundedBotRefusesPostgres pins the honest failure: a bot that
// never earned anything has an absent wallet row, which must read as an empty
// wallet rather than as an internal failure.
func TestGiftPurchaseForUnfundedBotRefusesPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	now := int(time.Now().Unix())
	users := NewUserStore(pool)
	bot := createTestUser(t, ctx, users, "+1897"+suffix+"01", "UnfundedBotGift", "")
	recipient := createTestUser(t, ctx, users, "+1897"+suffix+"02", "UnfundedRecipient", "")

	lifecycle := NewStarGiftLifecycleStore(pool, NewMessageStore(pool), 1_000_000)
	gifts := NewStarGiftStore(pool)
	baseDocumentID := time.Now().UnixNano() & 0x7ffffffffffff000
	entry, err := gifts.CreateCatalogRevision(ctx, domain.StarGiftCatalogWrite{
		Title: "Unfunded Gift " + suffix, Stars: 10, ConvertStars: 5, Enabled: true,
		Document: collectibleTestDocument(baseDocumentID, "unfunded.tgs"),
		Blob:     collectibleTestBlob(baseDocumentID, "unfunded"), Animation: collectibleTestAnimation("unfunded.tgs"),
		Actor: "integration", CommandID: "unfunded-catalog-" + suffix,
	})
	if err != nil {
		t.Fatalf("create unfunded catalog: %v", err)
	}
	if _, err := lifecycle.PurchaseStarGift(ctx, issueLifecyclePurchaseForm(t, ctx, lifecycle,
		domain.StarGiftPurchaseRequest{
			BuyerUserID: bot.ID, To: domain.Peer{Type: domain.PeerTypeUser, ID: recipient.ID},
			GiftID: entry.Gift.ID, CommandKey: "botapi-stargift:" + suffix + ":unfunded", Date: now,
			BuyerIsBot: true,
		})); !errors.Is(err, domain.ErrStarsInsufficient) {
		t.Fatalf("err = %v, want ErrStarsInsufficient", err)
	}
	if balance, err := lifecycle.BotStarsBalance(ctx, bot.ID); err != nil || balance != 0 {
		t.Fatalf("unfunded wallet = %d err %v, want 0", balance, err)
	}
}
