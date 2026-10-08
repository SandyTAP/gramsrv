package postgres

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// TestStarGiftResaleSendsGiftFromBuyerPostgres pins who is the author of the
// transferred service message a marketplace purchase produces.
//
// A resale only changes ownership: the buyer is the one handing the collectible
// over, so the recipient must read "the buyer sent you a gift worth N stars".
// Emitting the card from the seller instead made the recipient believe the
// seller gifted it, which is exactly what the report was about.
func TestStarGiftResaleSendsGiftFromBuyerPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	now := int(time.Now().Unix())
	users := NewUserStore(pool)
	seller := createTestUser(t, ctx, users, "+1883"+suffix+"01", "ResaleGiftSeller", "")
	buyer := createTestUser(t, ctx, users, "+1883"+suffix+"02", "ResaleGiftBuyer", "")
	recipient := createTestUser(t, ctx, users, "+1883"+suffix+"03", "ResaleGiftRecipient", "")
	sellerPeer := domain.Peer{Type: domain.PeerTypeUser, ID: seller.ID}

	stars := NewStarsStore(pool)
	for _, u := range []domain.User{seller, buyer, recipient} {
		if _, _, err := stars.EnsureGrant(ctx, u.ID, 10000, now); err != nil {
			t.Fatalf("grant: %v", err)
		}
	}

	gifts := NewStarGiftStore(pool)
	base := time.Now().UnixNano() & 0x7ffffffffffff000
	entry, err := gifts.CreateCatalogRevision(ctx, domain.StarGiftCatalogWrite{
		Title: "ResaleBuyerSender " + suffix, Stars: 50, ConvertStars: 20, Enabled: true,
		Document: collectibleTestDocument(base, "resale-buyer-sender.tgs"),
		Blob:     collectibleTestBlob(base, "resale-buyer-sender"), Animation: collectibleTestAnimation("resale-buyer-sender.tgs"),
		Actor: "integration", CommandID: "resale-buyer-sender-catalog-" + suffix,
	})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if _, err := gifts.PublishCollectibleRevision(ctx, domain.StarGiftCollectibleWrite{
		GiftID: entry.Gift.ID, UpgradeStars: 100, SupplyTotal: 20, SlugPrefix: "rbs-" + suffix,
		Models: []domain.StarGiftCollectibleAttribute{
			{Kind: domain.StarGiftCollectibleModel, Name: "Base", RarityKind: domain.StarGiftRarityPermille, RarityPermille: 1000,
				Document: collectibleTestDocumentPtr(base+1, "model.tgs"), Blob: collectibleTestBlobPtr(base+1, "model"), Animation: collectibleTestAnimationPtr("model.tgs")},
			{Kind: domain.StarGiftCollectibleModel, Name: "Base Two", RarityKind: domain.StarGiftRarityPermille, RarityPermille: 1000,
				Document: collectibleTestDocumentPtr(base+4, "model-two.tgs"), Blob: collectibleTestBlobPtr(base+4, "model-two"), Animation: collectibleTestAnimationPtr("model-two.tgs")},
		},
		Patterns: []domain.StarGiftCollectibleAttribute{
			{Kind: domain.StarGiftCollectiblePattern, Name: "Orbit", RarityKind: domain.StarGiftRarityPermille, RarityPermille: 1000,
				Document: collectibleTestPatternDocumentPtr(base+3, "pattern.tgs"), Blob: collectibleTestBlobPtr(base+3, "pattern"), Animation: collectibleTestAnimationPtr("pattern.tgs")},
			{Kind: domain.StarGiftCollectiblePattern, Name: "Orbit Two", RarityKind: domain.StarGiftRarityPermille, RarityPermille: 1000,
				Document: collectibleTestPatternDocumentPtr(base+5, "pattern-two.tgs"), Blob: collectibleTestBlobPtr(base+5, "pattern-two"), Animation: collectibleTestAnimationPtr("pattern-two.tgs")},
		},
		Backdrops: []domain.StarGiftCollectibleAttribute{
			{Kind: domain.StarGiftCollectibleBackdrop, Name: "Night", BackdropID: 91, CenterColor: 0x112233, EdgeColor: 0x223344, PatternColor: 0x334455, TextColor: 0xffffff, RarityKind: domain.StarGiftRarityPermille, RarityPermille: 1000},
			{Kind: domain.StarGiftCollectibleBackdrop, Name: "Day", BackdropID: 92, CenterColor: 0xaabbcc, EdgeColor: 0x778899, PatternColor: 0xddeeff, TextColor: 0x111111, RarityKind: domain.StarGiftRarityPermille, RarityPermille: 1000},
		},
		Actor: "integration", CommandID: "resale-buyer-sender-pool-" + suffix,
	}); err != nil {
		t.Fatalf("pool: %v", err)
	}

	messages := NewMessageStore(pool)
	lifecycle := NewStarGiftLifecycleStore(pool, messages, 1_000_000, WithStarGiftMarketPolicy(domain.StarGiftMarketPolicy{
		StarsProceedsPermille: 900, TONProceedsPermille: 900,
	}))
	upgrades := NewStarGiftUpgradeStore(pool, messages, WithStarGiftLifecyclePolicy(domain.StarGiftLifecyclePolicy{
		TransferStars: 25, DropOriginalDetailsStars: 25, OfferMinStars: 1, CraftChancePermille: 500,
	}))

	purchase := issueLifecyclePurchaseForm(t, ctx, lifecycle, domain.StarGiftPurchaseRequest{
		BuyerUserID: seller.ID, To: sellerPeer, GiftID: entry.Gift.ID, IncludeUpgrade: true,
		CommandKey: "resale-buyer-sender-purchase-" + suffix, Date: now,
	})
	bought, err := lifecycle.PurchaseStarGift(ctx, purchase)
	if err != nil {
		t.Fatalf("purchase: %v", err)
	}
	upgraded, err := upgrades.UpgradeStarGift(ctx, domain.StarGiftUpgradeRequest{
		UserID: seller.ID, Ref: domain.SavedStarGiftRef{Owner: sellerPeer, MsgID: bought.Saved.MsgID},
		RequirePrepaid: true, KeepOriginalDetails: true, CommandKey: "resale-buyer-sender-upgrade-" + suffix, Date: now + 1,
	})
	if err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	listed, err := lifecycle.SetStarGiftListing(ctx, domain.StarGiftListingRequest{
		ActorUserID: seller.ID, Ref: domain.SavedStarGiftRef{Owner: sellerPeer, MsgID: upgraded.Saved.MsgID},
		Amount: &domain.StarGiftAmount{Currency: domain.StarGiftCurrencyStars, Amount: 500}, Date: now + 2,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// The buyer buys the listing for somebody else, the way a gift-for-a-friend
	// marketplace purchase behaves.
	resold, err := lifecycle.PurchaseResaleStarGift(ctx, domain.StarGiftResalePurchaseRequest{
		BuyerUserID: buyer.ID, Slug: listed.Slug, To: domain.Peer{Type: domain.PeerTypeUser, ID: recipient.ID},
		Amount: domain.StarGiftAmount{Currency: domain.StarGiftCurrencyStars, Amount: 500}, FormID: 31201,
		CommandKey: "resale-buyer-sender-" + suffix, Date: now + 3,
	})
	if err != nil {
		t.Fatalf("resale: %v", err)
	}
	recipientPeer := domain.Peer{Type: domain.PeerTypeUser, ID: recipient.ID}
	if resold.Unique.Owner != recipientPeer || resold.Saved.Owner != recipientPeer {
		t.Fatalf("resale ownership = unique %+v saved %+v, want %+v", resold.Unique.Owner, resold.Saved.Owner, recipientPeer)
	}
	if resold.Saved.FromUserID != buyer.ID {
		t.Fatalf("saved gift from_user_id = %d, want the buyer %d", resold.Saved.FromUserID, buyer.ID)
	}
	var storedFromID int64
	if err := pool.QueryRow(ctx, `SELECT from_user_id FROM peer_star_gifts WHERE id=$1`, resold.Saved.ID).
		Scan(&storedFromID); err != nil || storedFromID != buyer.ID {
		t.Fatalf("stored from_user_id = %d err %v, want the buyer %d", storedFromID, err, buyer.ID)
	}
	if resold.Saved.MsgID <= 0 {
		t.Fatalf("recipient message id = %d, want a delivered service message", resold.Saved.MsgID)
	}

	// The card in the recipient's box must come from the buyer and must carry
	// the resale price, so the client renders "the buyer sent you a gift".
	var boxSenderID, privateMessageID int64
	var mediaJSON string
	if err := pool.QueryRow(ctx, `SELECT message_sender_id,private_message_id,media::text FROM message_boxes
WHERE owner_user_id=$1 AND box_id=$2 AND NOT deleted`, recipient.ID, resold.Saved.MsgID).
		Scan(&boxSenderID, &privateMessageID, &mediaJSON); err != nil {
		t.Fatalf("load recipient box: %v", err)
	}
	if boxSenderID != buyer.ID {
		t.Fatalf("recipient box sender = %d, want the buyer %d", boxSenderID, buyer.ID)
	}
	media, err := decodeMessageMedia(mediaJSON)
	if err != nil || media == nil || media.ServiceAction == nil || media.ServiceAction.StarGiftUnique == nil {
		t.Fatalf("decode recipient service message: media=%+v err=%v", media, err)
	}
	action := media.ServiceAction.StarGiftUnique
	if action.FromUserID != buyer.ID || !action.Transferred || action.ResaleAmount == nil ||
		action.ResaleAmount.Amount != 500 {
		t.Fatalf("recipient action = %+v, want from %d transferred with 500 XTR", action, buyer.ID)
	}

	// The seller keeps no copy of the transferred card: the buyer authored it.
	var sellerCards int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM message_boxes
WHERE owner_user_id=$1 AND private_message_id=$2 AND NOT deleted`, seller.ID, privateMessageID).Scan(&sellerCards); err != nil {
		t.Fatalf("count seller copies: %v", err)
	}
	if sellerCards != 0 {
		t.Fatalf("seller box holds %d copies of the buyer's gift card", sellerCards)
	}
}
