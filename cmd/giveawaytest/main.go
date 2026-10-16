// Command giveawaytest launches a Stars channel giveaway against the running
// telesrv database, bypassing the MTProto client path.
//
// It exists because the giveaway purchase lives entirely in the store layer:
// purchaseStarsGiveaway creates the channel message, funds the prize from the
// buyer's balance and records the campaign, all in one transaction. No session
// is required to exercise it.
//
// This proves the server side. It does NOT prove that a Telegram client can
// start a giveaway from its own UI, which needs the real client path.
//
//	go run ./cmd/giveawaytest -channel 932 -stars 5000 -winners 5 -hours 24
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"telesrv/internal/app/stargiveaways"
	"telesrv/internal/config"
	"telesrv/internal/domain"
	"telesrv/internal/store/postgres"
)

func main() {
	channelID := flag.Int64("channel", 932, "channel id to run the giveaway in")
	stars := flag.Int64("stars", 5000, "prize pool in stars")
	winners := flag.Int("winners", 5, "number of winners")
	hours := flag.Int("hours", 24, "hours until the draw")
	minutes := flag.Int("minutes", 0, "minutes until the draw (overrides -hours when set)")
	perUser := flag.Int64("per-user", 0, "stars per winner (0 = stars/winners)")
	description := flag.String("description", "", "prize description shown on the post")
	joinGiveaway := flag.Int64("join", 0, "join this giveaway id as the given user instead of launching one")
	joinAs := flag.Int64("as", 0, "user id to join as (0 picks the first eligible channel member)")
	refundGiveaway := flag.Int64("refund", 0, "refund every entry of this giveaway and close it as cancelled")
	flag.Parse()

	var err error
	if *refundGiveaway != 0 {
		err = refundAll(*refundGiveaway)
	} else if *joinGiveaway != 0 {
		err = join(*joinGiveaway, *channelID, *joinAs)
	} else {
		err = run(*channelID, *stars, *winners, *perUser, *hours, *minutes, *description)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "giveawaytest:", err)
		os.Exit(1)
	}
}

// join exercises the paid entry path: charge the fee, record the entry, and
// prove a repeat tap costs nothing.
func join(giveawayID, channelID, asUserID int64) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := postgres.Open(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer pool.Close()

	starsStore := postgres.NewStarsStore(pool)
	enter := stargiveaways.NewEnterService(starsStore, nil)

	run, err := starsStore.StarsGiveawayByID(ctx, giveawayID)
	if err != nil {
		return fmt.Errorf("read giveaway %d: %w", giveawayID, err)
	}

	participant := asUserID
	if participant == 0 {
		participant, err = pickParticipant(ctx, pool, run.BuyerUserID, channelID)
		if err != nil {
			return err
		}
	}

	if participant == run.BuyerUserID {
		return fmt.Errorf("user %d is the giveaway holder and may not enter their own giveaway", participant)
	}
	before, _ := starsBalance(ctx, pool, participant)
	holderBefore, _ := starsBalance(ctx, pool, run.BuyerUserID)

	entry, err := enter.JoinPaid(ctx, giveawayID, participant, 0)
	if err != nil {
		return fmt.Errorf("join: %w", err)
	}
	after, _ := starsBalance(ctx, pool, participant)
	holderAfter, _ := starsBalance(ctx, pool, run.BuyerUserID)
	fmt.Printf("first join: participant %d charged %d (fee %d)\n",
		participant, before-after, domain.StarsGiveawayEntryFeeStars)

	// Second tap must be free.
	entry2, err := enter.JoinPaid(ctx, giveawayID, participant, 0)
	if err != nil {
		return fmt.Errorf("repeat join: %w", err)
	}
	repeat, _ := starsBalance(ctx, pool, participant)
	// Compare against the balance after the FIRST join. Comparing against the
	// pre-join balance would measure both taps together and report a correct
	// single charge as a double charge.
	fmt.Printf("repeat join: participant %d charged %d (must be 0)\n", participant, after-repeat)

	fmt.Printf("  entry        giveaway %d user %d at %d\n", entry.GiveawayID, entry.UserID, entry.EnteredAt)
	fmt.Printf("  repeat entry giveaway %d user %d (zero-valued = no new entry)\n", entry2.GiveawayID, entry2.UserID)
	fmt.Printf("  holder       %d -> %d\n", holderBefore, holderAfter)

	if after-repeat != 0 {
		return fmt.Errorf("repeat join charged %d stars; a second tap must be free", after-repeat)
	}
	return nil
}

// pickParticipant chooses a channel member who is not the giveaway holder.
func pickParticipant(ctx context.Context, q *pgxpool.Pool, holderUserID, channelID int64) (int64, error) {
	var userID int64
	err := q.QueryRow(ctx, `
        SELECT user_id FROM channel_members
        WHERE channel_id = $1 AND user_id <> $2
        ORDER BY user_id
        LIMIT 1`, channelID, holderUserID).Scan(&userID)
	if err != nil {
		return 0, fmt.Errorf("pick a channel member of %d to join: %w", channelID, err)
	}
	return userID, nil
}

func run(channelID, stars int64, winners int, perUser int64, hours, minutes int, description string) error {
	if winners <= 0 || stars <= 0 || channelID <= 0 {
		return fmt.Errorf("channel, stars and winners must be positive")
	}
	// Minutes wins over hours so a ten minute run does not need a fractional
	// hour, and so an unset -hours still means "a day".
	if minutes > 0 {
		hours = 0
	}
	if perUser <= 0 {
		if stars%int64(winners) != 0 {
			return fmt.Errorf("stars %d does not divide into %d winners: the table enforces users*per_user_stars=stars", stars, winners)
		}
		perUser = stars / int64(winners)
	}
	if perUser*int64(winners) != stars {
		return fmt.Errorf("per-user %d x %d winners != %d stars", perUser, winners, stars)
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := zap.NewNop()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := postgres.Open(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer pool.Close()

	channelStore := postgres.NewChannelStore(pool, postgres.WithChannelLogger(logger))
	purchaseStore := postgres.NewStarsPurchaseStore(pool, nil, channelStore)
	if purchaseStore == nil {
		return fmt.Errorf("build purchase store")
	}

	// The buyer is resolved from the channel's creator: that account is the one
	// with the right to post there and the balance to fund the prize. Hardcoding
	// an id here would drift the moment the channel changes owner.
	buyerID, err := channelCreator(ctx, pool, channelID)
	if err != nil {
		return err
	}

	now := int(time.Now().UTC().Unix())
	form, err := purchaseStore.IssueStarsPurchaseForm(ctx, domain.StarsPurchaseForm{
		Kind:        domain.StarsPurchaseGiveaway,
		BuyerUserID: buyerID,
		Giveaway: &domain.StarsGiveawayPurchase{
			BoostPeer:         domain.Peer{Type: domain.PeerTypeChannel, ID: channelID},
			PrizeDescription:  description,
			RandomID:          randomID(),
			UntilDate:         now + hours*3600 + minutes*60,
			Users:             winners,
			PerUserStars:      perUser,
			YearlyBoosts:      0,
			WinnersAreVisible: true,
		},
		Stars:     stars,
		Currency:  "USD",
		Amount:    int64(winners) * perUser / 100,
		IssuedAt:  now,
		ExpiresAt: now + 600,
	})
	if err != nil {
		return fmt.Errorf("issue giveaway form: %w", err)
	}

	balanceBefore, _ := starsBalance(ctx, pool, buyerID)

	result, err := purchaseStore.PurchaseStars(ctx, domain.StarsPurchaseRequest{
		StarsPurchaseForm: form,
		Date:              now,
	})
	if err != nil {
		return fmt.Errorf("purchase giveaway: %w", err)
	}

	balanceAfter, _ := starsBalance(ctx, pool, buyerID)

	fmt.Printf("giveaway launched\n")
	fmt.Printf("  channel       %d\n", channelID)
	fmt.Printf("  buyer         %d\n", buyerID)
	fmt.Printf("  prize         %d stars\n", stars)
	fmt.Printf("  winners       %d x %d stars\n", winners, perUser)
	fmt.Printf("  form id       %d\n", form.FormID)
	fmt.Printf("  message id    %d\n", result.ChannelSend.Message.ID)
	fmt.Printf("  buyer stars   %d -> %d (spent %d)\n", balanceBefore, balanceAfter, balanceBefore-balanceAfter)
	fmt.Printf("  entry fee     %d stars per participant\n", domain.StarsGiveawayEntryFeeStars)

	if spent := balanceBefore - balanceAfter; spent != stars {
		return fmt.Errorf("prize was not funded: expected %d stars to leave the buyer, saw %d", stars, spent)
	}
	return nil
}

func channelCreator(ctx context.Context, q *pgxpool.Pool, channelID int64) (int64, error) {
	var creator int64
	err := q.QueryRow(ctx,
		`SELECT creator_user_id FROM channels WHERE id = $1`, channelID).Scan(&creator)
	if err != nil {
		return 0, fmt.Errorf("read channel %d creator: %w", channelID, err)
	}
	if creator == 0 {
		return 0, fmt.Errorf("channel %d has no creator", channelID)
	}
	return creator, nil
}

func starsBalance(ctx context.Context, q *pgxpool.Pool, userID int64) (int64, error) {
	var balance int64
	err := q.QueryRow(ctx,
		`SELECT balance FROM stars_balances WHERE user_id = $1`, userID).Scan(&balance)
	if err != nil {
		return 0, err
	}
	return balance, nil
}

// randomID mirrors the RPC path's non-zero random id requirement.
func randomID() int64 {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 1
	}
	id := int64(binary.LittleEndian.Uint64(buf[:]))
	if id == 0 {
		id = 1
	}
	return id
}

var _ = pgx.ErrNoRows

// refundAll returns every entry fee and closes the giveaway as cancelled.
//
// Used when a launch post disappears: the campaign row survives, but nobody can
// answer a deleted post, so anyone who paid an entry fee must get it back.
// Leaving them charged for a giveaway nobody could enter is the one outcome
// worth going out of the way to avoid.
func refundAll(giveawayID int64) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := postgres.Open(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer pool.Close()

	starsStore := postgres.NewStarsStore(pool)
	enter := stargiveaways.NewEnterService(starsStore, nil)

	run, err := starsStore.StarsGiveawayByID(ctx, giveawayID)
	if err != nil {
		return fmt.Errorf("read giveaway %d: %w", giveawayID, err)
	}
	entries, err := starsStore.ListStarsGiveawayEntries(ctx, giveawayID, 0)
	if err != nil {
		return fmt.Errorf("list entries: %w", err)
	}
	holderBefore, _ := starsBalance(ctx, pool, run.BuyerUserID)

	refunded := 0
	for _, e := range entries {
		if err := enter.RefundEntry(ctx, giveawayID, e.UserID, 0); err != nil {
			return fmt.Errorf("refund user %d: %w", e.UserID, err)
		}
		refunded++
	}
	if _, err := pool.Exec(ctx,
		`UPDATE stars_giveaways SET state='cancelled' WHERE id=$1 AND state='active'`, giveawayID); err != nil {
		return fmt.Errorf("cancel giveaway: %w", err)
	}
	holderAfter, _ := starsBalance(ctx, pool, run.BuyerUserID)
	fmt.Printf("refunded %d entries of giveaway %d\n", refunded, giveawayID)
	fmt.Printf("  holder %d -> %d\n", holderBefore, holderAfter)
	return nil
}
