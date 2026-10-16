package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
)

// The worker's unit tests mock the store, which is precisely why
// CommitStarsGiveawayDraw could ship an UPDATE with no RETURNING clause: the
// fake answered the method itself and the real SQL never ran. In production the
// statement updated nothing, reported "lost the race", and the worker silently
// never drew anything while every log line said success.
//
// These tests run the real statement, so a statement that executes, updates
// nothing and reports success fails here.

// seedDrawGiveaway inserts the one parent row stars_giveaways actually needs.
//
// stars_giveaways has a single foreign key, onto stars_purchase_forms. Entries
// reference users, but this test commits the draw directly and needs no
// entries, which keeps the fixture small enough to stay obviously correct.
func seedDrawGiveaway(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stars int64, users, until int) int64 {
	t.Helper()

	now := int(time.Now().Unix())
	formID := time.Now().UnixNano()
	if formID <= 0 {
		formID = 1
	}
	buyer := 900000000000 + formID%1000

	if _, err := pool.Exec(ctx, `
        INSERT INTO stars_purchase_forms
          (buyer_user_id, form_id, recipient_user_id, stars, currency, amount,
           issued_at, expires_at, kind, spend_peer_type, spend_peer_id, purpose_json)
        VALUES ($1, $2, NULL, $3, 'USD', $4, $5, $6, 'giveaway', NULL, NULL, '{"boost_peer":1}'::jsonb)`,
		buyer, formID, stars, stars, now, now+600); err != nil {
		t.Fatalf("insert purchase form: %v", err)
	}

	var giveawayID int64
	err := pool.QueryRow(ctx, `
        INSERT INTO stars_giveaways
          (buyer_user_id, form_id, channel_id, launch_message_id, random_id, stars,
           users, per_user_stars, yearly_boosts, until_date, purpose_json, state, created_at)
        VALUES ($1, $2, 932, $3, $4, $5, $6, $7, 0, $8, '{"boost_peer":1}'::jsonb, 'active', $9)
        RETURNING id`,
		buyer, formID, formID%100000+1, formID%99999+1, stars, users,
		stars/int64(users), until, now-60).Scan(&giveawayID)
	if err != nil {
		t.Fatalf("insert giveaway: %v", err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = pool.Exec(bg, `DELETE FROM stars_giveaways WHERE id = $1`, giveawayID)
		_, _ = pool.Exec(bg, `DELETE FROM stars_purchase_forms WHERE buyer_user_id = $1 AND form_id = $2`, buyer, formID)
	})
	return giveawayID
}

// TestCommitStarsGiveawayDrawPersistsTheOutcome is the regression test for the
// silent no-op. An UPDATE with no RETURNING reports success and writes nothing.
func TestCommitStarsGiveawayDrawPersistsTheOutcome(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	store := NewStarsStore(pool)

	now := int(time.Now().Unix())
	giveawayID := seedDrawGiveaway(t, ctx, pool, 5000, 5, now-10)

	draw := domain.StarsGiveawayDraw{
		GiveawayID: giveawayID, Winners: []int64{1, 2, 3, 4, 5},
		WinnerCount: 5, PerUserStars: 1000, DrawnAt: now, ActivatedCount: 6,
	}
	committed, err := store.CommitStarsGiveawayDraw(ctx, draw, draw.Winners)
	if err != nil {
		t.Fatalf("CommitStarsGiveawayDraw: %v", err)
	}
	if !committed {
		t.Fatal("the first commit reported a lost race. A first commit must always win; " +
			"an UPDATE with no RETURNING clause does exactly this")
	}

	got, err := store.StarsGiveawayByID(ctx, giveawayID)
	if err != nil {
		t.Fatalf("StarsGiveawayByID: %v", err)
	}
	if got.State != domain.StarsGiveawayCompleted {
		t.Fatalf("state = %q, want completed", got.State)
	}
	if got.DrawnAt == 0 {
		t.Fatal("drawn_at was not persisted")
	}
	if got.WinnerCount != 5 || got.ActivatedCount != 6 {
		t.Fatalf("winner_count=%d activated_count=%d, want 5 and 6", got.WinnerCount, got.ActivatedCount)
	}
}

// TestCommitStarsGiveawayDrawIsOnceOnly: a second draw must lose, and must not
// overwrite the first outcome.
func TestCommitStarsGiveawayDrawIsOnceOnly(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	store := NewStarsStore(pool)

	now := int(time.Now().Unix())
	giveawayID := seedDrawGiveaway(t, ctx, pool, 5000, 5, now-10)

	first := domain.StarsGiveawayDraw{
		GiveawayID: giveawayID, Winners: []int64{1, 2, 3, 4, 5},
		WinnerCount: 5, PerUserStars: 1000, DrawnAt: now, ActivatedCount: 6,
	}
	if _, err := store.CommitStarsGiveawayDraw(ctx, first, first.Winners); err != nil {
		t.Fatalf("first commit: %v", err)
	}

	second := first
	second.Winners = []int64{9, 10, 11, 12, 13}
	committed, err := store.CommitStarsGiveawayDraw(ctx, second, second.Winners)
	if err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if committed {
		t.Fatal("a second draw committed; a giveaway must be drawable exactly once")
	}
	got, err := store.StarsGiveawayByID(ctx, giveawayID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got.WinnerCount != 5 {
		t.Fatalf("winner_count = %d, want the first draw's 5", got.WinnerCount)
	}
}
