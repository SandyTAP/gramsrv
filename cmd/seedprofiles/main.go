package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDSN = "postgres://telesrv:telesrv@127.0.0.1:5432/telesrv_main?sslmode=disable"

// A seeded account: plausible profile, a collectible username and an emoji
// status, so the deployment does not read as a test environment full of
// placeholder rows.
//
// The identities here are invented. They are written to look like ordinary
// accounts rather than to resemble any particular person: a name borrowed from
// a real user turns an innocuous-looking profile into something that can be
// pointed at a victim, which is not worth the realism.
type seedAccount struct {
	FirstName    string
	LastName     string
	Username     string
	About        string
	Nickname     string // collectible username, the "NFT" look
	NickURL      string
	ProfileColor int64
	ProfileEmoji int64
	PremiumDays  int
	BasePhone    int64
}

var seeds = []seedAccount{
	{
		FirstName: "Артём", LastName: "Ковалёв",
		Username: "artemkv", About: "дизайн, фотография и много кофе ☕",
		Nickname: "artemcollector", NickURL: "https://collectible.example/1042",
		ProfileColor: 3, ProfileEmoji: 5368324170671202286, PremiumDays: 365,
		BasePhone: 79100010101,
	},
	{
		FirstName: "Мария", LastName: "Соколова",
		Username: "mariask", About: "путешествия, немного кода, много книг ✈️",
		Nickname: "mariastudio", NickURL: "https://collectible.example/2287",
		ProfileColor: 5, ProfileEmoji: 5194328741158821636, PremiumDays: 365,
		BasePhone: 79100010102,
	},
	{
		FirstName: "Денис", LastName: "Орлов",
		Username: "den_orlov", About: "музыка, гитара и Тбилиси 🎸",
		Nickname: "den_orlov_nft", NickURL: "https://collectible.example/3311",
		ProfileColor: 2, ProfileEmoji: 5368324170671202286, PremiumDays: 180,
		BasePhone: 79100010103,
	},
	{
		FirstName: "Ольга", LastName: "Миронова",
		Username: "olga_mironova", About: "мама двоих, сад и по выходным йога 🧘‍♀️",
		Nickname: "olgamir", NickURL: "https://collectible.example/4156",
		ProfileColor: 6, ProfileEmoji: 5356916065158339082, PremiumDays: 365,
		BasePhone: 79100010104,
	},
	{
		FirstName: "Илья", LastName: "Верников",
		Username: "ilynn", About: "кибербезопасность, велосипед и мемы про сети 🔐",
		Nickname: "ilynneth", NickURL: "https://collectible.example/5093",
		ProfileColor: 7, ProfileEmoji: 5194328741158821636, PremiumDays: 730,
		BasePhone: 79100010105,
	},
}

func main() {
	dsn := flag.String("dsn", defaultDSN, "postgres DSN")
	adminURL := flag.String("admin-api", "http://127.0.0.1:2599", "admin API base URL")
	token := flag.String("token", os.Getenv("TELESRV_ADMIN_API_TOKEN"), "admin API token")
	apply := flag.Bool("apply", false, "really write (default: dry run)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		fail("connect: %v", err)
	}
	defer pool.Close()

	client := &adminClient{base: strings.TrimRight(*adminURL, "/"), token: *token}
	now := time.Now().UTC()
	// One prefix per invocation: the audit log binds a command_id to the exact
	// request body, so a fixed id would reject any re-run whose payload differs.
	runID := now.Format("20060102t150405")
	seq := 0

	for _, s := range seeds {
		seq++
		userID, err := ensureUser(ctx, pool, s, now)
		if err != nil {
			fail("%s: create user: %v", s.Username, err)
		}
		if !*apply {
			fmt.Printf("[dry-run] %-14s would be user %d\n", s.Username, userID)
			continue
		}
		client.call(ctx, "/v1/accounts/set-profile", map[string]any{
			"command_id": fmt.Sprintf("seed-%s-profile-%d", runID, seq),
			"actor":      "seed",
			"reason":     "seed a realistic account profile",
			"user_id":    userID,
			"first_name": s.FirstName,
			"last_name":  s.LastName,
		})

		client.call(ctx, "/v1/accounts/set-username", map[string]any{
			"command_id": fmt.Sprintf("seed-%s-username-%d", runID, seq),
			"actor":      "seed",
			"reason":     "seed a realistic account username",
			"user_id":    userID,
			"username":   s.Username,
		})

		// The collectible username is the profile's "NFT": it is what makes an
		// ordinary-looking account look like it was bought rather than typed.
		if err := client.mintNickname(ctx, pool, runID, seq, s, userID); err != nil {
			fail("%s: mint collectible username: %v", s.Username, err)
		}

		// PeerColorInput is flat, and background_emoji_id is a string on the
		// wire because it is a 64-bit document id.
		client.call(ctx, "/v1/accounts/set-color", map[string]any{
			"command_id":          fmt.Sprintf("seed-%s-color-%d", runID, seq),
			"actor":               "seed",
			"reason":              "seed a realistic account profile colour",
			"user_id":             userID,
			"for_profile":         true,
			"has_color":           true,
			"color":               s.ProfileColor,
			"background_emoji_id": strconv.FormatInt(s.ProfileEmoji, 10),
		})

		if s.PremiumDays > 0 {
			client.call(ctx, "/v1/accounts/grant-premium", map[string]any{
				"command_id": fmt.Sprintf("seed-%s-premium-%d", runID, seq),
				"actor":      "seed",
				"reason":     "seed a realistic account premium",
				"user_id":    userID,
				"months":     s.PremiumDays / 30,
			})
		}

		// There is no admin endpoint for the bio, so it is written directly.
		if _, err := pool.Exec(ctx,
			`UPDATE users SET about = $2, updated_at = now() WHERE id = $1`,
			userID, s.About); err != nil {
			fail("%s: set about: %v", s.Username, err)
		}

		fmt.Printf("[ok] %-14s user %d  %s %s\n", s.Username, userID, s.FirstName, s.LastName)
	}

	if !*apply {
		fmt.Println("\nre-run with -apply to write")
	}
}

// ensureUser creates the account row if it is missing and returns its id.
//
// Access hashes are unique per account and never reused; the phone is what makes
// these accounts loginable, so each gets its own.
func ensureUser(ctx context.Context, pool *pgxpool.Pool, s seedAccount, now time.Time) (int64, error) {
	var id int64
	err := pool.QueryRow(ctx, `SELECT id FROM users WHERE phone = $1`, fmt.Sprint(s.BasePhone)).Scan(&id)
	if err == nil {
		return id, nil
	}

	err = pool.QueryRow(ctx, `
        INSERT INTO users (access_hash, phone, first_name, last_name, username, created_at, updated_at)
        VALUES ($1, $2, $3, $4, '', $5, $5)
        RETURNING id`,
		now.UnixNano(), fmt.Sprint(s.BasePhone), s.FirstName, s.LastName, now).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

type adminClient struct {
	base  string
	token string
}

// call posts one admin command. It exits the process on failure rather than
// returning: the caller is a seeding script whose only useful action on error
// is to stop before writing a half-decorated account.
func (c *adminClient) call(ctx context.Context, path string, body any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		fail("encode %s: %v", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, &buf)
	if err != nil {
		fail("build %s: %v", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fail("%s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fail("%s returned %d: %s", path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
}

func (c *adminClient) mintNickname(ctx context.Context, pool *pgxpool.Pool, runID string, seq int, s seedAccount, userID int64) error {
	var owned bool
	err := pool.QueryRow(ctx, `
        SELECT EXISTS (SELECT 1 FROM collectible_usernames
                        WHERE username_lower = $1 AND status = 'owned' AND owner_peer_id = $2)`,
		strings.ToLower(s.Nickname), userID).Scan(&owned)
	if err != nil {
		return err
	}
	if owned {
		fmt.Printf("[skip] %-14s already owns %s\n", s.Username, s.Nickname)
		return nil
	}

	// The numeric fields carry ",string" in their JSON tags: 64-bit ids and
	// nano-amounts must survive a round trip through a JavaScript client, so the
	// API takes them as strings. Sending a number is a 400, not a coercion.
	c.call(ctx, "/v1/collectible-usernames/mint", map[string]any{
		"command_id":      fmt.Sprintf("seed-%s-nft-%d", runID, seq),
		"actor":           "seed",
		"reason":          "seed a collectible username",
		"username":        s.Nickname,
		"owner_user_id":   strconv.FormatInt(userID, 10),
		"currency":        "TON",
		"amount":          "9",
		"crypto_currency": "TON",
		"crypto_amount":   "120000000",
		"url":             s.NickURL,
	})
	return nil
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "seedprofiles: "+format+"\n", args...)
	os.Exit(1)
}
