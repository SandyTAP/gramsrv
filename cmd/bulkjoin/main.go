// Command bulkjoin adds every human account on this server to one of our
// channels, by calling the server's own ChannelStore.InviteToChannel.
//
// It deliberately does not write SQL of its own: InviteToChannel is the exact
// code path a real invite takes, so it maintains the membership row, the user
// index, admin log, participant count, the service message, per-user dialogs,
// welcome deliveries and the membership read models in one transaction.
// Hand-rolled INSERTs would leave dialogs missing and the message/pts
// sequence to sort out by hand.
//
// Default is a dry run; -apply is what actually changes anything.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres"
)

const defaultDSN = "postgres://telesrv:telesrv@127.0.0.1:5432/telesrv_main?sslmode=disable"

func main() {
	channel := flag.Int64("channel", 1296, "channel id to add everyone to")
	inviter := flag.Int64("inviter", 1780243442, "admin user id recorded as the inviter")
	batch := flag.Int("batch", 100, "users per InviteToChannel call")
	dsn := flag.String("dsn", defaultDSN, "postgres DSN")
	apply := flag.Bool("apply", false, "really add people (default: dry run)")
	only := flag.String("only", "", "comma separated user ids to add instead of every missing account")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		fail("open pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		fail("ping: %v", err)
	}

	var title string
	var participants int
	var megagroup bool
	err = pool.QueryRow(ctx,
		`SELECT title, participants_count, megagroup FROM channels WHERE id = $1`, *channel).
		Scan(&title, &participants, &megagroup)
	if err != nil {
		fail("channel %d not found: %v", *channel, err)
	}

	// Only real accounts: bots have no business in a community chat, and
	// anyone with a view_messages restriction keeps their own ban.
	rows, err := pool.Query(ctx, `
		SELECT u.id
		FROM users u
		WHERE u.is_bot = false
		  AND u.deleted_at IS NULL
		  AND NOT EXISTS (
		    SELECT 1 FROM channel_members m
		    WHERE m.user_id = u.id AND m.channel_id = $1 AND m.status = 'active')
		  AND NOT EXISTS (
		    SELECT 1 FROM channel_members m
		    WHERE m.user_id = u.id AND m.channel_id = $1
		      AND (m.banned_rights->>'view_messages')::boolean IS TRUE)
		ORDER BY u.id`, *channel)
	if err != nil {
		fail("select targets: %v", err)
	}
	var targets []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			fail("scan: %v", err)
		}
		// 内置系统账号（777000 等）不是真人。is_bot=false 拦不住它们：Telegram
		// 把 777000 当真人行存，而客户端把它的消息渲染进独立的「Телеграм」会话，
		// 拉进社区频道等于凭空多出一个承载全频道历史的幽灵对话。
		if domain.IsSystemUserID(id) {
			continue
		}
		targets = append(targets, id)
	}
	rows.Close()

	// -only narrows the sweep to named accounts. Everything below still applies:
	// bots, deleted accounts, system users and people with a view_messages ban
	// are skipped, so a typo in the list cannot force an ineligible account in.
	if raw := strings.TrimSpace(*only); raw != "" {
		wanted := make(map[int64]struct{})
		for _, part := range strings.Split(raw, ",") {
			id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
			if err != nil {
				fail("-only: %q is not a user id", part)
			}
			wanted[id] = struct{}{}
		}
		filtered := targets[:0]
		for _, id := range targets {
			if _, ok := wanted[id]; ok {
				filtered = append(filtered, id)
			}
		}
		for id := range wanted {
			if !containsID(filtered, id) {
				fail("-only: user %d is not eligible for channel %d (bot, deleted, already a member, or banned)", id, *channel)
			}
		}
		targets = filtered
	}

	fmt.Printf("channel   : %d %q (megagroup=%v)\n", *channel, title, megagroup)
	fmt.Printf("currently : %d participants\n", participants)
	fmt.Printf("to add    : %d human accounts\n", len(targets))
	if len(targets) > 0 {
		preview := targets
		if len(preview) > 10 {
			preview = preview[:10]
		}
		fmt.Printf("sample    : %v\n", preview)
	}
	if !*apply {
		fmt.Println("\nDRY RUN — nothing written. Re-run with -apply.")
		return
	}
	if len(targets) == 0 {
		fmt.Println("\nnothing to do")
		return
	}

	store := postgres.NewChannelStore(pool)
	added := 0
	for start := 0; start < len(targets); start += *batch {
		end := start + *batch
		if end > len(targets) {
			end = len(targets)
		}
		chunk := targets[start:end]

		res, err := store.InviteToChannel(ctx, *channel, *inviter, chunk, 0)
		if err != nil {
			fail("invite chunk %d-%d: %v", start, end, err)
		}
		added += len(res.Members)
		fmt.Printf("  +%d added (running total %d/%d)\n", len(res.Members), added, len(targets))
	}

	fmt.Printf("\ndone: %d added to channel %d\n", added, *channel)

	var after int
	if err := pool.QueryRow(ctx,
		`SELECT participants_count FROM channels WHERE id = $1`, *channel).Scan(&after); err != nil {
		fail("verify: %v", err)
	}
	fmt.Printf("participants_count: %d -> %d\n", participants, after)
}

func fail(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

func containsID(ids []int64, want int64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
