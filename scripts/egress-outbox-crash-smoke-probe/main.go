package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/iamxvbaba/td/tg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/egress"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres"
)

const defaultPostgresDSN = "postgres://telesrv:telesrv@127.0.0.1:5432/telesrv?sslmode=disable"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run() error {
	postgresDSN := flag.String("postgres-dsn", defaultProbePostgresDSN(), "PostgreSQL DSN; defaults to TELESRV_TEST_POSTGRES_DSN, TELESRV_POSTGRES_DSN, then the local development DSN")
	timeout := flag.Duration("timeout", 15*time.Second, "overall probe timeout")
	leaseTimeout := flag.Duration("lease-timeout", time.Second, "dispatch outbox lease timeout used by the probe store")
	staleAge := flag.Duration("stale-age", 2*time.Second, "age written into the claimed head to simulate a crashed Egress lease")
	skipMigrate := flag.Bool("skip-migrate", false, "skip embedded PostgreSQL migrations before probing")
	flag.Parse()

	dsn := strings.TrimSpace(*postgresDSN)
	if dsn == "" {
		return fmt.Errorf("-postgres-dsn is required")
	}
	if *timeout <= 0 {
		return fmt.Errorf("-timeout must be positive")
	}
	if *leaseTimeout <= 0 {
		return fmt.Errorf("-lease-timeout must be positive")
	}
	if *staleAge <= *leaseTimeout {
		return fmt.Errorf("-stale-age must be greater than -lease-timeout")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	migration := "skipped"
	if !*skipMigrate {
		status, err := postgres.MigrateAndStatus(dsn)
		if err != nil {
			return fmt.Errorf("migrate postgres: %w", err)
		}
		migration = fmt.Sprintf("version=%d dirty=%t empty=%t", status.Version, status.Dirty, status.Empty)
	}

	pool, err := postgres.Open(ctx, dsn, postgres.WithMaxConns(4), postgres.WithMinConns(1))
	if err != nil {
		return err
	}
	defer pool.Close()

	user, err := createProbeUser(ctx, pool)
	if err != nil {
		return err
	}
	defer cleanupProbeUser(pool, user.ID)

	events := postgres.NewUpdateEventStore(pool)
	outbox := postgres.NewDispatchOutboxStore(pool, postgres.WithLeaseTimeout(*leaseTimeout))
	event, err := events.AppendAllocatedWithDispatch(ctx, user.ID, domain.UpdateEvent{
		Type:     domain.UpdateEventNoop,
		PtsCount: 1,
		Date:     int(time.Now().Unix()),
	}, [8]byte{}, 0)
	if err != nil {
		return fmt.Errorf("append probe update with dispatch: %w", err)
	}
	state, err := loadOutboxState(ctx, pool, user.ID, event.Pts)
	if err != nil {
		return err
	}
	if state.status != "pending" || state.attempts != 0 {
		return fmt.Errorf("initial outbox state = status=%s attempts=%d, want pending/0", state.status, state.attempts)
	}

	deliverer := &recordingOutboxDeliverer{}
	dispatcher := egress.NewOutboxDispatcher(events, outbox, deliverer, zap.NewNop(),
		egress.WithOutboxBatch(1),
		egress.WithOutboxUpdateBuilder(func(_ context.Context, requests []egress.OutboxUpdateRequest) ([][]byte, error) {
			updates := make([][]byte, len(requests))
			for i := range updates {
				raw, err := edgecontrol.EncodeOutboxUpdate(&tg.Updates{})
				if err != nil {
					panic(err)
				}
				updates[i] = raw
			}
			return updates, nil
		}),
	)

	if !dispatcher.DispatchOnce(ctx) {
		return fmt.Errorf("first dispatch did not claim the probe outbox row")
	}
	state, err = loadOutboxState(ctx, pool, user.ID, event.Pts)
	if err != nil {
		return err
	}
	if state.status != "dispatching" || state.attempts != 1 {
		return fmt.Errorf("after first dispatch state = status=%s attempts=%d, want dispatching/1", state.status, state.attempts)
	}
	if err := requireDeliveryRefs(deliverer.refs(), []edgecontrol.OutboxDeliveryRef{{
		OutboxID:     state.id,
		TargetUserID: user.ID,
		Pts:          event.Pts,
		Attempt:      1,
	}}); err != nil {
		return fmt.Errorf("first delivery refs: %w", err)
	}

	if err := forceOutboxHeadStale(ctx, pool, user.ID, event.Pts, *staleAge); err != nil {
		return err
	}
	if !dispatcher.DispatchOnce(ctx) {
		return fmt.Errorf("second dispatch did not reclaim the stale dispatching row")
	}
	state, err = loadOutboxState(ctx, pool, user.ID, event.Pts)
	if err != nil {
		return err
	}
	if state.status != "dispatching" || state.attempts != 2 {
		return fmt.Errorf("after reclaim state = status=%s attempts=%d, want dispatching/2", state.status, state.attempts)
	}
	if err := requireDeliveryRefs(deliverer.refs(), []edgecontrol.OutboxDeliveryRef{
		{OutboxID: state.id, TargetUserID: user.ID, Pts: event.Pts, Attempt: 1},
		{OutboxID: state.id, TargetUserID: user.ID, Pts: event.Pts, Attempt: 2},
	}); err != nil {
		return fmt.Errorf("reclaim delivery refs: %w", err)
	}

	staleAck := store.DispatchOutboxClientAck{
		OutboxID:     state.id,
		TargetUserID: user.ID,
		Pts:          event.Pts,
		Attempt:      1,
		AuthKeyID:    [8]byte{1},
		SessionID:    1001,
		ServerMsgID:  9001,
		AckedAt:      time.Now(),
	}
	if err := outbox.MarkClientAcked(ctx, staleAck); !errors.Is(err, store.ErrDispatchLeaseLost) {
		return fmt.Errorf("stale attempt ack err = %v, want ErrDispatchLeaseLost", err)
	}
	if _, err := loadOutboxState(ctx, pool, user.ID, event.Pts); err != nil {
		return fmt.Errorf("stale attempt ack removed or corrupted outbox row: %w", err)
	}

	currentAck := staleAck
	currentAck.Attempt = 2
	currentAck.ServerMsgID = 9002
	currentAck.AckedAt = time.Now()
	if err := outbox.MarkClientAcked(ctx, currentAck); err != nil {
		return fmt.Errorf("current attempt ack: %w", err)
	}
	if _, err := loadOutboxState(ctx, pool, user.ID, event.Pts); !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("current attempt ack left outbox row err=%v, want pgx.ErrNoRows", err)
	}

	fmt.Printf("egress outbox crash recovery probe ok: user_id=%d pts=%d outbox_id=%d attempts=2 migration=%s\n",
		user.ID, event.Pts, state.id, migration)
	return nil
}

func defaultProbePostgresDSN() string {
	if dsn := strings.TrimSpace(os.Getenv("TELESRV_TEST_POSTGRES_DSN")); dsn != "" {
		return dsn
	}
	if dsn := strings.TrimSpace(os.Getenv("TELESRV_POSTGRES_DSN")); dsn != "" {
		return dsn
	}
	return defaultPostgresDSN
}

func createProbeUser(ctx context.Context, pool *pgxpool.Pool) (domain.User, error) {
	now := time.Now().UnixNano()
	phone := fmt.Sprintf("+190090%012d", now%1_000_000_000_000)
	user, err := postgres.NewUserStore(pool).Create(ctx, domain.User{
		AccessHash: now & 0x7fffffffffffffff,
		Phone:      phone,
		FirstName:  "EgressProbe",
	})
	if err != nil {
		return domain.User{}, fmt.Errorf("create probe user: %w", err)
	}
	return user, nil
}

type postgresDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func cleanupProbeUser(db postgresDB, userID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The created user owns only probe update/outbox rows; user FK cascades clean
	// both durable event and outbox state after the ACK fencing checks complete.
	_, _ = db.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
}

type outboxState struct {
	id       int64
	status   string
	attempts int
}

func loadOutboxState(ctx context.Context, db postgresDB, userID int64, pts int) (outboxState, error) {
	var state outboxState
	err := db.QueryRow(ctx, `
SELECT id, status, attempts
FROM dispatch_outbox
WHERE target_user_id = $1 AND pts = $2`,
		userID,
		int32(pts),
	).Scan(&state.id, &state.status, &state.attempts)
	return state, err
}

func forceOutboxHeadStale(ctx context.Context, db postgresDB, userID int64, pts int, age time.Duration) error {
	seconds := durationSecondsCeil(age)
	tag, err := db.Exec(ctx, `
UPDATE dispatch_outbox
SET updated_at = now() - make_interval(secs => $3::int)
WHERE target_user_id = $1 AND pts = $2`,
		userID,
		int32(pts),
		int32(seconds),
	)
	if err != nil {
		return fmt.Errorf("force stale dispatch_outbox: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("force stale dispatch_outbox affected %d rows, want 1", tag.RowsAffected())
	}
	tag, err = db.Exec(ctx, `
UPDATE dispatch_outbox_user_heads
SET updated_at = now() - make_interval(secs => $2::int)
WHERE target_user_id = $1`,
		userID,
		int32(seconds),
	)
	if err != nil {
		return fmt.Errorf("force stale dispatch_outbox_user_heads: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("force stale dispatch_outbox_user_heads affected %d rows, want 1", tag.RowsAffected())
	}
	return nil
}

func durationSecondsCeil(d time.Duration) int {
	seconds := int(math.Ceil(d.Seconds()))
	if seconds < 1 {
		return 1
	}
	return seconds
}

type recordingOutboxDeliverer struct {
	mu   sync.Mutex
	seen []edgecontrol.OutboxDeliveryRef
}

func (d *recordingOutboxDeliverer) PushOutboxUpdate(_ context.Context, req edgecontrol.OutboxPushRequest) (edgecontrol.OutboxPushResult, error) {
	d.mu.Lock()
	d.seen = append(d.seen, req.DeliveryRef)
	d.mu.Unlock()
	return edgecontrol.OutboxPushResult{Sent: 1, Status: edgecontrol.OutboxDeliveryIndeterminate}, nil
}

func (d *recordingOutboxDeliverer) refs() []edgecontrol.OutboxDeliveryRef {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]edgecontrol.OutboxDeliveryRef(nil), d.seen...)
}

func requireDeliveryRefs(got, want []edgecontrol.OutboxDeliveryRef) error {
	if len(got) != len(want) {
		return fmt.Errorf("got %s, want %s", formatDeliveryRefs(got), formatDeliveryRefs(want))
	}
	for i := range got {
		if got[i] != want[i] {
			return fmt.Errorf("got %s, want %s", formatDeliveryRefs(got), formatDeliveryRefs(want))
		}
	}
	return nil
}

func formatDeliveryRefs(refs []edgecontrol.OutboxDeliveryRef) string {
	if len(refs) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(refs))
	for _, ref := range refs {
		parts = append(parts, fmt.Sprintf("{outbox_id:%d target:%d pts:%d attempt:%d}", ref.OutboxID, ref.TargetUserID, ref.Pts, ref.Attempt))
	}
	return "[" + strings.Join(parts, ",") + "]"
}
