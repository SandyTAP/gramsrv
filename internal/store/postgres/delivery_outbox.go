package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

const (
	defaultDeliveryLease              = 30 * time.Second
	defaultDeliveryPoisonCleanupBatch = 256
	maxDeliveryPoisonCleanupBatch     = 1000
)

var errInvalidDeliveryOutboxExclusionPair = errors.New("delivery outbox exclusion requires both raw auth key and session id")

type DeliveryOutboxStore struct {
	db           sqlcgen.DBTX
	leaseSeconds int32
}

type DeliveryOutboxOption func(*DeliveryOutboxStore)

func WithDeliveryLeaseTimeout(d time.Duration) DeliveryOutboxOption {
	return func(s *DeliveryOutboxStore) {
		if d > 0 {
			s.leaseSeconds = int32(d / time.Second)
			if s.leaseSeconds < 1 {
				s.leaseSeconds = 1
			}
		}
	}
}

func NewDeliveryOutboxStore(db sqlcgen.DBTX, opts ...DeliveryOutboxOption) *DeliveryOutboxStore {
	s := &DeliveryOutboxStore{
		db:           db,
		leaseSeconds: int32(defaultDeliveryLease / time.Second),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

func (s *DeliveryOutboxStore) Enqueue(ctx context.Context, item store.DeliveryOutboxEnqueue) (store.DeliveryOutboxItem, error) {
	if err := validateDeliveryOutboxEnqueue(item); err != nil {
		return store.DeliveryOutboxItem{}, err
	}
	var out store.DeliveryOutboxItem
	var excludeAuthKeyID int64
	err := s.db.QueryRow(ctx, `
INSERT INTO edge_delivery_outbox (
  target_user_id,
  payload,
  exclude_auth_key_id,
  exclude_session_id
) VALUES ($1, $2, $3, $4)
RETURNING id, target_user_id, exclude_auth_key_id, exclude_session_id, attempts, payload`,
		item.TargetUserID,
		item.Payload,
		authKeyIDToInt64(item.ExcludeAuthKeyID),
		item.ExcludeSessionID,
	).Scan(&out.ID, &out.TargetUserID, &excludeAuthKeyID, &out.ExcludeSessionID, &out.Attempts, &out.Payload)
	if err != nil {
		return store.DeliveryOutboxItem{}, fmt.Errorf("enqueue edge delivery outbox: %w", err)
	}
	out.ExcludeAuthKeyID = authKeyIDFromInt64(excludeAuthKeyID)
	out.Payload = append([]byte(nil), out.Payload...)
	return out, nil
}

func validateDeliveryOutboxEnqueue(item store.DeliveryOutboxEnqueue) error {
	if item.TargetUserID <= 0 {
		return fmt.Errorf("delivery outbox target user id is required")
	}
	if len(item.Payload) == 0 {
		return fmt.Errorf("delivery outbox payload is required")
	}
	hasAuthKey := item.ExcludeAuthKeyID != ([8]byte{})
	hasSession := item.ExcludeSessionID != 0
	if hasAuthKey != hasSession {
		return errInvalidDeliveryOutboxExclusionPair
	}
	return nil
}

func (s *DeliveryOutboxStore) ClaimPending(ctx context.Context, limit int) ([]store.DeliveryOutboxItem, error) {
	return s.claimPending(ctx, nil, limit)
}

func (s *DeliveryOutboxStore) ClaimPendingShards(ctx context.Context, shardCount int, shardIDs []int, limit int) ([]store.DeliveryOutboxItem, error) {
	if shardCount <= 0 || len(shardIDs) == 0 {
		return nil, nil
	}
	if shardCount != store.DispatchOutboxLogicalShards {
		return nil, fmt.Errorf("claim edge delivery outbox shards: shard count %d, want stable %d", shardCount, store.DispatchOutboxLogicalShards)
	}
	ids := make([]int16, 0, len(shardIDs))
	seen := make(map[int]struct{}, len(shardIDs))
	for _, id := range shardIDs {
		if id < 0 || id >= shardCount {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, int16(id))
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.claimPending(ctx, ids, limit)
}

func (s *DeliveryOutboxStore) claimPending(ctx context.Context, shardIDs []int16, limit int) ([]store.DeliveryOutboxItem, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	shardFilter := "TRUE"
	args := []any{s.leaseSeconds, int32(limit)}
	if len(shardIDs) > 0 {
		shardFilter = "d.logical_shard = ANY($3::smallint[])"
		args = append(args, shardIDs)
	}
	rows, err := s.db.Query(ctx, fmt.Sprintf(`
WITH picked AS (
  SELECT d.id
  FROM edge_delivery_outbox d
  WHERE %s
    AND NOT EXISTS (
      SELECT 1
      FROM edge_delivery_outbox earlier
      WHERE earlier.target_user_id = d.target_user_id
        AND earlier.id < d.id
    )
    AND (
      (d.status = 'pending' AND d.next_attempt_at <= now())
      OR
      (d.status = 'dispatching' AND d.updated_at <= now() - ($1::int * interval '1 second'))
    )
  ORDER BY d.target_user_id ASC, d.id ASC
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE edge_delivery_outbox d
SET
  status = 'dispatching',
  attempts = d.attempts + 1,
  next_attempt_at = now() + ($1::int * interval '1 second'),
  sent_sessions = 0,
  updated_at = now()
FROM picked
WHERE d.id = picked.id
RETURNING d.id, d.target_user_id, d.exclude_auth_key_id, d.exclude_session_id, d.attempts, d.payload`,
		shardFilter), args...)
	if err != nil {
		return nil, fmt.Errorf("claim edge delivery outbox: %w", err)
	}
	defer rows.Close()
	var out []store.DeliveryOutboxItem
	for rows.Next() {
		var item store.DeliveryOutboxItem
		var excludeAuthKeyID int64
		if err := rows.Scan(&item.ID, &item.TargetUserID, &excludeAuthKeyID, &item.ExcludeSessionID, &item.Attempts, &item.Payload); err != nil {
			return nil, fmt.Errorf("scan edge delivery outbox claim: %w", err)
		}
		item.ExcludeAuthKeyID = authKeyIDFromInt64(excludeAuthKeyID)
		item.Payload = append([]byte(nil), item.Payload...)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate edge delivery outbox claim: %w", err)
	}
	return out, nil
}

func (s *DeliveryOutboxStore) MarkDelivered(ctx context.Context, item store.DeliveryOutboxItem) error {
	tag, err := s.db.Exec(ctx, `
DELETE FROM edge_delivery_outbox
WHERE target_user_id = $1
  AND id = $2
  AND attempts = $3
  AND status = 'dispatching'`,
		item.TargetUserID, item.ID, item.Attempts)
	if err != nil {
		return fmt.Errorf("mark edge delivery delivered: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark edge delivery delivered: %w", store.ErrDispatchLeaseLost)
	}
	return nil
}

// MarkAbandoned deletes an exhausted non-PTS online-delivery task. The source
// account fact remains durable in its owning store; this only releases Egress.
func (s *DeliveryOutboxStore) MarkAbandoned(ctx context.Context, item store.DeliveryOutboxItem, lastError string) error {
	tag, err := s.db.Exec(ctx, `
DELETE FROM edge_delivery_outbox
WHERE target_user_id = $1
  AND id = $2
  AND attempts = $3
  AND status = 'dispatching'`,
		item.TargetUserID, item.ID, item.Attempts)
	if err != nil {
		return fmt.Errorf("mark edge delivery abandoned: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark edge delivery abandoned: %w", store.ErrDispatchLeaseLost)
	}
	return nil
}

func (s *DeliveryOutboxStore) MarkFailed(ctx context.Context, item store.DeliveryOutboxItem, lastError string) error {
	tag, err := s.db.Exec(ctx, `
UPDATE edge_delivery_outbox
SET
  status = 'failed',
  last_error = left($4::text, 2000),
  updated_at = now()
WHERE target_user_id = $1
  AND id = $2
  AND attempts = $3
  AND status = 'dispatching'`,
		item.TargetUserID, item.ID, item.Attempts, lastError)
	if err != nil {
		return fmt.Errorf("mark edge delivery failed: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark edge delivery failed: %w", store.ErrDispatchLeaseLost)
	}
	return nil
}

func (s *DeliveryOutboxStore) MarkClientAcked(ctx context.Context, ack store.DeliveryOutboxClientAck) error {
	if ack.OutboxID <= 0 || ack.TargetUserID <= 0 || ack.Attempt <= 0 || ack.SessionID == 0 || ack.ServerMsgID == 0 {
		return fmt.Errorf("mark edge delivery client acked: invalid ack")
	}
	if ack.AckedAt.IsZero() {
		ack.AckedAt = time.Now()
	}
	tag, err := s.db.Exec(ctx, `
DELETE FROM edge_delivery_outbox
WHERE target_user_id = $1
  AND id = $2
  AND attempts = $3
  AND status = 'dispatching'`,
		ack.TargetUserID, ack.OutboxID, ack.Attempt)
	if err != nil {
		return fmt.Errorf("mark edge delivery client acked: %w", err)
	}
	if tag.RowsAffected() != 1 {
		var status string
		var attempts int
		err := s.db.QueryRow(ctx, `
SELECT status, attempts
FROM edge_delivery_outbox
WHERE target_user_id = $1
  AND id = $2`,
			ack.TargetUserID, ack.OutboxID,
		).Scan(&status, &attempts)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("mark edge delivery client acked inspect current row: %w", err)
		}
		return fmt.Errorf("mark edge delivery client acked: %w: current status=%s attempts=%d ack_attempt=%d",
			store.ErrDispatchLeaseLost, status, attempts, ack.Attempt)
	}
	return nil
}

func (s *DeliveryOutboxStore) DeleteFailed(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	if olderThan <= 0 {
		olderThan = time.Minute
	}
	if limit <= 0 {
		limit = defaultDeliveryPoisonCleanupBatch
	}
	if limit > maxDeliveryPoisonCleanupBatch {
		limit = maxDeliveryPoisonCleanupBatch
	}
	olderThanSeconds := int32(olderThan / time.Second)
	if olderThanSeconds < 1 {
		olderThanSeconds = 1
	}
	tag, err := s.db.Exec(ctx, `
WITH doomed AS (
  SELECT id
  FROM edge_delivery_outbox
  WHERE status = 'failed'
    AND updated_at < now() - ($1::int * interval '1 second')
  ORDER BY updated_at ASC, target_user_id ASC, id ASC
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
DELETE FROM edge_delivery_outbox d
USING doomed
WHERE d.id = doomed.id`,
		olderThanSeconds, int32(limit))
	if err != nil {
		return 0, fmt.Errorf("delete failed edge delivery outbox: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
