package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

const (
	defaultChannelDeliveryWindowSize      = 32
	defaultChannelDeliveryWindowByteLimit = 1 << 20
	maxChannelDeliveryWindowByteLimit     = store.MaxDeliveryBatchBytes
	defaultChannelDeliveryLease           = 30 * time.Second
	maxChannelDeliveryLaneLimit           = 1000
	maxChannelDeliveryWindowSize          = store.MaxDeliveryBatchItems
)

type channelDeliveryTxBeginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

// ChannelDeliveryStore is physically isolated from both account outbox
// stores.  Its stream is channel_id and its sequence is channel PTS.
type ChannelDeliveryStore struct {
	db sqlcgen.DBTX
}

func NewChannelDeliveryStore(db sqlcgen.DBTX) *ChannelDeliveryStore {
	return &ChannelDeliveryStore{db: db}
}

func (s *ChannelDeliveryStore) begin(ctx context.Context) (pgx.Tx, error) {
	beginner, ok := s.db.(channelDeliveryTxBeginner)
	if !ok {
		return nil, fmt.Errorf("channel delivery store: database does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin channel delivery transaction: %w", err)
	}
	return tx, nil
}

func (s *ChannelDeliveryStore) ClaimWindows(ctx context.Context, req store.OutboxClaimRequest) ([]store.OutboxClaimWindow, error) {
	if req.QueueKind != store.OutboxQueueChannelPTS {
		return nil, fmt.Errorf("claim channel delivery windows: queue kind %d is not channel PTS", req.QueueKind)
	}
	owner := strings.TrimSpace(req.Owner)
	if owner == "" || owner != req.Owner || len(owner) > store.MaxDeliveryLeaseOwnerBytes {
		return nil, fmt.Errorf("claim channel delivery windows: owner is required and must fit 255 bytes")
	}
	shards, scoped, err := normalizeChannelDeliveryShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, fmt.Errorf("claim channel delivery windows: %w", err)
	}
	if scoped && len(shards) == 0 {
		return nil, nil
	}
	laneLimit := req.LaneLimit
	if laneLimit <= 0 {
		laneLimit = 64
	}
	if laneLimit > maxChannelDeliveryLaneLimit {
		laneLimit = maxChannelDeliveryLaneLimit
	}
	windowSize, byteLimit := normalizeChannelDeliveryWindow(req.WindowSize, req.WindowByteLimit)
	lease := req.LeaseDuration
	if lease <= 0 {
		lease = defaultChannelDeliveryLease
	}
	if lease.Microseconds() <= 0 {
		return nil, fmt.Errorf("claim channel delivery windows: lease duration is required")
	}
	if req.PhysicalDuration <= 0 {
		return nil, fmt.Errorf("claim channel delivery windows: physical duration is required")
	}
	if req.ClockSkewAllowance <= 0 {
		return nil, fmt.Errorf("claim channel delivery windows: clock skew allowance is required")
	}
	if lease <= outboxLeaseSafetyMargin ||
		req.PhysicalDuration >= lease-outboxLeaseSafetyMargin ||
		req.ClockSkewAllowance >= lease-outboxLeaseSafetyMargin-req.PhysicalDuration {
		return nil, fmt.Errorf("claim channel delivery windows: physical duration plus clock skew allowance and lease safety must be shorter than lease duration")
	}

	rows, err := s.db.Query(ctx, `
WITH candidates AS MATERIALIZED (
    SELECT l.channel_id, l.head_item_id, l.lease_fence AS old_lease_fence,
           nextval('public.channel_delivery_lease_fence_seq') AS lease_fence,
           statement_timestamp() + ($5::bigint * interval '1 microsecond') AS lease_until,
           statement_timestamp() + ($9::bigint * interval '1 microsecond') AS command_not_after,
           statement_timestamp() + (($9::bigint + $10::bigint) * interval '1 microsecond') AS evidence_deadline
    FROM channel_delivery_lanes l
    WHERE ($1::boolean = false OR l.logical_shard = ANY($2::smallint[]))
      AND (
          (l.state = 'ready' AND l.ready_at <= clock_timestamp())
          OR (l.state = 'leased' AND l.lease_until <= clock_timestamp())
      )
      AND l.lease_fence < $8
      AND EXISTS (
          SELECT 1 FROM channel_delivery_events h WHERE h.id = l.head_item_id AND h.channel_id = l.channel_id
      )
      AND NOT EXISTS (
          SELECT 1
          FROM channel_delivery_attempts a
          WHERE a.channel_id = l.channel_id
            AND a.lease_fence = l.lease_fence
            AND a.finalized_at IS NULL
            AND (a.targets_bound OR a.resolution <> 'pending')
      )
    ORDER BY l.ready_at, l.channel_id
    FOR UPDATE OF l SKIP LOCKED
    LIMIT $3
), window_items AS MATERIALIZED (
    SELECT l.channel_id, l.lease_fence, l.lease_until,
           l.command_not_after, l.evidence_deadline,
           e.id AS item_id, e.min_pts, e.max_pts, e.projection_kind,
           e.audience_kind, e.audience_user_ids, e.affected_user_ids, e.ordinal
    FROM candidates l
    CROSS JOIN LATERAL (
        WITH RECURSIVE chain AS (
            SELECT h.id, h.min_pts, h.max_pts, h.projection_kind, h.audience_kind,
                   h.audience_user_ids, h.affected_user_ids,
                   0 AS ordinal,
                   (32 + octet_length(h.projection_kind) + octet_length(h.audience_kind)
                       + 8 * cardinality(h.audience_user_ids)
                       + 8 * cardinality(h.affected_user_ids))::bigint AS used_bytes
            FROM channel_delivery_events h
            WHERE h.id = l.head_item_id AND h.channel_id = l.channel_id
          UNION ALL
            SELECT n.id, n.min_pts, n.max_pts, n.projection_kind, n.audience_kind,
                   n.audience_user_ids, n.affected_user_ids,
                   c.ordinal + 1,
                   c.used_bytes + 32 + octet_length(n.projection_kind)
                     + octet_length(n.audience_kind) + 8 * cardinality(n.audience_user_ids)
                     + 8 * cardinality(n.affected_user_ids)
            FROM chain c
            JOIN LATERAL (
                SELECT x.id, x.min_pts, x.max_pts, x.projection_kind,
                       x.audience_kind, x.audience_user_ids, x.affected_user_ids
                FROM channel_delivery_events x
                WHERE x.channel_id = l.channel_id AND x.min_pts = c.max_pts + 1
                ORDER BY x.id
                LIMIT 1
            ) n ON true
            WHERE c.ordinal + 1 < $6
              AND c.used_bytes + 32 + octet_length(n.projection_kind)
                    + octet_length(n.audience_kind) + 8 * cardinality(n.audience_user_ids)
                    + 8 * cardinality(n.affected_user_ids) <= $7
        )
        SELECT id, min_pts, max_pts, projection_kind, audience_kind, audience_user_ids,
               affected_user_ids, ordinal
        FROM chain
    ) e
), numbered AS MATERIALIZED (
    SELECT w.*,
           COALESCE((
               SELECT max(a.attempt_no)
               FROM channel_delivery_attempts a
               WHERE a.item_id = w.item_id
           ), 0) + 1 AS attempt_no
    FROM window_items w
), planned_tails AS MATERIALIZED (
    SELECT DISTINCT ON (channel_id) channel_id, item_id, max_pts
    FROM window_items
    ORDER BY channel_id, ordinal DESC
), leased AS MATERIALIZED (
    UPDATE channel_delivery_lanes l
    SET state = 'leased',
        lease_fence = c.lease_fence,
        lease_owner = $4,
        lease_until = c.lease_until,
        window_tail_item_id = tail.item_id,
        window_tail_sequence = tail.max_pts,
        last_error = NULL,
        updated_at = clock_timestamp()
    FROM candidates c
    JOIN planned_tails tail ON tail.channel_id = c.channel_id
    WHERE l.channel_id = c.channel_id
    RETURNING l.channel_id, l.lease_fence, l.lease_until, c.old_lease_fence
), inserted_attempts AS MATERIALIZED (
    INSERT INTO channel_delivery_attempts (
        item_id, lease_fence, channel_id, min_pts, max_pts,
        attempt_no, window_ordinal, owner, lease_until, command_not_after, evidence_deadline
    )
    SELECT item_id, lease_fence, channel_id, min_pts, max_pts,
           attempt_no, ordinal, $4, lease_until, command_not_after, evidence_deadline
    FROM numbered
    JOIN leased USING (channel_id, lease_fence, lease_until)
    RETURNING item_id, lease_fence, attempt_no
), superseded_unbound AS MATERIALIZED (
    UPDATE channel_delivery_attempts old
    SET finalized_at = clock_timestamp(),
        final_outcome = 'superseded',
        resolution_error = 'unbound channel delivery lease superseded after expiry'
    FROM leased fresh
    WHERE old.channel_id = fresh.channel_id
      AND old.lease_fence = fresh.old_lease_fence
      AND NOT old.targets_bound
      AND old.resolution = 'pending'
      AND old.finalized_at IS NULL
    RETURNING old.item_id, old.lease_fence
)
SELECT n.channel_id, n.lease_fence, n.lease_until,
       n.command_not_after, n.evidence_deadline,
       n.item_id, n.min_pts, n.max_pts, n.projection_kind, n.audience_kind,
       n.audience_user_ids, n.affected_user_ids,
       n.attempt_no
FROM numbered n
JOIN inserted_attempts a ON a.item_id = n.item_id AND a.lease_fence = n.lease_fence
JOIN leased l ON l.channel_id = n.channel_id AND l.lease_fence = n.lease_fence
ORDER BY n.channel_id, n.ordinal`,
		scoped,
		shards,
		laneLimit,
		req.Owner,
		lease.Microseconds(),
		windowSize,
		byteLimit,
		int64(math.MaxInt64),
		req.PhysicalDuration.Microseconds(),
		req.ClockSkewAllowance.Microseconds(),
	)
	if err != nil {
		return nil, fmt.Errorf("claim channel delivery windows: %w", err)
	}
	defer rows.Close()

	windows := make([]store.OutboxClaimWindow, 0, laneLimit)
	windowIndex := make(map[int64]int, laneLimit)
	for rows.Next() {
		var (
			channelID, itemID, fence                      int64
			leaseUntil, commandNotAfter, evidenceDeadline time.Time
			minPTS, maxPTS, attempt                       int
			projection, audience                          string
			audienceUserIDs                               []int64
			affectedUserIDs                               []int64
		)
		if err := rows.Scan(
			&channelID, &fence, &leaseUntil, &commandNotAfter, &evidenceDeadline,
			&itemID, &minPTS, &maxPTS, &projection, &audience, &audienceUserIDs, &affectedUserIDs, &attempt,
		); err != nil {
			return nil, fmt.Errorf("scan channel delivery claim: %w", err)
		}
		idx, ok := windowIndex[channelID]
		if !ok {
			idx = len(windows)
			windowIndex[channelID] = idx
			windows = append(windows, store.OutboxClaimWindow{
				QueueKind:  store.OutboxQueueChannelPTS,
				StreamID:   channelID,
				Owner:      req.Owner,
				LeaseFence: uint64(fence),
				LeaseUntil: leaseUntil,
			})
		}
		ref := store.OutboxAttemptRef{
			QueueKind:  store.OutboxQueueChannelPTS,
			StreamID:   channelID,
			ItemID:     itemID,
			Sequence:   int64(maxPTS),
			LeaseFence: uint64(fence),
			Attempt:    attempt,
		}
		windows[idx].Items = append(windows[idx].Items, store.OutboxClaimedItem{
			Ref:              ref,
			RecoveryPolicy:   store.OutboxRecoveryDifference,
			CommandNotAfter:  commandNotAfter,
			EvidenceDeadline: evidenceDeadline,
			Payload: store.ChannelDeliveryPayload{
				MinPTS:          minPTS,
				MaxPTS:          maxPTS,
				ProjectionKind:  domain.ChannelUpdateEventType(projection),
				AudienceKind:    store.ChannelDeliveryAudienceKind(audience),
				AudienceUserIDs: append([]int64(nil), audienceUserIDs...),
				AffectedUserIDs: append([]int64(nil), affectedUserIDs...),
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel delivery claims: %w", err)
	}
	return windows, nil
}

func (s *ChannelDeliveryStore) NextReadyAt(ctx context.Context, kind store.OutboxQueueKind, shardCount int, shardIDs []int) (store.OutboxNextReady, bool, error) {
	if kind != store.OutboxQueueChannelPTS {
		return store.OutboxNextReady{}, false, fmt.Errorf("next channel delivery ready at: queue kind %d is not channel PTS", kind)
	}
	shards, scoped, err := normalizeChannelDeliveryShards(shardCount, shardIDs)
	if err != nil {
		return store.OutboxNextReady{}, false, fmt.Errorf("next channel delivery ready at: %w", err)
	}
	if scoped && len(shards) == 0 {
		return store.OutboxNextReady{}, false, nil
	}
	var observed time.Time
	var readyAt *time.Time
	err = s.db.QueryRow(ctx, `
SELECT clock_timestamp(), min(
    CASE state
      WHEN 'ready' THEN ready_at
      WHEN 'leased' THEN CASE WHEN NOT EXISTS (
        SELECT 1
        FROM channel_delivery_attempts pending_finalize
        WHERE pending_finalize.channel_id = l.channel_id
          AND pending_finalize.lease_fence = l.lease_fence
          AND pending_finalize.item_id = l.head_item_id
          AND pending_finalize.resolution <> 'pending'
          AND pending_finalize.finalized_at IS NULL
          AND (pending_finalize.retry_at IS NULL OR pending_finalize.retry_at <= clock_timestamp())
      ) THEN LEAST(lease_until, COALESCE((
        SELECT CASE
          WHEN head_attempt.resolution = 'pending' THEN head_attempt.evidence_deadline
          ELSE head_attempt.retry_at
        END
        FROM channel_delivery_attempts head_attempt
        WHERE head_attempt.channel_id = l.channel_id
          AND head_attempt.item_id = l.head_item_id
          AND head_attempt.lease_fence = l.lease_fence
          AND head_attempt.finalized_at IS NULL
      ), lease_until)) END
    END
)
FROM channel_delivery_lanes l
WHERE ($1::boolean = false OR l.logical_shard = ANY($2::smallint[]))
  AND state IN ('ready', 'leased')`, scoped, shards).Scan(&observed, &readyAt)
	if err != nil {
		return store.OutboxNextReady{}, false, fmt.Errorf("next channel delivery ready at: %w", err)
	}
	if readyAt == nil {
		return store.OutboxNextReady{}, false, nil
	}
	return store.OutboxNextReady{ObservedAt: observed, ReadyAt: *readyAt}, true, nil
}

// DeleteFinalizedAttempts performs bounded capacity GC for durable channel
// delivery tombstones. Finalized attempts remain queryable while their event
// still exists; only tombstones whose immutable event has already been removed
// are eligible. Target rows are removed by the attempt FK's ON DELETE CASCADE.
func (s *ChannelDeliveryStore) DeleteFinalizedAttempts(ctx context.Context, finalizedBefore time.Time, limit int) (int, error) {
	if finalizedBefore.IsZero() {
		return 0, fmt.Errorf("delete finalized channel delivery attempts: finalized_before is required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("delete finalized channel delivery attempts: positive limit is required")
	}
	if limit > maxOutboxTombstoneDeleteLimit {
		limit = maxOutboxTombstoneDeleteLimit
	}
	tag, err := s.db.Exec(ctx, `
WITH picked AS MATERIALIZED (
  SELECT a.item_id, a.lease_fence
  FROM channel_delivery_attempts a
  WHERE a.finalized_at < $1
    AND NOT EXISTS (SELECT 1 FROM channel_delivery_events e WHERE e.id = a.item_id)
  ORDER BY a.finalized_at, a.item_id, a.lease_fence
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
DELETE FROM channel_delivery_attempts a
USING picked p
WHERE a.item_id = p.item_id AND a.lease_fence = p.lease_fence`, finalizedBefore, int32(limit))
	if err != nil {
		return 0, fmt.Errorf("delete finalized channel delivery attempts: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func normalizeChannelDeliveryWindow(size, byteLimit int) (int, int) {
	if size <= 0 {
		size = defaultChannelDeliveryWindowSize
	}
	if size > maxChannelDeliveryWindowSize {
		size = maxChannelDeliveryWindowSize
	}
	if byteLimit <= 0 {
		byteLimit = defaultChannelDeliveryWindowByteLimit
	}
	if byteLimit > maxChannelDeliveryWindowByteLimit {
		byteLimit = maxChannelDeliveryWindowByteLimit
	}
	return size, byteLimit
}

func normalizeChannelDeliveryShards(count int, ids []int) ([]int16, bool, error) {
	if count == 0 && len(ids) == 0 {
		return nil, false, nil
	}
	if count != store.ChannelDeliveryLogicalShards {
		return nil, false, fmt.Errorf("logical shard count = %d, want %d", count, store.ChannelDeliveryLogicalShards)
	}
	seen := make(map[int]struct{}, len(ids))
	out := make([]int16, 0, len(ids))
	for _, id := range ids {
		if id < 0 || id >= count {
			return nil, false, fmt.Errorf("logical shard id %d is outside [0,%d)", id, count)
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, int16(id))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, true, nil
}

func validateChannelAttemptRef(ref store.OutboxAttemptRef) error {
	if ref.QueueKind != store.OutboxQueueChannelPTS {
		return fmt.Errorf("queue kind %d is not channel PTS", ref.QueueKind)
	}
	if ref.StreamID <= 0 || ref.ItemID <= 0 || ref.Sequence <= 0 || ref.LeaseFence == 0 || ref.LeaseFence > uint64(^uint64(0)>>1) || ref.Attempt <= 0 {
		return errors.New("incomplete channel attempt reference")
	}
	return nil
}
