package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"telesrv/internal/store"
)

const defaultMemoryDeliveryLease = 30 * time.Second

type DeliveryOutboxStore struct {
	mu     sync.Mutex
	nextID int64
	items  []memoryDeliveryOutboxItem
}

type memoryDeliveryOutboxItem struct {
	store.DeliveryOutboxItem
	status        string
	nextAttemptAt time.Time
	updatedAt     time.Time
}

func NewDeliveryOutboxStore() *DeliveryOutboxStore {
	return &DeliveryOutboxStore{nextID: 1}
}

func (s *DeliveryOutboxStore) Enqueue(_ context.Context, item store.DeliveryOutboxEnqueue) (store.DeliveryOutboxItem, error) {
	if item.TargetUserID <= 0 {
		return store.DeliveryOutboxItem{}, fmt.Errorf("delivery outbox target user id is required")
	}
	if len(item.Payload) == 0 {
		return store.DeliveryOutboxItem{}, fmt.Errorf("delivery outbox payload is required")
	}
	if (item.ExcludeAuthKeyID != ([8]byte{})) != (item.ExcludeSessionID != 0) {
		return store.DeliveryOutboxItem{}, fmt.Errorf("delivery outbox exclusion requires both raw auth key and session id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	out := store.DeliveryOutboxItem{
		ID:               s.nextID,
		TargetUserID:     item.TargetUserID,
		ExcludeAuthKeyID: item.ExcludeAuthKeyID,
		ExcludeSessionID: item.ExcludeSessionID,
		Payload:          append([]byte(nil), item.Payload...),
	}
	s.nextID++
	s.items = append(s.items, memoryDeliveryOutboxItem{
		DeliveryOutboxItem: cloneDeliveryOutboxItem(out),
		status:             "pending",
		nextAttemptAt:      now,
		updatedAt:          now,
	})
	return cloneDeliveryOutboxItem(out), nil
}

func (s *DeliveryOutboxStore) ClaimPending(_ context.Context, limit int) ([]store.DeliveryOutboxItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	now := time.Now()
	blocked := make(map[int64]struct{})
	out := make([]store.DeliveryOutboxItem, 0, limit)
	for i := range s.items {
		item := &s.items[i]
		if _, ok := blocked[item.TargetUserID]; ok {
			continue
		}
		switch item.status {
		case "pending":
			if item.nextAttemptAt.After(now) {
				blocked[item.TargetUserID] = struct{}{}
				continue
			}
		case "dispatching":
			if now.Sub(item.updatedAt) < defaultMemoryDeliveryLease {
				blocked[item.TargetUserID] = struct{}{}
				continue
			}
		case "failed":
			blocked[item.TargetUserID] = struct{}{}
			continue
		default:
			continue
		}
		item.status = "dispatching"
		item.Attempts++
		item.nextAttemptAt = now.Add(defaultMemoryDeliveryLease)
		item.updatedAt = now
		out = append(out, cloneDeliveryOutboxItem(item.DeliveryOutboxItem))
		blocked[item.TargetUserID] = struct{}{}
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *DeliveryOutboxStore) MarkDelivered(_ context.Context, item store.DeliveryOutboxItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.items {
		current := s.items[i]
		if current.ID != item.ID || current.TargetUserID != item.TargetUserID {
			continue
		}
		if current.Attempts == item.Attempts && current.status == "dispatching" {
			s.items = append(s.items[:i], s.items[i+1:]...)
			return nil
		}
		return fmt.Errorf("mark edge delivery delivered: %w: current status=%s attempts=%d item_attempt=%d",
			store.ErrDispatchLeaseLost, current.status, current.Attempts, item.Attempts)
	}
	return store.ErrDispatchLeaseLost
}

func (s *DeliveryOutboxStore) MarkAbandoned(_ context.Context, item store.DeliveryOutboxItem, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.items {
		current := s.items[i]
		if current.ID != item.ID || current.TargetUserID != item.TargetUserID {
			continue
		}
		if current.Attempts == item.Attempts && current.status == "dispatching" {
			s.items = append(s.items[:i], s.items[i+1:]...)
			return nil
		}
		return fmt.Errorf("mark edge delivery abandoned: %w: current status=%s attempts=%d item_attempt=%d",
			store.ErrDispatchLeaseLost, current.status, current.Attempts, item.Attempts)
	}
	return store.ErrDispatchLeaseLost
}

func (s *DeliveryOutboxStore) MarkFailed(_ context.Context, item store.DeliveryOutboxItem, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.items {
		current := &s.items[i]
		if current.ID != item.ID || current.TargetUserID != item.TargetUserID {
			continue
		}
		if current.Attempts == item.Attempts && current.status == "dispatching" {
			current.status = "failed"
			current.updatedAt = time.Now()
			return nil
		}
		return fmt.Errorf("mark edge delivery failed: %w: current status=%s attempts=%d item_attempt=%d",
			store.ErrDispatchLeaseLost, current.status, current.Attempts, item.Attempts)
	}
	return store.ErrDispatchLeaseLost
}

func (s *DeliveryOutboxStore) MarkClientAcked(_ context.Context, ack store.DeliveryOutboxClientAck) error {
	if ack.OutboxID <= 0 || ack.TargetUserID <= 0 || ack.Attempt <= 0 || ack.SessionID == 0 || ack.ServerMsgID == 0 {
		return fmt.Errorf("mark edge delivery client acked: invalid ack")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.items {
		current := s.items[i]
		if current.ID != ack.OutboxID || current.TargetUserID != ack.TargetUserID {
			continue
		}
		if current.Attempts == ack.Attempt && current.status == "dispatching" {
			s.items = append(s.items[:i], s.items[i+1:]...)
			return nil
		}
		return fmt.Errorf("mark edge delivery client acked: %w: current status=%s attempts=%d ack_attempt=%d",
			store.ErrDispatchLeaseLost, current.status, current.Attempts, ack.Attempt)
	}
	return nil
}

func (s *DeliveryOutboxStore) DeleteFailed(_ context.Context, olderThan time.Duration, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if olderThan <= 0 {
		olderThan = time.Minute
	}
	if limit <= 0 {
		limit = 100
	}
	now := time.Now()
	deleted := 0
	out := s.items[:0]
	for _, item := range s.items {
		if deleted < limit && item.status == "failed" && now.Sub(item.updatedAt) >= olderThan {
			deleted++
			continue
		}
		out = append(out, item)
	}
	s.items = out
	return deleted, nil
}

func (s *DeliveryOutboxStore) Snapshot() []store.DeliveryOutboxItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]store.DeliveryOutboxItem, len(s.items))
	for i, item := range s.items {
		out[i] = cloneDeliveryOutboxItem(item.DeliveryOutboxItem)
	}
	return out
}

func cloneDeliveryOutboxItem(item store.DeliveryOutboxItem) store.DeliveryOutboxItem {
	item.Payload = append([]byte(nil), item.Payload...)
	return item
}
