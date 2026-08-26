package redisstore

import (
	"context"
	"encoding/binary"
	"errors"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// LayerAwareTempAuthKeyBindingStore makes the PostgreSQL identity fact and the
// Redis protocol-Layer identity/default merge one fail-closed operation at the
// service boundary. PostgreSQL Save is idempotent, so a Redis failure is safely
// retried by the client without a compatibility path.
type LayerAwareTempAuthKeyBindingStore struct {
	base   store.TempAuthKeyBindingStore
	layers store.AuthKeySessionLayerIdentityBinder
}

func NewLayerAwareTempAuthKeyBindingStore(
	base store.TempAuthKeyBindingStore,
	layers store.AuthKeySessionLayerIdentityBinder,
) (*LayerAwareTempAuthKeyBindingStore, error) {
	if base == nil || layers == nil {
		return nil, errors.New("temp auth key Layer binding dependencies are required")
	}
	return &LayerAwareTempAuthKeyBindingStore{base: base, layers: layers}, nil
}

func (s *LayerAwareTempAuthKeyBindingStore) Save(ctx context.Context, binding domain.TempAuthKeyBinding) error {
	if s == nil || s.base == nil || s.layers == nil {
		return store.ErrAuthKeySessionLayerStoreRequired
	}
	if err := s.base.Save(ctx, binding); err != nil {
		return err
	}
	var effective [8]byte
	binary.LittleEndian.PutUint64(effective[:], uint64(binding.PermAuthKeyID))
	return s.layers.BindAuthKeyLayerIdentity(
		ctx, binding.TempAuthKeyID, effective, binding.ExpiresAt,
	)
}

func (s *LayerAwareTempAuthKeyBindingStore) GetByTemp(
	ctx context.Context,
	tempAuthKeyID [8]byte,
) (domain.TempAuthKeyBinding, bool, error) {
	if s == nil || s.base == nil {
		return domain.TempAuthKeyBinding{}, false, store.ErrAuthKeySessionLayerStoreRequired
	}
	return s.base.GetByTemp(ctx, tempAuthKeyID)
}

func (s *LayerAwareTempAuthKeyBindingStore) DeleteExpired(
	ctx context.Context,
	expiredBefore int64,
	limit int,
) (int, error) {
	if s == nil || s.base == nil {
		return 0, store.ErrAuthKeySessionLayerStoreRequired
	}
	return s.base.DeleteExpired(ctx, expiredBefore, limit)
}

var _ store.TempAuthKeyBindingStore = (*LayerAwareTempAuthKeyBindingStore)(nil)
