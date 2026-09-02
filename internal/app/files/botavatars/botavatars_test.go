package botavatars

import (
	"context"
	"errors"
	"sync"
	"testing"

	"telesrv/internal/domain"
)

// txFakeAvatarSetter models a storage boundary whose SeedTx wraps fn in a
// transaction with serialising semantics: fn may observe the current state,
// and only when it returns nil are the created photos and bindings committed.
// If fn returns an error the transaction aborts and nothing is persisted, so a
// photo created and then abandoned is rolled back (never orphaned).
type txFakeAvatarSetter struct {
	// serialMu models the storage advisory lock: it serialises whole SeedTx
	// transactions across concurrent instances.
	serialMu sync.Mutex
	mu       sync.Mutex

	// current maps peerID -> photoID (the persisted active avatar).
	current map[int64]int64
	// createdPhotos counts photos created and committed to the backend.
	createdPhotos map[int64]bool
	// committed counts how many transactions committed successfully.
	committed int
	// seedTxErr, when set, makes SeedTx fail before running fn.
	seedTxErr error
	// readErr, when set, makes CurrentProfilePhotoKind fail.
	readErr error
	// createErr, when set, makes CreateAvatar* fail.
	createErr error
}

func newTxFakeAvatarSetter() *txFakeAvatarSetter {
	return &txFakeAvatarSetter{
		current:       make(map[int64]int64),
		createdPhotos: make(map[int64]bool),
	}
}

func (f *txFakeAvatarSetter) CurrentProfilePhotoKind(_ context.Context, _ domain.PeerType, peerID int64, _ domain.ProfilePhotoKind) (domain.Photo, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return domain.Photo{}, false, f.readErr
	}
	if id, ok := f.current[peerID]; ok {
		return domain.Photo{ID: id}, true, nil
	}
	return domain.Photo{}, false, nil
}

func (f *txFakeAvatarSetter) CreateAvatarFromBytes(_ context.Context, _ []byte) (domain.Photo, error) {
	return f.create()
}

func (f *txFakeAvatarSetter) CreateAvatarVideoFromBytes(_ context.Context, _ []byte, _ float64) (domain.Photo, error) {
	return f.create()
}

func (f *txFakeAvatarSetter) create() (domain.Photo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return domain.Photo{}, f.createErr
	}
	// Tentatively allocate an ID; only persisted if the tx commits.
	id := int64(len(f.createdPhotos) + 1)
	return domain.Photo{ID: id}, nil
}

func (f *txFakeAvatarSetter) SetCurrentProfilePhotoKind(_ context.Context, _ domain.PeerType, peerID int64, _ domain.ProfilePhotoKind, photoID int64, _ int) (domain.Photo, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.current[peerID] > 0 {
		// Simulate the storage invariant: an active avatar already exists.
		return domain.Photo{}, false, nil
	}
	f.current[peerID] = photoID
	f.createdPhotos[photoID] = true
	return domain.Photo{ID: photoID}, true, nil
}

func (f *txFakeAvatarSetter) SeedTx(ctx context.Context, fn func(ctx context.Context, tx AvatarSetter) error) error {
	// Serialise the whole transaction (models the storage advisory lock).
	f.serialMu.Lock()
	defer f.serialMu.Unlock()

	f.mu.Lock()
	if f.seedTxErr != nil {
		e := f.seedTxErr
		f.mu.Unlock()
		return e
	}
	f.mu.Unlock()

	// Snapshot the pre-transaction state so we can roll back on error.
	f.mu.Lock()
	prevCurrent := make(map[int64]int64, len(f.current))
	for k, v := range f.current {
		prevCurrent[k] = v
	}
	prevCreated := make(map[int64]bool, len(f.createdPhotos))
	for k, v := range f.createdPhotos {
		prevCreated[k] = v
	}
	f.mu.Unlock()

	if err := fn(ctx, f); err != nil {
		// Roll back: restore the committed state (media created and bindings
		// are discarded, leaving no orphan photos), and propagate the error so
		// Seed fails rather than pretending nothing happened.
		f.mu.Lock()
		f.current = prevCurrent
		f.createdPhotos = prevCreated
		f.mu.Unlock()
		return err
	}
	f.mu.Lock()
	f.committed++
	f.mu.Unlock()
	return nil
}

func TestSeedSkipsExistingAvatars(t *testing.T) {
	av := newTxFakeAvatarSetter()
	for peerID := range peersForSeed() {
		av.current[peerID] = int64(peerID)
	}

	if err := Seed(context.Background(), av, 1000); err != nil {
		t.Fatalf("Seed returned error: %v", err)
	}

	av.mu.Lock()
	defer av.mu.Unlock()
	for peerID, id := range av.current {
		if id != int64(peerID) {
			t.Fatalf("existing avatar for peer %d changed to %d", peerID, id)
		}
	}
	if len(av.createdPhotos) != 0 {
		t.Fatalf("expected no new photos, got %d", len(av.createdPhotos))
	}
}

func TestSeedReadErrorFailsWithoutWriting(t *testing.T) {
	av := newTxFakeAvatarSetter()
	av.readErr = errors.New("db connection lost")

	// A read error must fail the seed rather than seeding as if no avatar
	// existed. The transaction is aborted, so nothing is written.
	if err := Seed(context.Background(), av, 1000); err == nil {
		t.Fatalf("expected Seed to fail on read error")
	}

	av.mu.Lock()
	defer av.mu.Unlock()
	if len(av.createdPhotos) != 0 {
		t.Fatalf("no photos should be persisted after a read error, got %d", len(av.createdPhotos))
	}
	if len(av.current) != 0 {
		t.Fatalf("no avatars should be bound after a read error, got %d", len(av.current))
	}
}

func TestSeedCreateErrorRollsBackTransaction(t *testing.T) {
	av := newTxFakeAvatarSetter()
	av.createErr = errors.New("blob write failed")

	if err := Seed(context.Background(), av, 1000); err == nil {
		t.Fatalf("expected Seed to fail on create error")
	}

	av.mu.Lock()
	defer av.mu.Unlock()
	if len(av.current) != 0 {
		t.Fatalf("no avatars should be bound after a create error, got %d", len(av.current))
	}
}

func TestSeedConcurrentInstancesExactlyOneAvatarNoOrphan(t *testing.T) {
	// Two Seed calls share one backend (two server instances). Storage-level
	// serialisation means the first writes everything, the second observes an
	// existing avatar and writes nothing.
	av := newTxFakeAvatarSetter()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := Seed(context.Background(), av, 1000); err != nil {
				t.Errorf("Seed failed: %v", err)
			}
		}()
	}
	wg.Wait()

	av.mu.Lock()
	defer av.mu.Unlock()
	// Exactly one active avatar per peer, and every photo it references exists.
	for peerID, id := range av.current {
		if id == 0 {
			t.Fatalf("peer %d has a zero photo id", peerID)
		}
		if !av.createdPhotos[id] {
			t.Fatalf("peer %d references photo %d which was not created", peerID, id)
		}
	}
	// Every committed avatar must be the current one (no orphan bindings that
	// lost a race): committed photos == bound photos.
	if len(av.createdPhotos) != len(peersForSeed()) {
		t.Fatalf("expected exactly %d photos total, got %d", len(peersForSeed()), len(av.createdPhotos))
	}
	if len(av.current) != len(peersForSeed()) {
		t.Fatalf("expected %d avatars, got %d", len(peersForSeed()), len(av.current))
	}
}

func TestSeedSeedTxFailure(t *testing.T) {
	av := newTxFakeAvatarSetter()
	av.seedTxErr = errors.New("advisory lock timeout")

	if err := Seed(context.Background(), av, 1000); err == nil {
		t.Fatalf("expected Seed to fail when SeedTx fails")
	}
}

func TestSeedCreatesAllBots(t *testing.T) {
	av := newTxFakeAvatarSetter()

	if err := Seed(context.Background(), av, 1000); err != nil {
		t.Fatalf("Seed failed: %v", err)
	}

	av.mu.Lock()
	defer av.mu.Unlock()
	expectedPeers := map[int64]bool{}
	for peerID := range peersForSeed() {
		expectedPeers[peerID] = false
	}
	for peerID, id := range av.current {
		if id == 0 {
			t.Errorf("peer %d has a zero photo id", peerID)
		}
		expectedPeers[peerID] = true
	}
	for peer, found := range expectedPeers {
		if !found {
			t.Errorf("peer %d was not seeded", peer)
		}
	}
	if len(av.createdPhotos) != len(peersForSeed()) {
		t.Errorf("expected %d photos created, got %d", len(peersForSeed()), len(av.createdPhotos))
	}
}

func TestSeedUsesConfiguredPremiumBotID(t *testing.T) {
	const customPremiumID = int64(900000001)
	prev := domain.PremiumBotConfiguredUserID()
	if !domain.ConfigurePremiumBotUserID(customPremiumID) {
		t.Fatalf("failed to configure custom premium bot ID")
	}
	t.Cleanup(func() {
		domain.ConfigurePremiumBotUserID(prev)
	})

	av := newTxFakeAvatarSetter()
	if err := Seed(context.Background(), av, 1000); err != nil {
		t.Fatalf("Seed failed: %v", err)
	}

	av.mu.Lock()
	defer av.mu.Unlock()
	var foundCustom, foundDefault bool
	for peerID := range av.current {
		if peerID == customPremiumID {
			foundCustom = true
		}
		if peerID == domain.PremiumBotUserID {
			foundDefault = true
		}
	}
	if !foundCustom {
		t.Errorf("configured premium bot ID %d was not seeded", customPremiumID)
	}
	if foundDefault {
		t.Errorf("default premium bot ID %d should not be seeded when configured", domain.PremiumBotUserID)
	}
}
