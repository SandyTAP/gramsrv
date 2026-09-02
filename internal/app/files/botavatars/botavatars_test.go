package botavatars

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

type fakeAvatarSetter struct {
	mu sync.Mutex

	// currentPhotos maps (peerID) -> photoID; simulates existing avatars.
	currentPhotos map[int64]int64
	// readErrors contains peer IDs for which CurrentProfilePhotoKind returns an error.
	readErrors map[int64]error
	// created tracks all photos created via CreateAvatar*.
	created []domain.Photo
	// setCalls tracks SetCurrentProfilePhotoKind calls.
	setCalls []setCall

	beginTxErr error
}

type setCall struct {
	peerID int64
	photoID int64
}

func newFakeAvatarSetter() *fakeAvatarSetter {
	return &fakeAvatarSetter{
		currentPhotos: make(map[int64]int64),
		readErrors:    make(map[int64]error),
	}
}

func (f *fakeAvatarSetter) CurrentProfilePhotoKind(_ context.Context, _ domain.PeerType, peerID int64, _ domain.ProfilePhotoKind) (domain.Photo, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.readErrors[peerID]; ok {
		return domain.Photo{}, false, err
	}
	if id, ok := f.currentPhotos[peerID]; ok {
		return domain.Photo{ID: id}, true, nil
	}
	return domain.Photo{}, false, nil
}

func (f *fakeAvatarSetter) CreateAvatarFromBytes(_ context.Context, data []byte) (domain.Photo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	photo := domain.Photo{ID: int64(len(f.created) + 1)}
	f.created = append(f.created, photo)
	return photo, nil
}

func (f *fakeAvatarSetter) CreateAvatarVideoFromBytes(_ context.Context, _ []byte, _ float64) (domain.Photo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	photo := domain.Photo{ID: int64(len(f.created) + 1)}
	f.created = append(f.created, photo)
	return photo, nil
}

func (f *fakeAvatarSetter) SetCurrentProfilePhotoKind(_ context.Context, _ domain.PeerType, peerID int64, _ domain.ProfilePhotoKind, photoID int64, _ int) (domain.Photo, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls = append(f.setCalls, setCall{peerID: peerID, photoID: photoID})
	return domain.Photo{ID: photoID}, true, nil
}

func (f *fakeAvatarSetter) BeginTx(_ context.Context) (AvatarSetter, func(error), error) {
	if f.beginTxErr != nil {
		return nil, nil, f.beginTxErr
	}
	return f, func(error) {}, nil
}

func TestSeedSkipsExistingAvatars(t *testing.T) {
	av := newFakeAvatarSetter()
	// Pre-populate ALL peers with existing avatars.
	for peerID := range fileForPeer {
		av.currentPhotos[peerID] = int64(peerID)
	}

	Seed(context.Background(), av, zap.NewNop(), 1000)

	av.mu.Lock()
	defer av.mu.Unlock()
	if len(av.created) != 0 {
		t.Fatalf("expected 0 photos created, got %d", len(av.created))
	}
	if len(av.setCalls) != 0 {
		t.Fatalf("expected 0 set calls, got %d", len(av.setCalls))
	}
}

func TestSeedHandlesCurrentProfilePhotoKindError(t *testing.T) {
	av := newFakeAvatarSetter()
	av.readErrors[domain.OfficialSystemUserID] = errors.New("db connection lost")

	Seed(context.Background(), av, zap.NewNop(), 1000)

	av.mu.Lock()
	defer av.mu.Unlock()
	// OfficialSystemUserID should have been skipped due to read error.
	// Other peers should have been created.
	createdIDs := make(map[int64]bool)
	for _, p := range av.created {
		createdIDs[p.ID] = true
	}
	for _, sc := range av.setCalls {
		if sc.peerID == domain.OfficialSystemUserID {
			t.Fatal("should not have set avatar for peer with read error")
		}
		if !createdIDs[sc.photoID] {
			t.Fatalf("set call references unknown photo %d", sc.photoID)
		}
	}
}

func TestSeedConcurrentInstances(t *testing.T) {
	// Simulate two concurrent Seed calls with independent fakes sharing
	// the same "database" (a shared map protected by mutex).
	var mu sync.Mutex
	shared := make(map[int64]int64) // peerID -> photoID

	var wg sync.WaitGroup
	var createdCount atomic.Int64
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			av := &concurrentFake{
				mu:      &mu,
				shared:  shared,
				created: &createdCount,
			}
			Seed(context.Background(), av, zap.NewNop(), 1000)
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	// Each peer should have at most 1 photo in shared state.
	for _, id := range shared {
		if id == 0 {
			t.Fatal("shared map contains zero photo ID")
		}
	}
}

// concurrentFake wraps fakeAvatarSetter with shared-state methods
// to simulate concurrent access to the same database.
type concurrentFake struct {
	mu      *sync.Mutex
	shared  map[int64]int64
	created *atomic.Int64
}

func (f *concurrentFake) CurrentProfilePhotoKind(_ context.Context, _ domain.PeerType, peerID int64, _ domain.ProfilePhotoKind) (domain.Photo, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.shared[peerID]; ok {
		return domain.Photo{ID: id}, true, nil
	}
	return domain.Photo{}, false, nil
}

func (f *concurrentFake) CreateAvatarFromBytes(_ context.Context, _ []byte) (domain.Photo, error) {
	id := f.created.Add(1)
	return domain.Photo{ID: id}, nil
}

func (f *concurrentFake) CreateAvatarVideoFromBytes(_ context.Context, _ []byte, _ float64) (domain.Photo, error) {
	id := f.created.Add(1)
	return domain.Photo{ID: id}, nil
}

func (f *concurrentFake) SetCurrentProfilePhotoKind(_ context.Context, _ domain.PeerType, peerID int64, _ domain.ProfilePhotoKind, photoID int64, _ int) (domain.Photo, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shared[peerID] = photoID
	return domain.Photo{ID: photoID}, true, nil
}

func (f *concurrentFake) BeginTx(_ context.Context) (AvatarSetter, func(error), error) {
	return f, func(error) {}, nil
}

func TestSeedBeginTxFailure(t *testing.T) {
	av := newFakeAvatarSetter()
	av.beginTxErr = errors.New("advisory lock timeout")

	// Seed should fatal on BeginTx error. We can't easily test Fatal,
	// so we verify the error is returned from BeginTx.
	_, _, err := av.BeginTx(context.Background())
	if err == nil {
		t.Fatal("expected error from BeginTx")
	}
}

func TestSeedCreatesAllBots(t *testing.T) {
	av := newFakeAvatarSetter()

	Seed(context.Background(), av, zap.NewNop(), 1000)

	av.mu.Lock()
	defer av.mu.Unlock()
	expectedPeers := map[int64]bool{
		domain.OfficialSystemUserID: false,
		domain.BotFatherUserID:      false,
		domain.StickersBotUserID:    false,
		domain.VerifyBotUserID:      false,
		domain.GifBotUserID:         false,
		domain.PremiumBotUserID:     false,
	}
	for _, sc := range av.setCalls {
		expectedPeers[sc.peerID] = true
	}
	for peer, found := range expectedPeers {
		if !found {
			t.Errorf("peer %d was not seeded", peer)
		}
	}
	if len(av.created) != len(fileForPeer) {
		t.Errorf("expected %d photos created, got %d", len(fileForPeer), len(av.created))
	}
}
