package botavatars

import (
	"context"
	"embed"
	"io/fs"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

//go:embed *.jpg *.mp4
var assets embed.FS

// advisoryLockKey is a PostgreSQL advisory-lock key used to serialise
// bot-avatar seeding across concurrent instances.
const advisoryLockKey = int64(0x626f746176617461) // "botavata"

// fileForPeer maps a built-in system peer to its embedded avatar file.
// 777000 uses telegram.jpg (the system account); the rest are the branded bot icons.
var fileForPeer = map[int64]string{
	domain.OfficialSystemUserID: "telegram.jpg",
	domain.BotFatherUserID:      "botfather.jpg",
	domain.StickersBotUserID:    "stickers.jpg",
	domain.VerifyBotUserID:      "verifybot.jpg",
	domain.GifBotUserID:         "gif.jpg",
	domain.PremiumBotUserID:     "premiumbot.mp4",
}

// AvatarSetter is the subset of the files service used to assign avatars.
type AvatarSetter interface {
	CurrentProfilePhotoKind(ctx context.Context, ownerType domain.PeerType, ownerID int64, kind domain.ProfilePhotoKind) (domain.Photo, bool, error)
	CreateAvatarFromBytes(ctx context.Context, data []byte) (domain.Photo, error)
	CreateAvatarVideoFromBytes(ctx context.Context, data []byte, videoStartTs float64) (domain.Photo, error)
	SetCurrentProfilePhotoKind(ctx context.Context, ownerType domain.PeerType, ownerID int64, kind domain.ProfilePhotoKind, photoID int64, date int) (domain.Photo, bool, error)
	// BeginTx acquires a PostgreSQL advisory lock (serialising concurrent
	// Seed calls across instances) and returns a transaction-scoped
	// AvatarSetter. The returned release function MUST be called (deferred)
	// to unlock and rollback/commit the transaction.
	BeginTx(ctx context.Context) (txAv AvatarSetter, release func(error), err error)
}

// Seed assigns embedded avatars to the built-in system account and bots.
// It is idempotent: peers that already have a profile photo are skipped.
// The advisory lock guarantees that at most one instance seeds at a time,
// preventing orphan media from concurrent creates.
func Seed(ctx context.Context, av AvatarSetter, logger *zap.Logger, now int64) {
	txAv, release, err := av.BeginTx(ctx)
	if err != nil {
		logger.Fatal("bot avatar: begin tx", zap.Error(err))
	}
	defer release(nil)

	for peerID, fname := range fileForPeer {
		if _, ok, err := txAv.CurrentProfilePhotoKind(ctx, domain.PeerTypeUser, peerID, domain.ProfilePhotoKindProfile); err != nil {
			logger.Warn("bot avatar: read current", zap.Int64("peer", peerID), zap.Error(err))
			continue
		} else if ok {
			continue
		}
		data, err := fs.ReadFile(assets, fname)
		if err != nil {
			logger.Warn("bot avatar: read embedded asset", zap.Int64("peer", peerID), zap.String("file", fname), zap.Error(err))
			continue
		}
		var photo domain.Photo
		if isVideo(fname) {
			photo, err = txAv.CreateAvatarVideoFromBytes(ctx, data, 0)
		} else {
			photo, err = txAv.CreateAvatarFromBytes(ctx, data)
		}
		if err != nil {
			logger.Warn("bot avatar: create", zap.Int64("peer", peerID), zap.String("file", fname), zap.Error(err))
			continue
		}
		if _, found, err := txAv.SetCurrentProfilePhotoKind(ctx, domain.PeerTypeUser, peerID, domain.ProfilePhotoKindProfile, photo.ID, int(now)); err != nil || !found {
			logger.Warn("bot avatar: set current", zap.Int64("peer", peerID), zap.Error(err))
		}
	}
}

func isVideo(name string) bool {
	return len(name) > 4 && name[len(name)-4:] == ".mp4"
}
