package botavatars

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"telesrv/internal/domain"
)

//go:embed *.jpg *.mp4
var assets embed.FS

// peersForSeed returns the peer→avatar mapping, using the configured
// Premium bot ID instead of the compile-time constant so that deployments
// with TELESRV_PREMIUM_BOT_USER_ID get the avatar on the right identity.
func peersForSeed() map[int64]string {
	return map[int64]string{
		domain.OfficialSystemUserID:         "telegram.jpg",
		domain.BotFatherUserID:              "botfather.jpg",
		domain.StickersBotUserID:            "stickers.jpg",
		domain.VerifyBotUserID:              "verifybot.jpg",
		domain.GifBotUserID:                 "gif.jpg",
		domain.PremiumBotConfiguredUserID(): "premiumbot.mp4",
	}
}

// AvatarSetter is the subset of the files service used to assign avatars.
type AvatarSetter interface {
	CurrentProfilePhotoKind(ctx context.Context, ownerType domain.PeerType, ownerID int64, kind domain.ProfilePhotoKind) (domain.Photo, bool, error)
	CreateAvatarFromBytes(ctx context.Context, data []byte) (domain.Photo, error)
	CreateAvatarVideoFromBytes(ctx context.Context, data []byte, videoStartTs float64) (domain.Photo, error)
	SetCurrentProfilePhotoKind(ctx context.Context, ownerType domain.PeerType, ownerID int64, kind domain.ProfilePhotoKind, photoID int64, date int) (domain.Photo, bool, error)
	// SeedTx runs fn inside a single transaction that holds a serialising
	// advisory lock. If fn returns an error the whole transaction (including
	// any media created inside) is rolled back; otherwise it is committed.
	// This makes the check+create+bind sequence atomic and orphan-free across
	// concurrent or multi-instance startup. The tx AvatarSetter passed to fn
	// must route its calls through the same transaction and lock.
	SeedTx(ctx context.Context, fn func(ctx context.Context, tx AvatarSetter) error) error
}

// Seed assigns embedded avatars to the built-in system account and bots.
// It is idempotent: peers that already have a profile photo are skipped, and
// the whole run is atomic inside the advisory-locked SeedTx transaction. Any
// error fails the seed and aborts the transaction, so a partially written
// avatar photo is rolled back rather than orphaned.
func Seed(ctx context.Context, av AvatarSetter, now int64) error {
	return av.SeedTx(ctx, func(ctx context.Context, txAv AvatarSetter) error {
		for peerID, fname := range peersForSeed() {
			if _, ok, err := txAv.CurrentProfilePhotoKind(ctx, domain.PeerTypeUser, peerID, domain.ProfilePhotoKindProfile); err != nil {
				return fmt.Errorf("bot avatar: read current photo for peer %d: %w", peerID, err)
			} else if ok {
				continue
			}
			data, err := fs.ReadFile(assets, fname)
			if err != nil {
				return fmt.Errorf("bot avatar: read embedded asset %q: %w", fname, err)
			}
			var photo domain.Photo
			if isVideo(fname) {
				photo, err = txAv.CreateAvatarVideoFromBytes(ctx, data, 0)
			} else {
				photo, err = txAv.CreateAvatarFromBytes(ctx, data)
			}
			if err != nil {
				return fmt.Errorf("bot avatar: create avatar for peer %d: %w", peerID, err)
			}
			if _, found, err := txAv.SetCurrentProfilePhotoKind(ctx, domain.PeerTypeUser, peerID, domain.ProfilePhotoKindProfile, photo.ID, int(now)); err != nil {
				return fmt.Errorf("bot avatar: set current photo for peer %d: %w", peerID, err)
			} else if !found {
				return fmt.Errorf("bot avatar: bind current photo for peer %d: not found", peerID)
			}
		}
		return nil
	})
}

func isVideo(name string) bool {
	return len(name) > 4 && name[len(name)-4:] == ".mp4"
}
