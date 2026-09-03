package files

import (
	"context"
	"testing"

	"telesrv/internal/app/files/botavatars"
	"telesrv/internal/domain"
)

// TestBotAvatarsSeedViaRealService 集成测试：通过真实 files.Service 与真实
// LocalFS blob 后端调用 botavatars.Seed。Seed 内部经 SeedTx 构造的事务化
// 服务会读取 uploadParts / thumbs 等字段——这里确保它们在事务服务上完整保留，
// 否则 premiumbot.mp4 的 CreateAvatarVideoFromBytes 会因 upload part backend
// 未配置而失败。
func TestBotAvatarsSeedViaRealService(t *testing.T) {
	ctx := context.Background()

	media := newFakeMediaStore()
	blobs, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("new local fs backend: %v", err)
	}
	svc := NewService(media, blobs, 2)

	if err := botavatars.Seed(ctx, svc, 1750000000); err != nil {
		t.Fatalf("botavatars.Seed failed: %v", err)
	}

	// 每个系统 bot/账号都应有头像相册与当前照片。
	for peerID := range botavatarsPeersForSeed() {
		ref, err := media.CurrentProfilePhotosKind(ctx, domain.PeerTypeUser, []int64{peerID}, domain.ProfilePhotoKindProfile)
		if err != nil {
			t.Fatalf("current profile photos for peer %d: %v", peerID, err)
		}
		entry, found := ref[peerID]
		if !found {
			t.Fatalf("peer %d has no current profile photo after seed", peerID)
		}
		photo, ok, err := media.GetPhoto(ctx, entry.PhotoID)
		if err != nil {
			t.Fatalf("get photo %d: %v", entry.PhotoID, err)
		}
		if !ok {
			t.Fatalf("peer %d photo %d missing after seed", peerID, entry.PhotoID)
		}
		if photo.DCID != 2 {
			t.Fatalf("peer %d photo dc_id = %d, want 2", peerID, photo.DCID)
		}
		// 每个 photo size 都必须有真实 blob 可下载。
		for _, size := range photo.Sizes {
			blob, found, err := media.GetFileBlob(ctx, photoBlobKey(photo.ID, size.Type))
			if err != nil {
				t.Fatalf("peer %d blob %d:%s: %v", peerID, photo.ID, size.Type, err)
			}
			if !found {
				t.Fatalf("peer %d blob %d:%s missing after seed", peerID, photo.ID, size.Type)
			}
			chunk, found, err := svc.GetFile(ctx, domain.FileDownloadRequest{LocationKey: photoBlobKey(photo.ID, size.Type), Offset: 0, Limit: 1})
			if err != nil {
				t.Fatalf("peer %d getfile %d:%s: %v", peerID, photo.ID, size.Type, err)
			}
			if !found || chunk.Total != blob.Size || len(chunk.Bytes) == 0 {
				t.Fatalf("peer %d getfile %d:%s = found:%v total:%d got:%d want %d", peerID, photo.ID, size.Type, found, chunk.Total, len(chunk.Bytes), blob.Size)
			}
		}
	}

	// 幂等：二次 seed 不报错，且不再生成新照片。
	before := len(media.photos)
	if err := botavatars.Seed(ctx, svc, 1750000001); err != nil {
		t.Fatalf("second botavatars.Seed failed: %v", err)
	}
	if after := len(media.photos); after != before {
		t.Fatalf("idempotent seed created new photos: before=%d after=%d", before, after)
	}
}

func botavatarsPeersForSeed() map[int64]string {
	return map[int64]string{
		domain.OfficialSystemUserID:         "telegram.jpg",
		domain.BotFatherUserID:              "botfather.jpg",
		domain.StickersBotUserID:            "stickers.jpg",
		domain.VerifyBotUserID:              "verifybot.jpg",
		domain.GifBotUserID:                 "gif.jpg",
		domain.PremiumBotConfiguredUserID(): "premiumbot.mp4",
	}
}

func photoBlobKey(photoID int64, typ string) string {
	return "photo:" + itoa(photoID) + ":" + typ
}
