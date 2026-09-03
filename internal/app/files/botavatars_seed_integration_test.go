package files

import (
	"bytes"
	"context"
	"image"
	"image/png"
	"testing"

	"telesrv/internal/app/files/botavatars"
	"telesrv/internal/domain"
)

// countingThumbnailer records whether the video thumbnailer was invoked by the
// transaction-scoped service. If SeedTx dropped thumbs, the animated-avatar
// still would fall back to a generated placeholder and Extract would be 0.
// It returns a valid small PNG so the downstream avatar rendition pipeline
// (which decodes the still) can proceed.
type countingThumbnailer struct {
	extractCalls int
}

func (c *countingThumbnailer) Extract(_ context.Context, _ []byte, _ string) ([]byte, error) {
	c.extractCalls++
	var buf bytes.Buffer
	if err := png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 8, 8))); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// TestBotAvatarsSeedViaRealService 集成测试：通过真实 files.Service、真实
// permanent-blob 后端与真实、分开的 upload-staging 后端调用 botavatars.Seed。
// Seed 内部经 SeedTx 构造事务化服务——这里确保 uploadParts / thumbs 等字段在
// 事务服务上完整保留，且 upload parts 被写入单独的 staging backend（而非主
// blob 后端）。否则 premiumbot.mp4 的 CreateAvatarVideoFromBytes 会因 upload
// part backend 未配置而失败，或视频静态帧回落成生成占位图。
func TestBotAvatarsSeedViaRealService(t *testing.T) {
	ctx := context.Background()

	media := newFakeMediaStore()
	blobs, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("new local fs blob backend: %v", err)
	}
	// 独立的 upload-staging backend：与 permanent-blob 后端是分开的实例/目录。
	staging := &countingUploadPartBackend{LocalFS: mustLocalFS(t, "staging backend")}
	// Фейковый thumbnailer фиксирует, что SeedTx сохранил thumbs для видео-статика.
	thumb := &countingThumbnailer{}
	svc := NewService(media, blobs, 2, WithUploadPartBackend(staging), WithVideoThumbnailer(thumb))

	if err := botavatars.Seed(ctx, svc, 1750000000); err != nil {
		t.Fatalf("botavatars.Seed failed: %v", err)
	}

	// premiumbot.mp4 走 CreateAvatarVideoFromBytes -> SaveFilePart -> PutUploadPart。
	// uploadParts 必须被 SeedTx 保留，且确实落到了独立的 staging backend。
	if staging.putUploadPartCalls == 0 {
		t.Fatalf("staging backend was never used: uploadParts not carried into SeedTx")
	}
	// 视频静态帧必须从真实 thumbnailer 抽取，而不是回落成生成占位图——这证明
	// SeedTx 保留了 thumbs。
	if thumb.extractCalls == 0 {
		t.Fatalf("video thumbnailer never invoked: thumbs not carried into SeedTx")
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

func mustLocalFS(t *testing.T, desc string) *LocalFS {
	t.Helper()
	lfs, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("new local fs %s: %v", desc, err)
	}
	return lfs
}

func photoBlobKey(photoID int64, typ string) string {
	return "photo:" + itoa(photoID) + ":" + typ
}
