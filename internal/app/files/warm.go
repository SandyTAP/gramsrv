package files

import (
	"context"
	"fmt"

	"telesrv/internal/domain"
)

// WarmStats 汇报一次启动资源缓存预热结果。
type WarmStats struct {
	StickerSets int
	Documents   int
	Blobs       int
}

// WarmCaches 从已持久化的 sticker/reaction 元数据预热小 blob 字节缓存与完整 sticker set 缓存。
// SeedMedia 在已有数据时会跳过导入；该方法保证普通 server 重启后历史 sticker 首次渲染也不是冷缓存。
func (s *Service) WarmCaches(ctx context.Context) (WarmStats, error) {
	var stats WarmStats
	// 第一阶段：收集所有待预热文档（贴纸集 + reaction），按 doc ID 去重。
	seenDocs := make(map[int64]struct{})
	docs := make([]domain.Document, 0, 256)
	collect := func(doc domain.Document) {
		if doc.ID == 0 {
			return
		}
		if _, ok := seenDocs[doc.ID]; ok {
			return
		}
		seenDocs[doc.ID] = struct{}{}
		docs = append(docs, doc)
	}
	for _, kind := range []domain.StickerSetKind{
		domain.StickerSetKindStickers,
		domain.StickerSetKindEmoji,
		domain.StickerSetKindMasks,
		domain.StickerSetKindSystem,
	} {
		sets, err := s.media.ListStickerSets(ctx, kind)
		if err != nil {
			return stats, err
		}
		for _, set := range sets {
			setDocs, err := s.media.GetDocuments(ctx, set.DocumentIDs)
			if err != nil {
				return stats, err
			}
			ordered := orderDocuments(setDocs, set.DocumentIDs)
			s.stickerSetCache.put(set, ordered)
			stats.StickerSets++
			for _, doc := range ordered {
				collect(doc)
			}
		}
	}
	reactions, err := s.media.ListAvailableReactions(ctx)
	if err != nil {
		return stats, err
	}
	reactionIDs := make([]int64, 0, len(reactions)*4)
	for _, reaction := range reactions {
		reactionIDs = append(reactionIDs, reaction.DocumentIDs()...)
	}
	reactionDocs, err := s.media.GetDocuments(ctx, reactionIDs)
	if err != nil {
		return stats, err
	}
	for _, doc := range reactionDocs {
		collect(doc)
	}
	stats.Documents = len(docs)

	// 第二阶段：一发 ANY 查询批量取所有 location key 的 blob 元数据，替代过去逐个
	// GetFileBlob 的启动期 N+1（~2400 个 blob 各打一次 PG → 一次往返）。
	keys := make([]string, 0, len(docs)*2)
	for _, doc := range docs {
		keys = append(keys, blobLocationKeys(doc)...)
	}
	blobs, err := s.media.GetFileBlobs(ctx, keys)
	if err != nil {
		return stats, err
	}
	// 第三阶段：填充元数据缓存，并把小 blob 的全量字节读入字节缓存（blob backend 读，非 PG）。
	for _, key := range keys {
		blob, ok := blobs[key]
		if !ok {
			continue
		}
		s.blobCache.put(key, blob)
		warmed, err := s.warmBlobBytes(ctx, blob)
		if err != nil {
			return stats, err
		}
		if warmed {
			stats.Blobs++
		}
	}
	return stats, nil
}

// blobLocationKeys 返回一个文档需预热的全部 location key（主体 + 可下载缩略图）。
func blobLocationKeys(doc domain.Document) []string {
	if doc.ID == 0 {
		return nil
	}
	keys := make([]string, 0, 1+len(doc.Thumbs))
	keys = append(keys, fmt.Sprintf("doc:%d", doc.ID))
	for _, thumb := range doc.Thumbs {
		if !thumb.Downloadable() {
			continue
		}
		keys = append(keys, fmt.Sprintf("doc:%d:%s", doc.ID, thumb.Type))
	}
	return keys
}

// warmBlobBytes 把小 blob 的全量字节读入 byteCache（大 blob 跳过，仍由 GetRange 分段读）。
// 返回是否实际写入了字节缓存。
func (s *Service) warmBlobBytes(ctx context.Context, blob domain.FileBlob) (bool, error) {
	if blob.Size <= 0 || blob.Size > blobBytesCacheMaxEntryBytes || s.byteCache.has(blob.ObjectKey) {
		return false, nil
	}
	data, total, err := s.blobs.GetRange(ctx, blob.ObjectKey, 0, blobBytesCacheMaxEntryBytes+1)
	if err != nil {
		return false, fmt.Errorf("read blob %q: %w", blob.LocationKey, err)
	}
	if total <= blobBytesCacheMaxEntryBytes && int64(len(data)) == total {
		s.byteCache.putOwned(blob.ObjectKey, data)
		return true, nil
	}
	return false, nil
}
