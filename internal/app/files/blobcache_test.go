package files

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sync"
	"testing"

	"telesrv/internal/domain"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestBlobMetaCacheGetPutEvict(t *testing.T) {
	c := newBlobMetaCache(2)
	c.put("a", domain.FileBlob{LocationKey: "a", ObjectKey: "oa"})
	c.put("b", domain.FileBlob{LocationKey: "b", ObjectKey: "ob"})
	if b, ok := c.get("a"); !ok || b.ObjectKey != "oa" {
		t.Fatalf("get a = %+v ok=%v", b, ok)
	}
	// 容量 2：刚 access 过 a，再 put c 应淘汰最久未用的 b。
	c.put("c", domain.FileBlob{LocationKey: "c", ObjectKey: "oc"})
	if _, ok := c.get("b"); ok {
		t.Error("b should be evicted (least recently used)")
	}
	if _, ok := c.get("a"); !ok {
		t.Error("a should remain (recently used)")
	}
	if _, ok := c.get("c"); !ok {
		t.Error("c should be present")
	}
}

func TestBlobBytesCacheConcurrentSharedRangeOwnership(t *testing.T) {
	cache := newBlobBytesCache(2 << 20)
	cache.putOwned("blob", bytes.Repeat([]byte{'a'}, 64<<10))
	var wg sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				shared, ok := cache.getShared("blob")
				if !ok || len(shared) != 64<<10 {
					t.Errorf("shared cache read: ok=%v size=%d", ok, len(shared))
					return
				}
				before := shared[0]
				owned := sliceBlobBytes(shared, 0, int64(len(shared)))
				owned[0] ^= 0xff
				if shared[0] != before {
					t.Error("owned range mutation changed immutable cache entry")
					return
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		cache.putOwned("blob", bytes.Repeat([]byte{byte('a' + i%2)}, 64<<10))
	}
	wg.Wait()
}

// countingMediaStore 统计 GetFileBlob 次数，验证元数据缓存命中后不再查 PG。
type countingMediaStore struct {
	*fakeMediaStore
	getBlobCalls    int
	getSetByIDCalls int
}

func (c *countingMediaStore) GetFileBlob(ctx context.Context, key string) (domain.FileBlob, bool, error) {
	c.getBlobCalls++
	return c.fakeMediaStore.GetFileBlob(ctx, key)
}

func (c *countingMediaStore) GetStickerSetByID(ctx context.Context, id int64) (domain.StickerSet, bool, error) {
	c.getSetByIDCalls++
	return c.fakeMediaStore.GetStickerSetByID(ctx, id)
}

type countingBlobBackend struct {
	BlobBackend
	getRangeCalls int
}

func (c *countingBlobBackend) GetRange(ctx context.Context, objectKey string, offset, limit int64) ([]byte, int64, error) {
	c.getRangeCalls++
	return c.BlobBackend.GetRange(ctx, objectKey, offset, limit)
}

func TestGetFileCachesMetadataAndSmallBlobBytes(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("local fs: %v", err)
	}
	objectKey, err := local.Put(ctx, []byte("0123456789"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	media := newFakeMediaStore()
	if err := media.PutFileBlob(ctx, domain.FileBlob{LocationKey: "doc:42", Backend: domain.MediaBackendLocalFS, ObjectKey: objectKey, Size: 10, MimeType: "application/octet-stream"}); err != nil {
		t.Fatalf("put blob: %v", err)
	}
	counting := &countingMediaStore{fakeMediaStore: media}
	blobs := &countingBlobBackend{BlobBackend: local}
	svc := NewService(counting, blobs, 2)

	// 第一次：查 PG 一次并填充元数据缓存；小 blob 读整块进字节缓存后返回 [0,5)。
	c1, ok, err := svc.GetFile(ctx, domain.FileDownloadRequest{LocationKey: "doc:42", Offset: 0, Limit: 5})
	if err != nil || !ok {
		t.Fatalf("getfile1 ok=%v err=%v", ok, err)
	}
	if string(c1.Bytes) != "01234" {
		t.Errorf("chunk1 = %q, want 01234", c1.Bytes)
	}
	if c1.Total != 10 {
		t.Errorf("total = %d, want 10", c1.Total)
	}
	if counting.getBlobCalls != 1 {
		t.Errorf("getBlobCalls = %d, want 1", counting.getBlobCalls)
	}
	if blobs.getRangeCalls != 1 {
		t.Errorf("getRangeCalls = %d, want 1", blobs.getRangeCalls)
	}
	c1.Bytes[0] = 'x'
	c1Again, ok, err := svc.GetFile(ctx, domain.FileDownloadRequest{LocationKey: "doc:42", Offset: 0, Limit: 5})
	if err != nil || !ok || string(c1Again.Bytes) != "01234" {
		t.Fatalf("mutating returned chunk changed byte cache: chunk=%q ok=%v err=%v", c1Again.Bytes, ok, err)
	}

	// 后续请求：同 location 命中元数据与字节缓存；[5,10) 直接从内存切片。
	c2, ok, err := svc.GetFile(ctx, domain.FileDownloadRequest{LocationKey: "doc:42", Offset: 5, Limit: 5})
	if err != nil || !ok {
		t.Fatalf("getfile2 ok=%v err=%v", ok, err)
	}
	if string(c2.Bytes) != "56789" {
		t.Errorf("chunk2 = %q, want 56789", c2.Bytes)
	}
	if counting.getBlobCalls != 1 {
		t.Errorf("getBlobCalls = %d, want 1 (cache hit)", counting.getBlobCalls)
	}
	if blobs.getRangeCalls != 1 {
		t.Errorf("getRangeCalls = %d, want 1 (byte cache hit)", blobs.getRangeCalls)
	}
}

func TestGetFileLogsCacheHitMiss(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("local fs: %v", err)
	}
	objectKey, err := local.Put(ctx, []byte("0123456789"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	media := newFakeMediaStore()
	if err := media.PutFileBlob(ctx, domain.FileBlob{LocationKey: "doc:log", Backend: domain.MediaBackendLocalFS, ObjectKey: objectKey, Size: 10, MimeType: "application/octet-stream"}); err != nil {
		t.Fatalf("put blob: %v", err)
	}
	blobs := &countingBlobBackend{BlobBackend: local}
	core, logs := observer.New(zap.InfoLevel)
	svc := NewService(media, blobs, 2, WithLogger(zap.New(core)))

	if _, ok, err := svc.GetFile(ctx, domain.FileDownloadRequest{LocationKey: "doc:log", Offset: 0, Limit: 5}); err != nil || !ok {
		t.Fatalf("first getfile ok=%v err=%v", ok, err)
	}
	if _, ok, err := svc.GetFile(ctx, domain.FileDownloadRequest{LocationKey: "doc:log", Offset: 5, Limit: 5}); err != nil || !ok {
		t.Fatalf("second getfile ok=%v err=%v", ok, err)
	}

	entries := logs.FilterMessage("upload.getFile cache").All()
	if len(entries) != 2 {
		t.Fatalf("cache log entries = %d, want 2", len(entries))
	}
	first := entries[0].ContextMap()
	if first["source"] != "backend_fill_byte_cache" ||
		first["meta_cache_hit"] != false ||
		first["meta_cache_filled"] != true ||
		first["byte_cache_hit"] != false ||
		first["byte_cache_filled"] != true ||
		first["backend_read"] != true ||
		first["returned_bytes"] != int64(5) {
		t.Fatalf("first cache log = %#v, want backend fill miss", first)
	}
	second := entries[1].ContextMap()
	if second["source"] != "byte_cache" ||
		second["meta_cache_hit"] != true ||
		second["byte_cache_hit"] != true ||
		second["backend_read"] != false ||
		second["returned_bytes"] != int64(5) {
		t.Fatalf("second cache log = %#v, want byte cache hit", second)
	}
	if blobs.getRangeCalls != 1 {
		t.Fatalf("GetRange calls = %d, want only first miss to read backend", blobs.getRangeCalls)
	}
}

func TestGetFileDoesNotByteCacheLargeBlob(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("local fs: %v", err)
	}
	content := bytes.Repeat([]byte("x"), blobBytesCacheMaxEntryBytes+2)
	objectKey, err := local.Put(ctx, content)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	media := newFakeMediaStore()
	if err := media.PutFileBlob(ctx, domain.FileBlob{
		LocationKey: "doc:large",
		Backend:     domain.MediaBackendLocalFS,
		ObjectKey:   objectKey,
		Size:        int64(len(content)),
		MimeType:    "application/octet-stream",
	}); err != nil {
		t.Fatalf("put blob: %v", err)
	}
	blobs := &countingBlobBackend{BlobBackend: local}
	svc := NewService(media, blobs, 2)

	for i := 0; i < 2; i++ {
		chunk, ok, err := svc.GetFile(ctx, domain.FileDownloadRequest{LocationKey: "doc:large", Offset: 1, Limit: 7})
		if err != nil || !ok {
			t.Fatalf("getfile %d ok=%v err=%v", i, ok, err)
		}
		if string(chunk.Bytes) != "xxxxxxx" {
			t.Fatalf("chunk %d = %q, want seven x bytes", i, chunk.Bytes)
		}
	}
	if blobs.getRangeCalls != 2 {
		t.Errorf("getRangeCalls = %d, want 2 (large blob is not byte cached)", blobs.getRangeCalls)
	}
}

func TestGetFileHashesUsesBatchedSHA256Ranges(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("local fs: %v", err)
	}
	content := make([]byte, DefaultFileHashPartSize*11+5)
	for i := range content {
		content[i] = byte(i % 251)
	}
	objectKey, size, sum, err := local.PutReader(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	media := newFakeMediaStore()
	if err := media.PutFileBlob(ctx, domain.FileBlob{
		LocationKey: "doc:hashes",
		Backend:     domain.MediaBackendLocalFS,
		ObjectKey:   objectKey,
		Size:        size,
		SHA256:      sum,
		MimeType:    "application/octet-stream",
	}); err != nil {
		t.Fatalf("put blob: %v", err)
	}
	blobs := &countingBlobBackend{BlobBackend: local}
	svc := NewService(media, blobs, 2)

	first, found, err := svc.GetFileHashes(ctx, domain.FileHashRequest{LocationKey: "doc:hashes", Offset: 0})
	if err != nil || !found {
		t.Fatalf("first hashes found=%v err=%v", found, err)
	}
	if len(first) != DefaultFileHashRangeSize {
		t.Fatalf("first hashes = %d, want %d", len(first), DefaultFileHashRangeSize)
	}
	wantFirst := sha256.Sum256(content[:DefaultFileHashPartSize])
	if first[0].Offset != 0 || first[0].Limit != DefaultFileHashPartSize || !bytes.Equal(first[0].Hash, wantFirst[:]) {
		t.Fatalf("first hash = %+v, want offset 0 limit %d sha256(first part)", first[0], DefaultFileHashPartSize)
	}
	if first[len(first)-1].Offset != int64((DefaultFileHashRangeSize-1)*DefaultFileHashPartSize) {
		t.Fatalf("last first-batch offset = %d", first[len(first)-1].Offset)
	}

	second, found, err := svc.GetFileHashes(ctx, domain.FileHashRequest{
		LocationKey: "doc:hashes",
		Offset:      int64(DefaultFileHashRangeSize * DefaultFileHashPartSize),
	})
	if err != nil || !found {
		t.Fatalf("second hashes found=%v err=%v", found, err)
	}
	if len(second) != 2 {
		t.Fatalf("second hashes = %d, want 2", len(second))
	}
	if second[0].Offset != int64(DefaultFileHashRangeSize*DefaultFileHashPartSize) || second[0].Limit != DefaultFileHashPartSize {
		t.Fatalf("second[0] = %+v", second[0])
	}
	tail := content[DefaultFileHashPartSize*11:]
	wantTail := sha256.Sum256(tail)
	if second[1].Limit != len(tail) || !bytes.Equal(second[1].Hash, wantTail[:]) {
		t.Fatalf("tail hash = %+v, want len %d sha256(tail)", second[1], len(tail))
	}

	empty, found, err := svc.GetFileHashes(ctx, domain.FileHashRequest{
		LocationKey: "doc:hashes",
		Offset:      int64(len(content)),
	})
	if err != nil || !found || len(empty) != 0 {
		t.Fatalf("eof hashes len=%d found=%v err=%v", len(empty), found, err)
	}
	if blobs.getRangeCalls != 2 {
		t.Fatalf("GetRange calls = %d, want 2", blobs.getRangeCalls)
	}
}

func TestGetFileHashesUsesStoredWholeSHAForSinglePartBlob(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("local fs: %v", err)
	}
	content := []byte("tiny file")
	objectKey, _, sum, err := local.PutReader(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	media := newFakeMediaStore()
	if err := media.PutFileBlob(ctx, domain.FileBlob{
		LocationKey: "doc:tiny",
		Backend:     domain.MediaBackendLocalFS,
		ObjectKey:   objectKey,
		Size:        int64(len(content)),
		SHA256:      sum,
	}); err != nil {
		t.Fatalf("put blob: %v", err)
	}
	blobs := &countingBlobBackend{BlobBackend: local}
	svc := NewService(media, blobs, 2)

	hashes, found, err := svc.GetFileHashes(ctx, domain.FileHashRequest{LocationKey: "doc:tiny"})
	if err != nil || !found {
		t.Fatalf("hashes found=%v err=%v", found, err)
	}
	if len(hashes) != 1 || hashes[0].Limit != len(content) || !bytes.Equal(hashes[0].Hash, sum) {
		t.Fatalf("hashes = %+v, want stored whole sha", hashes)
	}
	if blobs.getRangeCalls != 0 {
		t.Fatalf("GetRange calls = %d, want 0", blobs.getRangeCalls)
	}
}

func TestGetFileRejectsStoredBackendMismatch(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("local fs: %v", err)
	}
	objectKey, err := local.Put(ctx, []byte("must-not-fallback"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	media := newFakeMediaStore()
	if err := media.PutFileBlob(ctx, domain.FileBlob{
		LocationKey: "doc:mismatch",
		Backend:     domain.MediaBackendS3,
		ObjectKey:   objectKey,
		Size:        int64(len("must-not-fallback")),
	}); err != nil {
		t.Fatalf("put blob: %v", err)
	}
	svc := NewService(media, local, 2)
	if _, found, err := svc.GetFile(ctx, domain.FileDownloadRequest{
		LocationKey: "doc:mismatch", Limit: 128 << 10,
	}); err == nil || found {
		t.Fatalf("mismatched backend found=%v err=%v", found, err)
	}
}

func TestWarmCachesPreloadsStickerSetAndSmallBlobs(t *testing.T) {
	ctx := context.Background()
	local, err := NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("local fs: %v", err)
	}
	mainKey, err := local.Put(ctx, []byte("sticker"))
	if err != nil {
		t.Fatalf("put main: %v", err)
	}
	thumbKey, err := local.Put(ctx, []byte("thumb"))
	if err != nil {
		t.Fatalf("put thumb: %v", err)
	}
	media := newFakeMediaStore()
	doc := domain.Document{
		ID:         100,
		AccessHash: 1,
		DCID:       2,
		MimeType:   "application/x-tgsticker",
		Size:       7,
		Thumbs: []domain.PhotoSize{
			{Kind: domain.PhotoSizeKindDefault, Type: "m", W: 128, H: 128, Size: 5},
		},
	}
	if err := media.PutDocument(ctx, doc); err != nil {
		t.Fatalf("put doc: %v", err)
	}
	if err := media.PutFileBlob(ctx, domain.FileBlob{LocationKey: "doc:100", Backend: domain.MediaBackendLocalFS, ObjectKey: mainKey, Size: 7, MimeType: doc.MimeType}); err != nil {
		t.Fatalf("put main blob: %v", err)
	}
	if err := media.PutFileBlob(ctx, domain.FileBlob{LocationKey: "doc:100:m", Backend: domain.MediaBackendLocalFS, ObjectKey: thumbKey, Size: 5, MimeType: "image/jpeg"}); err != nil {
		t.Fatalf("put thumb blob: %v", err)
	}
	set := domain.StickerSet{
		ID:         200,
		AccessHash: 2,
		ShortName:  "pack",
		Title:      "Pack",
		Kind:       domain.StickerSetKindStickers,
		Count:      1,
		DocumentIDs: []int64{
			doc.ID,
		},
	}
	if err := media.PutStickerSet(ctx, set); err != nil {
		t.Fatalf("put set: %v", err)
	}
	counting := &countingMediaStore{fakeMediaStore: media}
	blobs := &countingBlobBackend{BlobBackend: local}
	svc := NewService(counting, blobs, 2)

	stats, err := svc.WarmCaches(ctx)
	if err != nil {
		t.Fatalf("warm caches: %v", err)
	}
	if stats.StickerSets != 1 || stats.Documents != 1 || stats.Blobs != 2 {
		t.Fatalf("warm stats = %+v, want 1 set, 1 doc, 2 blobs", stats)
	}
	blobs.getRangeCalls = 0
	chunk, ok, err := svc.GetFile(ctx, domain.FileDownloadRequest{LocationKey: "doc:100", Offset: 0, Limit: 7})
	if err != nil || !ok {
		t.Fatalf("getfile ok=%v err=%v", ok, err)
	}
	if string(chunk.Bytes) != "sticker" {
		t.Fatalf("chunk = %q, want sticker", chunk.Bytes)
	}
	if blobs.getRangeCalls != 0 {
		t.Fatalf("prewarmed blob should be served from byte cache, GetRange calls = %d", blobs.getRangeCalls)
	}

	gotSet, docs, found, err := svc.ResolveStickerSet(ctx, domain.StickerSetRef{Kind: domain.StickerSetRefByID, ID: set.ID})
	if err != nil || !found {
		t.Fatalf("resolve found=%v err=%v", found, err)
	}
	if gotSet.ID != set.ID || len(docs) != 1 || docs[0].ID != doc.ID {
		t.Fatalf("resolve = set %+v docs %+v", gotSet, docs)
	}
	if counting.getSetByIDCalls != 0 {
		t.Fatalf("ResolveStickerSet should hit full-set cache, GetStickerSetByID calls = %d", counting.getSetByIDCalls)
	}
	docs[0].ID = 999
	_, docsAgain, found, err := svc.ResolveStickerSet(ctx, domain.StickerSetRef{Kind: domain.StickerSetRefByID, ID: set.ID})
	if err != nil || !found || docsAgain[0].ID != doc.ID {
		t.Fatalf("cached docs were mutated: found=%v err=%v docs=%+v", found, err, docsAgain)
	}
}
