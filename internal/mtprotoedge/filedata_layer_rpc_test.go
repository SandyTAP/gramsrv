package mtprotoedge

import (
	"context"
	"fmt"
	"testing"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"

	"telesrv/internal/domain"
)

func TestFileDataLayerRPCDispatchesUploadPartAtEdge(t *testing.T) {
	base := newFileDataTestBase()
	files := &fakeFileDataPlane{}
	layer := NewFileDataLayerRPC(base, files, nil)

	admitted := admitFileDataTestRPC(t, layer, &tg.UploadSaveFilePartRequest{
		FileID:   77,
		FilePart: 1,
		Bytes:    []byte("part-one"),
	})
	ctx := layer.WithLayerRPCIdentityHint(context.Background(), LayerRPCIdentityHint{
		UserID:         42,
		UserIDResolved: true,
	})
	result, method, err := layer.DispatchAdmitted(ctx, [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("DispatchAdmitted: %v", err)
	}
	if method != "upload.saveFilePart" {
		t.Fatalf("method = %q, want upload.saveFilePart", method)
	}
	if got, ok := result.CanonicalValue().(bool); !ok || !got {
		t.Fatalf("result = %#v, want true", result.CanonicalValue())
	}
	if base.dispatches != 0 {
		t.Fatalf("base dispatches = %d, want 0", base.dispatches)
	}
	if files.saveParts != 1 || files.lastUserID != 42 || files.lastFileID != 77 || files.lastPart != 1 {
		t.Fatalf("filedata save = parts:%d user:%d file:%d part:%d",
			files.saveParts, files.lastUserID, files.lastFileID, files.lastPart)
	}
}

func TestFileDataLayerRPCLeavesGroupCallGetFileOnCoreExec(t *testing.T) {
	base := newFileDataTestBase()
	files := &fakeFileDataPlane{}
	layer := NewFileDataLayerRPC(base, files, nil)

	admitted := admitFileDataTestRPC(t, layer, &tg.UploadGetFileRequest{
		Location: &tg.InputGroupCallStream{
			Call:   &tg.InputGroupCall{ID: 10, AccessHash: 20},
			TimeMs: 1000,
			Scale:  0,
		},
		Offset: 0,
		Limit:  1024,
	})
	result, method, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("DispatchAdmitted: %v", err)
	}
	if method != "upload.getFile" {
		t.Fatalf("method = %q, want upload.getFile", method)
	}
	if base.dispatches != 1 {
		t.Fatalf("base dispatches = %d, want 1", base.dispatches)
	}
	if files.getFiles != 0 {
		t.Fatalf("filedata getFiles = %d, want 0", files.getFiles)
	}
	file, ok := result.CanonicalValue().(*tg.UploadFile)
	if !ok || string(file.Bytes) != "core" {
		t.Fatalf("result = %#v, want core upload file", result.CanonicalValue())
	}
}

func TestFileDataLayerRPCDispatchesGetFileHashesAtEdge(t *testing.T) {
	base := newFileDataTestBase()
	files := &fakeFileDataPlane{
		hashes: []domain.FileHash{{
			Offset: 128 << 10,
			Limit:  4,
			Hash:   []byte("hash"),
		}},
	}
	layer := NewFileDataLayerRPC(base, files, nil)

	admitted := admitFileDataTestRPC(t, layer, &tg.UploadGetFileHashesRequest{
		Location: &tg.InputDocumentFileLocation{ID: 77},
		Offset:   128 << 10,
	})
	result, method, err := layer.DispatchAdmitted(context.Background(), [8]byte{1}, 2, 3, 4, admitted)
	if err != nil {
		t.Fatalf("DispatchAdmitted: %v", err)
	}
	if method != "upload.getFileHashes" {
		t.Fatalf("method = %q, want upload.getFileHashes", method)
	}
	if base.dispatches != 0 {
		t.Fatalf("base dispatches = %d, want 0", base.dispatches)
	}
	if files.getHashes != 1 || files.lastHashRequest.LocationKey != "doc:77" || files.lastHashRequest.Offset != 128<<10 {
		t.Fatalf("filedata hashes calls=%d request=%+v", files.getHashes, files.lastHashRequest)
	}
	hashes, ok := result.CanonicalValue().([]tg.FileHash)
	if !ok || len(hashes) != 1 || hashes[0].Limit != 4 || string(hashes[0].Hash) != "hash" {
		t.Fatalf("result = %#v, want one file hash", result.CanonicalValue())
	}
}

func admitFileDataTestRPC(t *testing.T, layer *FileDataLayerRPC, req bin.Object) tlprofile.Admission {
	t.Helper()
	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile228, req, &body); err != nil {
		t.Fatalf("encode rpc: %v", err)
	}
	admitted, err := layer.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatalf("AdmitLayer: %v", err)
	}
	if body.Len() != 0 {
		t.Fatalf("admission left %d bytes", body.Len())
	}
	return admitted
}

type fileDataTestBase struct {
	dispatcher *tlprofile.Dispatcher
	dispatches int
}

func newFileDataTestBase() *fileDataTestBase {
	d := tlprofile.NewDispatcher()
	d.OnWrappers(func(ctx context.Context, _ tlprofile.Admission, next tlprofile.Next) error {
		return next(ctx)
	})
	if err := d.Register(tlprofile.SemanticMethodUploadGetFile, func(context.Context, bin.Object) (any, error) {
		return &tg.UploadFile{Type: &tg.StorageFileUnknown{}, Bytes: []byte("core")}, nil
	}); err != nil {
		panic(err)
	}
	return &fileDataTestBase{dispatcher: d}
}

func (b *fileDataTestBase) AdmitLayer(profile tlprofile.Profile, body *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	return b.dispatcher.Admit(profile, body, limits)
}

func (b *fileDataTestBase) AdmitUnprofiled(body *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	return b.dispatcher.AdmitUnprofiled(body, limits)
}

func (b *fileDataTestBase) DispatchAdmitted(ctx context.Context, _ [8]byte, _ int64, _ int64, _ uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	b.dispatches++
	result, err := b.dispatcher.Dispatch(ctx, request)
	return result, fileDataMethodName(request.Call().Method()), err
}

type fakeFileDataPlane struct {
	saveParts       int
	getFiles        int
	getHashes       int
	lastUserID      int64
	lastFileID      int64
	lastPart        int
	lastHashRequest domain.FileHashRequest
	hashes          []domain.FileHash
}

func (f *fakeFileDataPlane) SaveFilePart(_ context.Context, ownerUserID, fileID int64, part int, _ []byte) (bool, error) {
	f.saveParts++
	f.lastUserID = ownerUserID
	f.lastFileID = fileID
	f.lastPart = part
	return true, nil
}

func (f *fakeFileDataPlane) SaveBigFilePart(context.Context, int64, int64, int, int, []byte) (bool, error) {
	return false, fmt.Errorf("unexpected SaveBigFilePart")
}

func (f *fakeFileDataPlane) GetFile(context.Context, domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	f.getFiles++
	return domain.FileChunk{Bytes: []byte("file")}, true, nil
}

func (f *fakeFileDataPlane) GetFileHashes(_ context.Context, req domain.FileHashRequest) ([]domain.FileHash, bool, error) {
	f.getHashes++
	f.lastHashRequest = req
	return append([]domain.FileHash(nil), f.hashes...), true, nil
}
