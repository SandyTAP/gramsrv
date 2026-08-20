package mtprotoedge

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
	"github.com/iamxvbaba/td/tlprofile"
	"go.uber.org/zap"

	filesapp "telesrv/internal/app/files"
	"telesrv/internal/domain"
	"telesrv/internal/postresponse"
)

const fileDataMaxUploadGetFileChunkLimit = 1 << 20
const fileDataLocalAdmissionTTL = 6 * time.Minute

// FileDataDataPlane is the Edge-visible File service contract. It deliberately
// carries only upload/download data-plane calls; message ownership, permissions
// and durable update facts remain Core-owned.
type FileDataDataPlane interface {
	SaveFilePart(ctx context.Context, ownerUserID, fileID int64, part int, data []byte) (bool, error)
	SaveBigFilePart(ctx context.Context, ownerUserID, fileID int64, part, totalParts int, data []byte) (bool, error)
	GetFile(ctx context.Context, req domain.FileDownloadRequest) (domain.FileChunk, bool, error)
	GetFileHashes(ctx context.Context, req domain.FileHashRequest) ([]domain.FileHash, bool, error)
}

type fileDataIdentityHintKey struct{}

// FileDataLayerRPC intercepts the high-volume upload/download data plane and
// delegates every business/control capability to the wrapped CoreExec handler.
type FileDataLayerRPC struct {
	base       LayerRPCHandler
	files      FileDataDataPlane
	dispatcher *tlprofile.Dispatcher
	classifier *tlprofile.Dispatcher
	log        *zap.Logger

	localMu         sync.Mutex
	localAdmissions map[tlprofile.PreparedIdentity]time.Time
}

func NewFileDataLayerRPC(base LayerRPCHandler, files FileDataDataPlane, log *zap.Logger) *FileDataLayerRPC {
	if log == nil {
		log = zap.NewNop()
	}
	return &FileDataLayerRPC{
		base:            base,
		files:           files,
		dispatcher:      newFileDataDispatcher(files),
		classifier:      newFileDataClassifierDispatcher(),
		log:             log,
		localAdmissions: make(map[tlprofile.PreparedIdentity]time.Time),
	}
}

func newFileDataDispatcher(files FileDataDataPlane) *tlprofile.Dispatcher {
	d := tlprofile.NewDispatcher()
	registerFileDataAdmissionPreflight(d)
	d.OnWrappers(func(ctx context.Context, _ tlprofile.Admission, next tlprofile.Next) error {
		return next(ctx)
	})
	mustRegisterFileDataRPC[*tg.UploadSaveFilePartRequest](d, tlprofile.SemanticMethodUploadSaveFilePart, func(ctx context.Context, req *tg.UploadSaveFilePartRequest) (any, error) {
		if files == nil {
			return false, tgerr.New(500, "FILE_SERVICE_UNAVAILABLE")
		}
		userID, ok := fileDataCurrentUserID(ctx)
		if !ok {
			return false, tgerr.New(400, "FILE_ID_INVALID")
		}
		if req.FilePart < 0 {
			return false, tgerr.New(400, "FILE_PART_INVALID")
		}
		saved, err := files.SaveFilePart(ctx, userID, req.FileID, req.FilePart, req.Bytes)
		if err != nil {
			return false, fileDataSaveErr(err)
		}
		return saved, nil
	})
	mustRegisterFileDataRPC[*tg.UploadSaveBigFilePartRequest](d, tlprofile.SemanticMethodUploadSaveBigFilePart, func(ctx context.Context, req *tg.UploadSaveBigFilePartRequest) (any, error) {
		if files == nil {
			return false, tgerr.New(500, "FILE_SERVICE_UNAVAILABLE")
		}
		userID, ok := fileDataCurrentUserID(ctx)
		if !ok {
			return false, tgerr.New(400, "FILE_ID_INVALID")
		}
		if req.FilePart < 0 {
			return false, tgerr.New(400, "FILE_PART_INVALID")
		}
		saved, err := files.SaveBigFilePart(ctx, userID, req.FileID, req.FilePart, req.FileTotalParts, req.Bytes)
		if err != nil {
			return false, fileDataSaveErr(err)
		}
		return saved, nil
	})
	mustRegisterFileDataRPC[*tg.UploadGetFileRequest](d, tlprofile.SemanticMethodUploadGetFile, func(ctx context.Context, req *tg.UploadGetFileRequest) (any, error) {
		if files == nil {
			return nil, tgerr.New(500, "FILE_SERVICE_UNAVAILABLE")
		}
		if req.Offset < 0 || req.Limit <= 0 || req.Limit > fileDataMaxUploadGetFileChunkLimit {
			return nil, tgerr.New(400, "LIMIT_INVALID")
		}
		key, ok := fileDataLocationKey(req.Location)
		if !ok {
			return nil, tgerr.New(400, "LOCATION_INVALID")
		}
		chunk, found, err := files.GetFile(ctx, domain.FileDownloadRequest{
			LocationKey: key,
			Offset:      req.Offset,
			Limit:       req.Limit,
		})
		if err != nil {
			return nil, tgerr.New(500, "INTERNAL_SERVER_ERROR")
		}
		if !found {
			return nil, tgerr.New(400, "LOCATION_INVALID")
		}
		return &tg.UploadFile{
			Type:  fileDataStorageFileType(chunk.MimeType, chunk.Bytes),
			Mtime: 0,
			Bytes: chunk.Bytes,
		}, nil
	})
	mustRegisterFileDataRPC[*tg.UploadGetFileHashesRequest](d, tlprofile.SemanticMethodUploadGetFileHashes, func(ctx context.Context, req *tg.UploadGetFileHashesRequest) (any, error) {
		if files == nil {
			return nil, tgerr.New(500, "FILE_SERVICE_UNAVAILABLE")
		}
		if req.Offset < 0 {
			return nil, tgerr.New(400, "OFFSET_INVALID")
		}
		key, ok := fileDataLocationKey(req.Location)
		if !ok {
			return nil, tgerr.New(400, "LOCATION_INVALID")
		}
		hashes, found, err := files.GetFileHashes(ctx, domain.FileHashRequest{
			LocationKey: key,
			Offset:      req.Offset,
		})
		if err != nil {
			return nil, tgerr.New(500, "INTERNAL_SERVER_ERROR")
		}
		if !found {
			return nil, tgerr.New(400, "LOCATION_INVALID")
		}
		return fileDataTGFileHashes(hashes), nil
	})
	return d
}

func newFileDataClassifierDispatcher() *tlprofile.Dispatcher {
	d := tlprofile.NewDispatcher()
	registerFileDataAdmissionPreflight(d)
	d.OnWrappers(func(ctx context.Context, _ tlprofile.Admission, next tlprofile.Next) error {
		return next(ctx)
	})
	mustRegisterFileDataRPC[*tg.UploadGetFileRequest](d, tlprofile.SemanticMethodUploadGetFile, func(_ context.Context, req *tg.UploadGetFileRequest) (any, error) {
		_, ok := fileDataLocationKey(req.Location)
		return ok, nil
	})
	return d
}

func registerFileDataAdmissionPreflight(d *tlprofile.Dispatcher) {
	if d == nil {
		panic("mtprotoedge filedata: nil dispatcher")
	}
	for _, fieldID := range []tlprofile.FieldID{
		tlprofile.FieldUploadSaveFilePartBytes,
		tlprofile.FieldUploadSaveBigFilePartBytes,
	} {
		fieldID := fieldID
		if err := d.OnFieldPreflight(fieldID, func(view tlprofile.FieldView) error {
			length, ok := view.BytesLength()
			if !ok {
				return tgerr.New(400, "INPUT_REQUEST_INVALID")
			}
			if length > filesapp.MaxUploadPartBytes {
				return tgerr.New(400, "FILE_PART_TOO_BIG")
			}
			return nil
		}); err != nil {
			panic(fmt.Sprintf("mtprotoedge filedata: register upload bytes field %#016x: %v", uint64(fieldID), err))
		}
	}
	if err := d.OnFieldPreflight(tlprofile.FieldUploadSaveBigFilePartFileTotalParts, func(view tlprofile.FieldView) error {
		totalParts, ok := view.Int32()
		if !ok {
			return tgerr.New(400, "INPUT_REQUEST_INVALID")
		}
		if totalParts <= 0 || totalParts > int32(filesapp.MaxUploadParts) {
			return tgerr.New(400, "FILE_PART_INVALID")
		}
		return nil
	}); err != nil {
		panic(fmt.Sprintf("mtprotoedge filedata: register upload total-parts field: %v", err))
	}
}

func mustRegisterFileDataRPC[T bin.Object](d *tlprofile.Dispatcher, method tlprofile.SemanticID, handler func(context.Context, T) (any, error)) {
	if err := d.Register(method, func(ctx context.Context, object bin.Object) (any, error) {
		request, ok := object.(T)
		if !ok {
			return nil, fmt.Errorf("mtprotoedge filedata: semantic %#016x decoded unexpected canonical request %T", uint64(method), object)
		}
		return handler(ctx, request)
	}); err != nil {
		panic(fmt.Sprintf("mtprotoedge filedata: register canonical RPC %#016x: %v", uint64(method), err))
	}
}

func (r *FileDataLayerRPC) AdmitLayer(profile tlprofile.Profile, b *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	return r.AdmitLayerWithOptions(profile, b, tlprofile.AdmissionOptions{Limits: limits})
}

func (r *FileDataLayerRPC) AdmitLayerWithOptions(profile tlprofile.Profile, b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	if admitted, ok := r.admitFileData(admitFileDataLayer, profile, b, options); ok {
		return admitted, nil
	}
	admitter, ok := r.base.(LayerRPCOptionsAdmitter)
	if ok {
		return admitter.AdmitLayerWithOptions(profile, b, options)
	}
	return r.base.AdmitLayer(profile, b, options.Limits)
}

func (r *FileDataLayerRPC) AdmitDefaultLayer(profile tlprofile.Profile, b *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	return r.AdmitDefaultLayerWithOptions(profile, b, tlprofile.AdmissionOptions{Limits: limits})
}

func (r *FileDataLayerRPC) AdmitDefaultLayerWithOptions(profile tlprofile.Profile, b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	if admitted, ok := r.admitFileData(admitFileDataDefault, profile, b, options); ok {
		return admitted, nil
	}
	if admitter, ok := r.base.(LayerRPCDefaultProfileOptionsAdmitter); ok {
		return admitter.AdmitDefaultLayerWithOptions(profile, b, options)
	}
	if admitter, ok := r.base.(LayerRPCDefaultProfileAdmitter); ok {
		return admitter.AdmitDefaultLayer(profile, b, options.Limits)
	}
	if admitter, ok := r.base.(LayerRPCOptionsAdmitter); ok {
		return admitter.AdmitLayerWithOptions(profile, b, options)
	}
	return r.base.AdmitLayer(profile, b, options.Limits)
}

func (r *FileDataLayerRPC) AdmitUnprofiled(b *bin.Buffer, limits tlprofile.Limits) (tlprofile.Admission, error) {
	return r.AdmitUnprofiledWithOptions(b, tlprofile.AdmissionOptions{Limits: limits})
}

func (r *FileDataLayerRPC) AdmitUnprofiledWithOptions(b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	if admitted, ok := r.admitFileData(admitFileDataUnprofiled, 0, b, options); ok {
		return admitted, nil
	}
	admitter, ok := r.base.(LayerRPCOptionsAdmitter)
	if ok {
		return admitter.AdmitUnprofiledWithOptions(b, options)
	}
	return r.base.AdmitUnprofiled(b, options.Limits)
}

type admitFileDataMode uint8

const (
	admitFileDataLayer admitFileDataMode = iota
	admitFileDataDefault
	admitFileDataUnprofiled
)

func (r *FileDataLayerRPC) admitFileData(mode admitFileDataMode, profile tlprofile.Profile, b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, bool) {
	if r == nil || r.dispatcher == nil || b == nil || len(b.Raw()) < bin.Word {
		return tlprofile.Admission{}, false
	}
	probe := &bin.Buffer{Buf: b.Raw()}
	admitted, err := r.admitWith(r.dispatcher, mode, profile, probe, options)
	if err != nil || probe.Len() != 0 {
		return tlprofile.Admission{}, false
	}
	method := admitted.Call().Method()
	if !fileDataFastPathMethod(method) {
		return tlprofile.Admission{}, false
	}
	if method == tlprofile.SemanticMethodUploadGetFile && !r.fileDataGetFileAllowed(mode, profile, b.Raw(), options) {
		return tlprofile.Admission{}, false
	}
	b.ResetTo(probe.Raw())
	if method == tlprofile.SemanticMethodUploadGetFile {
		r.rememberLocalAdmission(admitted)
	}
	return admitted, true
}

func (r *FileDataLayerRPC) admitWith(d *tlprofile.Dispatcher, mode admitFileDataMode, profile tlprofile.Profile, b *bin.Buffer, options tlprofile.AdmissionOptions) (tlprofile.Admission, error) {
	switch mode {
	case admitFileDataLayer:
		return d.AdmitWithOptions(profile, b, options)
	case admitFileDataDefault:
		return d.AdmitDefaultWithOptions(profile, b, options)
	case admitFileDataUnprofiled:
		return d.AdmitUnprofiledWithOptions(b, options)
	default:
		return tlprofile.Admission{}, fmt.Errorf("mtprotoedge filedata: invalid admission mode %d", mode)
	}
}

func (r *FileDataLayerRPC) fileDataGetFileAllowed(mode admitFileDataMode, profile tlprofile.Profile, raw []byte, options tlprofile.AdmissionOptions) bool {
	if r == nil || r.classifier == nil {
		return false
	}
	probe := &bin.Buffer{Buf: raw}
	admitted, err := r.admitWith(r.classifier, mode, profile, probe, options)
	if err != nil || probe.Len() != 0 {
		return false
	}
	result, err := r.classifier.Dispatch(context.Background(), admitted)
	if err != nil {
		return false
	}
	allowed, _ := result.CanonicalValue().(bool)
	return allowed
}

func (r *FileDataLayerRPC) rememberLocalAdmission(admitted tlprofile.Admission) {
	if r == nil {
		return
	}
	now := time.Now()
	cutoff := now.Add(-fileDataLocalAdmissionTTL)
	identity := admitted.Prepared().Identity()
	r.localMu.Lock()
	for key, createdAt := range r.localAdmissions {
		if createdAt.Before(cutoff) {
			delete(r.localAdmissions, key)
		}
	}
	r.localAdmissions[identity] = now
	r.localMu.Unlock()
}

func (r *FileDataLayerRPC) isLocalAdmission(request tlprofile.Admission) bool {
	if r == nil {
		return false
	}
	identity := request.Prepared().Identity()
	cutoff := time.Now().Add(-fileDataLocalAdmissionTTL)
	r.localMu.Lock()
	createdAt, ok := r.localAdmissions[identity]
	if ok && createdAt.Before(cutoff) {
		delete(r.localAdmissions, identity)
		ok = false
	}
	r.localMu.Unlock()
	return ok
}

func (r *FileDataLayerRPC) DispatchAdmitted(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	switch request.Call().Method() {
	case tlprofile.SemanticMethodUploadSaveFilePart,
		tlprofile.SemanticMethodUploadSaveBigFilePart,
		tlprofile.SemanticMethodUploadGetFileHashes:
		result, err := r.dispatcher.Dispatch(ctx, request)
		return result, fileDataMethodName(request.Call().Method()), err
	case tlprofile.SemanticMethodUploadGetFile:
		if !r.isLocalAdmission(request) {
			break
		}
		result, err := r.dispatcher.Dispatch(ctx, request)
		return result, fileDataMethodName(request.Call().Method()), err
	}
	return r.base.DispatchAdmitted(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
}

func fileDataFastPathMethod(method tlprofile.SemanticID) bool {
	switch method {
	case tlprofile.SemanticMethodUploadSaveFilePart,
		tlprofile.SemanticMethodUploadSaveBigFilePart,
		tlprofile.SemanticMethodUploadGetFile,
		tlprofile.SemanticMethodUploadGetFileHashes:
		return true
	default:
		return false
	}
}

func fileDataMethodName(method tlprofile.SemanticID) string {
	_, name, ok := tlprofile.SemanticName(method)
	if !ok || name == "" {
		return "unknown"
	}
	return name
}

func fileDataCurrentUserID(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	hint, ok := ctx.Value(fileDataIdentityHintKey{}).(LayerRPCIdentityHint)
	if !ok || !hint.UserIDResolved || hint.UserID == 0 {
		return 0, false
	}
	return hint.UserID, true
}

func (r *FileDataLayerRPC) WithLayerRPCIdentityHint(ctx context.Context, hint LayerRPCIdentityHint) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if decorator, ok := r.base.(LayerRPCIdentityHintContext); ok {
		if decorated := decorator.WithLayerRPCIdentityHint(ctx, hint); decorated != nil {
			ctx = decorated
		}
	}
	return context.WithValue(ctx, fileDataIdentityHintKey{}, hint)
}

func (r *FileDataLayerRPC) WithLayerRPCProfileEvidenceFresh(ctx context.Context, fresh bool) context.Context {
	if decorator, ok := r.base.(LayerRPCProfileEvidenceContext); ok {
		return decorator.WithLayerRPCProfileEvidenceFresh(ctx, fresh)
	}
	return ctx
}

func (r *FileDataLayerRPC) LayerRPCFlatBytesPayloadSize(wire []byte) (int, bool) {
	if sizer, ok := r.base.(LayerRPCFlatBytesPayloadSizer); ok {
		return sizer.LayerRPCFlatBytesPayloadSize(wire)
	}
	return 0, false
}

func (r *FileDataLayerRPC) NegotiatedSessionLayer(authKeyID [8]byte, sessionID int64) (int, bool) {
	if resolver, ok := r.base.(LayerRPCSessionProfileResolver); ok {
		return resolver.NegotiatedSessionLayer(authKeyID, sessionID)
	}
	return 0, false
}

func (r *FileDataLayerRPC) NegotiatedSessionLayerEvidence(authKeyID [8]byte, sessionID int64) (int, int64, bool) {
	if resolver, ok := r.base.(LayerRPCOrderedSessionProfileResolver); ok {
		return resolver.NegotiatedSessionLayerEvidence(authKeyID, sessionID)
	}
	return 0, 0, false
}

func (r *FileDataLayerRPC) FreezeNegotiatedSessionLayer(authKeyID [8]byte, sessionID int64, layer int) error {
	if registry, ok := r.base.(LayerRPCSessionProfileRegistry); ok {
		return registry.FreezeNegotiatedSessionLayer(authKeyID, sessionID, layer)
	}
	return fmt.Errorf("mtprotoedge filedata: base does not implement session profile registry")
}

func (r *FileDataLayerRPC) FreezeNegotiatedSessionLayerAt(authKeyID [8]byte, sessionID int64, layer int, msgID int64) (bool, error) {
	if registry, ok := r.base.(LayerRPCOrderedSessionProfileRegistry); ok {
		return registry.FreezeNegotiatedSessionLayerAt(authKeyID, sessionID, layer, msgID)
	}
	return false, fmt.Errorf("mtprotoedge filedata: base does not implement ordered session profile registry")
}

func (r *FileDataLayerRPC) ForgetNegotiatedSessionLayer(authKeyID [8]byte, sessionID int64) {
	if registry, ok := r.base.(LayerRPCSessionProfileRegistry); ok {
		registry.ForgetNegotiatedSessionLayer(authKeyID, sessionID)
	}
}

func (r *FileDataLayerRPC) ForgetNegotiatedAuthKey(authKeyID [8]byte) {
	if registry, ok := r.base.(LayerRPCSessionProfileRegistry); ok {
		registry.ForgetNegotiatedAuthKey(authKeyID)
	}
}

func (r *FileDataLayerRPC) ResolveNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) (int, int64, bool, error) {
	if resolver, ok := r.base.(LayerRPCDurableSessionProfileResolver); ok {
		return resolver.ResolveNegotiatedSessionLayerEvidence(ctx, rawAuthKeyID, sessionID)
	}
	return 0, 0, false, fmt.Errorf("mtprotoedge filedata: base does not implement durable session profile resolver")
}

func (r *FileDataLayerRPC) ResolveInheritedAuthKeyLayer(ctx context.Context, rawAuthKeyID [8]byte) (int, bool, error) {
	if resolver, ok := r.base.(LayerRPCInheritedAuthKeyProfileResolver); ok {
		return resolver.ResolveInheritedAuthKeyLayer(ctx, rawAuthKeyID)
	}
	return 0, false, fmt.Errorf("mtprotoedge filedata: base does not implement inherited auth-key profile resolver")
}

func (r *FileDataLayerRPC) AdvanceNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, layer int, msgID int64) (int, int64, bool, error) {
	if advancer, ok := r.base.(LayerRPCDurableSessionProfileAdvancer); ok {
		return advancer.AdvanceNegotiatedSessionLayerEvidence(ctx, rawAuthKeyID, sessionID, layer, msgID)
	}
	return 0, 0, false, fmt.Errorf("mtprotoedge filedata: base does not implement durable session profile advancer")
}

func (r *FileDataLayerRPC) DeleteNegotiatedSessionLayerEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) (bool, error) {
	if deleter, ok := r.base.(LayerRPCDurableSessionProfileDeleter); ok {
		return deleter.DeleteNegotiatedSessionLayerEvidence(ctx, rawAuthKeyID, sessionID)
	}
	return false, fmt.Errorf("mtprotoedge filedata: base does not implement durable session profile deleter")
}

func (r *FileDataLayerRPC) PublishAdmittedLayerProfileEvidence(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, safeFloor uint64, layer int) error {
	if publisher, ok := r.base.(LayerRPCAdmissionProfilePublisher); ok {
		return publisher.PublishAdmittedLayerProfileEvidence(ctx, rawAuthKeyID, sessionID, msgID, admissionSeq, safeFloor, layer)
	}
	return fmt.Errorf("mtprotoedge filedata: base does not implement layer profile publisher")
}

func (r *FileDataLayerRPC) PrepareAdmittedReplay(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (func() error, error) {
	switch request.Call().Method() {
	case tlprofile.SemanticMethodUploadSaveFilePart,
		tlprofile.SemanticMethodUploadSaveBigFilePart,
		tlprofile.SemanticMethodUploadGetFileHashes:
		return nil, nil
	case tlprofile.SemanticMethodUploadGetFile:
		if r.isLocalAdmission(request) {
			return nil, nil
		}
	}
	if preparer, ok := r.base.(LayerRPCReplayPreparer); ok {
		return preparer.PrepareAdmittedReplay(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
	}
	return nil, nil
}

func (r *FileDataLayerRPC) ObserveInitConnection(ctx context.Context, authKeyID [8]byte, sessionID int64, layer, apiID int, deviceModel, systemVersion, appVersion, systemLangCode, langPack, langCode string) error {
	if observer, ok := r.base.(RPCInitConnectionObserver); ok {
		return observer.ObserveInitConnection(ctx, authKeyID, sessionID, layer, apiID, deviceModel, systemVersion, appVersion, systemLangCode, langPack, langCode)
	}
	return fmt.Errorf("mtprotoedge filedata: base does not implement initConnection observer")
}

func (r *FileDataLayerRPC) RunPostResponseActions(ctx context.Context, actions []postresponse.Action) error {
	if len(actions) == 0 {
		return nil
	}
	if executor, ok := r.base.(postresponse.ActionExecutor); ok {
		return executor.RunPostResponseActions(ctx, actions)
	}
	return nil
}

func fileDataSaveErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrFilePartInvalid):
		return tgerr.New(400, "FILE_PART_INVALID")
	case errors.Is(err, domain.ErrFilePartsInvalid):
		return tgerr.New(400, "FILE_PARTS_INVALID")
	case errors.Is(err, domain.ErrFilePartTooBig):
		return tgerr.New(400, "FILE_PART_TOO_BIG")
	case errors.Is(err, domain.ErrUploadQuotaExceeded):
		return tgerr.New(420, "FLOOD_WAIT_60")
	case errors.Is(err, domain.ErrStorageFull):
		return tgerr.New(400, "STORAGE_FULL")
	default:
		return tgerr.New(500, "INTERNAL_SERVER_ERROR")
	}
}

func fileDataLocationKey(location tg.InputFileLocationClass) (string, bool) {
	switch loc := location.(type) {
	case *tg.InputFileLocation:
		return fileDataLegacyVolumeLocationKey(loc.VolumeID, loc.LocalID)
	case *tg.InputDocumentFileLocation:
		if loc.ID == 0 {
			return "", false
		}
		if loc.ThumbSize == "" {
			return fmt.Sprintf("doc:%d", loc.ID), true
		}
		return fmt.Sprintf("doc:%d:%s", loc.ID, loc.ThumbSize), true
	case *tg.InputPhotoFileLocation:
		if loc.ID == 0 || loc.ThumbSize == "" {
			return "", false
		}
		return fmt.Sprintf("photo:%d:%s", loc.ID, loc.ThumbSize), true
	case *tg.InputPeerPhotoFileLocation:
		if loc.PhotoID == 0 {
			return "", false
		}
		size := "a"
		if loc.Big {
			size = "c"
		}
		return fmt.Sprintf("photo:%d:%s", loc.PhotoID, size), true
	case *tg.InputPhotoLegacyFileLocation:
		photoID := loc.ID
		if photoID == 0 && loc.VolumeID < 0 {
			photoID = -loc.VolumeID
		}
		if photoID == 0 {
			return "", false
		}
		size, ok := fileDataLegacyPhotoSizeType(loc.LocalID)
		if !ok {
			return "", false
		}
		return fmt.Sprintf("photo:%d:%s", photoID, size), true
	case *tg.InputPeerPhotoFileLocationLegacy:
		photoID := int64(0)
		if loc.VolumeID < 0 {
			photoID = -loc.VolumeID
		}
		if photoID == 0 {
			return "", false
		}
		size := "a"
		if loc.Big {
			size = "c"
		}
		return fmt.Sprintf("photo:%d:%s", photoID, size), true
	default:
		return "", false
	}
}

func fileDataLegacyVolumeLocationKey(volumeID int64, localID int) (string, bool) {
	if volumeID >= 0 || localID <= 0 {
		return "", false
	}
	id := -volumeID
	if size, ok := fileDataLegacyPhotoSizeType(localID); ok {
		return fmt.Sprintf("photo:%d:%s", id, size), true
	}
	if size, ok := fileDataLegacyDocumentThumbSizeType(localID); ok {
		return fmt.Sprintf("doc:%d:%s", id, size), true
	}
	return "", false
}

func fileDataLegacyPhotoSizeType(localID int) (string, bool) {
	if localID < 1 || localID > 127 {
		return "", false
	}
	ch := byte(localID)
	if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
		return string(ch), true
	}
	return "", false
}

func fileDataLegacyDocumentThumbSizeType(localID int) (string, bool) {
	localID -= 1000
	if localID < 1 || localID > 127 {
		return "", false
	}
	ch := byte(localID)
	if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
		return string(ch), true
	}
	return "", false
}

func fileDataStorageFileType(mime string, data []byte) tg.StorageFileTypeClass {
	switch fileDataSniffImageType(data) {
	case "jpeg":
		return &tg.StorageFileJpeg{}
	case "png":
		return &tg.StorageFilePng{}
	case "gif":
		return &tg.StorageFileGif{}
	case "webp":
		return &tg.StorageFileWebp{}
	}
	switch {
	case strings.Contains(mime, "webp"):
		return &tg.StorageFileWebp{}
	case strings.Contains(mime, "jpeg"), strings.Contains(mime, "jpg"):
		return &tg.StorageFileJpeg{}
	case strings.Contains(mime, "png"):
		return &tg.StorageFilePng{}
	case strings.Contains(mime, "gif"):
		return &tg.StorageFileGif{}
	case strings.Contains(mime, "mp4"), strings.Contains(mime, "quicktime"), strings.Contains(mime, "video"):
		return &tg.StorageFileMov{}
	}
	return &tg.StorageFileUnknown{}
}

func fileDataSniffImageType(data []byte) string {
	if len(data) >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
		return "jpeg"
	}
	if len(data) >= 8 && data[0] == 0x89 && data[1] == 'P' && data[2] == 'N' && data[3] == 'G' {
		return "png"
	}
	if len(data) >= 6 && data[0] == 'G' && data[1] == 'I' && data[2] == 'F' {
		return "gif"
	}
	if len(data) >= 12 && data[0] == 'R' && data[1] == 'I' && data[2] == 'F' &&
		data[8] == 'W' && data[9] == 'E' && data[10] == 'B' && data[11] == 'P' {
		return "webp"
	}
	return ""
}

func fileDataTGFileHashes(hashes []domain.FileHash) []tg.FileHash {
	out := make([]tg.FileHash, 0, len(hashes))
	for _, hash := range hashes {
		out = append(out, tg.FileHash{
			Offset: hash.Offset,
			Limit:  hash.Limit,
			Hash:   append([]byte(nil), hash.Hash...),
		})
	}
	return out
}

var _ LayerRPCHandler = (*FileDataLayerRPC)(nil)
var _ LayerRPCOptionsAdmitter = (*FileDataLayerRPC)(nil)
var _ LayerRPCDefaultProfileAdmitter = (*FileDataLayerRPC)(nil)
var _ LayerRPCDefaultProfileOptionsAdmitter = (*FileDataLayerRPC)(nil)
var _ LayerRPCFlatBytesPayloadSizer = (*FileDataLayerRPC)(nil)
var _ LayerRPCOrderedSessionProfileRegistry = (*FileDataLayerRPC)(nil)
var _ LayerRPCDurableSessionProfileResolver = (*FileDataLayerRPC)(nil)
var _ LayerRPCInheritedAuthKeyProfileResolver = (*FileDataLayerRPC)(nil)
var _ LayerRPCDurableSessionProfileAdvancer = (*FileDataLayerRPC)(nil)
var _ LayerRPCDurableSessionProfileDeleter = (*FileDataLayerRPC)(nil)
var _ LayerRPCReplayPreparer = (*FileDataLayerRPC)(nil)
var _ LayerRPCProfileEvidenceContext = (*FileDataLayerRPC)(nil)
var _ LayerRPCIdentityHintContext = (*FileDataLayerRPC)(nil)
var _ LayerRPCAdmissionProfilePublisher = (*FileDataLayerRPC)(nil)
var _ RPCInitConnectionObserver = (*FileDataLayerRPC)(nil)
var _ postresponse.ActionExecutor = (*FileDataLayerRPC)(nil)
