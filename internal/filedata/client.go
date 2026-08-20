package filedata

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	filesapp "telesrv/internal/app/files"
	"telesrv/internal/domain"
	"telesrv/internal/filedata/filedatapb"
)

func DialGRPCRemote(ctx context.Context, cfg GRPCClientConfig) (*GRPCRemote, *grpc.ClientConn, error) {
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, nil, ErrGRPCTokenMissing
	}
	resolverProvider, err := grpcResolverProvider(cfg)
	if err != nil {
		return nil, nil, err
	}
	target := strings.TrimSpace(resolverProvider.Target())
	if target == "" {
		return nil, nil, ErrGRPCTargetsMissing
	}
	opts, err := clientDialOptions(cfg, resolverProvider)
	if err != nil {
		return nil, nil, err
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("filedata grpc client: %w", err)
	}
	remote := &GRPCRemote{
		client:         filedatapb.NewFileDataServiceClient(conn),
		health:         healthpb.NewHealthClient(conn),
		requestTimeout: clientRequestTimeout(cfg.RequestTimeout),
		log:            cfg.Logger,
		token:          strings.TrimSpace(cfg.Token),
	}
	if remote.log == nil {
		remote.log = zap.NewNop()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := remote.Check(ctx); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return remote, conn, nil
}

func (r *GRPCRemote) Check(ctx context.Context) error {
	if r == nil || r.client == nil {
		return ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	callCtx = r.withAuth(callCtx)
	if r.health != nil {
		res, err := r.health.Check(callCtx, &healthpb.HealthCheckRequest{Service: filedatapb.FileDataService_ServiceDesc.ServiceName})
		if err != nil {
			return fmt.Errorf("filedata grpc health: %w", err)
		}
		if res.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("filedata grpc health: status %s", res.GetStatus().String())
		}
	}
	info, err := r.client.GetInfo(callCtx, &filedatapb.FileDataInfoRequest{
		ProtocolVersion:             grpcProtocolVersion,
		MinSupportedProtocolVersion: grpcMinSupportedVersion,
		Capabilities:                append([]string(nil), grpcCapabilities...),
	})
	if err != nil {
		return fmt.Errorf("filedata grpc info: %w", err)
	}
	if info.GetError() != "" {
		return fmt.Errorf("%w: %s", ErrGRPCProtocolMismatch, info.GetError())
	}
	if !protocolRangesOverlap(grpcMinSupportedVersion, grpcProtocolVersion, info.GetMinSupportedProtocolVersion(), info.GetProtocolVersion()) {
		return fmt.Errorf("%w: client=%d..%d file=%d..%d",
			ErrGRPCProtocolMismatch,
			grpcMinSupportedVersion,
			grpcProtocolVersion,
			info.GetMinSupportedProtocolVersion(),
			info.GetProtocolVersion(),
		)
	}
	if strings.TrimSpace(info.GetBackend()) == "" {
		return ErrBackendMissing
	}
	r.backend = strings.TrimSpace(info.GetBackend())
	return nil
}

func (r *GRPCRemote) Name() string {
	if r == nil {
		return ""
	}
	return r.backend
}

func (r *GRPCRemote) Put(ctx context.Context, data []byte) (string, error) {
	key, _, _, err := r.PutReader(ctx, bytes.NewReader(data))
	return key, err
}

func (r *GRPCRemote) PutReader(ctx context.Context, src io.Reader) (string, int64, []byte, error) {
	if r == nil || r.client == nil {
		return "", 0, nil, ErrGRPCUnavailable
	}
	if src == nil {
		return "", 0, nil, fmt.Errorf("filedata put blob reader is nil")
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	stream, err := r.client.PutBlob(r.withAuth(callCtx))
	if err != nil {
		return "", 0, nil, fmt.Errorf("filedata grpc put_blob: %w", err)
	}
	buf := make([]byte, defaultGRPCPutBlobChunk)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if err := stream.Send(&filedatapb.PutBlobChunk{Data: append([]byte(nil), buf[:n]...)}); err != nil {
				return "", 0, nil, fmt.Errorf("filedata grpc put_blob send: %w", err)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", 0, nil, readErr
		}
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		return "", 0, nil, fmt.Errorf("filedata grpc put_blob close: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return "", 0, nil, err
	}
	return res.GetObjectKey(), res.GetSize(), append([]byte(nil), res.GetSha256()...), nil
}

func (r *GRPCRemote) Get(ctx context.Context, objectKey string) ([]byte, error) {
	data, _, err := r.GetRange(ctx, objectKey, 0, 0)
	return data, err
}

func (r *GRPCRemote) GetRange(ctx context.Context, objectKey string, offset, limit int64) ([]byte, int64, error) {
	if r == nil || r.client == nil {
		return nil, 0, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.GetBlobRange(r.withAuth(callCtx), &filedatapb.GetBlobRangeRequest{
		ObjectKey: objectKey,
		Offset:    offset,
		Limit:     limit,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("filedata grpc get_blob_range: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return nil, 0, err
	}
	return append([]byte(nil), res.GetData()...), res.GetTotal(), nil
}

func (r *GRPCRemote) PutUploadPart(ctx context.Context, ownerUserID, fileID int64, part int, data []byte) (filesapp.UploadPartObject, error) {
	if r == nil || r.client == nil {
		return filesapp.UploadPartObject{}, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.PutUploadPart(r.withAuth(callCtx), &filedatapb.PutUploadPartRequest{
		OwnerUserId: ownerUserID,
		FileId:      fileID,
		FilePart:    int32(part),
		Data:        append([]byte(nil), data...),
	})
	if err != nil {
		return filesapp.UploadPartObject{}, fmt.Errorf("filedata grpc put_upload_part: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return filesapp.UploadPartObject{}, err
	}
	return filesapp.UploadPartObject{
		Backend:   domain.MediaBackend(res.GetBackend()),
		ObjectKey: res.GetObjectKey(),
		Size:      res.GetSize(),
		SHA256:    append([]byte(nil), res.GetSha256()...),
	}, nil
}

func (r *GRPCRemote) GetUploadPart(ctx context.Context, objectKey string) ([]byte, error) {
	if r == nil || r.client == nil {
		return nil, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.GetUploadPart(r.withAuth(callCtx), &filedatapb.GetUploadPartRequest{ObjectKey: objectKey})
	if err != nil {
		return nil, fmt.Errorf("filedata grpc get_upload_part: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return nil, err
	}
	return append([]byte(nil), res.GetData()...), nil
}

func (r *GRPCRemote) OpenUploadPart(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	data, err := r.GetUploadPart(ctx, objectKey)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (r *GRPCRemote) DeleteUploadPart(ctx context.Context, objectKey string) error {
	if r == nil || r.client == nil {
		return ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.DeleteUploadPart(r.withAuth(callCtx), &filedatapb.DeleteUploadPartRequest{ObjectKey: objectKey})
	if err != nil {
		return fmt.Errorf("filedata grpc delete_upload_part: %w", err)
	}
	return errorFromPB(res.GetErrorKind(), res.GetError())
}

func (r *GRPCRemote) DeleteExpiredUploadParts(ctx context.Context, before time.Time, limit int) (int64, error) {
	if r == nil || r.client == nil {
		return 0, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.DeleteExpiredUploadParts(r.withAuth(callCtx), &filedatapb.DeleteExpiredUploadPartsRequest{
		BeforeUnixNano: before.UnixNano(),
		Limit:          int32(limit),
	})
	if err != nil {
		return 0, fmt.Errorf("filedata grpc delete_expired_upload_parts: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return res.GetDeleted(), err
	}
	return res.GetDeleted(), nil
}

func (r *GRPCRemote) AssembleUploadBlob(ctx context.Context, ownerUserID, fileID int64, expectedParts int) (filesapp.AssembledUploadBlob, error) {
	if r == nil || r.client == nil {
		return filesapp.AssembledUploadBlob{}, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.MaterializeUploadBlob(r.withAuth(callCtx), &filedatapb.MaterializeUploadBlobRequest{
		OwnerUserId:   ownerUserID,
		FileId:        fileID,
		ExpectedParts: int32(expectedParts),
	})
	if err != nil {
		return filesapp.AssembledUploadBlob{}, fmt.Errorf("filedata grpc materialize_upload_blob: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return filesapp.AssembledUploadBlob{}, err
	}
	return filesapp.AssembledUploadBlob{
		ObjectKey: res.GetObjectKey(),
		Size:      res.GetSize(),
		SHA256:    append([]byte(nil), res.GetSha256()...),
	}, nil
}

func (r *GRPCRemote) SaveFilePart(ctx context.Context, ownerUserID, fileID int64, part int, data []byte) (bool, error) {
	return r.saveFilePart(ctx, ownerUserID, fileID, part, 0, false, data)
}

func (r *GRPCRemote) SaveBigFilePart(ctx context.Context, ownerUserID, fileID int64, part, totalParts int, data []byte) (bool, error) {
	return r.saveFilePart(ctx, ownerUserID, fileID, part, totalParts, true, data)
}

func (r *GRPCRemote) saveFilePart(ctx context.Context, ownerUserID, fileID int64, part, totalParts int, big bool, data []byte) (bool, error) {
	if r == nil || r.client == nil {
		return false, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.SaveFilePart(r.withAuth(callCtx), &filedatapb.SaveFilePartRequest{
		OwnerUserId:    ownerUserID,
		FileId:         fileID,
		FilePart:       int32(part),
		FileTotalParts: int32(totalParts),
		Big:            big,
		Data:           append([]byte(nil), data...),
	})
	if err != nil {
		return false, fmt.Errorf("filedata grpc save_file_part: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return false, err
	}
	return res.GetSaved(), nil
}

func (r *GRPCRemote) GetFile(ctx context.Context, req domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	if r == nil || r.client == nil {
		return domain.FileChunk{}, false, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.GetFile(r.withAuth(callCtx), &filedatapb.GetFileRequest{
		LocationKey: req.LocationKey,
		Offset:      req.Offset,
		Limit:       int32(req.Limit),
	})
	if err != nil {
		return domain.FileChunk{}, false, fmt.Errorf("filedata grpc get_file: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return domain.FileChunk{}, false, err
	}
	return domain.FileChunk{
		Bytes:    append([]byte(nil), res.GetData()...),
		MimeType: res.GetMimeType(),
		Total:    res.GetTotal(),
	}, res.GetFound(), nil
}

func (r *GRPCRemote) GetFileHashes(ctx context.Context, req domain.FileHashRequest) ([]domain.FileHash, bool, error) {
	if r == nil || r.client == nil {
		return nil, false, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.GetFileHashes(r.withAuth(callCtx), &filedatapb.GetFileHashesRequest{
		LocationKey: req.LocationKey,
		Offset:      req.Offset,
	})
	if err != nil {
		return nil, false, fmt.Errorf("filedata grpc get_file_hashes: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return nil, false, err
	}
	hashes := make([]domain.FileHash, 0, len(res.GetHashes()))
	for _, hash := range res.GetHashes() {
		hashes = append(hashes, domain.FileHash{
			Offset: hash.GetOffset(),
			Limit:  int(hash.GetLimit()),
			Hash:   append([]byte(nil), hash.GetHash()...),
		})
	}
	return hashes, res.GetFound(), nil
}

func (r *GRPCRemote) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, r.requestTimeout)
}

func (r *GRPCRemote) withAuth(ctx context.Context) context.Context {
	if r == nil {
		return ctx
	}
	return appendBearerToken(ctx, r.token)
}

func clientRequestTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultGRPCClientTimeout
	}
	return d
}
