package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Blob layouts are content addressed: data/blobs/<aa>/<bb>/<full key>. Keeping
// this in one place means the verification stays in step with
// internal/app/files/blobfs.go.
const blobShard = 2

// blobIndex compares what the database references against what is on disk.
type blobIndex struct {
	// Referenced is the number of distinct objects file_blobs points at.
	Referenced int
	// OnDisk is the number of files found under the blob directory.
	OnDisk int
	// Missing lists referenced objects with no file. This is data loss.
	Missing []string
	// Orphan lists files no row references. Harmless, but usually means the
	// media tree is ahead of the database or rows were deleted.
	Orphan []string
	// Corrupt lists files whose content digest does not match their name.
	Corrupt []string
	// Bytes is the total size of the referenced objects found on disk.
	Bytes int64
}

// Healthy reports whether the media tree is consistent with the database.
func (b blobIndex) Healthy() bool {
	return len(b.Missing) == 0 && len(b.Corrupt) == 0
}

func (b blobIndex) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "referenced=%d on_disk=%d bytes=%d missing=%d orphan=%d corrupt=%d",
		b.Referenced, b.OnDisk, b.Bytes, len(b.Missing), len(b.Orphan), len(b.Corrupt))
	return sb.String()
}

// blobObjectPath returns the on-disk location of a content addressed object.
func blobObjectPath(root, objectKey string) string {
	if len(objectKey) < 2*blobShard {
		return filepath.Join(root, objectKey)
	}
	return filepath.Join(root,
		objectKey[:blobShard],
		objectKey[blobShard:2*blobShard],
		objectKey,
	)
}

// referencedObjectKeys reads the distinct object keys the database expects to
// exist on disk.
func (c pgClient) referencedObjectKeys() ([]string, error) {
	rows, err := c.query(c.endpoint.Database,
		"SELECT DISTINCT object_key FROM file_blobs WHERE backend = 'localfs' ORDER BY object_key")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if key := strings.TrimSpace(row); key != "" {
			out = append(out, key)
		}
	}
	return out, nil
}

// indexBlobs walks the media tree and returns the basenames it contains. Only
// files laid out as <shard>/<shard>/<key> are considered blobs, so unrelated
// files in the tree do not create false orphans.
func indexBlobs(root string) (map[string]string, error) {
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 3 {
			return nil
		}
		key := parts[2]
		if parts[0] != key[:min(blobShard, len(key))] {
			return nil
		}
		out[key] = path
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}
	return out, nil
}

// checkBlobs builds the cross-check. verifyDigests hashes every referenced file,
// which is the expensive part; it is opt-in because a large instance stores tens
// of gigabytes of media.
// objectLister is the part of pgClient checkBlobs needs. Depending on an
// interface keeps the comparison logic testable without a database.
type objectLister interface {
	referencedObjectKeys() ([]string, error)
}

// checkBlobs builds the cross-check. verifyDigests hashes every referenced file,
// which is the expensive part; it is opt-in because a large instance stores tens
// of gigabytes of media.
func checkBlobs(lister objectLister, blobDir string, verifyDigests bool) (blobIndex, error) {
	index := blobIndex{}
	if blobDir == "" {
		return index, fmt.Errorf("no blob directory configured")
	}
	keys, err := lister.referencedObjectKeys()
	if err != nil {
		return index, fmt.Errorf("read file_blobs: %w", err)
	}
	referenced := make(map[string]bool, len(keys))
	for _, key := range keys {
		referenced[key] = true
	}
	index.Referenced = len(referenced)
	onDisk, err := indexBlobs(blobDir)
	if err != nil {
		return index, err
	}
	index.OnDisk = len(onDisk)

	for key := range referenced {
		path, ok := onDisk[key]
		if !ok {
			index.Missing = append(index.Missing, key)
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			index.Missing = append(index.Missing, key)
			continue
		}
		index.Bytes += info.Size()
		if verifyDigests {
			ok, err := digestMatches(path, key)
			if err != nil {
				return index, fmt.Errorf("digest %s: %w", path, err)
			}
			if !ok {
				index.Corrupt = append(index.Corrupt, key)
			}
		}
	}
	for key := range onDisk {
		if !referenced[key] {
			index.Orphan = append(index.Orphan, key)
		}
	}
	sort.Strings(index.Missing)
	sort.Strings(index.Orphan)
	sort.Strings(index.Corrupt)
	return index, nil
}

// digestMatches verifies that a stored object's content digest equals its name.
func digestMatches(path, want string) (bool, error) {
	wantBytes, err := hex.DecodeString(want)
	if err != nil || len(wantBytes) != sha256.Size {
		// Not a content addressed name; nothing to verify.
		return true, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	return bytes.Equal(h.Sum(nil), wantBytes), nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
