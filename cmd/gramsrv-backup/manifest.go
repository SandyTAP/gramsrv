package main

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	toolName    = "gramsrv-backup"
	toolVersion = "1"
)

// Bundle relative paths. The layout is deliberately boring so that a bundle can
// be inspected with nothing but tar and a JSON viewer.
const (
	manifestName  = "MANIFEST.json"
	checksumsName = "SHA256SUMS"
)

// Artifact is one file inside a bundle. Paths are slash separated and relative
// to the bundle root so a bundle stays readable after it is moved to another
// host or unpacked on Windows.
type Artifact struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// TableCount records an exact row count for a table that existed at backup
// time. Missing tables are simply absent from the map.
type TableCount struct {
	Table string `json:"table"`
	Rows  int64  `json:"rows"`
}

// Component describes one captured subsystem.
type Component struct {
	Name string `json:"name"`
	// Database is the logical database name inside PostgreSQL.
	Database string `json:"database"`
	// PGVersion is the server version the dump was taken from. A dump can only
	// be restored into the same or a newer major version.
	PGVersion string `json:"pg_source_version"`
	// SchemaVersion is the numeric schema_migrations.version used by
	// telesrv, and SchemaDirty mirrors golang-migrate's dirty flag.
	SchemaVersion int64 `json:"schema_version"`
	SchemaDirty   bool  `json:"schema_dirty"`
	// SchemaLabel carries a non numeric schema version. grammystore applies
	// numbered SQL files and records their names, not a single integer.
	SchemaLabel string `json:"schema_label,omitempty"`
	// BlobObjects is the number of distinct objects the database references.
	// Zero means the component has no blob store or skipped the media tree.
	BlobObjects int64        `json:"blob_objects"`
	Tables      []TableCount `json:"tables"`
	Notes       []string     `json:"notes,omitempty"`
}

// schema renders the schema version for display.
func (c Component) schema() string {
	switch {
	case c.SchemaLabel != "":
		return c.SchemaLabel
	case c.SchemaVersion > 0:
		return strconv.FormatInt(c.SchemaVersion, 10)
	default:
		return "unknown"
	}
}

// Manifest is the bundle index. It is written last so it can describe every
// artifact, and it is intentionally free of secrets: only names, sizes and
// counts, never passwords, DSNs or host identifiers.
type Manifest struct {
	Tool       string      `json:"tool"`
	Version    string      `json:"tool_version"`
	CreatedAt  time.Time   `json:"created_at"`
	Host       string      `json:"host"`
	Commit     string      `json:"commit"`
	Branch     string      `json:"branch"`
	Frozen     bool        `json:"frozen"`
	Components []Component `json:"components"`
	Artifacts  []Artifact  `json:"artifacts"`
}

func newManifest(frozen bool) (*Manifest, error) {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return &Manifest{
		Tool:      toolName,
		Version:   toolVersion,
		CreatedAt: time.Now().UTC(),
		Host:      host,
		Commit:    gitRevision("."),
		Branch:    gitBranch("."),
		Frozen:    frozen,
	}, nil
}

// AddArtifact records an already written artifact.
func (m *Manifest) AddArtifact(rel string, size int64, sum string) {
	m.Artifacts = append(m.Artifacts, Artifact{Path: filepath.ToSlash(rel), Bytes: size, SHA256: sum})
}

// Artifact returns the recorded artifact with the given bundle-relative path.
func (m *Manifest) Artifact(rel string) (Artifact, bool) {
	rel = filepath.ToSlash(rel)
	for _, a := range m.Artifacts {
		if a.Path == rel {
			return a, true
		}
	}
	return Artifact{}, false
}

// Write serialises the manifest as indented JSON.
func (m *Manifest) Write(path string) error {
	sort.Slice(m.Artifacts, func(i, j int) bool { return m.Artifacts[i].Path < m.Artifacts[j].Path })
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

// ReadManifest loads a manifest from a bundle directory.
func ReadManifest(dir string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if m.Tool != toolName {
		return nil, fmt.Errorf("bundle was produced by %q, want %q", m.Tool, toolName)
	}
	return &m, nil
}

// WriteChecksums writes a sha256sum(1) compatible checksum file so operators can
// re-verify a bundle with standard tools instead of this tool.
func WriteChecksums(dir string, artifacts []Artifact) error {
	var b strings.Builder
	for _, a := range artifacts {
		fmt.Fprintf(&b, "%s  %s\n", a.SHA256, a.Path)
	}
	path := filepath.Join(dir, checksumsName)
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		return fmt.Errorf("write checksums: %w", err)
	}
	return nil
}

// ReadChecksums parses a sha256sum(1) file.
func ReadChecksums(path string) ([]Artifact, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read checksums: %w", err)
	}
	var out []Artifact
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		sum, name, ok := strings.Cut(line, "  ")
		if !ok {
			return nil, fmt.Errorf("%s line %d: expected \"<sha256>  <path>\"", path, i+1)
		}
		if len(sum) != 64 {
			return nil, fmt.Errorf("%s line %d: digest is %d characters, want 64", path, i+1, len(sum))
		}
		if _, err := hex.DecodeString(sum); err != nil {
			return nil, fmt.Errorf("%s line %d: digest is not hexadecimal", path, i+1)
		}
		out = append(out, Artifact{Path: name, SHA256: sum})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s lists no artifacts", path)
	}
	return out, nil
}

// VerifyFiles re-hashes every artifact in dir and reports the ones that do not
// match. A bundle is only trustworthy once this passes.
func VerifyFiles(dir string, artifacts []Artifact) error {
	var failures []string
	for _, a := range artifacts {
		path := filepath.Join(dir, filepath.FromSlash(a.Path))
		sum, size, err := hashFile(path)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", a.Path, err))
			continue
		}
		if sum != a.SHA256 {
			failures = append(failures, fmt.Sprintf("%s: sha256 %s, want %s", a.Path, sum, a.SHA256))
			continue
		}
		if a.Bytes != 0 && size != a.Bytes {
			failures = append(failures, fmt.Sprintf("%s: %d bytes, want %d", a.Path, size, a.Bytes))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d artifact(s) failed verification:\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
	return nil
}

// VerifyArchives streams every artifact through the decompressor, and walks tar
// members, so a bundle that is intact but unreadable is still reported. A
// matching checksum only proves the bytes survived the transfer; it says
// nothing about whether the tool that produced them framed them correctly.
func VerifyArchives(dir string, artifacts []Artifact) error {
	var failures []string
	for _, a := range artifacts {
		path := filepath.Join(dir, filepath.FromSlash(a.Path))
		detail, err := inspectArchive(path, a.Path)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", a.Path, err))
			continue
		}
		fmt.Printf("  %s ok (%s)\n", a.Path, detail)
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d artifact(s) are unreadable:\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
	return nil
}

// inspectArchive decompresses one artifact and returns a short description.
func inspectArchive(path, name string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	reader, err := decompressReader(f)
	if err != nil {
		return "", fmt.Errorf("not a readable zstd stream: %w", err)
	}
	defer reader.Close()

	if strings.HasSuffix(name, ".tar.zst") {
		tr := tar.NewReader(reader)
		var entries int
		var payload int64
		for {
			header, err := tr.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return "", fmt.Errorf("tar stream is truncated after %d entries: %w", entries, err)
			}
			entries++
			if header.Typeflag == tar.TypeReg {
				payload += header.Size
			}
		}
		return fmt.Sprintf("%d entries, %s payload", entries, humanBytes(payload)), nil
	}

	n, err := io.Copy(io.Discard, reader)
	if err != nil {
		return "", fmt.Errorf("zstd stream is truncated after %d bytes: %w", n, err)
	}
	return fmt.Sprintf("%s uncompressed", humanBytes(n)), nil
}

// hashFile returns the hex sha256 and the size of a file.
func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
