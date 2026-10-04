package main

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// archiveItem is one entry inside a tar artifact. Either source points at a
// path on disk, or generated carries in-memory content (rendered compose files,
// docker inspect output, systemd units copied out of /etc).
type archiveItem struct {
	source    string
	generated []byte
	name      string
	mode      fs.FileMode
	modTime   time.Time
	dir       bool
	symlink   string
}

type itemCollector struct {
	items []archiveItem
}

func (c *itemCollector) addFile(source, name string) {
	info, err := os.Lstat(source)
	if err != nil {
		return
	}
	c.items = append(c.items, archiveItem{
		source:  source,
		name:    name,
		mode:    info.Mode(),
		modTime: info.ModTime(),
		symlink: symlinkTarget(source, info),
	})
}

func (c *itemCollector) addGenerated(name string, content []byte, mode fs.FileMode) {
	c.items = append(c.items, archiveItem{generated: content, name: name, mode: mode})
}

func (c *itemCollector) addDir(name string) {
	c.items = append(c.items, archiveItem{name: name, mode: fs.ModeDir | 0o755, dir: true})
}

func symlinkTarget(path string, info fs.FileInfo) string {
	if info.Mode()&fs.ModeSymlink == 0 {
		return ""
	}
	target, err := os.Readlink(path)
	if err != nil {
		return ""
	}
	return target
}

// addTree appends every file below root under the archive prefix, keeping the
// relative layout. skip receives the slash separated relative path.
func (c *itemCollector) addTree(root, prefix string, skip func(rel string) bool) error {
	root = filepath.Clean(root)
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("read %s: %w", root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", root)
	}
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		c.addDir(prefix)
	}
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if skip != nil && skip(rel) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		name := rel
		if prefix != "" {
			name = prefix + "/" + rel
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}
		c.items = append(c.items, archiveItem{
			source:  path,
			name:    name,
			mode:    info.Mode(),
			modTime: info.ModTime(),
			dir:     info.IsDir(),
			symlink: symlinkTarget(path, info),
		})
		return nil
	})
}

// zstdLevel maps the CLI level onto the encoder presets. Media is already
// compressed, so the default trades a few percent of size for speed.
func zstdLevel(level int) zstd.EncoderLevel {
	switch level {
	case 1:
		return zstd.SpeedFastest
	case 3:
		return zstd.SpeedDefault
	case 4:
		return zstd.SpeedBetterCompression
	default:
		return zstd.SpeedDefault
	}
}

// writeArchive writes items as a zstd compressed tar stream at the requested
// level.
func writeArchive(items []archiveItem, dst io.Writer, level int) error {
	enc, err := zstd.NewWriter(dst, zstd.WithEncoderLevel(zstdLevel(level)), zstd.WithEncoderConcurrency(2))
	if err != nil {
		return fmt.Errorf("zstd encoder: %w", err)
	}
	tw := tar.NewWriter(enc)
	for _, item := range items {
		if err := writeArchiveItem(tw, item); err != nil {
			_ = enc.Close()
			return err
		}
	}
	if err := tw.Close(); err != nil {
		_ = enc.Close()
		return fmt.Errorf("close tar: %w", err)
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close zstd: %w", err)
	}
	return nil
}

func writeArchiveItem(tw *tar.Writer, item archiveItem) error {
	name := strings.TrimPrefix(filepath.ToSlash(item.name), "./")
	if name == "" {
		return errors.New("archive entry has an empty name")
	}
	header := &tar.Header{
		Name:     name,
		Mode:     int64(item.mode.Perm()),
		ModTime:  item.modTime,
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	}
	switch {
	case item.dir:
		header.Typeflag = tar.TypeDir
		header.Mode = int64(item.mode.Perm() | 0o700)
		header.Name = name + "/"
	case item.symlink != "":
		header.Typeflag = tar.TypeSymlink
		header.Linkname = item.symlink
	case item.generated != nil:
		header.Size = int64(len(item.generated))
	default:
		info, err := os.Lstat(item.source)
		if err != nil {
			return fmt.Errorf("stat %s: %w", item.source, err)
		}
		header.Mode = int64(info.Mode().Perm())
		header.ModTime = info.ModTime()
		// Directories carry their own mode, which matters for the trees that
		// hold keys: data/telegram-login is 0700 and must stay that way.
		if info.IsDir() {
			header.Typeflag = tar.TypeDir
			header.Mode = int64(info.Mode().Perm() | 0o700)
			header.Name = name + "/"
			break
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			header.Typeflag = tar.TypeSymlink
			header.Linkname = symlinkTarget(item.source, info)
			break
		}
		if !info.Mode().IsRegular() {
			// Sockets, devices and fifos have no meaningful restore.
			return nil
		}
		header.Size = info.Size()
	}
	if err := tw.WriteHeader(header); err != nil {
		return fmt.Errorf("write header %s: %w", name, err)
	}
	if header.Typeflag != tar.TypeReg || header.Size == 0 {
		return nil
	}
	var src io.Reader
	if item.generated != nil {
		src = strings.NewReader(string(item.generated))
	} else {
		f, err := os.Open(item.source)
		if err != nil {
			return fmt.Errorf("open %s: %w", item.source, err)
		}
		defer f.Close()
		src = f
	}
	n, err := io.Copy(tw, src)
	if err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	if n != header.Size {
		return fmt.Errorf("short write for %s: %d of %d bytes", name, n, header.Size)
	}
	return nil
}

// extractArchive unpacks a zstd compressed tar stream into dest. Existing files
// are preserved unless overwrite is set, so a restore cannot silently clobber a
// data directory an operator still cares about.
func extractArchive(src io.Reader, dest string, overwrite bool) ([]string, error) {
	dec, err := zstd.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("zstd reader: %w", err)
	}
	defer dec.Close()

	var written []string
	tr := tar.NewReader(dec)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return written, nil
		}
		if err != nil {
			return written, fmt.Errorf("read tar: %w", err)
		}
		clean, err := safeArchivePath(dest, header.Name)
		if err != nil {
			return written, err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			// MkdirAll applies the process umask and leaves an existing
			// directory alone, so the recorded mode is set explicitly. This is
			// what keeps a 0700 key directory private after a restore.
			mode := sanitizeMode(header.Mode)
			if err := os.MkdirAll(clean, mode|0o700); err != nil {
				return written, err
			}
			if err := os.Chmod(clean, mode); err != nil {
				return written, fmt.Errorf("chmod %s: %w", clean, err)
			}
		case tar.TypeSymlink:
			if err := replaceForWrite(clean, overwrite); err != nil {
				return written, err
			}
			if err := os.Symlink(header.Linkname, clean); err != nil {
				return written, fmt.Errorf("symlink %s: %w", clean, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(clean), 0o755); err != nil {
				return written, err
			}
			if err := replaceForWrite(clean, overwrite); err != nil {
				return written, err
			}
			mode := sanitizeMode(header.Mode)
			f, err := os.OpenFile(clean, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
			if err != nil {
				return written, fmt.Errorf("create %s: %w", clean, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return written, fmt.Errorf("write %s: %w", clean, err)
			}
			if err := f.Close(); err != nil {
				return written, err
			}
			// OpenFile only applies the mode when it creates the file, so a
			// pre-existing target keeps its old bits without this chmod.
			if err := os.Chmod(clean, mode); err != nil {
				return written, fmt.Errorf("chmod %s: %w", clean, err)
			}
			if !header.ModTime.IsZero() {
				_ = os.Chtimes(clean, header.ModTime, header.ModTime)
			}
			written = append(written, clean)
		default:
			// Hard links and other exotic types are not produced by this tool.
			continue
		}
	}
}

// safeArchivePath rejects absolute paths and traversal so a hostile bundle
// cannot write outside dest.
func safeArchivePath(dest, name string) (string, error) {
	cleaned := filepath.Clean(filepath.FromSlash(name))
	if filepath.IsAbs(cleaned) || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("archive entry %q escapes the destination", name)
	}
	target := filepath.Join(dest, cleaned)
	rel, err := filepath.Rel(dest, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("archive entry %q escapes the destination", name)
	}
	return target, nil
}

// sanitizeMode drops setuid, setgid and sticky bits: a restored bundle is data,
// and re-introducing privilege bits would be a surprise.
func sanitizeMode(mode int64) fs.FileMode {
	return fs.FileMode(mode).Perm()
}

func replaceForWrite(path string, overwrite bool) error {
	if !overwrite {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("%s already exists; pass --force to overwrite", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}
