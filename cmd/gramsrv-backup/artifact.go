package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

// hashingWriter tees artifact content into a sha256 digest and a byte counter,
// so a bundle can be verified without a second pass over the data.
type hashingWriter struct {
	digest  hash.Hash
	counter *countingWriter
	writer  io.Writer
}

func newHashingWriter(dst io.Writer) (*hashingWriter, error) {
	digest := sha256.New()
	counter := &countingWriter{}
	return &hashingWriter{
		digest:  digest,
		counter: counter,
		writer:  io.MultiWriter(dst, digest, counter),
	}, nil
}

func (h *hashingWriter) Write(p []byte) (int, error) { return h.writer.Write(p) }

func (h *hashingWriter) Size() int64 { return h.counter.n }

func (h *hashingWriter) Sum() string { return hex.EncodeToString(h.digest.Sum(nil)) }

type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

// compressFile streams a file through zstd into dst.
func compressFile(src string, dst io.Writer, level int) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	enc, err := zstd.NewWriter(dst, zstd.WithEncoderLevel(zstdLevel(level)))
	if err != nil {
		return fmt.Errorf("zstd encoder: %w", err)
	}
	if _, err := io.Copy(enc, f); err != nil {
		_ = enc.Close()
		return err
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close zstd: %w", err)
	}
	return nil
}

// decompressReader wraps r in a zstd reader.
func decompressReader(r io.Reader) (io.ReadCloser, error) {
	dec, err := zstd.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("zstd reader: %w", err)
	}
	return dec.IOReadCloser(), nil
}
