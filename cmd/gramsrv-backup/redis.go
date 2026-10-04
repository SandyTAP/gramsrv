package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// redisClient captures an RDB snapshot. Redis only holds rate limit counters,
// ephemeral keys and delivery bookkeeping for gramsrv, so the snapshot is a
// convenience rather than a requirement: --skip-redis is a valid choice when a
// fresh Redis is acceptable.
type redisClient struct {
	runner   cmdRunner
	addr     string
	password string
	db       int
}

func (c redisClient) env() []string {
	if c.password == "" {
		return nil
	}
	return []string{"REDISCLI_AUTH=" + c.password}
}

// snapshot returns the path of an RDB file plus a cleanup function. The file is
// produced by redis-cli inside whichever environment the client runs in, so in
// docker mode it is copied out of the container afterwards.
func (c redisClient) snapshot() (string, func(), error) {
	noop := func() {}
	if strings.TrimSpace(c.addr) == "" {
		return "", noop, fmt.Errorf("no Redis address configured")
	}
	host, port := splitHostPort(c.addr)
	if c.runner.mode == toolsDocker {
		return c.snapshotDocker(host, port)
	}
	f, err := os.CreateTemp("", "gramsrv-backup-redis-*.rdb")
	if err != nil {
		return "", noop, fmt.Errorf("create redis temp file: %w", err)
	}
	path := f.Name()
	f.Close()
	cleanup := func() { os.Remove(path) }
	args := append(c.clientArgs(host, port), "--rdb", path)
	var errBuf strings.Builder
	if err := c.runner.run(nil, io.Discard, &errBuf, c.env(), "redis-cli", args...); err != nil {
		cleanup()
		return "", noop, fmt.Errorf("redis-cli --rdb: %w: %s", err, tail(errBuf.String()))
	}
	return path, cleanup, nil
}

// clientArgs returns the connection flags. In docker mode redis-cli runs inside
// the Redis container, where the server listens on its own port rather than on
// the port published to the host, so the address is left out and redis-cli falls
// back to 127.0.0.1:6379.
func (c redisClient) clientArgs(host, port string) []string {
	args := make([]string, 0, 6)
	if c.runner.mode != toolsDocker {
		if host != "" {
			args = append(args, "-h", host)
		}
		if port != "" {
			args = append(args, "-p", port)
		}
	}
	return append(args, "-n", strconv.Itoa(c.db))
}

func (c redisClient) snapshotDocker(host, port string) (string, func(), error) {
	// Fixed-ish name inside the container: unique enough for one concurrent run
	// and predictable enough to clean up.
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", func() {}, fmt.Errorf("generate temp name: %w", err)
	}
	remote := "/tmp/gramsrv-backup-" + hex.EncodeToString(suffix[:]) + ".rdb"
	cleanupRemote := func() {
		_ = exec.Command("docker", "exec", c.runner.container, "rm", "-f", remote).Run()
	}
	local, err := os.CreateTemp("", "gramsrv-backup-redis-*.rdb")
	if err != nil {
		return "", func() {}, fmt.Errorf("create redis temp file: %w", err)
	}
	localPath := local.Name()
	local.Close()
	cleanup := func() {
		os.Remove(localPath)
		cleanupRemote()
	}
	args := append(c.clientArgs(host, port), "--rdb", remote)
	var errBuf strings.Builder
	if err := c.runner.run(nil, io.Discard, &errBuf, c.env(), "redis-cli", args...); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("redis-cli --rdb in %s: %w: %s", c.runner.container, err, tail(errBuf.String()))
	}
	cp := exec.Command("docker", "cp", c.runner.container+":"+remote, localPath)
	if out, err := cp.CombinedOutput(); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("docker cp redis snapshot: %w: %s", err, tail(string(out)))
	}
	return localPath, cleanup, nil
}

// splitHostPort accepts host:port, a bare host, a unix socket path and an
// already split "host port" pair, because Redis deployments vary. Redis clients
// default to port 6379 and localhost, which redis-cli applies on its own.
func splitHostPort(addr string) (string, string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", ""
	}
	if strings.HasPrefix(addr, "unix://") {
		return strings.TrimPrefix(addr, "unix://"), ""
	}
	host, port, found := strings.Cut(addr, ":")
	if !found {
		return addr, ""
	}
	if _, err := strconv.Atoi(port); err != nil {
		return addr, ""
	}
	return host, port
}

func splitLast(s, sep string) (string, string, error) {
	i := strings.LastIndex(s, sep)
	if i < 0 {
		return "", "", fmt.Errorf("no %q in %q", sep, s)
	}
	return s[:i], s[i+len(sep):], nil
}

// copyFileTo streams src into dst.
func copyFileTo(src io.Reader, dst io.Writer) error {
	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	return nil
}
