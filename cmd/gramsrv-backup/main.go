// Command gramsrv-backup captures and restores a gramsrv deployment: the
// PostgreSQL databases, the Redis snapshot, the data/ tree holding the server RSA
// key and the media store, and the configuration needed to bring the same
// instance up on another host.
//
// The tool is designed for migrations and for disaster recovery. A bundle is a
// directory that is safe to copy with rsync, and every artifact is described by
// MANIFEST.json and SHA256SUMS.
//
// Subcommands:
//
//	backup    write a new bundle
//	restore   replay a bundle onto this host
//	verify    re-hash a bundle and cross-check its media tree
//
// Run "gramsrv-backup <subcommand> -h" for the flags of each subcommand.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"

	"telesrv/internal/config"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, toolName+":", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return errors.New("missing subcommand")
	}
	switch args[0] {
	case "backup":
		return runBackup(args[1:])
	case "restore":
		return runRestore(args[1:])
	case "verify":
		return runVerify(args[1:])
	case "-h", "--help", "help":
		printUsage()
		return nil
	default:
		printUsage()
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func printUsage() {
	fmt.Fprint(os.Stderr, `usage: gramsrv-backup <subcommand> [flags]

  backup    capture PostgreSQL, Redis, the data/ tree and configuration
  restore   replay a bundle onto this host
  verify    re-hash a bundle and cross-check the media tree

`)
}

type componentFlags struct {
	names []string
}

func (c *componentFlags) register(fs *flag.FlagSet) {
	fs.Func("component", "component to capture: server, grammystore or all (repeatable, default server)",
		func(value string) error {
			for _, name := range strings.Split(value, ",") {
				name = strings.ToLower(strings.TrimSpace(name))
				if name == "" {
					continue
				}
				if name == "all" {
					c.names = append(c.names, componentServer, componentGrammystore)
					continue
				}
				c.names = append(c.names, name)
			}
			return nil
		})
}

func (c componentFlags) resolve() []string {
	if len(c.names) == 0 {
		return []string{componentServer}
	}
	seen := map[string]bool{}
	var out []string
	for _, name := range c.names {
		if seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	return out
}

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	return fs
}

func runBackup(args []string) error {
	fs := newFlagSet("backup")
	out := fs.String("out", "", "bundle directory to create (required)")
	var components componentFlags
	components.register(fs)
	repoDir := fs.String("repo-dir", ".", "deployment root that holds .env and data/")
	dsn := fs.String("dsn", "", "PostgreSQL DSN override (default: TELESRV_POSTGRES_DSN, then the loaded .env)")
	dataDir := fs.String("data-dir", "", "media and key tree (default: derived from TELESRV_BLOB_DIR)")
	pgTools := fs.String("pg-tools", toolsLocal, "where pg_dump/psql live: local or docker")
	pgContainer := fs.String("pg-container", "", "container providing pg_dump/psql in docker mode")
	pgContainerServer := fs.String("pg-container-server", "",
		"override -pg-container for the server component (its database usually runs in its own container)")
	pgContainerGrammystore := fs.String("pg-container-grammystore", "",
		"override -pg-container for the grammystore component")
	pgBinDir := fs.String("pg-bin-dir", "", "directory holding pg_dump/psql in local mode")
	redisTools := fs.String("redis-tools", toolsLocal, "where redis-cli lives: local or docker")
	redisContainer := fs.String("redis-container", "", "container providing redis-cli in docker mode")
	redisAddr := fs.String("redis-addr", "", "Redis address override (default: from .env)")
	includeData := fs.Bool("include-data", true, "archive the data/ tree")
	includeRedis := fs.Bool("include-redis", true, "capture a Redis RDB snapshot")
	freezeUnits := fs.String("freeze-units", "", "comma separated systemd units to stop for the capture")
	freezeContainers := fs.String("freeze-containers", "", "comma separated containers to stop for the capture")
	noRolePasswords := fs.Bool("no-role-passwords", false, "strip role password hashes from globals.sql")
	zstdLevel := fs.Int("zstd-level", 3, "compression level: 1 fastest .. 4 smallest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("-out is required")
	}
	if *zstdLevel < 1 || *zstdLevel > 4 {
		return errors.New("-zstd-level must be 1..4")
	}

	runner := cmdRunner{mode: *pgTools, container: *pgContainer, binaryDir: *pgBinDir}
	redisRunner := cmdRunner{mode: *redisTools, container: *redisContainer}

	// Each component usually runs against its own database container, so the
	// shared -pg-container is only a default that can be overridden per
	// component.
	runners := map[string]cmdRunner{}
	for _, name := range components.resolve() {
		componentRunner := runner
		switch name {
		case componentServer:
			if *pgContainerServer != "" {
				componentRunner.container = *pgContainerServer
			}
		case componentGrammystore:
			if *pgContainerGrammystore != "" {
				componentRunner.container = *pgContainerGrammystore
			}
		}
		if err := componentRunner.validate("pg"); err != nil {
			return fmt.Errorf("component %s: %w", name, err)
		}
		runners[name] = componentRunner
	}

	manifest, err := newManifest(false)
	if err != nil {
		return err
	}

	// Freeze before touching anything so the capture is a consistent snapshot.
	freeze := newFreezeSet(os.Stdout)
	frozen := false
	defer func() {
		if err := freeze.Release(); err != nil {
			fmt.Fprintln(os.Stderr, toolName+": restart failed:", err)
		}
	}()
	for _, unit := range splitList(*freezeUnits) {
		if err := freeze.StopUnit(unit); err != nil {
			return err
		}
		frozen = true
	}
	for _, container := range splitList(*freezeContainers) {
		if err := freeze.StopContainer(container); err != nil {
			return err
		}
		frozen = true
	}
	manifest.Frozen = frozen

	bundle, err := prepareBundle(*out)
	if err != nil {
		return err
	}

	opts := resolveOptions{repoDir: *repoDir, dsn: *dsn, dataDir: *dataDir, dsnOverride: *dsn != ""}
	resolved, err := resolveComponents(components.resolve(), opts)
	if err != nil {
		return err
	}

	for _, c := range resolved {
		fmt.Printf("capturing %s (database %s)\n", c.name, c.endpoint.Database)
		componentRunner := runners[c.name]
		client := c.client(componentRunner)
		meta, err := collectMetadata(client, c)
		if err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}

		schema, err := bundle.newArtifact(artifactPath(c.name, "schema.sql.zst"))
		if err != nil {
			return err
		}
		if err := writeFiltered(schema, *zstdLevel, func(w io.Writer) error { return client.writeSchema(w) }); err != nil {
			return err
		}
		if err := schema.close(); err != nil {
			return err
		}
		manifest.AddArtifact(schema.rel, schema.size(), schema.sum())
		fmt.Printf("  schema.sql.zst %s\n", humanBytes(schema.size()))

		if c.name == componentServer {
			globals, err := bundle.newArtifact(artifactPath(c.name, "globals.sql.zst"))
			if err != nil {
				return err
			}
			if err := writeFiltered(globals, *zstdLevel, func(w io.Writer) error {
				return client.writeGlobals(w, *noRolePasswords)
			}); err != nil {
				return err
			}
			if err := globals.close(); err != nil {
				return err
			}
			manifest.AddArtifact(globals.rel, globals.size(), globals.sum())
			fmt.Printf("  globals.sql.zst %s\n", humanBytes(globals.size()))
		}

		if *includeRedis && c.name == componentServer {
			if err := captureRedis(bundle, manifest, c, redisRunner, *redisAddr, *zstdLevel); err != nil {
				return err
			}
		}

		if *includeData && c.dataDir != "" {
			if err := captureData(bundle, manifest, c, *zstdLevel); err != nil {
				return err
			}
		}
		if err := captureConfig(bundle, manifest, c); err != nil {
			return err
		}

		manifest.Components = append(manifest.Components, meta)
		for _, note := range c.notes {
			fmt.Println("  note:", note)
		}
	}

	manifestPath := filepath.Join(bundle.dir, manifestName)
	if err := manifest.Write(manifestPath); err != nil {
		return err
	}
	if err := WriteChecksums(bundle.dir, manifest.Artifacts); err != nil {
		return err
	}
	fmt.Printf("bundle written to %s\n", bundle.dir)
	fmt.Printf("components: %d, artifacts: %d\n", len(manifest.Components), len(manifest.Artifacts))
	fmt.Println("verify it before relying on it: gramsrv-backup verify --from " + bundle.dir)
	return nil
}

func runVerify(args []string) error {
	fs := newFlagSet("verify")
	from := fs.String("from", "",
		"bundle directory to verify; omit to only cross-check the live media tree")
	dsn := fs.String("dsn", "", "PostgreSQL DSN for the media cross-check (default: read from .env)")
	dataDir := fs.String("data-dir", "", "media tree root holding blobs/ (default: derived from .env)")
	repoDir := fs.String("repo-dir", ".", "deployment root used to find .env")
	pgTools := fs.String("pg-tools", toolsLocal, "where psql lives: local or docker")
	pgContainer := fs.String("pg-container", "", "container providing psql in docker mode")
	pgBinDir := fs.String("pg-bin-dir", "", "directory holding psql in local mode")
	checkDigests := fs.Bool("check-digests", false, "also hash every media file (slow on large instances)")
	checkArchives := fs.Bool("check-archives", true, "decompress every artifact and walk tar members")
	verbose := fs.Bool("verbose", false, "list every missing, orphan and corrupt object")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *from != "" {
		if err := verifyBundle(*from, *checkArchives); err != nil {
			return err
		}
	}

	// Fall back to the deployment config so a live check needs no arguments.
	if *dsn == "" || *dataDir == "" {
		if cfg, err := config.Load(); err == nil {
			if *dsn == "" {
				*dsn = cfg.PostgresDSN
			}
			if *dataDir == "" {
				*dataDir = dataRootFromBlobDir(*repoDir, cfg.BlobDir)
			}
		}
	}
	if *dsn == "" || *dataDir == "" {
		if *from == "" {
			return errors.New("pass -from for a bundle check, or -dsn and -data-dir for a live media check")
		}
		fmt.Println("media cross-check skipped; pass -dsn and -data-dir to compare file_blobs against disk")
		return nil
	}

	runner := cmdRunner{mode: *pgTools, container: *pgContainer, binaryDir: *pgBinDir}
	if err := runner.validate("pg"); err != nil {
		return err
	}
	endpoint, err := parseDSN(*dsn)
	if err != nil {
		return err
	}
	client := pgClient{runner: runner, endpoint: endpoint}
	index, err := checkBlobs(client, filepath.Join(*dataDir, "blobs"), *checkDigests)
	if err != nil {
		return err
	}
	fmt.Printf("media: %s\n", index)
	if !index.Healthy() {
		return fmt.Errorf("media tree is inconsistent with the database")
	}
	if *verbose {
		for _, key := range index.Missing {
			fmt.Println("  missing:", key)
		}
		for _, key := range index.Corrupt {
			fmt.Println("  corrupt:", key)
		}
	}
	if len(index.Orphan) > 0 {
		fmt.Printf("  %d orphan file(s) are not referenced by any row; harmless, but they are dead weight\n", len(index.Orphan))
	}
	return nil
}

// verifyBundle re-hashes a bundle and confirms every artifact is readable.
func verifyBundle(dir string, checkArchives bool) error {
	manifest, err := ReadManifest(dir)
	if err != nil {
		return err
	}
	fmt.Printf("bundle %s created %s by %s\n", dir, manifest.CreatedAt.Format("2006-01-02T15:04:05Z"), manifest.Tool)
	fmt.Printf("source host %s commit %s frozen=%t\n", manifest.Host, manifest.Commit, manifest.Frozen)

	recorded, err := ReadChecksums(filepath.Join(dir, checksumsName))
	if err != nil {
		return err
	}
	if err := VerifyFiles(dir, recorded); err != nil {
		return err
	}
	fmt.Printf("checksums ok: %d artifacts\n", len(recorded))
	if checkArchives {
		fmt.Println("archive contents:")
		if err := VerifyArchives(dir, recorded); err != nil {
			return err
		}
	}

	for _, component := range manifest.Components {
		var rows []string
		for _, table := range component.Tables {
			rows = append(rows, fmt.Sprintf("%s=%d", table.Table, table.Rows))
		}
		fmt.Printf("%s: database=%s postgres=%s schema=%s tables: %s\n",
			component.Name, component.Database, component.PGVersion, component.schema(), strings.Join(rows, " "))
		for _, note := range component.Notes {
			fmt.Printf("  note: %s\n", note)
		}
	}
	return nil
}

func (r cmdRunner) validate(kind string) error {
	switch r.mode {
	case toolsLocal:
		return nil
	case toolsDocker:
		if r.container == "" {
			return fmt.Errorf("-%s-tools docker needs -%s-container", kind, kind)
		}
		return nil
	default:
		return fmt.Errorf("unknown -%s-tools mode %q, want local or docker", kind, r.mode)
	}
}

func splitList(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

// bundleWriter owns the bundle directory and the artifacts written into it.
type bundleWriter struct {
	dir string
}

func prepareBundle(dir string) (*bundleWriter, error) {
	if dir == "" {
		return nil, errors.New("no bundle directory")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create %s: %w", dir, err)
	}
	return &bundleWriter{dir: dir}, nil
}

// artifact is one file being written, with its running digest.
type artifact struct {
	rel  string
	path string
	file *os.File
	hash *hashingWriter
}

func (b *bundleWriter) newArtifact(rel string) (*artifact, error) {
	path := filepath.Join(b.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	hash, err := newHashingWriter(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &artifact{rel: rel, path: path, file: f, hash: hash}, nil
}

func (a *artifact) Writer() io.Writer { return a.hash }

// close flushes the file and freezes its size and digest.
func (a *artifact) close() error {
	if err := a.file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", a.path, err)
	}
	return nil
}

func (a *artifact) size() int64 { return a.hash.Size() }

func (a *artifact) sum() string { return a.hash.Sum() }

// writeFiltered streams a pg_dump style producer through the portability filter
// and a zstd encoder into the artifact. Nothing is buffered in memory: a dump of
// a large instance is hundreds of megabytes.
func writeFiltered(art *artifact, level int, produce func(io.Writer) error) error {
	enc, err := zstd.NewWriter(art.Writer(),
		zstd.WithEncoderLevel(zstdLevel(level)),
		zstd.WithEncoderConcurrency(2))
	if err != nil {
		return fmt.Errorf("zstd encoder: %w", err)
	}
	pipeR, pipeW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		produceErr := produce(pipeW)
		_ = pipeW.CloseWithError(produceErr)
		errCh <- produceErr
	}()
	filterErr := portabilityFilter(pipeR, enc)
	// Closing the pipe unblocks the producer if the filter failed early.
	_ = pipeR.CloseWithError(filterErr)
	produceErr := <-errCh
	if filterErr != nil {
		_ = enc.Close()
		return fmt.Errorf("filter %s: %w", art.rel, filterErr)
	}
	if produceErr != nil {
		_ = enc.Close()
		return produceErr
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close zstd for %s: %w", art.rel, err)
	}
	return nil
}

func captureRedis(bundle *bundleWriter, manifest *Manifest, c component, runner cmdRunner, addrOverride string, level int) error {
	cfg, err := redisConfig(addrOverride)
	if err != nil {
		return err
	}
	client := redisClient{runner: runner, addr: cfg.addr, password: cfg.password, db: cfg.db}
	path, cleanup, err := client.snapshot()
	if err != nil {
		return fmt.Errorf("redis snapshot: %w", err)
	}
	defer cleanup()

	art, err := bundle.newArtifact(artifactPath(c.name, "redis.rdb.zst"))
	if err != nil {
		return err
	}
	if err := compressFile(path, art.Writer(), level); err != nil {
		art.file.Close()
		return fmt.Errorf("compress redis snapshot: %w", err)
	}
	if err := art.close(); err != nil {
		return err
	}
	manifest.AddArtifact(art.rel, art.size(), art.sum())
	fmt.Printf("  redis.rdb.zst %s\n", humanBytes(art.size()))
	return nil
}

type redisSettings struct {
	addr     string
	password string
	db       int
}

// redisConfig reads Redis connection details from the loaded config so the tool
// uses the same instance as the server.
func redisConfig(addrOverride string) (redisSettings, error) {
	if addrOverride != "" {
		return redisSettings{addr: addrOverride}, nil
	}
	cfg, err := config.Load()
	if err != nil {
		return redisSettings{}, fmt.Errorf("load telesrv config: %w (pass -redis-addr)", err)
	}
	return redisSettings{addr: cfg.RedisAddr, password: cfg.RedisPassword, db: cfg.RedisDB}, nil
}

func captureData(bundle *bundleWriter, manifest *Manifest, c component, level int) error {
	if _, err := os.Stat(c.dataDir); err != nil {
		return fmt.Errorf("data directory %s: %w", c.dataDir, err)
	}
	collector := &itemCollector{}
	if err := collector.addTree(c.dataDir, "data", skipTransientData); err != nil {
		return err
	}
	art, err := bundle.newArtifact(artifactPath(c.name, "data.tar.zst"))
	if err != nil {
		return err
	}
	if err := writeArchive(collector.items, art.Writer(), level); err != nil {
		art.file.Close()
		return fmt.Errorf("archive %s: %w", c.dataDir, err)
	}
	if err := art.close(); err != nil {
		return err
	}
	manifest.AddArtifact(art.rel, art.size(), art.sum())
	fmt.Printf("  data.tar.zst %s (%d files)\n", humanBytes(art.size()), len(collector.items))
	return nil
}

func captureConfig(bundle *bundleWriter, manifest *Manifest, c component) error {
	collector := &itemCollector{}
	collector.addDir("config")
	for _, source := range c.files {
		if _, err := os.Stat(source.path); err != nil {
			if source.required {
				return fmt.Errorf("required file %s: %w", source.path, err)
			}
			continue
		}
		if source.dir {
			if err := collector.addTree(source.path, "config/"+source.name, nil); err != nil {
				return err
			}
			continue
		}
		collector.addFile(source.path, "config/"+source.name)
	}
	if len(collector.items) <= 1 {
		return nil
	}
	art, err := bundle.newArtifact(artifactPath(c.name, "config.tar.zst"))
	if err != nil {
		return err
	}
	if err := writeArchive(collector.items, art.Writer(), 3); err != nil {
		art.file.Close()
		return fmt.Errorf("archive configuration: %w", err)
	}
	if err := art.close(); err != nil {
		return err
	}
	manifest.AddArtifact(art.rel, art.size(), art.sum())
	fmt.Printf("  config.tar.zst %s (%d files)\n", humanBytes(art.size()), len(collector.items)-1)
	return nil
}

// transientDataDirs are spool directories inside the data tree that hold
// in-flight work rather than durable state: unfinished multipart upload chunks
// and the staging area used while an object is promoted to the blob store. An
// interrupted upload cannot resume on another host, and the GC workers would
// delete these files anyway, so archiving them only adds noise.
var transientDataDirs = []string{
	"blobs/upload_parts",
	"blob-staging",
}

func skipTransientData(rel string) bool {
	for _, dir := range transientDataDirs {
		if rel == dir || strings.HasPrefix(rel, dir+"/") {
			return true
		}
	}
	return false
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for size := n / unit; size >= unit; size /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGT"[exp])
}
