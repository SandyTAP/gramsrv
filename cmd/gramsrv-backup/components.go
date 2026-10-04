package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"telesrv/internal/config"
)

const (
	componentServer      = "server"
	componentGrammystore = "grammystore"
)

// serverTables are the tables whose exact row counts make a migration auditable
// at a glance: identities, sessions, media metadata and the Stars ledger.
var serverTables = []string{
	"users",
	"auth_keys",
	"channels",
	"dialogs",
	"documents",
	"private_messages",
	"message_boxes",
	"file_blobs",
	"stars_transactions",
	"schema_migrations",
}

var grammystoreTables = []string{
	"users",
	"sales",
	"numbers",
	"spin_awards",
	"promos",
	"settings",
	"schema_migrations",
}

// component is everything the tool needs to know about one captured subsystem.
type component struct {
	name     string
	endpoint pgEndpoint
	// dataDir is the media and key tree, empty when the component has none.
	dataDir string
	// blobDir is the content addressed media store inside dataDir.
	blobDir  string
	files    []configSource
	tables   []string
	notes    []string
	blobsRef bool
}

type configSource struct {
	path string
	name string
	// dir archives a whole directory tree instead of a single file.
	dir bool
	// required turns a missing path into a hard error. Secrets are required:
	// a bundle without .env cannot bring the instance back with the same
	// credentials.
	required bool
}

func (c component) client(runner cmdRunner) pgClient {
	return pgClient{runner: runner, endpoint: c.endpoint}
}

// artifactPath returns the bundle-relative path of a component artifact.
func artifactPath(component, name string) string {
	return component + "/" + name
}

// resolveComponents turns the --component selection into concrete components.
func resolveComponents(names []string, opts resolveOptions) ([]component, error) {
	var out []component
	for _, name := range names {
		switch name {
		case componentServer:
			c, err := serverComponent(opts)
			if err != nil {
				return nil, err
			}
			out = append(out, c)
		case componentGrammystore:
			c, err := grammystoreComponent(opts)
			if err != nil {
				return nil, err
			}
			out = append(out, c)
		default:
			return nil, fmt.Errorf("unknown component %q, want %s or %s", name, componentServer, componentGrammystore)
		}
	}
	return out, nil
}

type resolveOptions struct {
	repoDir string
	dsn     string
	dataDir string
	// dsnOverride marks an explicit --dsn so config.Load is not consulted.
	dsnOverride bool
}

func serverComponent(opts resolveOptions) (component, error) {
	repoDir := opts.repoDir
	envPath := filepath.Join(repoDir, ".env")

	dsn := opts.dsn
	blobDir := ""
	if !opts.dsnOverride {
		cfg, err := config.Load()
		if err != nil {
			return component{}, fmt.Errorf(
				"load telesrv config: %w (pass --dsn and --data-dir to back up a deployment whose config does not load)",
				err)
		}
		if dsn == "" {
			dsn = cfg.PostgresDSN
		}
		blobDir = cfg.BlobDir
	}
	if dsn == "" {
		return component{}, fmt.Errorf("no PostgreSQL DSN for %s; pass --dsn", componentServer)
	}
	endpoint, err := parseDSN(dsn)
	if err != nil {
		return component{}, err
	}

	dataDir := opts.dataDir
	if dataDir == "" {
		dataDir = dataRootFromBlobDir(repoDir, blobDir)
	}
	if dataDir != "" {
		blobDir = filepath.Join(dataDir, "blobs")
	}

	c := component{
		name:     componentServer,
		endpoint: endpoint,
		dataDir:  dataDir,
		blobDir:  blobDir,
		tables:   serverTables,
		files: []configSource{
			{path: envPath, name: "env/telesrv.env", required: true},
			{path: "/etc/systemd/system/gramsrv.service", name: "systemd/gramsrv.service"},
			{path: "/etc/systemd/system/gramsrv-admin.service", name: "systemd/gramsrv-admin.service"},
			{path: filepath.Join(repoDir, "deploy", "docker-compose.yml"), name: "repo/deploy/docker-compose.yml"},
			{path: filepath.Join(repoDir, "deploy", "docker-compose_test.go"), name: "repo/deploy/docker-compose_test.go"},
			{path: filepath.Join(repoDir, "deploy", "postgres-init"), name: "repo/deploy/postgres-init", dir: true},
			{path: filepath.Join(repoDir, "deploy", "docker", "compose.yaml"), name: "repo/deploy/docker/compose.yaml"},
			{path: filepath.Join(repoDir, "deploy", "docker", "docker-entrypoint.sh"), name: "repo/deploy/docker/docker-entrypoint.sh"},
			{path: filepath.Join(repoDir, ".env.example"), name: "repo/.env.example"},
		},
	}
	if blobDir != "" {
		c.blobsRef = true
	}
	return c, nil
}

func grammystoreComponent(opts resolveOptions) (component, error) {
	dir := filepath.Join(opts.repoDir, "cmd", "bots", "grammystore")
	envPath := filepath.Join(dir, ".env")
	env, err := loadEnvFile(envPath)
	if err != nil {
		return component{}, err
	}
	db := env["POSTGRES_DB"]
	user := env["POSTGRES_USER"]
	if db == "" || user == "" {
		return component{}, fmt.Errorf("%s must define POSTGRES_DB and POSTGRES_USER", envPath)
	}
	host := env["POSTGRES_HOST"]
	if host == "" {
		host = "127.0.0.1"
	}
	port := env["POSTGRES_PORT"]
	if port == "" {
		port = "5432"
	}
	return component{
		name: componentGrammystore,
		endpoint: pgEndpoint{
			Host:     host,
			Port:     port,
			User:     user,
			Password: env["POSTGRES_PASSWORD"],
			Database: db,
		},
		tables: grammystoreTables,
		files: []configSource{
			{path: envPath, name: "env/grammystore.env", required: true},
			{path: filepath.Join(dir, "docker-compose.yml"), name: "repo/docker-compose.yml"},
			{path: filepath.Join(dir, "docker-compose-dev.yml"), name: "repo/docker-compose-dev.yml"},
			{path: filepath.Join(dir, "Dockerfile"), name: "repo/Dockerfile"},
			{path: filepath.Join(dir, "package.json"), name: "repo/package.json"},
			{path: filepath.Join(dir, "package-lock.json"), name: "repo/package-lock.json"},
		},
	}, nil
}

// dataRootFromBlobDir turns TELESRV_BLOB_DIR back into the tree that also holds
// the RSA key, the login signing keys and the update catalog.
func dataRootFromBlobDir(repoDir, blobDir string) string {
	blobDir = strings.TrimSpace(blobDir)
	if blobDir == "" {
		return filepath.Join(repoDir, "data")
	}
	if !filepath.IsAbs(blobDir) {
		blobDir = filepath.Join(repoDir, blobDir)
	}
	blobDir = filepath.Clean(blobDir)
	if filepath.Base(blobDir) == "blobs" {
		return filepath.Dir(blobDir)
	}
	return blobDir
}

// loadEnvFile parses a KEY=VALUE environment file. Missing files are an error;
// callers decide whether that is fatal.
func loadEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer f.Close()
	out := map[string]string{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(scanner.Text()), "export "))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		out[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return out, nil
}

// collectMetadata records the facts an operator needs to trust a bundle: server
// version, schema version and exact row counts. Every statement is read only;
// this tool never migrates a database.
func collectMetadata(client pgClient, c component) (Component, error) {
	meta := Component{Name: c.name, Database: c.endpoint.Database}
	version, err := client.serverVersion()
	if err != nil {
		return meta, err
	}
	meta.PGVersion = version

	if rows, err := client.query(c.endpoint.Database, "SELECT version, dirty FROM schema_migrations"); err == nil && len(rows) > 0 {
		version, dirty, _ := strings.Cut(rows[0], "|")
		if n, convErr := strconv.ParseInt(strings.TrimSpace(version), 10, 64); convErr == nil {
			meta.SchemaVersion = n
		}
		meta.SchemaDirty = strings.EqualFold(strings.TrimSpace(dirty), "t")
	} else if rows, err := client.query(c.endpoint.Database, "SELECT max(version) FROM schema_migrations"); err == nil && len(rows) > 0 {
		// grammystore applies numbered .sql files and records their names.
		meta.SchemaLabel = strings.TrimSpace(rows[0])
		if n, convErr := strconv.ParseInt(meta.SchemaLabel, 10, 64); convErr == nil {
			meta.SchemaVersion = n
		}
	}
	if meta.SchemaDirty {
		meta.Notes = append(meta.Notes, "schema_migrations is dirty: the source database needs golang-migrate force before this dump is trustworthy")
	}

	existing := map[string]bool{}
	if rows, err := client.query(c.endpoint.Database, existingTablesQuery(c.tables)); err == nil {
		for _, row := range rows {
			existing[strings.TrimSpace(row)] = true
		}
	}
	for _, table := range c.tables {
		if !existing[table] {
			continue
		}
		rows, err := client.query(c.endpoint.Database, "SELECT count(*) FROM "+quoteIdentifier(table))
		if err != nil || len(rows) == 0 {
			continue
		}
		n, convErr := strconv.ParseInt(strings.TrimSpace(rows[0]), 10, 64)
		if convErr != nil {
			continue
		}
		meta.Tables = append(meta.Tables, TableCount{Table: table, Rows: n})
	}
	if c.blobsRef && existing["file_blobs"] {
		if rows, err := client.query(c.endpoint.Database, "SELECT count(DISTINCT object_key) FROM file_blobs WHERE backend = 'localfs'"); err == nil && len(rows) > 0 {
			if n, convErr := strconv.ParseInt(strings.TrimSpace(rows[0]), 10, 64); convErr == nil {
				meta.BlobObjects = n
			}
		}
	}
	meta.Notes = append(meta.Notes, c.notes...)
	return meta, nil
}

func existingTablesQuery(tables []string) string {
	quoted := make([]string, 0, len(tables))
	for _, table := range tables {
		quoted = append(quoted, quoteLiteral(table))
	}
	sort.Strings(quoted)
	return "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace" +
		" WHERE n.nspname = 'public' AND c.relname IN (" + strings.Join(quoted, ", ") + ")"
}
