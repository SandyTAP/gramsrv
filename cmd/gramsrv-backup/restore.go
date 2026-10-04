package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// runRestore replays a bundle onto this host. It is deliberately conservative:
// artifacts are verified before anything is written, the schema restore runs in
// a single transaction, and existing files are only replaced with -force.
func runRestore(args []string) error {
	fs := newFlagSet("restore")
	from := fs.String("from", "", "bundle directory to restore (required)")
	repoDir := fs.String("repo-dir", ".", "deployment root that receives .env and data/")
	var components componentFlags
	components.register(fs)
	dsn := fs.String("dsn", "", "PostgreSQL DSN to restore into (default: TELESRV_POSTGRES_DSN, then the loaded .env)")
	pgTools := fs.String("pg-tools", toolsLocal, "where pg_dump/psql live: local or docker")
	pgContainer := fs.String("pg-container", "", "container providing pg_dump/psql in docker mode")
	pgBinDir := fs.String("pg-bin-dir", "", "directory holding pg_dump/psql in local mode")
	restoreGlobals := fs.Bool("restore-globals", true, "replay globals.sql so roles and passwords match the bundle")
	createDatabase := fs.Bool("create-database", true, "create the target database when missing")
	withData := fs.Bool("with-data", true, "restore the data/ tree")
	withConfig := fs.Bool("with-config", true, "restore .env and compose files")
	force := fs.Bool("force", false, "overwrite existing files in the data tree")
	dryRun := fs.Bool("dry-run", false, "report the plan without writing anything")
	skipVerify := fs.Bool("skip-verify", false, "do not re-hash artifacts first (not recommended)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *from == "" {
		return errors.New("-from is required")
	}

	manifest, err := ReadManifest(*from)
	if err != nil {
		return err
	}
	selected := selectComponents(manifest, components.resolve())
	if len(selected) == 0 {
		return fmt.Errorf("bundle has no component to restore")
	}

	if !*skipVerify {
		recorded, err := ReadChecksums(filepath.Join(*from, checksumsName))
		if err != nil {
			return err
		}
		if err := VerifyFiles(*from, recorded); err != nil {
			return err
		}
		fmt.Printf("verified %d artifacts\n", len(recorded))
	}

	runner := cmdRunner{mode: *pgTools, container: *pgContainer, binaryDir: *pgBinDir}
	if err := runner.validate("pg"); err != nil {
		return err
	}

	for _, meta := range selected {
		c, err := componentFromManifest(meta, *repoDir, *dsn)
		if err != nil {
			return err
		}
		fmt.Printf("restoring %s into database %s\n", c.name, c.endpoint.Database)

		if *dryRun {
			printRestorePlan(c, meta)
			continue
		}

		client := c.client(runner)

		if c.name == componentServer && *restoreGlobals {
			if artifact, ok := manifest.Artifact(artifactPath(c.name, "globals.sql.zst")); ok {
				if err := withArtifact(*from, artifact, func(r io.Reader) error {
					return client.applyGlobals(r)
				}); err != nil {
					return err
				}
				fmt.Println("  globals.sql replayed")
			}
		}

		if *createDatabase {
			if err := client.ensureDatabase(c.endpoint.Database); err != nil {
				return err
			}
		}

		target, err := client.serverVersion()
		if err != nil {
			return err
		}
		sourceMajor, err := majorVersion(meta.PGVersion)
		if err != nil {
			return fmt.Errorf("%s: unreadable source version %q", c.name, meta.PGVersion)
		}
		targetMajor, err := majorVersion(target)
		if err != nil {
			return err
		}
		// A plain SQL dump from a newer server can use syntax the older one does
		// not understand. Failing here is far cheaper than debugging a partial
		// restore later.
		if targetMajor < sourceMajor {
			return fmt.Errorf(
				"%s: dump is from PostgreSQL %d but this server is %s; restore into the same or a newer major version",
				c.name, sourceMajor, target)
		}

		schemaArtifact, ok := manifest.Artifact(artifactPath(c.name, "schema.sql.zst"))
		if !ok {
			return fmt.Errorf("bundle has no %s", artifactPath(c.name, "schema.sql.zst"))
		}
		if err := withArtifact(*from, schemaArtifact, func(r io.Reader) error {
			return client.applySchema(r, c.endpoint.Database)
		}); err != nil {
			return err
		}
		fmt.Println("  schema restored")

		if *withData && c.dataDir != "" {
			if artifact, ok := manifest.Artifact(artifactPath(c.name, "data.tar.zst")); ok {
				written, err := extractBundleFile(*from, artifact, *repoDir, *force)
				if err != nil {
					return err
				}
				fmt.Printf("  data tree restored: %d files under %s\n", written, c.dataDir)
			}
		}
		if *withConfig {
			if artifact, ok := manifest.Artifact(artifactPath(c.name, "config.tar.zst")); ok {
				written, err := extractBundleFile(*from, artifact, *repoDir, *force)
				if err != nil {
					return err
				}
				fmt.Printf("  configuration restored: %d files under %s\n", written, *repoDir)
			}
		}
	}

	if *dryRun {
		fmt.Println("dry run: nothing was written")
		return nil
	}
	fmt.Println("restore finished; verify before serving traffic:")
	fmt.Printf("  gramsrv-backup verify --from %s -dsn <dsn> -data-dir %s/data\n", *from, *repoDir)
	return nil
}

// selectComponents resolves the requested component names against the bundle.
func selectComponents(manifest *Manifest, names []string) []Component {
	if len(names) == 0 {
		return manifest.Components
	}
	var out []Component
	for _, meta := range manifest.Components {
		for _, name := range names {
			if meta.Name == name {
				out = append(out, meta)
				break
			}
		}
	}
	return out
}

// componentFromManifest rebuilds a component for restore. The DSN comes from the
// operator, not from the bundle: a bundle is frequently restored onto a host
// whose database has a different address.
func componentFromManifest(meta Component, repoDir, dsnOverride string) (component, error) {
	opts := resolveOptions{repoDir: repoDir, dsn: dsnOverride, dsnOverride: dsnOverride != ""}
	switch meta.Name {
	case componentServer:
		c, err := serverComponent(opts)
		if err != nil {
			return component{}, err
		}
		if dsnOverride == "" {
			// Fall back to the database name the bundle came from.
			endpoint, err := parseDSN(c.endpoint.mustDSN())
			if err != nil {
				return component{}, err
			}
			endpoint.Database = meta.Database
			c.endpoint = endpoint
		}
		return c, nil
	case componentGrammystore:
		return grammystoreComponent(opts)
	default:
		return component{}, fmt.Errorf("bundle contains unknown component %q", meta.Name)
	}
}

// mustDSN renders the endpoint as a DSN. Used only where a round trip through
// parseDSN is more convenient than threading the struct around.
func (e pgEndpoint) mustDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", e.User, e.Password, e.Host, e.Port, e.Database)
}

func printRestorePlan(c component, meta Component) {
	fmt.Printf("  would restore database %s (schema %s, postgres %s)\n", c.endpoint.Database, meta.schema(), meta.PGVersion)
	if c.dataDir != "" {
		fmt.Printf("  would extract the data tree into %s\n", c.dataDir)
	}
	for _, source := range c.files {
		fmt.Printf("  would place %s\n", filepath.Join(c.dataDir, "..", source.name))
	}
}

// withArtifact decompresses an artifact and hands it to fn.
func withArtifact(dir string, a Artifact, fn func(io.Reader) error) error {
	path := filepath.Join(dir, filepath.FromSlash(a.Path))
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", a.Path, err)
	}
	defer f.Close()
	reader, err := decompressReader(f)
	if err != nil {
		return fmt.Errorf("%s: %w", a.Path, err)
	}
	defer reader.Close()
	if err := fn(reader); err != nil {
		return fmt.Errorf("%s: %w", a.Path, err)
	}
	return nil
}

// extractBundleFile unpacks a tar artifact into dest.
func extractBundleFile(dir string, a Artifact, dest string, force bool) (int, error) {
	path := filepath.Join(dir, filepath.FromSlash(a.Path))
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", a.Path, err)
	}
	defer f.Close()
	written, err := extractArchive(f, dest, force)
	if err != nil {
		return len(written), fmt.Errorf("%s: %w", a.Path, err)
	}
	return len(written), nil
}
