package main

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	toolsLocal  = "local"
	toolsDocker = "docker"
)

// pgEndpoint is a parsed libpq connection target. The password is carried in
// memory only and is always handed to child processes through the environment,
// never through argv, so it never shows up in ps output.
type pgEndpoint struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

// maintenanceDatabase is the database used for CREATE DATABASE and role dumps.
func (e pgEndpoint) maintenanceDatabase() string {
	return "postgres"
}

func (e pgEndpoint) args() []string {
	args := make([]string, 0, 6)
	if e.Host != "" {
		args = append(args, "--host="+e.Host)
	}
	if e.Port != "" {
		args = append(args, "--port="+e.Port)
	}
	if e.User != "" {
		args = append(args, "--username="+e.User)
	}
	return args
}

func (e pgEndpoint) env() []string {
	if e.Password == "" {
		return nil
	}
	return []string{"PGPASSWORD=" + e.Password}
}

// parseDSN accepts both the URI form used by TELESRV_POSTGRES_DSN and the
// libpq keyword/value form.
func parseDSN(dsn string) (pgEndpoint, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return pgEndpoint{}, fmt.Errorf("empty PostgreSQL DSN")
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return pgEndpoint{}, fmt.Errorf("parse DSN: %w", err)
		}
		ep := pgEndpoint{Host: u.Hostname(), Port: u.Port(), Database: strings.TrimPrefix(u.Path, "/")}
		if u.User != nil {
			ep.User = u.User.Username()
			if pw, ok := u.User.Password(); ok {
				ep.Password = pw
			}
		}
		if ep.Port == "" {
			ep.Port = "5432"
		}
		if ep.User == "" {
			return ep, fmt.Errorf("DSN has no user; add one or rely on the local peer")
		}
		return ep, nil
	}
	ep := pgEndpoint{}
	for _, field := range strings.Fields(dsn) {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "host":
			ep.Host = value
		case "port":
			ep.Port = value
		case "user":
			ep.User = value
		case "password":
			ep.Password = value
		case "dbname":
			ep.Database = value
		}
	}
	if ep.Port == "" {
		ep.Port = "5432"
	}
	if ep.Database == "" {
		return ep, fmt.Errorf("DSN has no dbname")
	}
	return ep, nil
}

// quoteIdentifier quotes a SQL identifier. Only ever used with names this tool
// itself produced or that the operator passed on the command line.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// cmdRunner executes PostgreSQL and Redis client binaries either from PATH or
// inside a container through docker exec.
type cmdRunner struct {
	mode      string
	container string
	binaryDir string
}

func (r cmdRunner) describe() string {
	if r.mode == toolsDocker {
		return "docker exec " + r.container
	}
	return r.binaryDir
}

// run executes name with args, wiring stdin/stdout/stderr and the extra
// environment. In docker mode the environment is passed with --env-file so no
// secret reaches the host process table.
func (r cmdRunner) run(
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	env []string,
	name string,
	args ...string,
) error {
	if r.mode == toolsDocker {
		return r.runDocker(stdin, stdout, stderr, env, name, args...)
	}
	if r.binaryDir != "" {
		name = filepath.Join(r.binaryDir, name)
	}
	cmd := exec.Command(name, args...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = append(os.Environ(), env...)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

func (r cmdRunner) runDocker(
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	env []string,
	name string,
	args ...string,
) error {
	if r.container == "" {
		return fmt.Errorf("docker mode needs a container name (--pg-container / --redis-container)")
	}
	full := []string{"exec", "--interactive"}
	if len(env) > 0 {
		file, err := writeEnvFile(env)
		if err != nil {
			return err
		}
		defer os.Remove(file)
		full = append(full, "--env-file", file)
	}
	full = append(full, r.container, name)
	full = append(full, args...)
	cmd := exec.Command("docker", full...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker %s: %w", strings.Join(full[1:], " "), err)
	}
	return nil
}

// writeEnvFile writes environment entries to a private temporary file.
func writeEnvFile(env []string) (string, error) {
	f, err := os.CreateTemp("", "gramsrv-backup-env-*")
	if err != nil {
		return "", fmt.Errorf("create env file: %w", err)
	}
	defer f.Close()
	lines := append([]string(nil), env...)
	sort.Strings(lines)
	for _, line := range lines {
		if _, err := f.WriteString(line + "\n"); err != nil {
			os.Remove(f.Name())
			return "", fmt.Errorf("write env file: %w", err)
		}
	}
	if err := f.Chmod(0o600); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("chmod env file: %w", err)
	}
	return f.Name(), nil
}

// portabilityFilter removes statements that tie a plain SQL dump to the
// pg_dump major version that produced it:
//
//   - \restrict and \unrestrict are psql meta-commands introduced in PostgreSQL
//     17.5 as a mitigation for CVE-2025-8714. Older psql builds reject them and
//     non-psql tools cannot parse them at all.
//   - transaction_timeout only exists on PostgreSQL 17 and newer, so restoring
//     the dump on an older server fails on the SET.
//
// Everything else is left byte for byte intact.
func portabilityFilter(r io.Reader, w io.Writer) error {
	br := bufio.NewReaderSize(r, 1<<20)
	bw := bufio.NewWriterSize(w, 1<<20)
	for {
		line, err := br.ReadString('\n')
		if line != "" && !portabilityFiltered(line) {
			if _, werr := bw.WriteString(line); werr != nil {
				return werr
			}
		}
		if err != nil {
			if err == io.EOF {
				return bw.Flush()
			}
			return err
		}
	}
}

func portabilityFiltered(line string) bool {
	trimmed := strings.TrimRight(line, "\r\n")
	if strings.HasPrefix(trimmed, `\restrict `) || strings.HasPrefix(trimmed, `\unrestrict `) {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(trimmed), "SET transaction_timeout = 0;")
}

// pgClient bundles the connection target with the runner used to reach it.
type pgClient struct {
	runner   cmdRunner
	endpoint pgEndpoint
}

// dumpArgs returns the flags that keep a dump portable: plain SQL that any
// psql can replay, drop-and-recreate so it can be replayed over an existing
// database, and no ownership or privilege statements so it can land in whatever
// role the target uses. --create is deliberately omitted so the target database
// name is not baked into the artifact.
func dumpArgs(endpoint pgEndpoint) []string {
	args := []string{
		"--format=plain",
		"--clean",
		"--if-exists",
		"--no-owner",
		"--no-privileges",
		"--no-comments",
	}
	return append(args, endpoint.args()...)
}

// writeSchema streams pg_dump output through the portability filter into dst.
func (c pgClient) writeSchema(dst io.Writer) error {
	// stderr is captured so a failed dump explains itself in the error, not in
	// the middle of an otherwise empty artifact.
	var errBuf strings.Builder
	args := append(dumpArgs(c.endpoint), "--dbname="+c.endpoint.Database)
	err := c.runner.run(nil, dst, &errBuf, c.endpoint.env(), "pg_dump", args...)
	if err != nil {
		return fmt.Errorf("pg_dump %s: %w: %s", c.endpoint.Database, err, tail(errBuf.String()))
	}
	return nil
}

// writeGlobals streams pg_dumpall --globals-only, which carries the roles and
// their password hashes so a restored instance accepts the same credentials as
// the .env file it is restored with.
func (c pgClient) writeGlobals(dst io.Writer, noRolePasswords bool) error {
	var errBuf strings.Builder
	// --dbname is deliberately not passed: pg_dumpall 17.x fails with
	// "missing = after <db> in connection info string" when it is combined with
	// --globals-only, and the maintenance database is chosen automatically.
	args := append(c.endpoint.args(), "--globals-only")
	if noRolePasswords {
		args = append(args, "--no-role-passwords")
	}
	err := c.runner.run(nil, dst, &errBuf, c.endpoint.env(), "pg_dumpall", args...)
	if err != nil {
		return fmt.Errorf("pg_dumpall --globals-only: %w: %s", err, tail(errBuf.String()))
	}
	return nil
}

// applyGlobals replays a globals dump. Failures on already existing roles are
// tolerated because the target host may ship its own superuser.
func (c pgClient) applyGlobals(r io.Reader) error {
	var errBuf strings.Builder
	args := append(c.endpoint.args(), "--dbname="+c.endpoint.maintenanceDatabase(), "--quiet")
	err := c.runner.run(r, io.Discard, &errBuf, c.endpoint.env(), "psql", args...)
	if err != nil {
		return fmt.Errorf("psql globals: %w: %s", err, tail(errBuf.String()))
	}
	return nil
}

// ensureDatabase creates the target database when it is missing.
func (c pgClient) ensureDatabase(name string) error {
	exists, err := c.query(c.endpoint.maintenanceDatabase(), "SELECT 1 FROM pg_database WHERE datname = "+quoteLiteral(name))
	if err != nil {
		return err
	}
	if len(exists) > 0 && exists[0] == "1" {
		return nil
	}
	stmt := "CREATE DATABASE " + quoteIdentifier(name)
	if c.endpoint.User != "" {
		stmt += " OWNER " + quoteIdentifier(c.endpoint.User)
	}
	if _, err := c.query(c.endpoint.maintenanceDatabase(), stmt); err != nil {
		return err
	}
	return nil
}

// applySchema replays a plain SQL dump. ON_ERROR_STOP plus --single-transaction
// makes the restore all or nothing: a truncated dump or a version mismatch fails
// loudly instead of leaving a half migrated schema behind.
func (c pgClient) applySchema(r io.Reader, database string) error {
	var errBuf strings.Builder
	args := append(c.endpoint.args(),
		"--dbname="+database,
		"--no-psqlrc",
		"--quiet",
		"--set=ON_ERROR_STOP=1",
		"--single-transaction",
	)
	err := c.runner.run(r, io.Discard, &errBuf, c.endpoint.env(), "psql", args...)
	if err != nil {
		return fmt.Errorf("restore schema into %s: %w: %s", database, err, tail(errBuf.String()))
	}
	return nil
}

// query runs a single statement and returns its rows as strings.
func (c pgClient) query(database, statement string) ([]string, error) {
	var out strings.Builder
	var errBuf strings.Builder
	args := append(c.endpoint.args(),
		"--dbname="+database,
		"--no-psqlrc",
		"--tuples-only",
		"--no-align",
		"--quiet",
		"--set=ON_ERROR_STOP=1",
		"--command="+statement,
	)
	err := c.runner.run(nil, &out, &errBuf, c.endpoint.env(), "psql", args...)
	if err != nil {
		return nil, fmt.Errorf("psql query: %w: %s", err, tail(errBuf.String()))
	}
	var rows []string
	for _, line := range strings.Split(out.String(), "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) != "" {
			rows = append(rows, line)
		}
	}
	return rows, nil
}

// serverVersion reports the major.minor version of the target server.
func (c pgClient) serverVersion() (string, error) {
	rows, err := c.query(c.endpoint.maintenanceDatabase(), "SHOW server_version")
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", fmt.Errorf("psql returned no server_version")
	}
	return rows[0], nil
}

// majorVersion extracts the leading integer of a server version string.
func majorVersion(version string) (int, error) {
	dot := strings.IndexByte(version, '.')
	head := version
	if dot > 0 {
		head = version[:dot]
	}
	return strconv.Atoi(head)
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// tail keeps error output readable when psql or pg_dump is verbose.
func tail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 400 {
		return s
	}
	return "..." + s[len(s)-400:]
}
