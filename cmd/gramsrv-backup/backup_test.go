package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseDSNURI(t *testing.T) {
	endpoint, err := parseDSN("postgres://telesrv:s3cr3t@127.0.0.1:5432/telesrv_main?sslmode=disable")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if endpoint.Host != "127.0.0.1" || endpoint.Port != "5432" {
		t.Errorf("host/port = %s:%s, want 127.0.0.1:5432", endpoint.Host, endpoint.Port)
	}
	if endpoint.User != "telesrv" || endpoint.Password != "s3cr3t" {
		t.Errorf("credentials = %s/%s, want telesrv/s3cr3t", endpoint.User, endpoint.Password)
	}
	if endpoint.Database != "telesrv_main" {
		t.Errorf("database = %s, want telesrv_main", endpoint.Database)
	}
}

// A password containing reserved characters must survive the round trip, since
// generated passwords are hex but hand written ones are not.
func TestParseDSNURIEncodedPassword(t *testing.T) {
	endpoint, err := parseDSN("postgres://user:p%40ss%3Aword@db.internal:6543/app")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if endpoint.Password != "p@ss:word" {
		t.Errorf("password = %q, want %q", endpoint.Password, "p@ss:word")
	}
	if endpoint.Port != "6543" {
		t.Errorf("port = %s, want 6543", endpoint.Port)
	}
}

func TestParseDSNKeywordForm(t *testing.T) {
	endpoint, err := parseDSN("host=127.0.0.1 port=5432 user=telesrv password=pw dbname=telesrv_main")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if endpoint.Database != "telesrv_main" || endpoint.User != "telesrv" {
		t.Errorf("endpoint = %+v", endpoint)
	}
}

func TestParseDSNDefaultsPort(t *testing.T) {
	endpoint, err := parseDSN("postgres://telesrv@127.0.0.1/telesrv_main")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if endpoint.Port != "5432" {
		t.Errorf("port = %s, want the libpq default 5432", endpoint.Port)
	}
}

func TestParseDSNRejectsEmpty(t *testing.T) {
	if _, err := parseDSN("   "); err == nil {
		t.Fatal("parseDSN accepted an empty DSN")
	}
}

// The whole portability contract of the dump: PG17-only constructs must not
// survive into an artifact, and nothing else may change.
func TestPortabilityFilterStripsVersionSpecificStatements(t *testing.T) {
	input := strings.Join([]string{
		"-- PostgreSQL database dump",
		"\\restrict Eta1GZugW4C8YFAJ9KbO53U4CBQZ7OB8JL1HIXnK4",
		"SET statement_timeout = 0;",
		"SET transaction_timeout = 0;",
		"CREATE TABLE public.t (id bigint);",
		"\\unrestrict Eta1GZugW4C8YFAJ9KbO53U4CBQZ7OB8JL1HIXnK4",
		"",
	}, "\n")

	var out bytes.Buffer
	if err := portabilityFilter(strings.NewReader(input), &out); err != nil {
		t.Fatalf("portabilityFilter: %v", err)
	}
	got := out.String()
	for _, unwanted := range []string{"\\restrict", "\\unrestrict", "transaction_timeout"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("output still contains %q:\n%s", unwanted, got)
		}
	}
	for _, wanted := range []string{"SET statement_timeout = 0;", "CREATE TABLE public.t (id bigint);"} {
		if !strings.Contains(got, wanted) {
			t.Errorf("output lost %q:\n%s", wanted, got)
		}
	}
}

func TestPortabilityFilterKeepsFinalLineWithoutNewline(t *testing.T) {
	var out bytes.Buffer
	if err := portabilityFilter(strings.NewReader("SELECT 1;"), &out); err != nil {
		t.Fatalf("portabilityFilter: %v", err)
	}
	if out.String() != "SELECT 1;" {
		t.Errorf("output = %q, want %q", out.String(), "SELECT 1;")
	}
}

func TestPortabilityFilterAcceptsCarriageReturns(t *testing.T) {
	var out bytes.Buffer
	if err := portabilityFilter(strings.NewReader("SET transaction_timeout = 0;\r\nSELECT 1;\r\n"), &out); err != nil {
		t.Fatalf("portabilityFilter: %v", err)
	}
	if strings.Contains(out.String(), "transaction_timeout") {
		t.Errorf("CRLF terminated statement survived: %q", out.String())
	}
}

func TestDumpArgsArePortable(t *testing.T) {
	args := strings.Join(dumpArgs(pgEndpoint{Host: "h", Port: "5432", User: "u"}), " ")
	for _, wanted := range []string{"--format=plain", "--clean", "--if-exists", "--no-owner", "--no-privileges"} {
		if !strings.Contains(args, wanted) {
			t.Errorf("dump args missing %q: %s", wanted, args)
		}
	}
	// --create would bake the database name into the artifact and block
	// restoring into telesrv_v2 or a scratch database.
	if strings.Contains(args, "--create") {
		t.Errorf("dump args must not use --create: %s", args)
	}
}

func TestQuoteIdentifier(t *testing.T) {
	if got := quoteIdentifier("telesrv_main"); got != `"telesrv_main"` {
		t.Errorf("quoteIdentifier = %s", got)
	}
	if got := quoteIdentifier(`we"ird`); got != `"we""ird"` {
		t.Errorf("quoteIdentifier = %s, want doubled inner quote", got)
	}
}

func TestQuoteLiteral(t *testing.T) {
	if got := quoteLiteral("o'brien"); got != "'o''brien'" {
		t.Errorf("quoteLiteral = %s", got)
	}
}

func TestMajorVersion(t *testing.T) {
	cases := map[string]int{"17.11": 17, "16.15": 16, "18": 18}
	for input, want := range cases {
		got, err := majorVersion(input)
		if err != nil {
			t.Fatalf("majorVersion(%q): %v", input, err)
		}
		if got != want {
			t.Errorf("majorVersion(%q) = %d, want %d", input, got, want)
		}
	}
	if _, err := majorVersion("unknown"); err == nil {
		t.Error("majorVersion accepted a non-numeric version")
	}
}

func TestBlobObjectPathLayout(t *testing.T) {
	key := strings.Repeat("ab", 32)
	got := blobObjectPath("/srv/data/blobs", key)
	want := filepath.Join("/srv/data/blobs", "ab", "ab", key)
	if got != want {
		t.Errorf("blobObjectPath = %s, want %s", got, want)
	}
}

func TestCheckBlobsDetectsMissingAndOrphans(t *testing.T) {
	dir := t.TempDir()
	blobDir := filepath.Join(dir, "blobs")

	present := strings.Repeat("a", 64)
	missing := strings.Repeat("b", 64)
	orphan := strings.Repeat("c", 64)
	for _, key := range []string{present, orphan} {
		path := blobObjectPath(blobDir, key)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	index, err := checkBlobs(stubLister{keys: []string{present, missing}}, blobDir, false)
	if err != nil {
		t.Fatalf("checkBlobs: %v", err)
	}
	if index.Referenced != 2 {
		t.Errorf("Referenced = %d, want 2", index.Referenced)
	}
	if len(index.Missing) != 1 || index.Missing[0] != missing {
		t.Errorf("Missing = %v, want [%s]", index.Missing, missing)
	}
	if len(index.Orphan) != 1 || index.Orphan[0] != orphan {
		t.Errorf("Orphan = %v, want [%s]", index.Orphan, orphan)
	}
	if index.Healthy() {
		t.Error("Healthy() = true with a missing object")
	}
}

func TestCheckBlobsVerifiesDigests(t *testing.T) {
	blobDir := t.TempDir()
	// The object name is its sha256, so a corrupted body must be reported.
	key := sha256Hex([]byte("good"))
	corrupt := sha256Hex([]byte("good"))
	for _, k := range []string{key, corrupt} {
		path := blobObjectPath(blobDir, k)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(blobObjectPath(blobDir, key), []byte("good"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blobObjectPath(blobDir, corrupt), []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}

	index, err := checkBlobs(stubLister{keys: []string{key, corrupt}}, blobDir, true)
	if err != nil {
		t.Fatalf("checkBlobs: %v", err)
	}
	if len(index.Corrupt) != 1 || index.Corrupt[0] != corrupt {
		t.Errorf("Corrupt = %v, want [%s]", index.Corrupt, corrupt)
	}
}

// stubLister answers referencedObjectKeys without a database.
type stubLister struct{ keys []string }

func (s stubLister) referencedObjectKeys() ([]string, error) { return s.keys, nil }

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	manifest, err := newManifest(true)
	if err != nil {
		t.Fatalf("newManifest: %v", err)
	}
	manifest.AddArtifact("server/schema.sql.zst", 12, strings.Repeat("a", 64))
	manifest.AddArtifact("server/data.tar.zst", 34, strings.Repeat("b", 64))
	path := filepath.Join(dir, manifestName)
	if err := manifest.Write(path); err != nil {
		t.Fatalf("Write: %v", err)
	}
	loaded, err := ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if !loaded.Frozen {
		t.Error("Frozen flag lost in round trip")
	}
	if len(loaded.Artifacts) != 2 {
		t.Fatalf("artifacts = %d, want 2", len(loaded.Artifacts))
	}
	if _, ok := loaded.Artifact("server/data.tar.zst"); !ok {
		t.Error("artifact lookup by path failed")
	}
}

func TestReadManifestRejectsForeignBundle(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, manifestName), []byte(`{"tool":"something-else"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadManifest(dir); err == nil {
		t.Fatal("ReadManifest accepted a bundle from another tool")
	}
}

func TestVerifyFilesDetectsTampering(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "payload")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	sum, size, err := hashFile(path)
	if err != nil {
		t.Fatalf("hashFile: %v", err)
	}
	artifacts := []Artifact{{Path: "payload", SHA256: sum, Bytes: size}}
	if err := VerifyFiles(dir, artifacts); err != nil {
		t.Fatalf("VerifyFiles on a good bundle: %v", err)
	}
	if err := os.WriteFile(path, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyFiles(dir, artifacts); err == nil {
		t.Fatal("VerifyFiles accepted a modified artifact")
	}
}

func TestChecksumsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	artifacts := []Artifact{
		{Path: "server/schema.sql.zst", SHA256: strings.Repeat("a", 64), Bytes: 1},
		{Path: "server/data.tar.zst", SHA256: strings.Repeat("b", 64), Bytes: 2},
	}
	if err := WriteChecksums(dir, artifacts); err != nil {
		t.Fatalf("WriteChecksums: %v", err)
	}
	loaded, err := ReadChecksums(filepath.Join(dir, checksumsName))
	if err != nil {
		t.Fatalf("ReadChecksums: %v", err)
	}
	if len(loaded) != 2 || loaded[0].Path != "server/schema.sql.zst" {
		t.Fatalf("loaded = %+v", loaded)
	}
}

func TestReadChecksumsRejectsMalformed(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, checksumsName), []byte("not-a-digest payload\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadChecksums(filepath.Join(dir, checksumsName)); err == nil {
		t.Fatal("ReadChecksums accepted a malformed line")
	}
}

func TestArchiveRoundTripPreservesModes(t *testing.T) {
	src := t.TempDir()
	secret := filepath.Join(src, "server_rsa.pem")
	if err := os.WriteFile(secret, []byte("-----BEGIN RSA PRIVATE KEY-----"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A key directory is 0700 on disk and must not become world readable.
	if err := os.MkdirAll(filepath.Join(src, "telegram-login"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "telegram-login", "signing-keys.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	// An empty directory has no files to imply it.
	if err := os.MkdirAll(filepath.Join(src, "updates", "files"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, "blobs", "aa"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "blobs", "aa", "object"), []byte("body"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("server_rsa.pem", filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}

	collector := &itemCollector{}
	if err := collector.addTree(src, "data", nil); err != nil {
		t.Fatalf("addTree: %v", err)
	}
	var archive bytes.Buffer
	if err := writeArchive(collector.items, &archive, 1); err != nil {
		t.Fatalf("writeArchive: %v", err)
	}

	dest := t.TempDir()
	written, err := extractArchive(bytes.NewReader(archive.Bytes()), dest, false)
	if err != nil {
		t.Fatalf("extractArchive: %v", err)
	}
	// Three regular files; directories and the symlink are restored but are not
	// counted, since only files carry content.
	if len(written) != 3 {
		t.Errorf("wrote %d files, want 3: %v", len(written), written)
	}
	info, err := os.Stat(filepath.Join(dest, "data", "server_rsa.pem"))
	if err != nil {
		t.Fatal(err)
	}
	// Secrets must not become world readable on restore.
	if info.Mode().Perm() != 0o600 {
		t.Errorf("restored file mode = %v, want 0600", info.Mode().Perm())
	}
	loginDir, err := os.Stat(filepath.Join(dest, "data", "telegram-login"))
	if err != nil {
		t.Fatal(err)
	}
	if loginDir.Mode().Perm() != 0o700 {
		t.Errorf("restored directory mode = %v, want 0700", loginDir.Mode().Perm())
	}
	// Without an explicit directory entry an empty directory disappears.
	if _, err := os.Stat(filepath.Join(dest, "data", "updates", "files")); err != nil {
		t.Errorf("empty directory was not preserved: %v", err)
	}
	link, err := os.Readlink(filepath.Join(dest, "data", "link"))
	if err != nil {
		t.Fatalf("symlink not restored: %v", err)
	}
	if link != "server_rsa.pem" {
		t.Errorf("symlink target = %s", link)
	}
}

// A restore must never silently clobber an existing tree; -force is the escape
// hatch and it has to be explicit.
func TestExtractArchiveRefusesToOverwriteWithoutForce(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "file"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	collector := &itemCollector{}
	if err := collector.addTree(src, "data", nil); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := writeArchive(collector.items, &archive, 1); err != nil {
		t.Fatal(err)
	}

	dest := t.TempDir()
	existing := filepath.Join(dest, "data", "file")
	if err := os.MkdirAll(filepath.Dir(existing), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(existing, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := extractArchive(bytes.NewReader(archive.Bytes()), dest, false); err == nil {
		t.Fatal("extractArchive overwrote an existing file without force")
	}
	if _, err := extractArchive(bytes.NewReader(archive.Bytes()), dest, true); err != nil {
		t.Fatalf("extractArchive with force: %v", err)
	}
	body, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "new" {
		t.Errorf("content = %q, want the archived value", body)
	}
}

func TestSafeArchivePathRejectsTraversal(t *testing.T) {
	dest := t.TempDir()
	for _, name := range []string{"../escape", "/etc/passwd", "a/../../escape"} {
		if _, err := safeArchivePath(dest, name); err == nil {
			t.Errorf("safeArchivePath accepted %q", name)
		}
	}
	if _, err := safeArchivePath(dest, "data/blobs/aa/object"); err != nil {
		t.Errorf("safeArchivePath rejected a legitimate path: %v", err)
	}
}

func TestSanitizeModeDropsPrivilegeBits(t *testing.T) {
	// setuid on a regular file, setgid on a directory, and the sticky bit.
	if got := sanitizeMode(0o4644); got != 0o644 {
		t.Errorf("sanitizeMode(0o4644) = %v, want 0644", got)
	}
	if got := sanitizeMode(0o2755); got != 0o755 {
		t.Errorf("sanitizeMode(0o2755) = %v, want 0755", got)
	}
	if got := sanitizeMode(0o1777); got != 0o777 {
		t.Errorf("sanitizeMode(0o1777) = %v, want 0777", got)
	}
}

func TestLoadEnvFileParsesQuotesAndExports(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	content := strings.Join([]string{
		"# comment",
		"",
		"export POSTGRES_USER=grammystore",
		"POSTGRES_PASSWORD=\"p@ss word\"",
		"SINGLE_QUOTED='literal'",
		"MALFORMED_LINE",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	env, err := loadEnvFile(path)
	if err != nil {
		t.Fatalf("loadEnvFile: %v", err)
	}
	if env["POSTGRES_USER"] != "grammystore" {
		t.Errorf("POSTGRES_USER = %q", env["POSTGRES_USER"])
	}
	if env["POSTGRES_PASSWORD"] != "p@ss word" {
		t.Errorf("POSTGRES_PASSWORD = %q", env["POSTGRES_PASSWORD"])
	}
	if env["SINGLE_QUOTED"] != "literal" {
		t.Errorf("SINGLE_QUOTED = %q", env["SINGLE_QUOTED"])
	}
}

func TestDataRootFromBlobDir(t *testing.T) {
	if got := dataRootFromBlobDir("/srv", "data/blobs"); got != filepath.Join("/srv", "data") {
		t.Errorf("dataRootFromBlobDir = %s", got)
	}
	if got := dataRootFromBlobDir("/srv", "/mnt/media/blobs"); got != "/mnt/media" {
		t.Errorf("dataRootFromBlobDir = %s", got)
	}
	// A non standard blob directory must not have its parent treated as the
	// key tree, or the RSA key would be looked up in the wrong place.
	if got := dataRootFromBlobDir("/srv", "/mnt/custom"); got != "/mnt/custom" {
		t.Errorf("dataRootFromBlobDir = %s", got)
	}
}

func TestSkipTransientData(t *testing.T) {
	skipped := []string{
		"blobs/upload_parts",
		"blobs/upload_parts/1780243298/-3906190596265207604/000337-48892a5b.part",
		"blob-staging",
		"blob-staging/spool/object",
	}
	for _, rel := range skipped {
		if !skipTransientData(rel) {
			t.Errorf("skipTransientData(%q) = false, want true", rel)
		}
	}
	kept := []string{
		"blobs/ab/cd/objectkey",
		"server_rsa.pem",
		"telegram-login/signing-keys.json",
		"updates/manifest.json",
		"identity/icon.jpg",
		// A prefix that merely starts with the same letters must not be dropped.
		"blobs/upload_partsinventory.json",
	}
	for _, rel := range kept {
		if skipTransientData(rel) {
			t.Errorf("skipTransientData(%q) = true, want false", rel)
		}
	}
}

func TestAddTreeHonoursSkip(t *testing.T) {
	src := t.TempDir()
	for _, rel := range []string{"blobs/ab/cd/object", "blobs/upload_parts/1/2/3.part", "server_rsa.pem"} {
		path := filepath.Join(src, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	collector := &itemCollector{}
	if err := collector.addTree(src, "data", skipTransientData); err != nil {
		t.Fatalf("addTree: %v", err)
	}
	var names []string
	for _, item := range collector.items {
		names = append(names, item.name)
	}
	// Directories are kept so empty ones survive a restore:
	// data, data/blobs, data/blobs/ab, data/blobs/ab/cd, the object and the key.
	if len(names) != 6 {
		t.Errorf("collected %v, want the directories plus the object and the key", names)
	}
	for _, name := range names {
		if strings.Contains(name, "upload_parts") {
			t.Errorf("transient upload part was archived: %s", name)
		}
	}
}

func TestSplitHostPort(t *testing.T) {
	cases := []struct{ in, host, port string }{
		{"127.0.0.1:6399", "127.0.0.1", "6399"},
		{"redis.internal", "redis.internal", ""},
		{"unix:///run/redis.sock", "/run/redis.sock", ""},
	}
	for _, tc := range cases {
		host, port := splitHostPort(tc.in)
		if host != tc.host || port != tc.port {
			t.Errorf("splitHostPort(%q) = %s,%s want %s,%s", tc.in, host, port, tc.host, tc.port)
		}
	}
}

func TestComponentFlagsResolve(t *testing.T) {
	var empty componentFlags
	if got := empty.resolve(); len(got) != 1 || got[0] != componentServer {
		t.Errorf("default components = %v, want [server]", got)
	}

	var parsed componentFlags
	fs := newFlagSet("test")
	parsed.register(fs)
	if err := fs.Parse([]string{"-component", "all"}); err != nil {
		t.Fatal(err)
	}
	got := parsed.resolve()
	if len(got) != 2 || got[0] != componentServer || got[1] != componentGrammystore {
		t.Errorf("all = %v", got)
	}
}

func TestComponentFlagsDeduplicate(t *testing.T) {
	var parsed componentFlags
	fs := newFlagSet("test")
	parsed.register(fs)
	if err := fs.Parse([]string{"-component", "server,grammystore", "-component", "server"}); err != nil {
		t.Fatal(err)
	}
	if got := parsed.resolve(); len(got) != 2 {
		t.Errorf("resolve = %v, want each component once", got)
	}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestResolveComponentsRejectsUnknown(t *testing.T) {
	_, err := resolveComponents([]string{"nope"}, resolveOptions{repoDir: t.TempDir()})
	if err == nil {
		t.Fatal("resolveComponents accepted an unknown component")
	}
}

func TestSelectComponents(t *testing.T) {
	manifest := &Manifest{Components: []Component{
		{Name: componentServer, Database: "telesrv_main"},
		{Name: componentGrammystore, Database: "grammystore"},
	}}
	if got := selectComponents(manifest, nil); len(got) != 2 {
		t.Errorf("nil selection = %d components, want all", len(got))
	}
	got := selectComponents(manifest, []string{componentGrammystore})
	if len(got) != 1 || got[0].Name != componentGrammystore {
		t.Errorf("selection = %+v", got)
	}
	if got := selectComponents(manifest, []string{"absent"}); len(got) != 0 {
		t.Errorf("absent selection = %+v", got)
	}
}

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{
		512:                        "512 B",
		1024:                       "1.0 KiB",
		1024 * 1024:                "1.0 MiB",
		3 * 1024 * 1024 * 1024 / 2: "1.5 GiB",
	}
	for input, want := range cases {
		if got := humanBytes(input); got != want {
			t.Errorf("humanBytes(%d) = %s, want %s", input, got, want)
		}
	}
}

func TestWriteEnvFileIsPrivate(t *testing.T) {
	path, err := writeEnvFile([]string{"PGPASSWORD=secret"})
	if err != nil {
		t.Fatalf("writeEnvFile: %v", err)
	}
	defer os.Remove(path)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("env file mode = %v, want 0600: it carries a password", info.Mode().Perm())
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "PGPASSWORD=secret") {
		t.Errorf("env file body = %q", body)
	}
}

func TestTailTruncatesLongOutput(t *testing.T) {
	long := strings.Repeat("x", 1000)
	got := tail(long)
	if len(got) > 403 {
		t.Errorf("tail returned %d characters, want a short suffix", len(got))
	}
	if !strings.HasSuffix(got, "x") {
		t.Error("tail dropped the end of the message, which is where the error is")
	}
	if got := tail("  short  "); got != "short" {
		t.Errorf("tail = %q", got)
	}
}

func TestIndexBlobsIgnoresUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("d", 64)
	path := blobObjectPath(dir, key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Not sharded, so not a blob.
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	index, err := indexBlobs(dir)
	if err != nil {
		t.Fatalf("indexBlobs: %v", err)
	}
	if len(index) != 1 {
		t.Errorf("indexed %d files, want only the sharded object: %v", len(index), index)
	}
}

// Freezing exists to make the media tree byte consistent, so a failure partway
// through a capture must still put the writers back.
func TestFreezeSetRestartsEverythingStopped(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "systemctl.log")
	script := "#!/bin/sh\necho \"$@\" >> " + logPath + "\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "systemctl"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	var log bytes.Buffer
	freeze := newFreezeSet(&log)
	if err := freeze.StopUnit("gramsrv.service"); err != nil {
		t.Fatalf("StopUnit: %v", err)
	}
	if err := freeze.StopUnit("gramsrv-admin.service"); err != nil {
		t.Fatalf("StopUnit: %v", err)
	}
	if got := freeze.Stopped(); len(got) != 2 {
		t.Fatalf("Stopped() = %v", got)
	}
	if err := freeze.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(body)
	if !strings.Contains(got, "stop gramsrv.service") || !strings.Contains(got, "stop gramsrv-admin.service") {
		t.Errorf("units were not stopped:\n%s", got)
	}
	// Newest first, so the dependency order of a restore is respected.
	adminAt := strings.Index(got, "start gramsrv-admin.service")
	serverAt := strings.Index(got, "start gramsrv.service")
	if adminAt < 0 || serverAt < 0 || adminAt > serverAt {
		t.Errorf("restart order is wrong:\n%s", got)
	}
	if freeze.Stopped() != nil && len(freeze.Stopped()) != 0 {
		t.Error("Release did not clear the stopped list")
	}
}

func TestFreezeSetStopUnitIgnoresBlankNames(t *testing.T) {
	freeze := newFreezeSet(io.Discard)
	if err := freeze.StopUnit("   "); err != nil {
		t.Errorf("StopUnit(blank) = %v", err)
	}
	if len(freeze.Stopped()) != 0 {
		t.Error("a blank unit name was recorded as stopped")
	}
}

func TestFreezeSetReleaseWithoutStopsSucceeds(t *testing.T) {
	if err := newFreezeSet(io.Discard).Release(); err != nil {
		t.Errorf("Release with nothing stopped = %v", err)
	}
}

func TestCmdRunnerValidate(t *testing.T) {
	if err := (cmdRunner{mode: toolsDocker}).validate("pg"); err == nil {
		t.Error("docker mode without a container was accepted")
	}
	if err := (cmdRunner{mode: toolsDocker, container: "c"}).validate("pg"); err != nil {
		t.Errorf("docker mode with a container was rejected: %v", err)
	}
	if err := (cmdRunner{mode: "nope"}).validate("pg"); err == nil {
		t.Error("an unknown tools mode was accepted")
	}
	if err := (cmdRunner{mode: toolsLocal}).validate("pg"); err != nil {
		t.Errorf("local mode was rejected: %v", err)
	}
}

// stubReader is a tiny io.Reader used to keep the artifact helpers honest about
// short writes.
type stubReader struct{ data []byte }

func (s *stubReader) Read(p []byte) (int, error) {
	if len(s.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.data)
	s.data = s.data[n:]
	return n, nil
}
