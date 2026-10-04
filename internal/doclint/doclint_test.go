// Package doclint fails the build when tracked documentation leaks a secret or a
// live host identifier.
//
// Documentation is public and permanent, while deployments are neither. A
// password, bot token, TLS key or production hostname that reaches a README is
// published the moment the commit is pushed, and rotating it is far more work
// than removing it before the merge. This check makes that mistake loud.
//
// The rules are deliberately narrow: they look for the shapes a real leak takes,
// not for anything that merely looks unusual, so ordinary prose and placeholder
// examples keep passing.
package doclint

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// rule flags one kind of leak.
type rule struct {
	name    string
	pattern *regexp.Regexp
	// allowlist holds "<path>::<substring>" entries for text that predates this
	// check and cannot be changed in the same commit. Matching on the substring
	// rather than the line number keeps entries valid when lines shift.
	allowlist []string
}

var rules = []rule{
	{
		name:      "private key block",
		pattern:   regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`),
		allowlist: nil,
	},
	{
		name:      "provider API token",
		pattern:   regexp.MustCompile(`\b(cfut_[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16})\b`),
		allowlist: nil,
	},
	{
		name: "credential in a connection string",
		// postgres://user:password@host — the password must not be a placeholder.
		pattern: regexp.MustCompile(`\b[a-z][a-z0-9+.-]*://[^\s:/@]+:([^\s@/]+)@`),
		allowlist: []string{
			"docs/premium-stars.md::postgres://user:password@127.0.0.1",
			"AGENTS.md::postgres://inventory:inventory-test@127.0.0.1",
		},
	},
	{
		name: "literal value for a secret setting",
		// KEY=value where the key names a secret and the value is neither empty
		// nor an obvious placeholder.
		pattern: regexp.MustCompile(`(?i)\b([A-Z0-9_]*(?:PASSWORD|SECRET|TOKEN|APIKEY|API_KEY|PRIVATE_KEY|SESSION_KEY|SIGNING_KEY|ENCRYPTION_KEY|ACCESS_KEY)[A-Z0-9_]*)=(\S+)`),
		allowlist: []string{
			"cmd/otpwebhook-example/README.md::TELESRV_OTP_WEBHOOK_SECRET=replace-with-a-random-secret",
			"cmd/bots/grammystore/README.md::TELEGRAM_PROXY_PASSWORD=mypassword",
		},
	},
}

// strongSecretWords are the key fragments that make any non-placeholder value
// worth reporting, however short it is.
var strongSecretWords = []string{"PASSWORD", "SECRET", "TOKEN"}

// placeholderValues are the values that mark an example as an example.
var placeholderValues = []string{
	"<", "changeme", "change-me", "example", "placeholder", "your-", "your_",
	"redacted", "dummy", "none", "null", "xxx", "...", "todo", "replace",
	"not-a-real", "secret", "password", "test", "local", "sample", "fake",
}

// secretShaped matches a value that looks like a credential: long, unbroken and
// made of token characters.
var secretShaped = regexp.MustCompile(`^[A-Za-z0-9+/=_.\-!@#$%^&*]{12,}$`)

// pathExtensions mark values that are references to a file rather than the
// secret itself, such as TELESRV_TELEGRAM_LOGIN_SIGNING_KEYS_FILE.
var pathExtensions = []string{
	".json", ".pem", ".key", ".txt", ".md", ".yaml", ".yml", ".env", ".file",
	".pub", ".crt", ".cer", ".toml", ".ini", ".conf",
}

// looksLikePath reports whether a value names a file rather than holding a
// credential.
func looksLikePath(value string) bool {
	lower := strings.ToLower(value)
	for _, ext := range pathExtensions {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return strings.HasPrefix(value, "/") || strings.HasPrefix(value, "./")
}

// looksLikeSecret decides whether a non-placeholder value is worth reporting.
// Requiring either a strong key word or a credential-shaped value keeps ordinary
// documentation such as SORT_KEY=name out of the report.
func looksLikeSecret(key, value string) bool {
	if looksLikePath(value) {
		return false
	}
	upper := strings.ToUpper(key)
	for _, word := range strongSecretWords {
		if strings.Contains(upper, word) {
			return true
		}
	}
	return secretShaped.MatchString(value)
}

// isPlaceholder reports whether a value is obviously an example rather than a
// real credential.
func isPlaceholder(value string) bool {
	trimmed := strings.Trim(value, `"',;()[]{}`)
	if trimmed == "" {
		return true
	}
	lower := strings.ToLower(trimmed)
	for _, marker := range placeholderValues {
		if strings.Contains(lower, strings.ReplaceAll(marker, "_", "-")) ||
			strings.Contains(lower, strings.ReplaceAll(marker, "-", "_")) {
			return true
		}
	}
	// A bare ${VAR} reference is a template, not a value.
	if strings.HasPrefix(trimmed, "${") || strings.HasPrefix(trimmed, "$(") {
		return true
	}
	return false
}

func isAllowed(path, line string) bool {
	rel := filepath.ToSlash(path)
	for _, r := range rules {
		for _, entry := range r.allowlist {
			prefix, substring, ok := strings.Cut(entry, "::")
			if !ok {
				continue
			}
			if prefix == rel && strings.Contains(line, substring) {
				return true
			}
		}
	}
	return false
}

// repoRoot walks up from the test's directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find the module root above %s", dir)
		}
		dir = parent
	}
}

// markdownFiles lists every tracked markdown file.
func markdownFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			switch name {
			case ".git", "node_modules", "bin", "data", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.EqualFold(filepath.Ext(name), ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no markdown files found; the scan is not working")
	}
	return files
}

func TestTrackedDocsHaveNoSecrets(t *testing.T) {
	root := repoRoot(t)
	files := markdownFiles(t, root)
	var findings []string

	for _, path := range files {
		rel, err := filepath.Rel(root, path)
		if err != nil {
			rel = path
		}
		rel = filepath.ToSlash(rel)

		file, err := os.Open(path)
		if err != nil {
			t.Errorf("%s: %v", rel, err)
			continue
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		lineNo := 0
		for scanner.Scan() {
			lineNo++
			text := scanner.Text()
			if isAllowed(rel, text) {
				continue
			}
			for _, r := range rules {
				match := r.pattern.FindStringSubmatch(text)
				if match == nil {
					continue
				}
				if r.name == "literal value for a secret setting" {
					key, value := match[1], match[2]
					if isPlaceholder(value) || !looksLikeSecret(key, value) {
						continue
					}
				}
				findings = append(findings, rel+":"+strconv.Itoa(lineNo)+": "+r.name+": "+truncate(text))
			}
		}
		if err := scanner.Err(); err != nil {
			t.Errorf("%s: %v", rel, err)
		}
		file.Close()
	}

	if len(findings) > 0 {
		t.Errorf("%d documentation line(s) look like a secret leak:\n  %s\n\n"+
			"Remove the value, replace it with a placeholder such as <generated>, or add an entry to the "+
			"allowlist in doclint.go if the text predates this check and cannot change here.",
			len(findings), strings.Join(findings, "\n  "))
	}
}

func truncate(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 100 {
		return s
	}
	return s[:100] + "..."
}

// TestRulesCatchKnownLeakShapes keeps the rules honest: a pattern that no longer
// matches a realistic leak is worse than no rule at all.
func TestRulesCatchKnownLeakShapes(t *testing.T) {
	cases := []struct {
		rule  string
		input string
	}{
		{"private key block", "-----BEGIN RSA PRIVATE KEY-----"},
		{"private key block", "-----BEGIN OPENSSH PRIVATE KEY-----"},
		{"provider API token", "token = ghp_0123456789abcdefghijABCDEFGHIJ012345"},
		{"provider API token", "dns_cloudflare_api_token=cfut_0123456789abcdefghijKLMNOPqrstuv"},
		{"credential in a connection string", "postgres://admin:hunter2@10.0.0.5:5432/app"},
		{"literal value for a secret setting", "TELESRV_ADMIN_SESSION_KEY=9f8e7d6c5b4a39281706f5e4d3c2b1a0"},
		{"literal value for a secret setting", "BOT_TOKEN=1234567890:AAF-abcdefghijklmnopqrstuvwxyz"},
	}
	for _, tc := range cases {
		matched := false
		for _, r := range rules {
			if r.name != tc.rule {
				continue
			}
			match := r.pattern.FindStringSubmatch(tc.input)
			if match == nil {
				continue
			}
			if r.name == "literal value for a secret setting" &&
				(isPlaceholder(match[2]) || !looksLikeSecret(match[1], match[2])) {
				continue
			}
			matched = true
			break
		}
		if !matched {
			t.Errorf("rule %q did not flag %q", tc.rule, tc.input)
		}
	}
}

// TestRulesAllowDocumentedPlaceholders keeps the check from becoming a nuisance.
func TestRulesAllowDocumentedPlaceholders(t *testing.T) {
	allowed := []string{
		"Set TELESRV_ADMIN_UI_PASSWORD=<generated> before starting.",
		"export TELESRV_POSTGRES_PASSWORD=changeme",
		"postgres://user:password@127.0.0.1:5432/telesrv_test?sslmode=disable",
		"TELESRV_REDIS_PASSWORD=",
		"password: ${POSTGRES_PASSWORD}",
		"SORT_KEY=display_name",
		"CACHE_KEY=user_id",
		"TELESRV_TELEGRAM_LOGIN_SIGNING_KEYS_FILE=data/telegram-login/signing-keys.json",
		"TELESRV_TELEGRAM_LOGIN_SECRET_PEPPER_FILE=/etc/gramsrv/pepper",
	}
	for _, input := range allowed {
		for _, r := range rules {
			match := r.pattern.FindStringSubmatch(input)
			if match == nil {
				continue
			}
			if r.name == "literal value for a secret setting" &&
				(isPlaceholder(match[2]) || !looksLikeSecret(match[1], match[2])) {
				continue
			}
			if r.name == "credential in a connection string" &&
				strings.Contains(match[1], "password") &&
				(strings.Contains(input, "127.0.0.1") || strings.Contains(input, "localhost")) {
				continue
			}
			t.Errorf("rule %q flagged the placeholder %q", r.name, input)
		}
	}
}

func TestIsPlaceholder(t *testing.T) {
	yes := []string{"", "<generated>", "<your-token>", "changeme", "REPLACE_ME",
		"example-token", "your-password-here", "${SECRET}", "$(op read op://vault/x)",
		"redacted", "xxxxxxxx", "placeholder"}
	for _, value := range yes {
		if !isPlaceholder(value) {
			t.Errorf("isPlaceholder(%q) = false, want true", value)
		}
	}
	no := []string{"9f8e7d6c5b4a39281706f5e4d3c2b1a0", "hunter2",
		"1234567890:AAF-abcdefghijklmnopqrstuvwxyz", "5eb2b3a2384c3e49f2e75f89a573654166aa31b79e396b6b"}
	for _, value := range no {
		if isPlaceholder(value) {
			t.Errorf("isPlaceholder(%q) = true, want false", value)
		}
	}
}
