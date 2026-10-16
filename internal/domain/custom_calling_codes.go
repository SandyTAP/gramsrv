package domain

import (
	"sort"
	"strings"
	"sync/atomic"
)

// Custom calling codes.
//
// A deployment can sell numbers in calling codes that no real numbering plan
// uses. libphonenumber, which backs NormalizePhone, has metadata only for real
// plans, so it rejects those numbers outright -- or, worse, silently reinterprets
// a four digit code like 1488 as country code 148 with a national number
// starting 8. Either way the identity the user typed is not the identity the
// code is delivered to, so no OTP ever arrives.
//
// The registry below lets the deployment declare its own calling codes. Codes
// registered here are matched longest-first and accepted verbatim: the server
// is the authority on which numbers exist on it, so a number it issues must be
// a number it accepts.
//
// Real country codes must NOT be registered. Registering one would bypass
// libphonenumber and lose trunk-prefix normalisation for every user in that
// country. SeedCustomCallingCodes filters for that reason.
var customCallingCodes atomic.Pointer[[]string]

// SetCustomCallingCodes replaces the registry with codes, longest first.
//
// Safe for concurrent use. Passing an empty slice restores the default of "no
// custom codes", which is the correct behaviour for a deployment that has none:
// NormalizePhone then behaves exactly as it did before this existed.
func SetCustomCallingCodes(codes []string) {
	cleaned := make([]string, 0, len(codes))
	seen := make(map[string]struct{}, len(codes))
	for _, code := range codes {
		code = strings.TrimSpace(strings.TrimPrefix(code, "+"))
		if code == "" || !isAllDigits(code) {
			continue
		}
		if _, dup := seen[code]; dup {
			continue
		}
		seen[code] = struct{}{}
		cleaned = append(cleaned, code)
	}
	// Longest first so 1488 is matched before 148, and so the caller never has
	// to care whether one registered code is a prefix of another.
	sort.Slice(cleaned, func(i, j int) bool { return len(cleaned[i]) > len(cleaned[j]) })
	customCallingCodes.Store(&cleaned)
}

// CustomCallingCodes returns the registered codes, longest first.
func CustomCallingCodes() []string {
	if p := customCallingCodes.Load(); p != nil {
		return append([]string(nil), (*p)...)
	}
	return nil
}

// matchCustomCallingCode splits digits into a registered calling code and the
// subscriber part.
func matchCustomCallingCode(digits string) (code, national string, ok bool) {
	p := customCallingCodes.Load()
	if p == nil {
		return "", "", false
	}
	for _, candidate := range *p {
		if len(digits) <= len(candidate) || !strings.HasPrefix(digits, candidate) {
			continue
		}
		return candidate, digits[len(candidate):], true
	}
	return "", "", false
}

// MinCustomPhoneDigits and MaxCustomPhoneDigits bound a custom number's total
// length, matching E.164's 7..15 rather than inventing local policy.
const (
	MinCustomPhoneDigits = 7
	MaxCustomPhoneDigits = 15
)

// normaliseCustomPhone validates a number in a registered calling code.
func normaliseCustomPhone(digits string) (string, bool) {
	_, national, ok := matchCustomCallingCode(digits)
	if !ok {
		return "", false
	}
	total := len(digits)
	if total < MinCustomPhoneDigits || total > MaxCustomPhoneDigits {
		return "", false
	}
	// A subscriber part of one or two digits is not a typo to be accepted: it
	// is how a user pasting the wrong thing ends up with a real account. The
	// shortest plan this registry has been used for leaves far more than three.
	if len(national) < 3 {
		return "", false
	}
	return digits, true
}

func isAllDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}
