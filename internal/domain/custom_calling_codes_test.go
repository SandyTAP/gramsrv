package domain

import "testing"

// The deployment sells numbers in calling codes no numbering plan defines. If
// NormalizePhone keeps delegating those to libphonenumber, auth rejects the
// number the bot just issued and the login code is never delivered -- which is
// exactly the "issued a number, cannot log in" report this fixes.

func TestNormalizePhoneAcceptsRegisteredCustomCodes(t *testing.T) {
	SetCustomCallingCodes([]string{"69", "67", "999", "1488", "42"})
	t.Cleanup(func() { SetCustomCallingCodes(nil) })

	// Shapes exactly as /root/bot/bot.py gen_number produces them for the X*
	// regions: calling code + X_CODES (which is [""]) + a 7 digit tail.
	for _, tc := range []struct {
		name  string
		dial  string
		wants string
	}{
		{"X1 +69", "69 12 34567", "691234567"},
		{"X2 +67", "67 12 34567", "671234567"},
		{"X3 +999", "999 12 34567", "9991234567"},
		{"X4 +1488", "1488 12 34567", "14881234567"},
		{"X5 +42", "42 12 34567", "421234567"},
		{"X1 spaced as displayed", "+69 12 34567", "691234567"},
	} {
		got := NormalizePhone(tc.dial)
		if got != tc.wants {
			t.Errorf("%s: NormalizePhone(%q) = %q, want %q", tc.name, tc.dial, got, tc.wants)
		}
		if !ValidPhone(got) {
			t.Errorf("%s: ValidPhone(%q) = false", tc.name, got)
		}
	}
}

// A four digit calling code is the dangerous case: handed to libphonenumber it
// parses 148 as the country code and 8... as the national number, rewriting the
// identity into something the user never typed. The digits must come back
// untouched.
func TestCustomFourDigitCodeIsNotReinterpreted(t *testing.T) {
	SetCustomCallingCodes([]string{"1488"})
	t.Cleanup(func() { SetCustomCallingCodes(nil) })

	const dial = "1488 12 34567"
	if got := NormalizePhone(dial); got != "14881234567" {
		t.Fatalf("NormalizePhone(%q) = %q, want the digits unchanged", dial, got)
	}
}

// Registering a code must not change how a real country behaves. These all go
// through libphonenumber, and the trunk-prefix cases specifically would be
// mangled if the custom path swallowed them.
func TestRealCountriesKeepLibphonenumberBehaviour(t *testing.T) {
	SetCustomCallingCodes([]string{"69", "67", "999", "1488", "42"})
	t.Cleanup(func() { SetCustomCallingCodes(nil) })

	for _, tc := range []struct{ name, dial, wants string }{
		{"US", "1 202 555 0123", "12025550123"},
		{"GB", "44 7412 345678", "447412345678"},
		{"MX is a real region here", "52 12 34567", "521234567"},
		{"RU trunk prefix stripped", "7 926 123 45 67", "79261234567"},
		{"IT leading zero kept", "39 02 1234 5678", "390212345678"},
		{"KZ", "7 701 123 45 67", "77011234567"},
	} {
		got := NormalizePhone(tc.dial)
		if got != tc.wants {
			t.Errorf("%s: NormalizePhone(%q) = %q, want %q", tc.name, tc.dial, got, tc.wants)
		}
	}
}

// With no registry the behaviour must be byte-for-byte what it always was, so a
// deployment that sells no custom numbers is unaffected by this file.
func TestNoRegistryLeavesBehaviourUnchanged(t *testing.T) {
	SetCustomCallingCodes(nil)
	if got := CustomCallingCodes(); len(got) != 0 {
		t.Fatalf("CustomCallingCodes() = %v, want empty", got)
	}
	// These are exactly the numbers that failed before the fix.
	for _, dial := range []string{"69 12 34567", "67 12 34567", "999 12 34567"} {
		if got := NormalizePhone(dial); got != "" {
			t.Errorf("NormalizePhone(%q) = %q with no registry, want rejected", dial, got)
		}
	}
}

func TestSetCustomCallingCodesIsSanitised(t *testing.T) {
	t.Cleanup(func() { SetCustomCallingCodes(nil) })

	SetCustomCallingCodes([]string{"+69", " 42 ", "1488", "1488", "", "abc", "1x2"})
	got := CustomCallingCodes()
	want := []string{"1488", "69", "42"}
	if len(got) != len(want) {
		t.Fatalf("CustomCallingCodes() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("CustomCallingCodes() = %v, want %v (longest first)", got, want)
		}
	}
}

// A one or two digit subscriber part is what a mistyped number looks like, and
// it must not become a real account.
func TestCustomCodeRejectsAbsurdlyShortSubscribers(t *testing.T) {
	SetCustomCallingCodes([]string{"69"})
	t.Cleanup(func() { SetCustomCallingCodes(nil) })

	if got := NormalizePhone("6912"); got != "" {
		t.Errorf("NormalizePhone(%q) = %q, want rejected as too short", "6912", got)
	}
	if got := NormalizePhone("691234567"); got == "" {
		t.Error("a legitimate custom number was rejected")
	}
}
