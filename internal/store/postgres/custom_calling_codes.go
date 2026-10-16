package postgres

import (
	"context"
	"fmt"

	"github.com/nyaruka/phonenumbers"

	"telesrv/internal/domain"
)

// LoadCustomCallingCodes finds the calling codes this deployment invented.
//
// A deployment can sell numbers in codes no numbering plan defines. Those must
// be registered in domain so NormalizePhone accepts the numbers the bot issues:
// without that, libphonenumber rejects them outright, or -- for a four digit
// code such as 1488 -- silently reinterprets the identity and the OTP is
// delivered to an account the user never typed.
//
// A code counts as custom only when both hold:
//
//   - the library has no metadata for it at all (so it cannot be a real plan),
//   - the region we sell it under is not a region the library maps to it.
//
// Both halves matter. Dropping the first would register genuinely real
// countries and bypass trunk-prefix normalisation for their users. Dropping the
// second would let a real code pass merely because our own row disagrees about
// which ISO it belongs to.
func (s *StarsStore) LoadCustomCallingCodes(ctx context.Context) ([]string, error) {
	rows, err := s.db.Query(ctx, `
        SELECT DISTINCT cc.country_code, c.iso2
        FROM country_codes cc
        JOIN countries c ON c.iso2 = cc.iso2`)
	if err != nil {
		return nil, fmt.Errorf("query country codes: %w", err)
	}
	defer rows.Close()

	supported := phonenumbers.GetSupportedCallingCodes()
	var custom []string
	for rows.Next() {
		var raw, iso2 string
		if err := rows.Scan(&raw, &iso2); err != nil {
			return nil, fmt.Errorf("scan country code: %w", err)
		}
		code, err := callingCodeInt(raw)
		if err != nil {
			continue
		}
		if _, known := supported[code]; known {
			continue
		}
		if phonenumbers.GetCountryCodeForRegion(iso2) == code {
			continue
		}
		custom = append(custom, raw)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate country codes: %w", err)
	}
	domain.SetCustomCallingCodes(custom)
	return custom, nil
}

// callingCodeInt parses a stored calling code, rejecting anything that is not
// plain digits. A malformed row must not panic the bootstrap.
func callingCodeInt(raw string) (int, error) {
	if raw == "" {
		return 0, fmt.Errorf("empty calling code")
	}
	n := 0
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, fmt.Errorf("calling code %q is not numeric", raw)
		}
		n = n*10 + int(raw[i]-'0')
	}
	return n, nil
}
