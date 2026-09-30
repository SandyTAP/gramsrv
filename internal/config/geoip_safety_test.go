package config

import (
	"strings"
	"testing"
)

func TestExampleConfigurationKeepsGeoIPOptIn(t *testing.T) {
	env, err := readEnvFile("../../.env.example")
	if err != nil {
		t.Fatal(err)
	}
	if value := strings.TrimSpace(env["TELESRV_GEOIP_ENDPOINTS"]); value != "" {
		t.Fatal("copying .env.example must not enable third-party session IP disclosure")
	}
}

func TestGeoIPConfigValidationDoesNotEchoCredentials(t *testing.T) {
	const marker = "geoip-test-credential-marker"
	for _, endpoint := range []string{
		"https://geo.example.test/?key=" + marker,
		"https://geo.example.test/%zz/{ip}?key=" + marker,
		"ftp://geo.example.test/{ip}?key=" + marker,
		"https:///{ip}?key=" + marker,
	} {
		err := validateGeoIPConfig(Config{GeoIPEndpoints: []string{"https://api.ipapi.is/?q={ip}", endpoint}})
		if err == nil || strings.Contains(err.Error(), marker) || !strings.Contains(err.Error(), "entry 2") {
			t.Fatalf("expected indexed, credential-safe configuration error, got %v", err)
		}
	}
}
