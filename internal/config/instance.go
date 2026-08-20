package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// ResolveInstanceID returns the configured stable instance ID, or a generated
// process-local ID for short-lived development runs.
func ResolveInstanceID(configured string) string {
	if configured = strings.TrimSpace(configured); configured != "" {
		return configured
	}
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "host"
	}
	host = strings.NewReplacer(" ", "-", ":", "-", "/", "-", "\\", "-").Replace(strings.TrimSpace(host))
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
}
