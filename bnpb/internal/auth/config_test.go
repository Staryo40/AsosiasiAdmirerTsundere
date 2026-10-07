package auth

import (
	"strings"
	"testing"
	"time"
)

func validEnvironment() map[string]string {
	return map[string]string{
		"BNPB_AUTH_INTERNAL_KEY":   strings.Repeat("i", 32),
		"BNPB_MEDIA_CLIENT_ID":     "media",
		"BNPB_MEDIA_CLIENT_SECRET": strings.Repeat("m", 32),
		"BNPB_FIELD_CLIENT_ID":     "field-team",
		"BNPB_FIELD_CLIENT_SECRET": strings.Repeat("f", 32),
		"BNPB_OPS_CLIENT_ID":       "internal-ops",
		"BNPB_OPS_CLIENT_SECRET":   strings.Repeat("o", 32),
	}
}

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, exists := values[name]
		return value, exists
	}
}

func TestConfigUsesRequiredDefaults(t *testing.T) {
	config, err := loadConfig(lookupFrom(validEnvironment()))
	if err != nil {
		t.Fatal(err)
	}
	if config.AccessTokenTTL != 60*time.Second || config.RefreshSessionTTL != 8*time.Hour {
		t.Fatalf("unexpected TTL defaults: access=%s refresh=%s", config.AccessTokenTTL, config.RefreshSessionTTL)
	}
	if len(config.clients) != 3 {
		t.Fatalf("expected three clients, got %d", len(config.clients))
	}
}

func TestConfigRejectsMissingAndWeakSecrets(t *testing.T) {
	for _, name := range []string{
		"BNPB_AUTH_INTERNAL_KEY",
		"BNPB_MEDIA_CLIENT_SECRET",
		"BNPB_FIELD_CLIENT_SECRET",
		"BNPB_OPS_CLIENT_SECRET",
	} {
		t.Run(name, func(t *testing.T) {
			values := validEnvironment()
			delete(values, name)
			if _, err := loadConfig(lookupFrom(values)); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("missing %s was not rejected clearly: %v", name, err)
			}

			values = validEnvironment()
			values[name] = "too-short"
			if _, err := loadConfig(lookupFrom(values)); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("weak %s was not rejected clearly: %v", name, err)
			}
		})
	}
}

func TestConfigRejectsDuplicateClientIDs(t *testing.T) {
	values := validEnvironment()
	values["BNPB_FIELD_CLIENT_ID"] = values["BNPB_MEDIA_CLIENT_ID"]
	if _, err := loadConfig(lookupFrom(values)); err == nil || !strings.Contains(err.Error(), "unique") {
		t.Fatalf("duplicate IDs were not rejected: %v", err)
	}
}

func TestConfigValidatesTTLRelationship(t *testing.T) {
	values := validEnvironment()
	values["BNPB_AUTH_ACCESS_TOKEN_TTL"] = "2h"
	values["BNPB_AUTH_REFRESH_SESSION_TTL"] = "1h"
	if _, err := loadConfig(lookupFrom(values)); err == nil || !strings.Contains(err.Error(), "at least") {
		t.Fatalf("shorter refresh session was not rejected: %v", err)
	}

	values = validEnvironment()
	values["BNPB_AUTH_ACCESS_TOKEN_TTL"] = "not-a-duration"
	if _, err := loadConfig(lookupFrom(values)); err == nil || !strings.Contains(err.Error(), "BNPB_AUTH_ACCESS_TOKEN_TTL") {
		t.Fatalf("invalid access TTL was not rejected clearly: %v", err)
	}
}
