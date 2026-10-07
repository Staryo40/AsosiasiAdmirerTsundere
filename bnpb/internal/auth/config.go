// Package auth implements BNPB downstream-client authentication.
package auth

import (
	"crypto/sha256"
	"fmt"
	"os"
	"regexp"
	"time"
)

const (
	defaultAccessTokenTTL    = 60 * time.Second
	defaultRefreshSessionTTL = 8 * time.Hour
)

var clientIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// Role is the server-assigned authorization role returned by introspection.
type Role string

const (
	RoleMedia Role = "media"
	RoleField Role = "field"
	RoleOps   Role = "ops"
)

type clientCredential struct {
	id         string
	role       Role
	idDigest   [sha256.Size]byte
	secretHash [sha256.Size]byte
}

// Config contains validated Auth Service settings. Only hashes of credentials
// are retained, so formatting Config cannot reveal their original values.
type Config struct {
	AccessTokenTTL    time.Duration
	RefreshSessionTTL time.Duration
	internalKeyHash   [sha256.Size]byte
	clients           []clientCredential
}

// LoadConfig reads Auth Service settings from the process environment.
func LoadConfig() (Config, error) {
	return loadConfig(os.LookupEnv)
}

func loadConfig(lookup func(string) (string, bool)) (Config, error) {
	accessTTL, err := durationSetting(lookup, "BNPB_AUTH_ACCESS_TOKEN_TTL", defaultAccessTokenTTL)
	if err != nil {
		return Config{}, err
	}
	refreshTTL, err := durationSetting(lookup, "BNPB_AUTH_REFRESH_SESSION_TTL", defaultRefreshSessionTTL)
	if err != nil {
		return Config{}, err
	}
	if refreshTTL < accessTTL {
		return Config{}, fmt.Errorf("BNPB_AUTH_REFRESH_SESSION_TTL: must be at least BNPB_AUTH_ACCESS_TOKEN_TTL")
	}

	internalKey, err := requiredSecret(lookup, "BNPB_AUTH_INTERNAL_KEY")
	if err != nil {
		return Config{}, err
	}

	definitions := []struct {
		prefix string
		role   Role
	}{
		{"BNPB_MEDIA", RoleMedia},
		{"BNPB_FIELD", RoleField},
		{"BNPB_OPS", RoleOps},
	}
	clients := make([]clientCredential, 0, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		id, err := requiredSetting(lookup, definition.prefix+"_CLIENT_ID")
		if err != nil {
			return Config{}, err
		}
		if !clientIDPattern.MatchString(id) {
			return Config{}, fmt.Errorf("%s_CLIENT_ID: must match %s", definition.prefix, clientIDPattern)
		}
		if _, duplicate := seen[id]; duplicate {
			return Config{}, fmt.Errorf("%s_CLIENT_ID: client IDs must be unique", definition.prefix)
		}
		seen[id] = struct{}{}

		secret, err := requiredSecret(lookup, definition.prefix+"_CLIENT_SECRET")
		if err != nil {
			return Config{}, err
		}
		clients = append(clients, newClientCredential(id, secret, definition.role))
	}

	return Config{
		AccessTokenTTL:    accessTTL,
		RefreshSessionTTL: refreshTTL,
		internalKeyHash:   sha256.Sum256([]byte(internalKey)),
		clients:           clients,
	}, nil
}

func newClientCredential(id, secret string, role Role) clientCredential {
	return clientCredential{
		id:         id,
		role:       role,
		idDigest:   sha256.Sum256([]byte(id)),
		secretHash: sha256.Sum256([]byte(secret)),
	}
}

func requiredSecret(lookup func(string) (string, bool), name string) (string, error) {
	value, err := requiredSetting(lookup, name)
	if err != nil {
		return "", err
	}
	if len(value) < 32 {
		return "", fmt.Errorf("%s: must contain at least 32 bytes", name)
	}
	return value, nil
}

func requiredSetting(lookup func(string) (string, bool), name string) (string, error) {
	value, exists := lookup(name)
	if !exists || value == "" {
		return "", fmt.Errorf("%s: required environment variable is missing or empty", name)
	}
	return value, nil
}

func envOrDefault(lookup func(string) (string, bool), name, fallback string) string {
	if value, exists := lookup(name); exists {
		return value
	}
	return fallback
}

func durationSetting(lookup func(string) (string, bool), name string, fallback time.Duration) (time.Duration, error) {
	configured := envOrDefault(lookup, name, fallback.String())
	value, err := time.ParseDuration(configured)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s: must be a positive Go duration", name)
	}
	return value, nil
}
