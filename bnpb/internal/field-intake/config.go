// Package fieldintake stores Tim Lapangan field reports from the field.reports queue exactly once.
// It keeps its own broker code on purpose: services must not share business logic.
package fieldintake

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

const (
	defaultBrokerAddr = "rabbitmq:5672"
	defaultDBPath     = "/data/field-intake.db"
	reportQueue       = "field.reports"
)

type Config struct {
	BrokerURL   string
	DBPath      string
	InternalKey string
}

func LoadConfig() (Config, error) {
	return loadConfig(os.LookupEnv)
}

func loadConfig(lookup func(string) (string, bool)) (Config, error) {
	user, err := required(lookup, "BNPB_FIELD_INTAKE_RABBITMQ_USER")
	if err != nil {
		return Config{}, err
	}
	password, err := required(lookup, "BNPB_FIELD_INTAKE_RABBITMQ_PASSWORD")
	if err != nil {
		return Config{}, err
	}
	key, err := required(lookup, "BNPB_FIELD_INTAKE_INTERNAL_KEY")
	if err != nil {
		return Config{}, err
	}
	if len(key) < 32 || strings.TrimSpace(key) != key {
		return Config{}, fmt.Errorf("BNPB_FIELD_INTAKE_INTERNAL_KEY: must contain at least 32 bytes without surrounding whitespace")
	}
	return Config{
		BrokerURL:   fmt.Sprintf("amqp://%s@%s/", url.UserPassword(user, password).String(), envOrDefault(lookup, "BNPB_RABBITMQ_ADDR", defaultBrokerAddr)),
		DBPath:      envOrDefault(lookup, "BNPB_FIELD_INTAKE_DB_PATH", defaultDBPath),
		InternalKey: key,
	}, nil
}

func envOrDefault(lookup func(string) (string, bool), name, fallback string) string {
	if value, exists := lookup(name); exists && value != "" {
		return value
	}
	return fallback
}

func required(lookup func(string) (string, bool), name string) (string, error) {
	value, exists := lookup(name)
	if !exists || value == "" {
		return "", fmt.Errorf("%s: required environment variable is missing or empty", name)
	}
	return value, nil
}
