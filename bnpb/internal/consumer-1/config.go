// Package consumer1 records the latest dashboard state per hazard_id from
// the hazard.events broker stream. Its own copy of the connect/consume/ack
// pattern is intentional: KI.1 forbids a shared business-logic library
// across services, so this is not imported by consumer-2 or consumer-3.
package consumer1

import (
	"fmt"
	"net/url"
	"os"
)

const (
	defaultBrokerAddr = "rabbitmq:5672"
	defaultDBPath     = "/data/consumer-1.db"
	queueRole         = "dashboard"
)

// Config holds this consumer's own settings, separate from the shared
// internal/platform/config fields service.Bootstrap already loaded.
type Config struct {
	BrokerURL string
	Queue     string
	DBPath    string
}

// LoadConfig reads this consumer's RabbitMQ identity and storage path.
func LoadConfig() (Config, error) {
	return loadConfig(os.LookupEnv)
}

func loadConfig(lookup func(string) (string, bool)) (Config, error) {
	user, err := required(lookup, "BNPB_CONSUMER_1_RABBITMQ_USER")
	if err != nil {
		return Config{}, err
	}
	password, err := required(lookup, "BNPB_CONSUMER_1_RABBITMQ_PASSWORD")
	if err != nil {
		return Config{}, err
	}
	addr := envOrDefault(lookup, "BNPB_RABBITMQ_ADDR", defaultBrokerAddr)
	dbPath := envOrDefault(lookup, "BNPB_CONSUMER_1_DB_PATH", defaultDBPath)

	return Config{
		BrokerURL: brokerURL(user, password, addr),
		Queue:     "hazard.events." + queueRole,
		DBPath:    dbPath,
	}, nil
}

func brokerURL(user, password, addr string) string {
	return fmt.Sprintf("amqp://%s@%s/", url.UserPassword(user, password).String(), addr)
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
