// Package consumer3 records a regional-portal feed entry for every hazard
// event from the hazard.events broker stream. It exists purely to prove
// that a third consumer can bind to the existing exchange without any
// producer change. Its own copy of the connect/consume/ack pattern is
// intentional: services must not share a business-logic library, so this
// is not imported by consumer-1 or consumer-2.
package consumer3

import (
	"fmt"
	"net/url"
	"os"
)

const (
	defaultBrokerAddr = "rabbitmq:5672"
	defaultDBPath     = "/data/consumer-3.db"
	queueRole         = "demo"
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
	user, err := required(lookup, "BNPB_CONSUMER_3_RABBITMQ_USER")
	if err != nil {
		return Config{}, err
	}
	password, err := required(lookup, "BNPB_CONSUMER_3_RABBITMQ_PASSWORD")
	if err != nil {
		return Config{}, err
	}
	addr := envOrDefault(lookup, "BNPB_RABBITMQ_ADDR", defaultBrokerAddr)
	dbPath := envOrDefault(lookup, "BNPB_CONSUMER_3_DB_PATH", defaultDBPath)

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
