package consumer1

import (
	"strings"
	"testing"
)

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, exists := values[name]
		return value, exists
	}
}

func validEnvironment() map[string]string {
	return map[string]string{
		"BNPB_CONSUMER_1_RABBITMQ_USER":     "dashboard",
		"BNPB_CONSUMER_1_RABBITMQ_PASSWORD": "s3cret",
	}
}

func TestConfigBuildsTheBrokerURLAndQueueName(t *testing.T) {
	cfg, err := loadConfig(lookupFrom(validEnvironment()))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Queue != "hazard.events.dashboard" {
		t.Fatalf("unexpected queue name: %s", cfg.Queue)
	}
	if !strings.Contains(cfg.BrokerURL, "dashboard:s3cret@") {
		t.Fatalf("broker URL does not carry the configured credentials: %s", cfg.BrokerURL)
	}
	if cfg.DBPath != defaultDBPath {
		t.Fatalf("unexpected default db path: %s", cfg.DBPath)
	}
}

func TestConfigRejectsMissingCredentials(t *testing.T) {
	for _, name := range []string{"BNPB_CONSUMER_1_RABBITMQ_USER", "BNPB_CONSUMER_1_RABBITMQ_PASSWORD"} {
		values := validEnvironment()
		delete(values, name)
		if _, err := loadConfig(lookupFrom(values)); err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("missing %s was not rejected clearly: %v", name, err)
		}
	}
}

func TestConfigHonorsOverriddenAddrAndDBPath(t *testing.T) {
	values := validEnvironment()
	values["BNPB_RABBITMQ_ADDR"] = "broker.internal:5672"
	values["BNPB_CONSUMER_1_DB_PATH"] = "/custom/path.db"
	cfg, err := loadConfig(lookupFrom(values))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cfg.BrokerURL, "@broker.internal:5672/") {
		t.Fatalf("broker address override not applied: %s", cfg.BrokerURL)
	}
	if cfg.DBPath != "/custom/path.db" {
		t.Fatalf("db path override not applied: %s", cfg.DBPath)
	}
}
