// Package satellite holds the single connection to the Feed Satelit and turns its stream into fire events.
package satellite

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	defaultBaseURL = "https://satellite.naufarrel.dev"
	defaultDBPath  = "/data/satellite.db"
)

type LeaseConfig struct {
	Name, Namespace, Identity string
	Duration, RenewEvery      time.Duration
	// Step down before the lease can expire for others, so two holders never overlap.
	StepDownAfter time.Duration
}

type Config struct {
	BaseURL       *url.URL
	Token         string
	DBPath        string
	IdleTimeout   time.Duration
	Lease         LeaseConfig
	AggregatorURL *url.URL
	// Without it fires are still recognised and kept in the outbox, but nothing is delivered.
	IngestToken string
}

func LoadConfig() (Config, error) {
	return loadConfig(os.LookupEnv)
}

func loadConfig(lookup func(string) (string, bool)) (Config, error) {
	cfg := Config{IdleTimeout: 30 * time.Second, DBPath: envOrDefault(lookup, "BNPB_SATELLITE_DB_PATH", defaultDBPath)}
	token, _ := lookup("BNPB_SATELLITE_TOKEN")
	if strings.TrimSpace(token) == "" || strings.TrimSpace(token) != token {
		return Config{}, fmt.Errorf("BNPB_SATELLITE_TOKEN: required, without surrounding whitespace")
	}
	cfg.Token = token
	base, err := url.Parse(envOrDefault(lookup, "BNPB_SATELLITE_BASE_URL", defaultBaseURL))
	if err != nil || (base.Scheme != "https" && base.Scheme != "http") || base.Host == "" || base.User != nil || strings.Trim(base.Path, "/") != "" {
		return Config{}, fmt.Errorf("BNPB_SATELLITE_BASE_URL: must be an HTTP(S) origin without path or credentials")
	}
	cfg.BaseURL = base
	aggregator, err := url.Parse(envOrDefault(lookup, "BNPB_SATELLITE_AGGREGATOR_URL", "http://aggregator:8083"))
	if err != nil || (aggregator.Scheme != "https" && aggregator.Scheme != "http") || aggregator.Host == "" || aggregator.User != nil || strings.Trim(aggregator.Path, "/") != "" {
		return Config{}, fmt.Errorf("BNPB_SATELLITE_AGGREGATOR_URL: must be an HTTP(S) origin without path or credentials")
	}
	cfg.AggregatorURL = aggregator
	cfg.IngestToken, _ = lookup("BNPB_SATELLITE_INGEST_TOKEN")
	if cfg.IngestToken != "" && (len(cfg.IngestToken) < 32 || strings.TrimSpace(cfg.IngestToken) != cfg.IngestToken) {
		return Config{}, fmt.Errorf("BNPB_SATELLITE_INGEST_TOKEN: must contain at least 32 bytes without surrounding whitespace")
	}
	cfg.Lease.Name, _ = lookup("BNPB_SATELLITE_LEASE_NAME")
	if cfg.Lease.Name == "" {
		return cfg, nil
	}
	cfg.Lease.Namespace = envOrDefault(lookup, "BNPB_SATELLITE_LEASE_NAMESPACE", "")
	if cfg.Lease.Namespace == "" {
		if raw, err := os.ReadFile(serviceAccountDir + "/namespace"); err == nil {
			cfg.Lease.Namespace = strings.TrimSpace(string(raw))
		}
	}
	cfg.Lease.Identity = envOrDefault(lookup, "BNPB_SATELLITE_IDENTITY", "")
	if cfg.Lease.Identity == "" {
		cfg.Lease.Identity, _ = os.Hostname()
	}
	if cfg.Lease.Namespace == "" || cfg.Lease.Identity == "" {
		return Config{}, fmt.Errorf("BNPB_SATELLITE_LEASE_NAME: needs a namespace and an identity")
	}
	cfg.Lease.Duration, cfg.Lease.RenewEvery, cfg.Lease.StepDownAfter = 15*time.Second, 5*time.Second, 10*time.Second
	return cfg, nil
}

func envOrDefault(lookup func(string) (string, bool), name, fallback string) string {
	if value, exists := lookup(name); exists && value != "" {
		return value
	}
	return fallback
}
