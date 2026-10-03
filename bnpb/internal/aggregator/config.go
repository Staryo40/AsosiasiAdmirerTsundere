// Package aggregator owns ingestion, canonical storage and the internal read API.
package aggregator

import (
	"fmt"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type SourceConfig struct {
	BaseURL      string
	HTTPTimeout  time.Duration
	PollInterval time.Duration
}

type Config struct {
	BMKG, PVMBG                                     SourceConfig
	BMKGAPIKey, PVMBGReaderClientID                 string
	PVMBGReaderClientSecret, InternalKey            string
	DatabaseURL, AMQPURL, HazardExchange            string
	PVMBGCycleTimeout, PollOverlap, RetryMaxBackoff time.Duration
	WarningReconcileInterval, SourceStaleAfter      time.Duration
	DBReadTimeout, DBWriteTimeout, PublishTimeout   time.Duration
	MaxResponseBytes                                int64
	IngestChunkRecords, DBReadMaxConns              int
	DBWriteMaxConns, ReadMaxInFlight                int
	ListDefaultLimit, ListMaxLimit, OutboxBatchSize int
}

func LoadConfig() (Config, error) {
	return loadConfig(os.LookupEnv)
}

func loadConfig(lookup func(string) (string, bool)) (Config, error) {
	cfg := Config{}
	for _, setting := range []struct {
		name, fallback string
		target         *string
	}{
		{"BNPB_BMKG_BASE_URL", "http://bmkg:8081", &cfg.BMKG.BaseURL},
		{"BNPB_PVMBG_BASE_URL", "http://pvmbg:8082", &cfg.PVMBG.BaseURL},
		{"BNPB_BMKG_API_KEY", "", &cfg.BMKGAPIKey},
		{"BNPB_PVMBG_READER_CLIENT_ID", "", &cfg.PVMBGReaderClientID},
		{"BNPB_PVMBG_READER_CLIENT_SECRET", "", &cfg.PVMBGReaderClientSecret},
		{"BNPB_AGGREGATOR_INTERNAL_KEY", "", &cfg.InternalKey},
		{"BNPB_CANONICAL_DATABASE_URL", "", &cfg.DatabaseURL},
		{"BNPB_AMQP_URL", "", &cfg.AMQPURL},
		{"BNPB_HAZARD_EXCHANGE", "hazard.events", &cfg.HazardExchange},
	} {
		configured, exists := lookup(setting.name)
		if !exists {
			configured = setting.fallback
		}
		if strings.TrimSpace(configured) == "" || strings.IndexFunc(configured, unicode.IsControl) >= 0 {
			return Config{}, fmt.Errorf("%s: required nonempty value without control characters", setting.name)
		}
		*setting.target = configured
	}
	if err := loadBudgets(lookup, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func loadBudgets(lookup func(string) (string, bool), cfg *Config) error {
	for _, setting := range []struct {
		name              string
		fallback, maximum time.Duration
		target            *time.Duration
	}{
		{"BNPB_BMKG_HTTP_TIMEOUT", time.Second, time.Minute, &cfg.BMKG.HTTPTimeout},
		{"BNPB_PVMBG_HTTP_TIMEOUT", 5 * time.Second, time.Minute, &cfg.PVMBG.HTTPTimeout},
		{"BNPB_BMKG_POLL_INTERVAL", 3 * time.Second, time.Minute, &cfg.BMKG.PollInterval},
		{"BNPB_PVMBG_POLL_INTERVAL", 3 * time.Second, time.Minute, &cfg.PVMBG.PollInterval},
		{"BNPB_PVMBG_CYCLE_TIMEOUT", 12 * time.Second, 5 * time.Minute, &cfg.PVMBGCycleTimeout},
		{"BNPB_POLL_OVERLAP", 2 * time.Second, time.Minute, &cfg.PollOverlap},
		{"BNPB_RETRY_MAX_BACKOFF", 15 * time.Second, 5 * time.Minute, &cfg.RetryMaxBackoff},
		{"BNPB_WARNING_RECONCILE_INTERVAL", time.Minute, time.Hour, &cfg.WarningReconcileInterval},
		{"BNPB_SOURCE_STALE_AFTER", 20 * time.Second, 5 * time.Minute, &cfg.SourceStaleAfter},
		{"BNPB_DB_READ_TIMEOUT", 200 * time.Millisecond, time.Minute, &cfg.DBReadTimeout},
		{"BNPB_DB_WRITE_TIMEOUT", 2 * time.Second, time.Minute, &cfg.DBWriteTimeout},
		{"BNPB_PUBLISH_TIMEOUT", 5 * time.Second, time.Minute, &cfg.PublishTimeout},
	} {
		configured, exists := lookup(setting.name)
		if !exists {
			configured = setting.fallback.String()
		}
		duration, err := time.ParseDuration(configured)
		if err != nil || duration < time.Millisecond || duration > setting.maximum {
			return fmt.Errorf("%s: duration must be between 1ms and %s", setting.name, setting.maximum)
		}
		*setting.target = duration
	}
	return loadCounts(lookup, cfg)
}

func loadCounts(lookup func(string) (string, bool), cfg *Config) error {
	responseBytes := 0
	for _, setting := range []struct {
		name              string
		fallback, maximum int
		target            *int
	}{
		{"BNPB_INGEST_MAX_RESPONSE_BYTES", 32 << 20, 128 << 20, &responseBytes},
		{"BNPB_INGEST_CHUNK_RECORDS", 100, 1000, &cfg.IngestChunkRecords},
		{"BNPB_DB_READ_MAX_CONNS", 16, 64, &cfg.DBReadMaxConns},
		{"BNPB_DB_WRITE_MAX_CONNS", 4, 64, &cfg.DBWriteMaxConns},
		{"BNPB_READ_MAX_IN_FLIGHT", 32, 1024, &cfg.ReadMaxInFlight},
		{"BNPB_LIST_DEFAULT_LIMIT", 100, 500, &cfg.ListDefaultLimit},
		{"BNPB_LIST_MAX_LIMIT", 500, 500, &cfg.ListMaxLimit},
		{"BNPB_OUTBOX_BATCH_SIZE", 100, 1000, &cfg.OutboxBatchSize},
	} {
		configured, exists := lookup(setting.name)
		if !exists {
			configured = strconv.Itoa(setting.fallback)
		}
		count, err := strconv.Atoi(configured)
		if err != nil || count < 1 || count > setting.maximum {
			return fmt.Errorf("%s: integer must be between 1 and %d", setting.name, setting.maximum)
		}
		*setting.target = count
	}
	cfg.MaxResponseBytes = int64(responseBytes)
	return nil
}

func (cfg Config) validate() error {
	for _, setting := range []struct {
		name, address string
		schemes       []string
	}{
		{"BNPB_BMKG_BASE_URL", cfg.BMKG.BaseURL, []string{"http", "https"}},
		{"BNPB_PVMBG_BASE_URL", cfg.PVMBG.BaseURL, []string{"http", "https"}},
		{"BNPB_CANONICAL_DATABASE_URL", cfg.DatabaseURL, []string{"postgres", "postgresql"}},
		{"BNPB_AMQP_URL", cfg.AMQPURL, []string{"amqp", "amqps"}},
	} {
		if err := validateURL(setting.name, setting.address, setting.schemes); err != nil {
			return err
		}
	}
	return cfg.validateBudgets()
}

func validateURL(name, address string, schemes []string) error {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Hostname() == "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return fmt.Errorf("%s: invalid absolute URL", name)
	}
	if !slices.Contains(schemes, parsed.Scheme) {
		return fmt.Errorf("%s: unsupported URL scheme", name)
	}
	if err := validateURLPort(name, parsed); err != nil {
		return err
	}
	if parsed.Scheme == "http" || parsed.Scheme == "https" {
		return validateSourceOrigin(name, parsed)
	}
	return nil
}

func validateURLPort(name string, parsed *url.URL) error {
	if parsed.Port() != "" {
		port, err := strconv.Atoi(parsed.Port())
		if err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("%s: invalid URL port", name)
		}
	}
	return nil
}

func validateSourceOrigin(name string, parsed *url.URL) error {
	if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || strings.Trim(parsed.Path, "/") != "" {
		return fmt.Errorf("%s: source URL must be an origin without credentials, query or path", name)
	}
	return nil
}

func (cfg Config) validateBudgets() error {
	for _, check := range []struct {
		invalid bool
		message string
	}{
		{cfg.PVMBGCycleTimeout < 2*cfg.PVMBG.HTTPTimeout, "BNPB_PVMBG_CYCLE_TIMEOUT: must cover two BNPB_PVMBG_HTTP_TIMEOUT requests"},
		{cfg.SourceStaleAfter <= cfg.PVMBGCycleTimeout || cfg.SourceStaleAfter <= cfg.BMKG.HTTPTimeout+cfg.BMKG.PollInterval || cfg.SourceStaleAfter <= cfg.PVMBG.HTTPTimeout+cfg.PVMBG.PollInterval, "BNPB_SOURCE_STALE_AFTER: must exceed source request/cycle budgets"},
		{cfg.ListDefaultLimit > cfg.ListMaxLimit, "BNPB_LIST_DEFAULT_LIMIT: must not exceed BNPB_LIST_MAX_LIMIT"},
		{cfg.DBReadMaxConns+cfg.DBWriteMaxConns > 64, "BNPB_DB_READ_MAX_CONNS + BNPB_DB_WRITE_MAX_CONNS: must not exceed 64"},
	} {
		if check.invalid {
			return fmt.Errorf("%s", check.message)
		}
	}
	return nil
}
