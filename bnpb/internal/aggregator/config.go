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

const (
	defaultDBReadTimeout = 200 * time.Millisecond
	readResponseHeadroom = time.Second
)

type SourceConfig struct {
	BaseURL      string
	HTTPTimeout  time.Duration
	PollInterval time.Duration
}

type pantaiResilience struct {
	BreakerFailures  int
	BreakerCooldown  time.Duration
	MaxInFlightCalls int
}

type Config struct {
	BMKG, PVMBG, Pantai                             SourceConfig
	PantaiResilience                                pantaiResilience
	BMKGAPIKey, PVMBGReaderClientID, PantaiAuthToken string
	PVMBGReaderClientSecret, InternalKey            string
	DatabaseURL, WriterURL, AMQPURL, HazardExchange string
	ShardURLs, ShardWriterURLs                      [3]string
	ShardPlacement                                  string
	ShardRepairCandidates                           string
	Authority                                       AuthorityConfig
	Audit                                           AuditConfig
	Catalog                                         CatalogConfig
	PVMBGAdaptation                                 pvmbgAdaptation
	PVMBGCycleTimeout, PollOverlap, RetryMaxBackoff time.Duration
	WarningReconcileInterval, SourceStaleAfter      time.Duration
	DBReadTimeout, DBWriteTimeout, PublishTimeout   time.Duration
	MaxResponseBytes                                int64
	IngestChunkRecords, DBReadMaxConns              int
	DBWriteMaxConns, ReadMaxInFlight                int
	ListDefaultLimit, ListMaxLimit, OutboxBatchSize int
}

type CatalogConfig struct {
	Token string
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
		{"BNPB_PANTAI_BASE_URL", "http://sensor-pantai:8080", &cfg.Pantai.BaseURL},
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
	if token, exists := lookup("BNPB_PANTAI_AUTH_TOKEN"); exists {
		cfg.PantaiAuthToken = strings.TrimSpace(token)
	}
	if writerURL, ok := lookup("BNPB_CANONICAL_WRITER_DATABASE_URL"); ok {
		cfg.WriterURL = strings.TrimSpace(writerURL)
	}
	shardURLsConfigured := 0
	for shardID := range cfg.ShardURLs {
		name := fmt.Sprintf("BNPB_SHARD_%d_DATABASE_URL", shardID)
		address, exists := lookup(name)
		if exists {
			shardURLsConfigured++
			if strings.TrimSpace(address) == "" || strings.IndexFunc(address, unicode.IsControl) >= 0 {
				return Config{}, fmt.Errorf("%s: required nonempty URL without control characters", name)
			}
			cfg.ShardURLs[shardID] = address
		}
		writerName := fmt.Sprintf("BNPB_SHARD_%d_WRITER_DATABASE_URL", shardID)
		writerAddress, writerExists := lookup(writerName)
		if writerExists {
			if strings.TrimSpace(writerAddress) == "" || strings.IndexFunc(writerAddress, unicode.IsControl) >= 0 {
				return Config{}, fmt.Errorf("%s: required nonempty URL without control characters", writerName)
			}
			cfg.ShardWriterURLs[shardID] = writerAddress
		}
	}
	if shardURLsConfigured != 0 && shardURLsConfigured != len(cfg.ShardURLs) {
		return Config{}, fmt.Errorf("BNPB_SHARD_0_DATABASE_URL through BNPB_SHARD_2_DATABASE_URL: configure all three or none")
	}
	cfg.ShardPlacement, _ = lookup("BNPB_SHARD_PLACEMENT")
	cfg.ShardRepairCandidates, _ = lookup("BNPB_SHARD_REPAIR_CANDIDATES")
	var err error
	cfg.Authority, err = loadAuthorityConfig(lookup)
	if err != nil {
		return Config{}, err
	}
	cfg.Audit, err = loadAuditConfig(lookup)
	if err != nil {
		return Config{}, err
	}
	cfg.Catalog, err = loadCatalogConfig(lookup)
	if err != nil {
		return Config{}, err
	}
	cfg.PVMBGAdaptation, err = loadPVMBGAdaptation(lookup)
	if err != nil {
		return Config{}, err
	}
	credentials := []string{cfg.InternalKey, cfg.BMKGAPIKey, cfg.PVMBGReaderClientID, cfg.PVMBGReaderClientSecret, cfg.PantaiAuthToken}
	for _, credential := range credentials {
		if cfg.Audit.Token != "" && credential != "" && cfg.Audit.Token == credential {
			return Config{}, fmt.Errorf("BNPB_AUDIT_TOKEN: must not reuse another service credential")
		}
	}
	for _, credential := range append(credentials, cfg.Audit.Token) {
		if cfg.Catalog.Token != "" && credential != "" && cfg.Catalog.Token == credential {
			return Config{}, fmt.Errorf("BNPB_CATALOG_TOKEN: must not reuse another service credential")
		}
	}
	if err := loadBudgets(lookup, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
func loadCatalogConfig(lookup func(string) (string, bool)) (CatalogConfig, error) {
	token, configured := lookup("BNPB_CATALOG_TOKEN")
	if !configured {
		return CatalogConfig{}, nil
	}
	if strings.TrimSpace(token) == "" || strings.IndexFunc(token, unicode.IsControl) >= 0 {
		return CatalogConfig{}, fmt.Errorf("BNPB_CATALOG_TOKEN: must be nonempty and contain no control characters")
	}
	return CatalogConfig{Token: token}, nil
}

func readWorkBudget(configuredBudget, writeTimeout time.Duration) time.Duration {
	if writeTimeout <= 0 {
		return configuredBudget
	}
	// Reserve time for rollback and JSON emission; keep some work time on short deadlines.
	return min(configuredBudget, max(writeTimeout-rollbackTimeout-readResponseHeadroom, writeTimeout/4))
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
		{"BNPB_PANTAI_POLL_INTERVAL", 3 * time.Second, time.Minute, &cfg.Pantai.PollInterval},
		{"BNPB_PANTAI_HTTP_TIMEOUT", 2 * time.Second, time.Minute, &cfg.Pantai.HTTPTimeout},
		{"BNPB_PANTAI_BREAKER_COOLDOWN", 30 * time.Second, 5 * time.Minute, &cfg.PantaiResilience.BreakerCooldown},
		{"BNPB_PVMBG_CYCLE_TIMEOUT", 12 * time.Second, 5 * time.Minute, &cfg.PVMBGCycleTimeout},
		{"BNPB_POLL_OVERLAP", 2 * time.Second, time.Minute, &cfg.PollOverlap},
		{"BNPB_RETRY_MAX_BACKOFF", 15 * time.Second, 5 * time.Minute, &cfg.RetryMaxBackoff},
		{"BNPB_WARNING_RECONCILE_INTERVAL", time.Minute, time.Hour, &cfg.WarningReconcileInterval},
		{"BNPB_SOURCE_STALE_AFTER", 20 * time.Second, 5 * time.Minute, &cfg.SourceStaleAfter},
		{"BNPB_DB_READ_TIMEOUT", defaultDBReadTimeout, time.Minute, &cfg.DBReadTimeout},
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
		{"BNPB_READ_MAX_IN_FLIGHT", 4, 1024, &cfg.ReadMaxInFlight},
		{"BNPB_LIST_DEFAULT_LIMIT", 100, 500, &cfg.ListDefaultLimit},
		{"BNPB_LIST_MAX_LIMIT", 500, 500, &cfg.ListMaxLimit},
		{"BNPB_OUTBOX_BATCH_SIZE", 100, 1000, &cfg.OutboxBatchSize},
		{"BNPB_PANTAI_BREAKER_FAILURES", 5, 64, &cfg.PantaiResilience.BreakerFailures},
		{"BNPB_PANTAI_MAX_IN_FLIGHT", 1, 8, &cfg.PantaiResilience.MaxInFlightCalls},
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
		{"BNPB_PANTAI_BASE_URL", cfg.Pantai.BaseURL, []string{"http", "https"}},
		{"BNPB_CANONICAL_DATABASE_URL", cfg.DatabaseURL, []string{"postgres", "postgresql"}},
		{"BNPB_AMQP_URL", cfg.AMQPURL, []string{"amqp", "amqps"}},
	} {
		if err := validateURL(setting.name, setting.address, setting.schemes); err != nil {
			return err
		}
	}
	routed, err := validateShardURLs(cfg)
	if err != nil {
		return err
	}
	primaryURLs := []string{cfg.DatabaseURL}
	if cfg.WriterURL != "" {
		primaryURLs = append(primaryURLs, cfg.WriterURL)
	}
	for index := range cfg.ShardURLs {
		if cfg.ShardURLs[index] != "" {
			primaryURLs = append(primaryURLs, cfg.ShardURLs[index])
		}
		if cfg.ShardWriterURLs[index] != "" {
			primaryURLs = append(primaryURLs, cfg.ShardWriterURLs[index])
		}
	}
	if err := validatePhysicalCopyURLs(cfg.Audit.CopyURLs, primaryURLs); err != nil {
		return fmt.Errorf("BNPB_STORAGE_COPY_DATABASE_URLS: invalid physical copy configuration")
	}
	if routed {
		if err := validateRoutedWriters(cfg); err != nil {
			return err
		}
		if cfg.Catalog.Token != "" && cfg.Authority.OwnerEndpoint == "" {
			return fmt.Errorf("BNPB_AUTHORITY_OWNER_ENDPOINT: required when BNPB_CATALOG_TOKEN is configured")
		}
		if err := cfg.Authority.validate(); err != nil {
			return err
		}
	} else if cfg.WriterURL != "" {
		if err := validateURL("BNPB_CANONICAL_WRITER_DATABASE_URL", cfg.WriterURL, []string{"postgres", "postgresql"}); err != nil {
			return err
		}
		adminURL, _ := url.Parse(cfg.DatabaseURL)
		writerURL, _ := url.Parse(cfg.WriterURL)
		if !sameDatabaseTarget(adminURL, writerURL) || adminURL.User == nil || writerURL.User == nil || adminURL.User.Username() == writerURL.User.Username() {
			return fmt.Errorf("BNPB_CANONICAL_WRITER_DATABASE_URL: must use distinct credentials for the same database node, database, and schema")
		}
	}
	return cfg.validateBudgets()
}

func validateShardURLs(cfg Config) (bool, error) {
	routed := cfg.ShardURLs[0] != "" || cfg.ShardURLs[1] != "" || cfg.ShardURLs[2] != ""
	if !routed {
		return false, nil
	}
	for shardID, address := range cfg.ShardURLs {
		name := fmt.Sprintf("BNPB_SHARD_%d_DATABASE_URL", shardID)
		if err := validateURL(name, address, []string{"postgres", "postgresql"}); err != nil {
			return true, err
		}
		if address == cfg.DatabaseURL {
			return true, fmt.Errorf("%s: must differ from BNPB_CANONICAL_DATABASE_URL", name)
		}
		for prior := 0; prior < shardID; prior++ {
			if address == cfg.ShardURLs[prior] {
				return true, fmt.Errorf("%s: shard database URLs must be distinct", name)
			}
		}
	}
	return true, nil
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
		{cfg.SourceStaleAfter <= cfg.PVMBGCycleTimeout || cfg.SourceStaleAfter <= cfg.BMKG.HTTPTimeout+cfg.BMKG.PollInterval || cfg.SourceStaleAfter <= cfg.PVMBG.HTTPTimeout+cfg.PVMBG.PollInterval || cfg.SourceStaleAfter <= cfg.Pantai.HTTPTimeout+cfg.Pantai.PollInterval, "BNPB_SOURCE_STALE_AFTER: must exceed source request/cycle budgets"},
		{cfg.ListDefaultLimit > cfg.ListMaxLimit, "BNPB_LIST_DEFAULT_LIMIT: must not exceed BNPB_LIST_MAX_LIMIT"},
		{cfg.ReadMaxInFlight > cfg.DBReadMaxConns, "BNPB_READ_MAX_IN_FLIGHT: must not exceed BNPB_DB_READ_MAX_CONNS"},
		{cfg.DBReadMaxConns+cfg.DBWriteMaxConns > 64, "BNPB_DB_READ_MAX_CONNS + BNPB_DB_WRITE_MAX_CONNS: must not exceed 64"},
	} {
		if check.invalid {
			return fmt.Errorf("%s", check.message)
		}
	}
	return nil
}
