package aggregator

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
)

type AuditConfig struct {
	Token, UnprocessedURL, UnprocessedToken string
	CopyURLs                                []string
	ChecksumTimeout                         time.Duration
}

type auditAPI struct {
	store *Store
	cfg   Config
	slots chan struct{}
}

type auditError struct {
	Error auditErrorDetail `json:"error"`
}
type auditErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func registerAuditRoutes4(mux *http.ServeMux, store *Store, cfg Config, writeTimeout time.Duration) {
	api := auditAPI{store: store, cfg: cfg, slots: make(chan struct{}, cfg.ReadMaxInFlight)}
	readBudget := cfg.DBReadTimeout
	if readBudget <= 0 {
		readBudget = defaultDBReadTimeout
	}
	register := func(path string, handler http.HandlerFunc, budget time.Duration) {
		mux.Handle("GET /internal/audit/"+path, api.authenticate(withAuditReadBudget(handler, budget, writeTimeout)))
	}
	register("events", api.events, readBudget)
	register("checksum", api.checksum, cfg.Audit.ChecksumTimeout)
	register("location", api.location, readBudget)
	register("unprocessed", api.unprocessed, readBudget)
}

func withAuditReadBudget(next http.HandlerFunc, configuredBudget, writeTimeout time.Duration) http.Handler {
	budget := readWorkBudget(configuredBudget, writeTimeout)
	cleanupWindow := writeTimeout - min(readResponseHeadroom, writeTimeout/4)
	limitRollback := writeTimeout > 0 && cleanupWindow-budget < rollbackTimeout
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := r.Context()
		if limitRollback {
			// A short write timeout cannot fit a full rollback and still leave room to emit JSON.
			ctx = context.WithValue(ctx, rollbackDeadlineKey{}, start.Add(cleanupWindow))
		}
		ctx, cancel := context.WithDeadline(ctx, start.Add(budget))
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (api auditAPI) authenticate(next http.Handler) http.Handler {
	expected := sha256.Sum256([]byte(api.cfg.Audit.Token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		credentials := r.Header.Values("Authorization")
		if len(credentials) == 0 {
			auditWriteError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		if len(credentials) != 1 || api.cfg.Audit.Token == "" {
			auditWriteError(w, http.StatusForbidden, "forbidden", "token tidak valid")
			return
		}
		const prefix = "Bearer "
		credential := credentials[0]
		if len(credential) <= len(prefix) || credential[:len(prefix)] != prefix {
			auditWriteError(w, http.StatusForbidden, "forbidden", "token tidak valid")
			return
		}
		provided := sha256.Sum256([]byte(credential[len(prefix):]))
		if subtle.ConstantTimeCompare(expected[:], provided[:]) != 1 {
			auditWriteError(w, http.StatusForbidden, "forbidden", "token tidak valid")
			return
		}
		select {
		case api.slots <- struct{}{}:
			defer func() { <-api.slots }()
			next.ServeHTTP(w, r)
		default:
			auditWriteError(w, http.StatusTooManyRequests, "too_many_requests", "too many requests")
		}
	})
}

func auditWriteError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(auditError{Error: auditErrorDetail{Code: code, Message: message}})
}

func auditWriteJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func loadAuditConfig(lookup func(string) (string, bool)) (AuditConfig, error) {
	cfg := AuditConfig{ChecksumTimeout: 5 * time.Second}
	names := []string{"BNPB_AUDIT_TOKEN", "BNPB_UNPROCESSED_API_URL", "BNPB_UNPROCESSED_API_TOKEN", "BNPB_STORAGE_COPY_DATABASE_URLS"}
	explicit := false
	values := make(map[string]string, len(names))
	for _, name := range names {
		value, exists := lookup(name)
		if exists {
			explicit = true
		}
		maximum := 8192
		if name == names[3] {
			maximum = 32768
		}
		if len(value) > maximum || strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return AuditConfig{}, fmt.Errorf("%s: value exceeds bounds or contains control characters", name)
		}
		values[name] = value
	}
	cfg.Token = values[names[0]]
	cfg.UnprocessedURL = values[names[1]]
	cfg.UnprocessedToken = values[names[2]]
	if value, exists := lookup("BNPB_AUDIT_CHECKSUM_TIMEOUT"); exists {
		explicit = true
		duration, err := time.ParseDuration(value)
		if err != nil || duration < time.Millisecond || duration > time.Minute {
			return AuditConfig{}, errors.New("BNPB_AUDIT_CHECKSUM_TIMEOUT: duration must be between 1ms and 1m")
		}
		cfg.ChecksumTimeout = duration
	}
	if explicit && strings.TrimSpace(cfg.Token) == "" {
		return AuditConfig{}, errors.New("BNPB_AUDIT_TOKEN: required when audit is configured")
	}
	if cfg.UnprocessedURL != "" {
		parsed, parseErr := url.Parse(cfg.UnprocessedURL)
		if parseErr != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" || parsed.Opaque != "" {
			return AuditConfig{}, errors.New("BNPB_UNPROCESSED_API_URL: invalid absolute HTTP URL")
		}
		if err := validateURLPort(names[1], parsed); err != nil {
			return AuditConfig{}, err
		}
		if strings.TrimSpace(cfg.UnprocessedToken) == "" {
			return AuditConfig{}, errors.New("BNPB_UNPROCESSED_API_TOKEN: required with BNPB_UNPROCESSED_API_URL")
		}
	} else if cfg.UnprocessedToken != "" {
		return AuditConfig{}, errors.New("BNPB_UNPROCESSED_API_URL: required with BNPB_UNPROCESSED_API_TOKEN")
	}
	copyURLs := values[names[3]]
	if copyURLs != "" {
		if len(copyURLs) > 32768 {
			return AuditConfig{}, errors.New("BNPB_STORAGE_COPY_DATABASE_URLS: value exceeds bounds")
		}
		trimmed := strings.TrimSpace(copyURLs)
		if !strings.HasPrefix(trimmed, "[") || json.Unmarshal([]byte(trimmed), &cfg.CopyURLs) != nil || len(cfg.CopyURLs) > 32 {
			return AuditConfig{}, errors.New("BNPB_STORAGE_COPY_DATABASE_URLS: expected a bounded JSON URL list")
		}
		for _, address := range cfg.CopyURLs {
			if len(address) > 4096 || strings.TrimSpace(address) == "" || strings.IndexFunc(address, unicode.IsControl) >= 0 {
				return AuditConfig{}, errors.New("BNPB_STORAGE_COPY_DATABASE_URLS: invalid URL entry")
			}
			if err := validateURL(names[3], address, []string{"postgres", "postgresql"}); err != nil {
				return AuditConfig{}, err
			}
		}
	}
	return cfg, nil
}
