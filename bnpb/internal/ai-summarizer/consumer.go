package aisummarizer

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/config"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/service"
)

const (
	internalKeyHeader = "X-BNPB-Internal-Key"
	firstSummaryDelay = 10 * time.Second
)

func Initialize(ctx context.Context, mux *http.ServeMux, _ config.Config, logger *slog.Logger) (service.Shutdown, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}
	store, err := OpenStore(ctx, cfg.DBPath)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: callTimeout}
	var providers []provider
	for _, settings := range cfg.Providers {
		if settings.Key == "" || settings.Model == "" {
			logging.Event(ctx, logger, logging.Record{Level: slog.LevelWarn, Event: "provider_skipped", Message: "provider has no key or model and is left out"}, "provider", settings.Name)
			continue
		}
		providers = append(providers, newProvider(settings, client))
	}
	summarizer := &summarizer{cfg: cfg, store: store, chain: newChain(providers, cfg.Fault), logger: logger, now: time.Now}
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "bnpb_summary_age_seconds",
		Help: "Seconds since the latest situation summary was written.",
	}, summarizer.age)

	mux.HandleFunc("GET /internal/situation-summary", summarizer.internal(summarizer.handleLatest))
	mux.HandleFunc("POST /internal/summarize", summarizer.internal(summarizer.handleSummarize))
	mux.HandleFunc("POST /internal/fault", summarizer.internal(summarizer.handleFault))

	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); run(ctx, cfg, store, logger) }()
	go func() { defer workers.Done(); summarizer.loop(ctx) }()
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()

	return func(shutdownCtx context.Context) error {
		cancel()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			go func() { <-done; _ = store.Close() }()
			return shutdownCtx.Err()
		}
		return store.Close()
	}, nil
}

type summarizer struct {
	cfg    Config
	store  *Store
	chain  *chain
	logger *slog.Logger
	now    func() time.Time

	mu        sync.Mutex
	latest    time.Time
	attempts  []attempt
	summaryMu sync.Mutex
}

func (s *summarizer) loop(ctx context.Context) {
	if waitBackoff(ctx, firstSummaryDelay) != nil {
		return
	}
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		if _, err := s.refresh(ctx, false); err != nil && ctx.Err() == nil {
			logging.Event(ctx, s.logger, logging.Record{Level: slog.LevelError, Event: "summary_failed", Message: "could not write a situation summary"}, "category", "store")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// refresh writes a new summary when the events changed, when the last one is a template, or when forced.
func (s *summarizer) refresh(ctx context.Context, force bool) (Summary, error) {
	s.summaryMu.Lock()
	defer s.summaryMu.Unlock()
	now := s.now()
	events, err := s.store.Recent(ctx, now.Add(-s.cfg.Window))
	if err != nil {
		return Summary{}, err
	}
	digest := buildDigest(events, s.cfg.Window, now)
	previous, found, err := s.store.Latest(ctx)
	if err != nil {
		return Summary{}, err
	}
	if !force && found && previous.DigestHash == digest.hash() && previous.Provider != templateName {
		s.markWritten(previous.GeneratedAt, nil)
		return previous, nil
	}
	summary, attempts := s.chain.summarize(ctx, digest)
	if err := s.store.SaveSummary(ctx, summary); err != nil {
		return Summary{}, err
	}
	s.markWritten(summary.GeneratedAt, attempts)
	logging.Event(ctx, s.logger, logging.Record{Level: slog.LevelInfo, Event: "summary_written", Message: "situation summary written"},
		"provider", summary.Provider, "model", summary.Model, "event_count", summary.EventCount, "attempts", attempts)
	return summary, nil
}

func (s *summarizer) markWritten(at time.Time, attempts []attempt) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest = at
	if attempts != nil {
		s.attempts = attempts
	}
}

func (s *summarizer) age() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest.IsZero() {
		return 0
	}
	return s.now().Sub(s.latest).Seconds()
}

type summaryResponse struct {
	Summary       string    `json:"summary"`
	Provider      string    `json:"provider"`
	Model         string    `json:"model,omitempty"`
	AIGenerated   bool      `json:"ai_generated"`
	GeneratedAt   time.Time `json:"generated_at"`
	AgeSeconds    int       `json:"age_seconds"`
	EventCount    int       `json:"event_count"`
	WindowMinutes int       `json:"window_minutes"`
	Fault         string    `json:"fault,omitempty"`
	LastAttempts  []attempt `json:"last_attempts"`
}

func (s *summarizer) response(summary Summary) summaryResponse {
	s.mu.Lock()
	attempts := append([]attempt{}, s.attempts...)
	s.mu.Unlock()
	return summaryResponse{
		Summary: summary.Text, Provider: summary.Provider, Model: summary.Model, AIGenerated: summary.Provider != templateName,
		GeneratedAt: summary.GeneratedAt.UTC(), AgeSeconds: max(int(s.now().Sub(summary.GeneratedAt).Seconds()), 0),
		EventCount: summary.EventCount, WindowMinutes: int(s.cfg.Window.Minutes()), Fault: s.chain.currentFault(), LastAttempts: attempts,
	}
}

func (s *summarizer) handleLatest(w http.ResponseWriter, request *http.Request) {
	summary, found, err := s.store.Latest(request.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", "summary store is unavailable")
		return
	}
	if !found {
		writeError(w, http.StatusServiceUnavailable, "summary_not_ready", "no situation summary has been written yet")
		return
	}
	writeJSON(w, http.StatusOK, s.response(summary))
}

func (s *summarizer) handleSummarize(w http.ResponseWriter, request *http.Request) {
	summary, err := s.refresh(request.Context(), true)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", "summary store is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, s.response(summary))
}

func (s *summarizer) handleFault(w http.ResponseWriter, request *http.Request) {
	var body struct {
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, request.Body, 1024)).Decode(&body); err != nil || !validFault(body.Mode) {
		writeError(w, http.StatusBadRequest, "invalid_fault", "mode must be empty, slow, wrong, or down")
		return
	}
	s.chain.setFault(body.Mode)
	logging.Event(request.Context(), s.logger, logging.Record{Level: slog.LevelWarn, Event: "fault_set", Message: "summary fault mode changed"}, "fault", body.Mode)
	writeJSON(w, http.StatusOK, map[string]string{"fault": body.Mode})
}

func (s *summarizer) internal(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		if subtle.ConstantTimeCompare([]byte(request.Header.Get(internalKeyHeader)), []byte(s.cfg.InternalKey)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid internal key")
			return
		}
		next(w, request)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]map[string]string{"error": {"code": code, "message": message}})
}
