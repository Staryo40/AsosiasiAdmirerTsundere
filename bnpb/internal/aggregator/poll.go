package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

type poller struct {
	cfg                  Config
	store                *Store
	sources              sourceClients
	logger               *slog.Logger
	reader               readerSession
	lastWarningReconcile time.Time
}

func (app application) pollWorkers(cfg Config, logger *slog.Logger) []func(context.Context) {
	poller := &poller{cfg: cfg, store: app.store, sources: app.sources, logger: logger,
		lastWarningReconcile: time.Now(), reader: readerSession{client: app.sources.pvmbg,
			clientID: cfg.PVMBGReaderClientID, clientSecret: cfg.PVMBGReaderClientSecret}}
	workers := make([]func(context.Context), 0, 3)
	for _, endpoint := range []Endpoint{SeismicEndpoint, WarningEndpoint, VolcanicEndpoint} {
		interval := cfg.BMKG.PollInterval
		if endpoint == VolcanicEndpoint {
			interval = cfg.PVMBG.PollInterval
		}
		workers = append(workers, func(ctx context.Context) {
			runPollLoop(ctx, interval, cfg.RetryMaxBackoff, func(ctx context.Context) error {
				return poller.pollEndpoint(ctx, endpoint)
			})
		})
	}
	return workers
}

// Schedule from completion, so a slow endpoint cannot accumulate ticks or fetches.
func runPollLoop(ctx context.Context, interval, maximum time.Duration, poll func(context.Context) error) {
	failures := 0
	for ctx.Err() == nil {
		err := poll(ctx)
		if ctx.Err() != nil {
			return
		}
		delay := interval
		if err == nil {
			failures = 0
		} else {
			failures = min(failures+1, 64)
			delay = failureDelay(interval, maximum, failures, err)
		}
		if !waitPoll(ctx, delay) {
			return
		}
	}
}

func waitPoll(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func failureDelay(interval, maximum time.Duration, failures int, err error) time.Duration {
	ceiling := min(interval, maximum)
	for attempt := 1; attempt < failures && ceiling < maximum; attempt++ {
		ceiling = min(2*ceiling, maximum)
	}
	delay := ceiling/2 + time.Duration(rand.Int64N(int64(ceiling-ceiling/2)+1))
	var failure *sourceError
	if errors.As(err, &failure) {
		// Retry-After is an upstream lower bound, even when above our backoff cap.
		delay = max(delay, failure.retryAfter)
	}
	return delay
}

func (poller *poller) pollEndpoint(ctx context.Context, endpoint Endpoint) error {
	ctx = correlation.WithID(ctx, correlation.NewID())
	started := time.Now().UTC()
	err := poller.pollOnce(ctx, endpoint, started)
	if ctx.Err() != nil {
		return err
	}
	category, level := "", slog.LevelInfo
	if err != nil {
		category, level = pollCategory(err), slog.LevelWarn
		if stateErr := poller.store.recordPollFailure(ctx, endpoint, started, category); stateErr != nil {
			logging.Event(ctx, poller.logger, logging.Record{Level: slog.LevelError, Event: "poll_state_failed", Message: "could not persist poll failure"}, "endpoint", string(endpoint), "category", "store")
		}
	}
	logging.Event(ctx, poller.logger, logging.Record{Level: level, Event: "agency_poll", Message: "agency poll completed"},
		"endpoint", string(endpoint), "category", category, "duration_ms", time.Since(started).Milliseconds())
	return err
}

func (poller *poller) pollOnce(ctx context.Context, endpoint Endpoint, started time.Time) error {
	checkpoint, err := poller.store.pollCheckpoint(ctx, endpoint)
	if err != nil {
		return err
	}
	if err := poller.store.recordPollAttempt(ctx, endpoint, started); err != nil {
		return err
	}
	query := pollQuery(checkpoint, poller.cfg.PollOverlap, started, endpoint)
	reconcile := endpoint == WarningEndpoint && started.Sub(poller.lastWarningReconcile) >= poller.cfg.WarningReconcileInterval
	if reconcile {
		query = nil
	}
	records, err := poller.fetch(ctx, endpoint, query)
	if err != nil {
		return err
	}
	if err := poller.ingestResponse(ctx, IngestChunk{Endpoint: endpoint, Records: records, ReceivedAt: time.Now().UTC()}, started); err != nil {
		return err
	}
	if reconcile || (endpoint == WarningEndpoint && checkpoint == nil) {
		poller.lastWarningReconcile = started
	}
	return nil
}

func pollQuery(checkpoint *time.Time, overlap time.Duration, started time.Time, endpoint Endpoint) url.Values {
	if checkpoint == nil {
		return nil
	}
	boundary := *checkpoint
	if endpoint == WarningEndpoint && boundary.After(started) {
		boundary = started
	}
	return url.Values{"since": {boundary.Add(-overlap).UTC().Format(time.RFC3339Nano)}}
}

func (poller *poller) fetch(ctx context.Context, endpoint Endpoint, query url.Values) ([]json.RawMessage, error) {
	if endpoint == VolcanicEndpoint {
		ctx, cancel := context.WithTimeout(ctx, poller.cfg.PVMBGCycleTimeout)
		defer cancel()
		return poller.reader.reports(ctx, query)
	}
	path := "/" + strings.ReplaceAll(publicEndpoint(endpoint), "_", "-")
	return poller.sources.bmkg.fetchRecords(ctx, sourceRequest{method: "GET", path: path, query: query})
}

type orderedRecord struct {
	raw       json.RawMessage
	timestamp time.Time
	identity  string
}

func orderRecords(records []json.RawMessage, endpoint Endpoint) ([]orderedRecord, bool) {
	ordered := make([]orderedRecord, 0, len(records))
	hold := false
	for _, raw := range records {
		record, usable := orderedSourceRecord(raw, endpoint)
		hold = hold || !usable
		ordered = append(ordered, record)
	}
	slices.SortStableFunc(ordered, func(a, b orderedRecord) int {
		if ordering := a.timestamp.Compare(b.timestamp); ordering != 0 {
			return ordering
		}
		return strings.Compare(a.identity, b.identity)
	})
	return ordered, hold
}

func orderedSourceRecord(raw json.RawMessage, endpoint Endpoint) (orderedRecord, bool) {
	record := orderedRecord{raw: raw}
	fields, err := parseObject(raw)
	if err != nil {
		return record, false
	}
	identity, identityErr := textField(fields, ingestEndpoints[endpoint].identity)
	record.identity = identity
	stamp, stampErr := timeField(fields, ingestEndpoints[endpoint].timestamp)
	if stampErr == nil {
		record.timestamp = databaseTime(stamp)
	}
	return record, identityErr == nil && stampErr == nil
}

func (poller *poller) ingestResponse(ctx context.Context, response IngestChunk, started time.Time) error {
	ordered, hold := orderRecords(response.Records, response.Endpoint)
	policy := checkpointPolicy{hold: hold, started: &started}
	if response.Endpoint == WarningEndpoint {
		ceiling := databaseTime(started)
		policy.ceiling = &ceiling
	}
	return poller.ingestOrderedResponse(ctx, response, ordered, policy)
}

func (poller *poller) ingestOrderedResponse(ctx context.Context, response IngestChunk, ordered []orderedRecord, policy checkpointPolicy) error {
	// An empty array still commits a successful observation without moving the cursor.
	for offset := 0; ; {
		end := min(offset+poller.cfg.IngestChunkRecords, len(ordered))
		chunk := responseChunk(response, ordered[offset:end])
		policy.completePoll = end == len(ordered)
		if policy.completePoll {
			completed := time.Now().UTC()
			policy.completed = &completed
		}
		if err := poller.store.ingestChunk(ctx, chunk, policy); err != nil {
			return err
		}
		if policy.completePoll {
			return nil
		}
		offset = end
	}
}

func responseChunk(response IngestChunk, ordered []orderedRecord) IngestChunk {
	chunk := IngestChunk{Endpoint: response.Endpoint, ReceivedAt: response.ReceivedAt}
	for _, record := range ordered {
		chunk.Records = append(chunk.Records, record.raw)
	}
	return chunk
}

func pollCategory(err error) string {
	var failure *sourceError
	if !errors.As(err, &failure) {
		return "store"
	}
	switch failure.statusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "auth"
	case http.StatusTooManyRequests:
		return "rate_limited"
	}
	if failure.statusCode >= 500 {
		return "outage"
	}
	switch failure.category {
	case "timeout":
		return "timeout"
	case "transport_error", "read_error":
		return "network"
	default:
		return "invalid_response"
	}
}
