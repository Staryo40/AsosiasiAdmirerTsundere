
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

type pollReceipt struct {
	response           IngestChunk
	started            time.Time
	holdCheckpoint     bool
	reconcilesWarnings bool
}

func (app application) pollWorkers(cfg Config, logger *slog.Logger) []func(context.Context) {
	poller := &poller{cfg: cfg, store: app.store, sources: app.sources, logger: logger,
		lastWarningReconcile: time.Now(), reader: readerSession{client: app.sources.pvmbg,
			clientID: cfg.PVMBGReaderClientID, clientSecret: cfg.PVMBGReaderClientSecret}}
	workers := make([]func(context.Context), 0, 3)
	for _, endpoint := range []Endpoint{SeismicEndpoint, WarningEndpoint, VolcanicEndpoint} {
		workers = append(workers, poller.endpointWorker(pollInterval(cfg, endpoint), func(ctx context.Context) error {
			return poller.pollEndpoint(ctx, endpoint)
		}))
	}
	return workers
}

func (poller *poller) endpointWorker(interval time.Duration, poll func(context.Context) error) func(context.Context) {
	return func(ctx context.Context) {
		// After handoff, leave one full local interval between activation and fresh dispatch.
		if poller.store.shards[0] != nil && !waitPoll(ctx, interval) {
			return
		}
		runPollLoop(ctx, interval, poller.cfg.RetryMaxBackoff, poll)
	}
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
	poller.trackUnreadableResponse(ctx, endpoint, started, err)
	fields := []any{"endpoint", string(endpoint), "period", pollPeriod(poller.cfg, endpoint, started),
		"category", category, "duration_ms", time.Since(started).Milliseconds()}
	if owner, epoch, ok := pollAuthorityToken(ctx); ok {
		fields = append(fields, "owner", owner, "epoch", epoch)
	}
	if lease, ok := pollLeaseFromContext(ctx); ok {
		fields = append(fields, "lease_holder", lease.holder, "lease_generation", lease.generation)
	}
	logging.Event(ctx, poller.logger, logging.Record{Level: level, Event: "agency_poll", Message: "agency poll completed"}, fields...)
	return err
}

func (poller *poller) trackUnreadableResponse(ctx context.Context, endpoint Endpoint, started time.Time, pollErr error) {
	var failure *sourceError
	var err error
	switch {
	case pollErr == nil:
		err = poller.store.resolveUnreadableResponses(ctx, endpoint)
	case errors.As(pollErr, &failure) && failure.payload != nil:
		err = poller.store.recordUnreadableResponse(ctx, endpoint, started, failure)
	default:
		return
	}
	if err != nil {
		logging.Event(ctx, poller.logger, logging.Record{Level: slog.LevelError, Event: "failed_message_state_failed", Message: "could not update unreadable response state"}, "endpoint", string(endpoint), "category", "store")
	}
}

func (poller *poller) pollOnce(ctx context.Context, endpoint Endpoint, started time.Time) error {
	position, err := poller.store.pollPosition(ctx, endpoint)
	if err != nil {
		return err
	}
	previous := endpointObservation(endpointSnapshot{endpoint: endpoint, pollObservation: position.observation}, started, poller.cfg)
	if err := poller.store.recordPollAttempt(ctx, endpoint, started, previous); err != nil {
		return err
	}
	if err := poller.importEndpoint(ctx, endpoint, position.checkpoint, started); err != nil {
		return err
	}
	poller.logRecovery(ctx, endpoint, previous)
	return nil
}

func (poller *poller) importEndpoint(ctx context.Context, endpoint Endpoint, checkpoint *time.Time, started time.Time) error {
	receipt, err := poller.nextPollReceipt(ctx, endpoint, checkpoint, started)
	if err != nil {
		return err
	}
	if err := poller.ingestResponse(ctx, receipt); err != nil {
		return err
	}
	if receipt.reconcilesWarnings {
		poller.lastWarningReconcile = receipt.started
	}
	if poller.store.shards[0] != nil {
		return poller.store.clearPollReceipt(ctx, receipt.response)
	}
	return nil
}

func (poller *poller) nextPollReceipt(ctx context.Context, endpoint Endpoint, checkpoint *time.Time, started time.Time) (pollReceipt, error) {
	routed := poller.store.shards[0] != nil
	if routed {
		receipt, err := poller.store.pendingPollReceipt(ctx, endpoint)
		if err != nil || receipt.response.InputID != "" {
			return receipt, err
		}
	}
	receipt, err := poller.fetchPollReceipt(ctx, endpoint, checkpoint, started)
	if err != nil || !routed {
		return receipt, err
	}
	return receipt, poller.store.stagePollReceipt(ctx, receipt)
}

func (poller *poller) fetchPollReceipt(ctx context.Context, endpoint Endpoint, checkpoint *time.Time, started time.Time) (pollReceipt, error) {
	query := pollQuery(checkpoint, poller.cfg.PollOverlap, started, endpoint)
	if endpoint == WarningEndpoint && started.Sub(poller.lastWarningReconcile) >= poller.cfg.WarningReconcileInterval {
		query = nil
	}
	requestCtx, err := poller.pollRequestContext(ctx, endpoint, started)
	if err != nil {
		return pollReceipt{}, err
	}
	records, err := poller.fetch(requestCtx, endpoint, query)
	if err != nil {
		return pollReceipt{}, err
	}
	received := time.Now().UTC()
	ordered, hold := orderRecords(records, endpoint)
	// Persist the assigned ordinals, not a sort that a replacement could reinterpret.
	for index, record := range ordered {
		records[index] = record.raw
	}
	return pollReceipt{
		response: IngestChunk{Endpoint: endpoint, Records: records, ReceivedAt: received, InputID: correlation.NewID()},
		started:  started, holdCheckpoint: hold, reconcilesWarnings: endpoint == WarningEndpoint && query == nil,
	}, nil
}

func (poller *poller) logRecovery(ctx context.Context, endpoint Endpoint, previous pollObservation) {
	if previous.Status == "healthy" {
		return
	}
	logging.Event(ctx, poller.logger, logging.Record{Level: slog.LevelInfo, Event: "agency_poll_recovered", Message: "agency endpoint recovered"},
		"endpoint", string(endpoint), "status", "healthy", "previous_status", previous.Status,
		"previous_error_category", previous.ErrorCategory, "stale_since", previous.StaleSince)
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

func (poller *poller) pollRequestContext(ctx context.Context, endpoint Endpoint, started time.Time) (context.Context, error) {
	metadata := pollRequestMetadata{endpoint: endpoint, period: pollPeriod(poller.cfg, endpoint, started)}
	if poller.store.shards[0] != nil {
		if _, _, ok := pollAuthorityToken(ctx); !ok {
			return nil, errStaleMutationAuthority
		}
		metadata.authority = poller.store.authority
		metadata.authorityTimeout = poller.store.writeTimeout
	}
	return context.WithValue(ctx, pollRequestMetadataKey{}, metadata), nil
}

func pollInterval(cfg Config, endpoint Endpoint) time.Duration {
	if endpoint == VolcanicEndpoint {
		return cfg.PVMBG.PollInterval
	}
	return cfg.BMKG.PollInterval
}

func pollPeriod(cfg Config, endpoint Endpoint, started time.Time) string {
	return started.UTC().Truncate(pollInterval(cfg, endpoint)).Format(time.RFC3339Nano)
}

func (poller *poller) fetch(ctx context.Context, endpoint Endpoint, query url.Values) ([]json.RawMessage, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
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

func (poller *poller) ingestResponse(ctx context.Context, receipt pollReceipt) error {
	response := receipt.response
	policy := checkpointPolicy{hold: receipt.holdCheckpoint, started: &receipt.started}
	if response.Endpoint == WarningEndpoint {
		ceiling := databaseTime(receipt.started)
		policy.ceiling = &ceiling
	}
	// An empty response still commits a successful poll observation.
	for offset := 0; ; {
		end := min(offset+poller.cfg.IngestChunkRecords, len(response.Records))
		chunk := response
		chunk.Records = response.Records[offset:end]
		chunk.InputOffset += offset
		policy.completePoll = end == len(response.Records)
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

func pollCategory(err error) string {
	var failure *sourceError
	if errors.Is(err, errStalePollLease) {
		return "lease_lost"
	}
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
	case "circuit_open", "bulkhead_rejected":
		return failure.category
	case "transport_error", "read_error":
		return "network"
	default:
		return "invalid_response"
	}
}
