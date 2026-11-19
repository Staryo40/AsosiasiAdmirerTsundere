package aggregator

import (
	"context"
	"log/slog"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

func (app application) pantaiWorker(cfg Config, client *pantaiClient, logger *slog.Logger) func(context.Context) {
	if client == nil || !client.configured() {
		return func(context.Context) {}
	}
	worker := pantaiPoller{cfg: cfg, store: app.store, client: client, logger: logger}
	return func(ctx context.Context) {
		runPollLoop(ctx, cfg.Pantai.PollInterval, cfg.RetryMaxBackoff, worker.poll)
	}
}

type pantaiPoller struct {
	cfg    Config
	store  *Store
	client *pantaiClient
	logger *slog.Logger
}

func (worker *pantaiPoller) poll(ctx context.Context) error {
	ctx = correlation.WithID(ctx, correlation.NewID())
	started := time.Now().UTC()
	err := worker.pollOnce(ctx, started)
	if ctx.Err() != nil {
		return err
	}
	category, level := "", slog.LevelInfo
	if err != nil {
		category, level = pollCategory(err), slog.LevelWarn
		if stateErr := worker.store.recordPollFailure(ctx, CoastalEndpoint, started, category); stateErr != nil {
			logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelError, Event: "poll_state_failed", Message: "could not persist poll failure"},
				"endpoint", string(CoastalEndpoint), "category", "store")
		}
	}
	logging.Event(ctx, worker.logger, logging.Record{Level: level, Event: "pantai_poll", Message: "coastal poll completed"},
		"endpoint", string(CoastalEndpoint), "category", category, "breaker", worker.client.breakerState(),
		"duration_ms", time.Since(started).Milliseconds())
	return err
}

func (worker *pantaiPoller) pollOnce(ctx context.Context, started time.Time) error {
	position, err := worker.store.pollPosition(ctx, CoastalEndpoint)
	if err != nil {
		return err
	}
	previous := endpointObservation(endpointSnapshot{endpoint: CoastalEndpoint, pollObservation: position.observation}, started, worker.cfg)
	if err := worker.store.recordPollAttempt(ctx, CoastalEndpoint, started, previous); err != nil {
		return err
	}
	cursor, err := worker.store.pollResumeToken(ctx, CoastalEndpoint)
	if err != nil {
		return err
	}
	response, err := worker.client.observations(ctx, cursor)
	if err != nil {
		return err
	}
	if len(response.Items) == 0 {
		return worker.store.completeCoastalPoll(ctx, CoastalEndpoint, started, response.NextCursor)
	}
	chunk := IngestChunk{Endpoint: CoastalEndpoint, Records: response.Items, ReceivedAt: time.Now().UTC()}
	if err := worker.ingestCoastal(ctx, chunk, response, started); err != nil {
		return err
	}
	worker.logRecovery(ctx, previous)
	return nil
}

func (worker *pantaiPoller) ingestCoastal(ctx context.Context, chunk IngestChunk, response pantaiObservations, started time.Time) error {
	ordered, hold := orderRecords(chunk.Records, chunk.Endpoint)
	policy := checkpointPolicy{hold: hold, started: &started, completePoll: true, completed: &started}
	for offset := 0; ; {
		end := min(offset+worker.cfg.IngestChunkRecords, len(ordered))
		part := responseChunk(chunk, ordered[offset:end])
		if end == len(ordered) {
			completed := time.Now().UTC()
			policy.completed = &completed
		}
		if err := worker.store.ingestCoastalChunk(ctx, part, response, worker.client, policy); err != nil {
			return err
		}
		if end == len(ordered) {
			return worker.store.completeCoastalPoll(ctx, CoastalEndpoint, started, response.NextCursor)
		}
		offset = end
	}
}

func (worker *pantaiPoller) logRecovery(ctx context.Context, previous pollObservation) {
	if previous.Status == "healthy" {
		return
	}
	logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelInfo, Event: "pantai_poll_recovered", Message: "coastal endpoint recovered"},
		"endpoint", string(CoastalEndpoint), "previous_error_category", previous.ErrorCategory)
}