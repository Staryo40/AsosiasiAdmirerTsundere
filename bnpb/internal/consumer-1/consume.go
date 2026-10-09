package consumer1

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/amqppublish"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

const (
	exchange            = "hazard.events"
	maxDeliveryAttempts = 3
	prefetchCount       = 1
	pruneInterval       = 30 * time.Minute
)

var reconnectBackoff = []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 15 * time.Second}

// envelope is the consumer side of contracts/v1/schemas/hazard-notification.schema.json.
type envelope struct {
	MessageID     string `json:"message_id"`
	HazardID      string `json:"hazard_id"`
	Revision      int64  `json:"revision"`
	CorrelationID string `json:"correlation_id"`
	EventType     string `json:"event_type"`
	HazardEvent   struct {
		Source     string    `json:"source"`
		HazardType string    `json:"hazard_type"`
		Severity   string    `json:"severity"`
		AreaName   string    `json:"area_name"`
		OccurredAt time.Time `json:"occurred_at"`
		IngestedAt time.Time `json:"ingested_at"`
	} `json:"hazard_event"`
}

type consumer struct {
	cfg              Config
	store            *Store
	logger           *slog.Logger
	publisher        *consumerSession
	transferFailures int
}

// run owns the AMQP connection for the process lifetime: connect, declare,
// consume, reconnect on loss, until ctx is canceled.
func run(ctx context.Context, cfg Config, store *Store, logger *slog.Logger) {
	pruned := make(chan struct{})
	go func() { defer close(pruned); pruneLoop(ctx, store, logger) }()
	defer func() { <-pruned }()
	worker := &consumer{cfg: cfg, store: store, logger: logger}
	defer worker.closePublisher()
	for attempt := 0; ctx.Err() == nil; attempt++ {
		if err := worker.consumeOnce(ctx); err != nil && ctx.Err() == nil {
			logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "broker_connection_lost", Message: "AMQP connection or channel closed"}, "category", "broker")
		}
		if ctx.Err() != nil {
			return
		}
		if waitBackoff(ctx, backoffFor(attempt)) != nil {
			return
		}
	}
}

func backoffFor(attempt int) time.Duration {
	if attempt >= len(reconnectBackoff) {
		return reconnectBackoff[len(reconnectBackoff)-1]
	}
	return reconnectBackoff[attempt]
}

func (worker *consumer) consumeOnce(ctx context.Context) error {
	ctx = correlation.WithID(ctx, correlation.NewID())
	session, err := worker.openSession(ctx)
	if err != nil {
		return err
	}
	defer session.close()
	stop := amqppublish.Interrupt(ctx, session.socket)
	defer stop()
	return worker.receive(ctx, session)
}
func (worker *consumer) receive(ctx context.Context, session *consumerSession) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case amqpErr := <-session.closed:
			if amqpErr != nil {
				return amqpErr
			}
			return nil
		case delivery, ok := <-session.deliveries:
			if !ok {
				return nil
			}
			if err := worker.handleDelivery(ctx, session, delivery); err != nil {
				return err
			}
		}
	}
}

func (worker *consumer) handleDelivery(ctx context.Context, session *consumerSession, delivery amqp.Delivery) error {
	// The AMQP message-id property, not the JSON body's own message_id
	// field: it is set by the publisher independently of the body, so
	// attempt-counting and dedupe still work even if the body fails to
	// parse at all (otherwise every unparseable message would share one
	// empty-string identity and loop forever).
	messageID := delivery.MessageId
	var env envelope
	var applied ApplyResult
	err := json.Unmarshal(delivery.Body, &env)
	ctx = correlation.MessageContext(ctx, delivery.CorrelationId, env.CorrelationID)
	logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelInfo, Event: "message_received", Message: "broker delivery received"}, "message_id", messageID)
	if err == nil {
		applied, err = worker.applyEnvelope(ctx, messageID, env)
	}

	switch {
	case errors.Is(err, errDuplicateMessage):
		logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelInfo, Event: "message_duplicate_skipped", Message: "message_id already processed"}, "message_id", messageID)
	case err == nil:
		worker.logApplied(ctx, env, applied)
	default:
		return worker.handleFailure(ctx, session, delivery, err)
	}
	return worker.acknowledge(ctx, session, delivery)
}

func pruneLoop(ctx context.Context, store *Store, logger *slog.Logger) {
	ticker := time.NewTicker(pruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := store.PruneOldRecords(ctx); err != nil {
				logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "prune_failed", Message: "could not prune old dedupe records"}, "error", err.Error())
			}
		}
	}
}

func waitBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (worker *consumer) applyEnvelope(ctx context.Context, messageID string, env envelope) (ApplyResult, error) {
	return worker.store.Apply(ctx, HazardUpdate{
		MessageID:  messageID,
		HazardID:   env.HazardID,
		Source:     env.HazardEvent.Source,
		HazardType: env.HazardEvent.HazardType,
		Severity:   env.HazardEvent.Severity,
		AreaName:   env.HazardEvent.AreaName,
		OccurredAt: env.HazardEvent.OccurredAt,
		IngestedAt: env.HazardEvent.IngestedAt,
		Revision:   env.Revision,
	})
}

func (worker *consumer) logApplied(ctx context.Context, env envelope, applied ApplyResult) {
	if applied.Applied {
		logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelInfo, Event: "dashboard_update_recorded", Message: "hazard state updated"}, "hazard_id", env.HazardID, "revision", env.Revision)
	} else {

		logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelInfo, Event: "stale_update_ignored", Message: "newest-wins guard kept the existing state"}, "hazard_id", env.HazardID, "revision", env.Revision)
	}
}
