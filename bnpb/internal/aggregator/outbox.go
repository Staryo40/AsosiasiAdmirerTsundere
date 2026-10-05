package aggregator

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

// Publisher adapts a broker to durable, routed acceptance. Nil means acceptance,
// not consumer processing. Implementations must interrupt I/O on cancellation.
// A single dispatcher owns Publish and Close; calls are never concurrent.
type Publisher interface {
	Publish(context.Context, Publication) error
	Close()
}

// Broker adapters expose fixed categories rather than URLs or broker replies.
type publicationError struct{ category string }

func (err *publicationError) Error() string { return "publication failed: " + err.category }

// Publication carries the stored body unchanged across retries and adapters.
type Publication struct {
	MessageID     string    `json:"message_id"`
	HazardID      string    `json:"hazard_id"`
	Revision      int64     `json:"revision"`
	CorrelationID string    `json:"correlation_id"`
	EventType     string    `json:"event_type"`
	SchemaVersion int       `json:"schema_version"`
	ProducedAt    time.Time `json:"produced_at"`
	Payload       []byte    `json:"-"`
}

type outboxMessage struct {
	Publication
	Sequence      int64
	Attempts      int
	NextAttemptAt time.Time
}

const outboxPollInterval = 250 * time.Millisecond

type outboxDispatcher struct {
	store     *Store
	publisher Publisher
	cfg       Config
	logger    *slog.Logger
}

func (dispatcher *outboxDispatcher) run(ctx context.Context) {
	defer dispatcher.publisher.Close()
	failures := 0
	for ctx.Err() == nil {
		delay, err := dispatcher.dispatchPending(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			failures = min(failures+1, 64)
			delay = max(delay, failureDelay(outboxPollInterval, dispatcher.cfg.RetryMaxBackoff, failures, err))
		} else {
			failures = 0
		}
		if !waitPoll(ctx, delay) {
			return
		}
	}
}

func (dispatcher *outboxDispatcher) dispatchPending(ctx context.Context) (time.Duration, error) {
	messages, err := dispatcher.store.pendingOutbox(ctx, dispatcher.cfg.OutboxBatchSize)
	if err != nil {
		dispatcher.logOutcome(ctx, Publication{}, err)
		return 0, err
	}
	for _, message := range messages {
		if delay := time.Until(message.NextAttemptAt); delay > 0 {
			return delay, nil
		}
		if err := dispatcher.dispatchMessage(ctx, message); err != nil {
			return dispatcher.retryMessage(ctx, message, err)
		}
	}
	return outboxPollInterval, nil
}

func (dispatcher *outboxDispatcher) dispatchMessage(ctx context.Context, message outboxMessage) error {
	if err := dispatcher.store.attemptOutbox(ctx, message.Sequence); err != nil {
		return err
	}
	err := dispatcher.publishMessage(ctx, message.Publication)
	if err == nil {
		err = dispatcher.store.sentOutbox(ctx, message.Sequence)
	}
	if err == nil {
		dispatcher.logOutcome(ctx, message.Publication, nil)
	}
	return err
}

func (dispatcher *outboxDispatcher) publishMessage(ctx context.Context, message Publication) error {
	publishCtx, cancel := context.WithTimeout(ctx, dispatcher.cfg.PublishTimeout)
	started := time.Now()
	err := dispatcher.publisher.Publish(publishCtx, message)
	duration := time.Since(started)
	cancel()
	ctx = correlation.WithID(ctx, message.CorrelationID)
	level, category := publicationStatus(err, "broker")
	logging.Event(ctx, dispatcher.logger, logging.Record{Level: level, Event: "broker_publication", Message: "broker publication completed"},
		"message_id", message.MessageID, "hazard_id", message.HazardID, "revision", message.Revision,
		"category", category, "duration_ms", duration.Milliseconds())
	return err
}

func (dispatcher *outboxDispatcher) retryMessage(ctx context.Context, message outboxMessage, failure error) (time.Duration, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	delay := failureDelay(outboxPollInterval, dispatcher.cfg.RetryMaxBackoff, min(message.Attempts, 63)+1, failure)
	err := dispatcher.store.retryOutbox(ctx, message.Sequence, time.Now().Add(delay))
	dispatcher.logOutcome(ctx, message.Publication, failure)
	return delay, errors.Join(failure, err)
}

func (dispatcher *outboxDispatcher) logOutcome(ctx context.Context, message Publication, err error) {
	ctx = correlation.WithID(ctx, message.CorrelationID)
	level, category := publicationStatus(err, "store")
	logging.Event(ctx, dispatcher.logger, logging.Record{Level: level, Event: "outbox_publication", Message: "outbox publication completed"},
		"message_id", message.MessageID, "hazard_id", message.HazardID, "revision", message.Revision, "category", category)
}

func publicationStatus(err error, defaultCategory string) (slog.Level, string) {
	if err == nil {
		return slog.LevelInfo, ""
	}
	var publicationErr *publicationError
	if errors.As(err, &publicationErr) {
		return slog.LevelWarn, publicationErr.category
	}
	return slog.LevelWarn, defaultCategory
}
