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
	store         *Store
}

const outboxPollInterval = 250 * time.Millisecond

type outboxDispatcher struct {
	store     *Store
	publisher Publisher
	cfg       Config
	logger    *slog.Logger
	nextShard int
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
	if dispatcher.store == nil {
		return outboxPollInterval, errors.New("outbox: store unavailable")
	}
	if dispatcher.store.shards[0] != nil {
		if dispatcher.store.authority == nil {
			return outboxPollInterval, errors.New("outbox: owner authority unavailable")
		}
		var err error
		ctx, err = dispatcher.store.authority.currentContext(ctx)
		if err != nil {
			return outboxPollInterval, err
		}
	}
	messages, readErr := dispatcher.pendingMessages(ctx)
	if readErr != nil {
		dispatcher.logOutcome(ctx, Publication{}, readErr)
	}
	now := time.Now()
	delay := outboxPollInterval
	var firstFailure error
	var blocked [logicalShardCount]*Store
	blockedCount := 0
	for _, message := range messages {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		isBlocked := false
		for _, store := range blocked[:blockedCount] {
			if store == message.store {
				isBlocked = true
				break
			}
		}
		if isBlocked {
			continue
		}
		if wait := outboxWait(message, now, dispatcher.cfg.RetryMaxBackoff); wait > 0 {
			delay = minPositiveDelay(delay, wait)
			blocked[blockedCount] = message.store
			blockedCount++
			continue
		}
		if err := dispatcher.dispatchMessage(ctx, message); err != nil {
			retryDelay, retryErr := dispatcher.retryMessage(ctx, message, err)
			delay = minPositiveDelay(delay, retryDelay)
			blocked[blockedCount] = message.store
			blockedCount++
			if firstFailure == nil {
				firstFailure = retryErr
			}
		}
	}
	return delay, errors.Join(readErr, firstFailure)
}

func (dispatcher *outboxDispatcher) pendingMessages(ctx context.Context) ([]outboxMessage, error) {
	if dispatcher.store.shards[0] == nil {
		messages, err := dispatcher.store.pendingOutbox(ctx, dispatcher.cfg.OutboxBatchSize)
		if err != nil {
			return nil, err
		}
		return messages, nil
	}
	shards := dispatcher.store.logicalReadStores()
	heads := fanoutLogicalReads(ctx, shards, func(ctx context.Context, shard logicalReadStore) ([]outboxMessage, error) {
		return shard.Store.pendingOutbox(ctx, dispatcher.cfg.OutboxBatchSize)
	})
	var readErr error
	for _, head := range heads {
		readErr = errors.Join(readErr, head.Err)
	}
	startShard := dispatcher.nextShard
	dispatcher.nextShard = (startShard + 1) % len(heads)
	return roundRobinOutboxHeads(heads, startShard, dispatcher.cfg.OutboxBatchSize), readErr
}

// Skip an unavailable shard as a whole so its later rows cannot overtake its head.
func roundRobinOutboxHeads(heads []logicalReadResult[[]outboxMessage], startShard, limit int) []outboxMessage {
	batch := make([]outboxMessage, 0, limit)
	for depth := 0; len(batch) < limit; depth++ {
		initialLength := len(batch)
		for offset := range heads {
			head := heads[(startShard+offset)%len(heads)]
			if head.Err != nil || depth >= len(head.Value) {
				continue
			}
			batch = append(batch, head.Value[depth])
			if len(batch) == limit {
				return batch
			}
		}
		if len(batch) == initialLength {
			break
		}
	}
	return batch
}

func minPositiveDelay(current, candidate time.Duration) time.Duration {
	if candidate <= 0 {
		return current
	}
	return min(current, candidate)
}

func outboxWait(message outboxMessage, now time.Time, maxBackoff time.Duration) time.Duration {
	if message.Attempts == 0 {
		return 0
	}
	wait := message.NextAttemptAt.Sub(now)
	if wait <= 0 || wait > maxBackoff {
		return 0
	}
	return wait
}

func (dispatcher *outboxDispatcher) dispatchMessage(ctx context.Context, message outboxMessage) error {
	ctx = correlation.WithID(ctx, message.CorrelationID)
	if message.store == nil {
		return errors.New("outbox: owning store unavailable")
	}
	if err := message.store.attemptOutbox(ctx, message.Sequence); err != nil {
		return err
	}
	err := dispatcher.publishMessage(ctx, message.Publication)
	if err == nil {
		err = message.store.sentOutbox(ctx, message.Sequence)
	}
	if err == nil {
		dispatcher.logOutcome(ctx, message.Publication, nil)
	}
	return err
}

func (dispatcher *outboxDispatcher) publishMessage(ctx context.Context, message Publication) error {
	if dispatcher.store != nil && dispatcher.store.shards[0] != nil {
		if dispatcher.store.authority == nil {
			return errors.New("outbox: owner authority unavailable")
		}
		var err error
		ctx, err = dispatcher.store.authority.currentContext(ctx)
		if err != nil {
			return err
		}
	}
	ctx = correlation.WithID(ctx, message.CorrelationID)
	publishCtx, cancel := context.WithTimeout(ctx, dispatcher.cfg.PublishTimeout)
	started := time.Now()
	err := dispatcher.publisher.Publish(publishCtx, message)
	duration := time.Since(started)
	cancel()
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
	if message.store == nil {
		return 0, errors.New("outbox: owning store unavailable")
	}
	ctx = correlation.WithID(ctx, message.CorrelationID)
	delay := failureDelay(outboxPollInterval, dispatcher.cfg.RetryMaxBackoff, min(message.Attempts, 63)+1, failure)
	err := message.store.retryOutbox(ctx, message.Sequence, time.Now().Add(delay))
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
