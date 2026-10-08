package consumer2

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

const (
	exchange            = "hazard.events"
	maxDeliveryAttempts = 3
	prefetchCount       = 1
	pruneInterval        = 30 * time.Minute
)

var reconnectBackoff = []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 15 * time.Second}

// envelope is the consumer side of contracts/v1/schemas/hazard-notification.schema.json.
type envelope struct {
	HazardID    string `json:"hazard_id"`
	HazardEvent struct {
		Severity   string    `json:"severity"`
		AreaName   string    `json:"area_name"`
		OccurredAt time.Time `json:"occurred_at"`
	} `json:"hazard_event"`
	CorrelationID string `json:"correlation_id"`
}

// run owns the AMQP connection for the process lifetime: connect, declare,
// consume, reconnect on loss, until ctx is canceled.
func run(ctx context.Context, cfg Config, store *Store, logger *slog.Logger) {
	go pruneLoop(ctx, store, logger)
	for attempt := 0; ctx.Err() == nil; attempt++ {
		if err := consumeOnce(ctx, cfg, store, logger); err != nil && ctx.Err() == nil {
			logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "broker_connection_lost", Message: "AMQP connection or channel closed"}, "error", err.Error())
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-time.After(backoffFor(attempt)):
		case <-ctx.Done():
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

func consumeOnce(ctx context.Context, cfg Config, store *Store, logger *slog.Logger) error {
	connection, err := amqp.DialConfig(cfg.BrokerURL, amqp.Config{Dial: amqp.DefaultDial(5 * time.Second)})
	if err != nil {
		return err
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	failedQueue := cfg.Queue + ".failed"
	if err := declareTopology(channel, cfg.Queue, failedQueue); err != nil {
		return err
	}
	if err := channel.Qos(prefetchCount, 0, false); err != nil {
		return err
	}

	deliveries, err := channel.Consume(cfg.Queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	closed := channel.NotifyClose(make(chan *amqp.Error, 1))
	logging.Event(ctx, logger, logging.Record{Level: slog.LevelInfo, Event: "broker_connected", Message: "consuming from " + cfg.Queue}, "queue", cfg.Queue)

	for {
		select {
		case <-ctx.Done():
			return nil
		case amqpErr := <-closed:
			if amqpErr != nil {
				return amqpErr
			}
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			handleDelivery(ctx, channel, store, logger, failedQueue, delivery)
		}
	}
}

// The exchange itself is declared by the Aggregator (and provisioned ahead
// of time by broker-init); a consumer only owns its own queue, matching the
// "configure" permission it was actually granted (Decision 1).
func declareTopology(channel *amqp.Channel, queue, failedQueue string) error {
	quorum := amqp.Table{"x-queue-type": "quorum"}
	if _, err := channel.QueueDeclare(queue, true, false, false, false, quorum); err != nil {
		return err
	}
	if _, err := channel.QueueDeclare(failedQueue, true, false, false, false, quorum); err != nil {
		return err
	}
	return channel.QueueBind(queue, "", exchange, false, nil)
}

func handleDelivery(ctx context.Context, channel *amqp.Channel, store *Store, logger *slog.Logger, failedQueue string, delivery amqp.Delivery) {
	// The AMQP message-id property, not the JSON body's own message_id
	// field: it is set by the publisher independently of the body, so
	// attempt-counting and dedupe still work even if the body fails to
	// parse at all (otherwise every unparseable message would share one
	// empty-string identity and loop forever).
	messageID := delivery.MessageId
	var env envelope
	var result ApplyResult
	err := json.Unmarshal(delivery.Body, &env)
	if err == nil {
		ctx = correlation.WithID(ctx, env.CorrelationID)
		result, err = store.Apply(ctx, HazardAlert{
			MessageID:  messageID,
			HazardID:   env.HazardID,
			Severity:   env.HazardEvent.Severity,
			AreaName:   env.HazardEvent.AreaName,
			OccurredAt: env.HazardEvent.OccurredAt,
		})
	}

	switch {
	case errors.Is(err, errDuplicateMessage):
		logging.Event(ctx, logger, logging.Record{Level: slog.LevelInfo, Event: "message_duplicate_skipped", Message: "message_id already processed"}, "message_id", messageID)
		_ = delivery.Ack(false)
	case err == nil && result.AlertSent:
		// Logged only after Apply's transaction committed, so a log line
		// never claims an alert the DB doesn't record (Decision 4).
		logging.Event(ctx, logger, logging.Record{Level: slog.LevelWarn, Event: "alert_sent", Message: "alert recorded for a SIAGA/AWAS hazard"}, "hazard_id", env.HazardID, "severity", env.HazardEvent.Severity)
		_ = delivery.Ack(false)
	case err == nil:
		logging.Event(ctx, logger, logging.Record{Level: slog.LevelInfo, Event: "message_ignored", Message: "severity does not warrant an alert"}, "hazard_id", env.HazardID, "severity", env.HazardEvent.Severity)
		_ = delivery.Ack(false)
	default:
		handleFailure(ctx, channel, store, logger, failedQueue, delivery, messageID, err)
	}
}

func handleFailure(ctx context.Context, channel *amqp.Channel, store *Store, logger *slog.Logger, failedQueue string, delivery amqp.Delivery, messageID string, cause error) {
	attempts, attemptErr := store.IncrementAttempts(ctx, messageID)
	if attemptErr != nil {
		logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "attempt_tracking_failed", Message: "could not record a delivery attempt"}, "error", attemptErr.Error())
		_ = delivery.Nack(false, true)
		return
	}
	logging.Event(ctx, logger, logging.Record{Level: slog.LevelWarn, Event: "message_processing_failed", Message: "processing failed"}, "message_id", messageID, "attempts", attempts, "error", cause.Error())

	if attempts < maxDeliveryAttempts {
		_ = delivery.Nack(false, true)
		return
	}

	publishErr := channel.PublishWithContext(ctx, "", failedQueue, false, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: messageID, Body: delivery.Body,
	})
	if publishErr != nil {
		logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "failed_queue_publish_failed", Message: "could not publish to the failed queue"}, "message_id", messageID, "error", publishErr.Error())
		_ = delivery.Nack(false, true)
		return
	}
	logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "message_moved_to_failed_queue", Message: "delivery limit reached"}, "message_id", messageID, "attempts", attempts)
	_ = delivery.Ack(false)
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
