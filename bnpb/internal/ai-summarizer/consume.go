package aisummarizer

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
	exchange      = "hazard.events"
	prefetchCount = 8
)

var reconnectBackoff = []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 15 * time.Second}

type envelope struct {
	SchemaVersion int    `json:"schema_version"`
	Revision      int64  `json:"revision"`
	EventType     string `json:"event_type"`
	HazardID      string `json:"hazard_id"`
	HazardEvent   struct {
		HazardType string                     `json:"hazard_type"`
		Source     string                     `json:"source"`
		Severity   string                     `json:"severity"`
		AreaName   string                     `json:"area_name"`
		Attributes map[string]json.RawMessage `json:"attributes"`
	} `json:"hazard_event"`
	CorrelationID string `json:"correlation_id"`
}

type consumer struct {
	cfg    Config
	store  *Store
	logger *slog.Logger
}

func run(ctx context.Context, cfg Config, store *Store, logger *slog.Logger) {
	worker := &consumer{cfg: cfg, store: store, logger: logger}
	for attempt := 0; ctx.Err() == nil; attempt++ {
		if err := worker.consumeOnce(ctx); err != nil && ctx.Err() == nil {
			logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "broker_connection_lost", Message: "AMQP connection or channel closed"}, "category", "broker")
		}
		if waitBackoff(ctx, reconnectBackoff[min(attempt, len(reconnectBackoff)-1)]) != nil {
			return
		}
	}
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

// A summary is best effort: an unreadable message is acknowledged and skipped rather than parked,
// and only a store failure asks the broker to redeliver.
func (worker *consumer) handleDelivery(ctx context.Context, session *consumerSession, delivery amqp.Delivery) error {
	var env envelope
	parseErr := json.Unmarshal(delivery.Body, &env)
	ctx = correlation.MessageContext(ctx, delivery.CorrelationId, env.CorrelationID)
	if parseErr != nil || env.SchemaVersion != 1 || env.HazardID == "" || delivery.MessageId == "" {
		logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelWarn, Event: "message_skipped", Message: "unreadable hazard notification skipped"}, "message_id", delivery.MessageId)
		return worker.settle(ctx, session, "ack", func() error { return delivery.Ack(false) })
	}
	err := worker.store.Apply(ctx, Event{
		MessageID: delivery.MessageId, HazardID: env.HazardID, Revision: env.Revision,
		Source: env.HazardEvent.Source, HazardType: env.HazardEvent.HazardType, Severity: env.HazardEvent.Severity,
		AreaName: env.HazardEvent.AreaName, Magnitude: number(env.HazardEvent.Attributes, "magnitude"),
		WaveHeight: number(env.HazardEvent.Attributes, "wave_height_m"), ReceivedAt: time.Now(),
	})
	switch {
	case errors.Is(err, errDuplicateMessage), err == nil:
		return worker.settle(ctx, session, "ack", func() error { return delivery.Ack(false) })
	default:
		logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelError, Event: "event_store_failed", Message: "could not store hazard event"}, "message_id", delivery.MessageId, "category", "store")
		if err := worker.settle(ctx, session, "nack", func() error { return delivery.Nack(false, true) }); err != nil {
			return err
		}
		// Pause so a broken store does not spin on redeliveries.
		return waitBackoff(ctx, time.Second)
	}
}

func number(attributes map[string]json.RawMessage, name string) *float64 {
	var value float64
	if raw, ok := attributes[name]; ok && json.Unmarshal(raw, &value) == nil {
		return &value
	}
	return nil
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
