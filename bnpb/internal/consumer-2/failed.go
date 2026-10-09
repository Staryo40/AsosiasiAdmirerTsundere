package consumer2

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/amqplog"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/amqppublish"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

func (worker *consumer) handleFailure(ctx context.Context, session *consumerSession, delivery amqp.Delivery, cause error) error {
	attempts, err := worker.store.IncrementAttempts(ctx, delivery.MessageId)
	if err != nil {
		logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelError, Event: "attempt_tracking_failed", Message: "could not record a delivery attempt"}, "category", "store")
		return worker.requeue(ctx, session, delivery)
	}
	logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelWarn, Event: "message_processing_failed", Message: "processing failed"},
		"message_id", delivery.MessageId, "attempts", attempts, "category", processingCategory(cause))
	if attempts < maxDeliveryAttempts {
		return worker.requeue(ctx, session, delivery)
	}
	return worker.moveFailed(ctx, session, delivery, attempts)
}

func processingCategory(cause error) string {
	var syntax *json.SyntaxError
	var fieldType *json.UnmarshalTypeError
	if errors.As(cause, &syntax) || errors.As(cause, &fieldType) {
		return "payload"
	}
	return "store"
}

func (worker *consumer) moveFailed(ctx context.Context, session *consumerSession, delivery amqp.Delivery, attempts int64) error {
	if err := worker.publishFailed(ctx, delivery); err != nil {
		logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelError, Event: "failed_queue_publish_failed", Message: "could not confirm failed publication"},
			"message_id", delivery.MessageId, "attempts", attempts, "category", failedPublicationCategory(err))
		return worker.retryTransfer(ctx, session, delivery)
	}
	worker.transferFailures = 0
	logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelInfo, Event: "failed_queue_publication_confirmed", Message: "failed publication accepted by broker"},
		"message_id", delivery.MessageId, "attempts", attempts)
	if err := worker.acknowledge(ctx, session, delivery); err != nil {
		return err
	}
	logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelError, Event: "message_moved_to_failed_queue", Message: "failed publication confirmed and source ACK written"},
		"message_id", delivery.MessageId, "attempts", attempts)
	return nil
}

func failedPublicationCategory(err error) string {
	var failure *amqppublish.Error
	if errors.As(err, &failure) {
		return failure.Category
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "transport"
}

func (worker *consumer) retryTransfer(ctx context.Context, session *consumerSession, delivery amqp.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := worker.requeue(ctx, session, delivery); err != nil {
		return err
	}
	delay := backoffFor(worker.transferFailures)
	worker.transferFailures = min(worker.transferFailures+1, len(reconnectBackoff)-1)
	return waitBackoff(ctx, delay)
}

func (worker *consumer) publishFailed(ctx context.Context, delivery amqp.Delivery) (err error) {
	ctx, cancel := context.WithTimeout(ctx, brokerIOTimeout)
	defer cancel()
	finish := amqplog.Start(ctx, worker.logger, "failed_queue_publish_to_confirm")
	defer func() {
		finish(err)
		if err != nil {
			worker.closePublisher()
		}
	}()
	if ctx.Err() != nil {
		return amqppublish.ContextError(ctx)
	}
	if err = worker.preparePublisher(ctx); err != nil {
		return err
	}
	return worker.publisher.confirmed.Publish(ctx, "", worker.cfg.Queue+".failed", amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: delivery.MessageId, Body: delivery.Body,
		CorrelationId: correlation.ID(ctx),
	})
}

func (worker *consumer) preparePublisher(ctx context.Context) (err error) {
	if worker.publisher != nil && !worker.publisher.connection.IsClosed() && !worker.publisher.channel.IsClosed() {
		return nil
	}
	worker.closePublisher()
	worker.publisher, err = worker.openTransport(ctx)
	if err != nil {
		return err
	}
	worker.publisher.confirmed, err = amqppublish.New(ctx, worker.publisher.socket, worker.publisher.connection, worker.publisher.channel)
	if err != nil {
		return err
	}
	return worker.publisher.finishSetup(ctx)
}

func (worker *consumer) closePublisher() {
	if worker.publisher != nil {
		worker.publisher.close()
		worker.publisher = nil
	}
}
