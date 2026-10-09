package aggregator

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/amqppublish"
	amqp "github.com/rabbitmq/amqp091-go"
)

type amqpPublisher struct {
	address, exchange string
	socket            net.Conn
	connection        *amqp.Connection
	channel           *amqp.Channel
	transport         *amqppublish.Channel
}

func newAMQPPublisher(cfg Config) *amqpPublisher {
	return &amqpPublisher{address: cfg.AMQPURL, exchange: cfg.HazardExchange}
}

func (publisher *amqpPublisher) Publish(ctx context.Context, message Publication) (err error) {
	if ctx.Err() != nil {
		return publicationContextError(ctx)
	}
	if err := publisher.connect(ctx); err != nil {
		return err
	}
	err = adaptPublicationError(publisher.transport.Publish(ctx, publisher.exchange, "", amqpPublication(message)))
	if ctx.Err() != nil {
		err = publicationContextError(ctx)
	}
	if err != nil {
		publisher.Close()
	}
	return err
}

func publicationContextError(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded {
		return &publicationError{"timeout"}
	}
	return &publicationError{"canceled"}
}

func adaptPublicationError(err error) error {
	var failure *amqppublish.Error
	if errors.As(err, &failure) {
		return &publicationError{failure.Category}
	}
	return err
}

func amqpPublication(message Publication) amqp.Publishing {
	return amqp.Publishing{ContentType: "application/json", ContentEncoding: "utf-8", DeliveryMode: amqp.Persistent,
		MessageId: message.MessageID, CorrelationId: message.CorrelationID, Type: message.EventType,
		Timestamp: message.ProducedAt, AppId: "bnpb-aggregator", Body: message.Payload,
		Headers: amqp.Table{"hazard_id": message.HazardID, "revision": message.Revision, "schema_version": int32(message.SchemaVersion)}}
}

func (publisher *amqpPublisher) connect(ctx context.Context) error {
	if publisher.connection != nil && !publisher.connection.IsClosed() && !publisher.channel.IsClosed() {
		return nil
	}
	publisher.Close()
	var stop func()
	connection, err := amqp.DialConfig(publisher.address, amqp.Config{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		Dial: func(network, address string) (net.Conn, error) {
			socket, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			publisher.socket = socket
			stop = amqppublish.Interrupt(ctx, socket)
			deadline, _ := ctx.Deadline()
			if err := socket.SetDeadline(deadline); err != nil {
				return nil, err
			}
			return socket, nil
		}})
	if err == nil {
		publisher.connection = connection
		deadline, _ := ctx.Deadline()
		err = publisher.socket.SetDeadline(deadline)
		if err == nil {
			err = publisher.prepareChannel(ctx)
		}
		if err == nil {
			err = publisher.socket.SetDeadline(time.Time{})
		}
	}
	if stop != nil {
		stop()
	}
	if err != nil || ctx.Err() != nil {
		publisher.Close()
		return &publicationError{"connect"}
	}
	return nil
}

func (publisher *amqpPublisher) prepareChannel(ctx context.Context) error {
	channel, err := publisher.connection.Channel()
	if err != nil {
		return err
	}
	publisher.channel = channel
	if err := channel.ExchangeDeclare(publisher.exchange, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	publisher.transport, err = amqppublish.New(ctx, publisher.socket, publisher.connection, channel)
	return err
}

// Close the raw TCP socket first, including for TLS: no close handshake can
// hold shutdown behind a blocked publisher. Automatic library recovery is off.
func (publisher *amqpPublisher) Close() {
	if publisher.socket != nil {
		_ = publisher.socket.Close()
		publisher.socket = nil
	}
	if publisher.connection != nil {
		_ = publisher.connection.CloseDeadline(time.Now())
		publisher.connection = nil
	}
	publisher.channel = nil
	publisher.transport = nil
}

var _ Publisher = (*amqpPublisher)(nil)
var _ error = (*publicationError)(nil)
