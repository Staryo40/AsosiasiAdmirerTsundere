package aggregator

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type amqpPublisher struct {
	address, exchange string
	socket            net.Conn
	connection        *amqp.Connection
	channel           *amqp.Channel
	returns           chan amqp.Return
	confirms          chan amqp.Confirmation
	channelClosed     chan *amqp.Error
	connectionClosed  chan *amqp.Error
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
	stop := interruptSocket(ctx, publisher.socket)
	defer func() {
		stop()
		if ctx.Err() != nil {
			err = publicationContextError(ctx)
		}
		if err != nil {
			publisher.Close()
		}
	}()
	return publisher.publishConfirmed(ctx, message)
}

func publicationContextError(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded {
		return &publicationError{"timeout"}
	}
	return &publicationError{"canceled"}
}

func (publisher *amqpPublisher) publishConfirmed(ctx context.Context, message Publication) error {
	deadline, _ := ctx.Deadline()
	if err := publisher.socket.SetWriteDeadline(deadline); err != nil {
		return &publicationError{"transport"}
	}
	tag := publisher.channel.GetNextPublishSeqNo()
	if err := publisher.channel.PublishWithContext(ctx, publisher.exchange, "", true, false, amqpPublication(message)); err != nil {
		return &publicationError{"transport"}
	}
	if err := publisher.awaitConfirmation(ctx, tag); err != nil {
		return err
	}
	if err := publisher.socket.SetWriteDeadline(time.Time{}); err != nil {
		return &publicationError{"transport"}
	}
	return nil
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
			stop = interruptSocket(ctx, socket)
			deadline, _ := ctx.Deadline()
			if err := socket.SetDeadline(deadline); err != nil {
				return nil, err
			}
			return socket, nil
		}})
	if err == nil {
		publisher.connection = connection
		err = publisher.prepareChannel()
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

func (publisher *amqpPublisher) prepareChannel() error {
	publisher.connectionClosed = publisher.connection.NotifyClose(make(chan *amqp.Error, 1))
	channel, err := publisher.connection.Channel()
	if err != nil {
		return err
	}
	publisher.channel = channel
	publisher.channelClosed = channel.NotifyClose(make(chan *amqp.Error, 1))
	publisher.returns = channel.NotifyReturn(make(chan amqp.Return, 1))
	publisher.confirms = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	if err := channel.ExchangeDeclare(publisher.exchange, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	return channel.Confirm(false)
}

func (publisher *amqpPublisher) awaitConfirmation(ctx context.Context, tag uint64) error {
	select {
	case <-ctx.Done():
		return &publicationError{"timeout"}
	case <-publisher.channelClosed:
		return &publicationError{"closed"}
	case <-publisher.connectionClosed:
		return &publicationError{"closed"}
	case _, open := <-publisher.returns:
		if !open {
			return &publicationError{"closed"}
		}
		return &publicationError{"returned"}
	case confirmation, open := <-publisher.confirms:
		if !open {
			return &publicationError{"closed"}
		}
		return publisher.checkConfirmation(confirmation, tag)
	}
}

func (publisher *amqpPublisher) checkConfirmation(confirmation amqp.Confirmation, tag uint64) error {
	if confirmation.DeliveryTag != tag {
		return &publicationError{"closed"}
	}
	if !confirmation.Ack {
		return &publicationError{"nack"}
	}
	return publisher.checkReturned()
}

// v1.15.0 dispatches each channel's return synchronously before its ACK.
// With one in flight and empty capacity-one listeners, return is queued by ACK
// receipt even if select chooses ACK first. Recheck it before accepting the ACK.
func (publisher *amqpPublisher) checkReturned() error {
	select {
	case _, open := <-publisher.returns:
		if open {
			return &publicationError{"returned"}
		}
		return &publicationError{"closed"}
	default:
		return nil
	}
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
}

// Join a cancellation callback that started; never leave a timeout goroutine
// publishing after its caller has moved on to a fresh connection.
func interruptSocket(ctx context.Context, socket net.Conn) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = socket.Close()
		close(done)
	})
	return func() {
		if !stop() {
			<-done
		}
	}
}

var _ Publisher = (*amqpPublisher)(nil)
var _ error = (*publicationError)(nil)
