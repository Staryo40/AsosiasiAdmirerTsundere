// Package amqppublish implements routed, confirmed publication on caller-owned AMQP resources.
package amqppublish

import (
	"context"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Error exposes a safe category without broker replies or credential-bearing URLs.
type Error struct{ Category string }

func (err *Error) Error() string { return "publication failed: " + err.Category }

// Channel has one owner and at most one in-flight publication. The caller
// opens/closes resources and replaces them after any failed publication.
type Channel struct {
	socket                          net.Conn
	channel                         *amqp.Channel
	returns                         chan amqp.Return
	confirms                        chan amqp.Confirmation
	channelClosed, connectionClosed chan *amqp.Error
	valid                           bool
}

func New(ctx context.Context, socket net.Conn, connection *amqp.Connection, channel *amqp.Channel) (*Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, ContextError(ctx)
	}
	publisher := &Channel{socket: socket, channel: channel, valid: true,
		returns: channel.NotifyReturn(make(chan amqp.Return, 1)), confirms: channel.NotifyPublish(make(chan amqp.Confirmation, 1)),
		channelClosed: channel.NotifyClose(make(chan *amqp.Error, 1)), connectionClosed: connection.NotifyClose(make(chan *amqp.Error, 1))}
	if err := channel.Confirm(false); err != nil {
		return nil, &Error{"connect"}
	}
	return publisher, nil
}

func ContextError(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded {
		return &Error{"timeout"}
	}
	return &Error{"canceled"}
}

func (publisher *Channel) Publish(ctx context.Context, exchange, key string, message amqp.Publishing) (err error) {
	if ctx.Err() != nil {
		return ContextError(ctx)
	}
	if !publisher.valid {
		return &Error{"closed"}
	}
	stop := Interrupt(ctx, publisher.socket)
	defer func() {
		stop()
		if ctx.Err() != nil {
			err = ContextError(ctx)
		}
		if err != nil {
			publisher.valid = false
			_ = publisher.socket.Close()
		}
	}()
	if err := publisher.checkIdle(); err != nil {
		return err
	}
	return publisher.publishConfirmed(ctx, exchange, key, message)
}

func (publisher *Channel) publishConfirmed(ctx context.Context, exchange, key string, message amqp.Publishing) error {
	deadline, _ := ctx.Deadline()
	if err := publisher.socket.SetWriteDeadline(deadline); err != nil {
		return &Error{"transport"}
	}
	tag := publisher.channel.GetNextPublishSeqNo()
	if err := publisher.channel.PublishWithContext(ctx, exchange, key, true, false, message); err != nil {
		return &Error{"transport"}
	}
	if err := publisher.awaitConfirmation(ctx, tag); err != nil {
		return err
	}
	if err := publisher.socket.SetWriteDeadline(time.Time{}); err != nil {
		return &Error{"transport"}
	}
	return nil
}

// A stale notification would make the next tag ambiguous; do not reuse it.
func (publisher *Channel) checkIdle() error {
	select {
	case <-publisher.confirms:
		return &Error{"closed"}
	case <-publisher.returns:
		return &Error{"closed"}
	case <-publisher.channelClosed:
		return &Error{"closed"}
	case <-publisher.connectionClosed:
		return &Error{"closed"}
	default:
		return nil
	}
}

func (publisher *Channel) awaitConfirmation(ctx context.Context, tag uint64) error {
	select {
	case <-ctx.Done():
		return ContextError(ctx)
	case <-publisher.channelClosed:
		return &Error{"closed"}
	case <-publisher.connectionClosed:
		return &Error{"closed"}
	case _, open := <-publisher.returns:
		if !open {
			return &Error{"closed"}
		}
		return &Error{"returned"}
	case confirmation, open := <-publisher.confirms:
		if !open {
			return &Error{"closed"}
		}
		return publisher.checkConfirmation(confirmation, tag)
	}
}

func (publisher *Channel) checkConfirmation(confirmation amqp.Confirmation, tag uint64) error {
	if confirmation.DeliveryTag != tag {
		return &Error{"closed"}
	}
	if !confirmation.Ack {
		return &Error{"nack"}
	}
	// v1.15.0 synchronously dispatches return before ACK. With one in flight
	// and initially empty capacity-one listeners, ACK receipt makes return visible.
	select {
	case _, open := <-publisher.returns:
		if !open {
			return &Error{"closed"}
		}
		return &Error{"returned"}
	default:
		return nil
	}
}

// Interrupt closes the raw socket on cancellation (also underneath TLS).
// Call the returned function exactly once to stop and join a started callback.
func Interrupt(ctx context.Context, socket net.Conn) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = socket.Close(); close(done) })
	return func() {
		if !stop() {
			<-done
		}
	}
}
