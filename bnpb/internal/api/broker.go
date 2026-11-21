package api

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/amqppublish"
)

const (
	fieldReportQueue     = "field.reports"
	fieldReportChannels  = 4
	fieldReportPublishBy = 2 * time.Second
)

var errIntakeUnconfigured = errors.New("field report intake is not configured")

type reportPublisher interface {
	Publish(ctx context.Context, message amqp.Publishing) error
	Close()
}

// Each slot owns one connection with at most one confirmed publication in flight.
type brokerPublisher struct {
	address string
	slots   chan *publisherSlot
}

type publisherSlot struct {
	socket     net.Conn
	connection *amqp.Connection
	transport  *amqppublish.Channel
}

func newBrokerPublisher(user, password, address string) *brokerPublisher {
	publisher := &brokerPublisher{
		address: fmt.Sprintf("amqp://%s@%s/", url.UserPassword(user, password).String(), address),
		slots:   make(chan *publisherSlot, fieldReportChannels),
	}
	for range fieldReportChannels {
		publisher.slots <- &publisherSlot{}
	}
	return publisher
}

// Publishes to the default exchange with the queue as routing key; mandatory turns a missing queue into an error.
func (publisher *brokerPublisher) Publish(ctx context.Context, message amqp.Publishing) error {
	var slot *publisherSlot
	select {
	case slot = <-publisher.slots:
	case <-ctx.Done():
		return amqppublish.ContextError(ctx)
	}
	defer func() { publisher.slots <- slot }()
	if err := slot.connect(ctx, publisher.address); err != nil {
		return err
	}
	err := slot.transport.Publish(ctx, "", fieldReportQueue, message)
	if err != nil {
		slot.close()
	}
	return err
}

func (publisher *brokerPublisher) Close() {
	for range fieldReportChannels {
		(<-publisher.slots).close()
	}
}

func (slot *publisherSlot) connect(ctx context.Context, address string) error {
	if slot.connection != nil && !slot.connection.IsClosed() && slot.transport != nil {
		return nil
	}
	slot.close()
	var stop func()
	connection, err := amqp.DialConfig(address, amqp.Config{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		Dial: func(network, target string) (net.Conn, error) {
			socket, err := (&net.Dialer{}).DialContext(ctx, network, target)
			if err != nil {
				return nil, err
			}
			slot.socket = socket
			stop = amqppublish.Interrupt(ctx, socket)
			deadline, _ := ctx.Deadline()
			return socket, socket.SetDeadline(deadline)
		}})
	if err == nil {
		slot.connection = connection
		var channel *amqp.Channel
		if channel, err = connection.Channel(); err == nil {
			slot.transport, err = amqppublish.New(ctx, slot.socket, connection, channel)
		}
		if err == nil {
			err = slot.socket.SetDeadline(time.Time{})
		}
	}
	if stop != nil {
		stop()
	}
	if err != nil || ctx.Err() != nil {
		slot.close()
		return &amqppublish.Error{Category: "connect"}
	}
	return nil
}

func (slot *publisherSlot) close() {
	if slot.socket != nil {
		_ = slot.socket.Close()
		slot.socket = nil
	}
	if slot.connection != nil {
		_ = slot.connection.CloseDeadline(time.Now())
		slot.connection = nil
	}
	slot.transport = nil
}

type unconfiguredPublisher struct{}

func (unconfiguredPublisher) Publish(context.Context, amqp.Publishing) error {
	return errIntakeUnconfigured
}
func (unconfiguredPublisher) Close() {}
