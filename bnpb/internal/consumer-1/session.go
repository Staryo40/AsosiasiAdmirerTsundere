package consumer1

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/amqplog"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/amqppublish"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

const brokerIOTimeout = 5 * time.Second

type consumerSession struct {
	socket     net.Conn
	connection *amqp.Connection
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
	closed     <-chan *amqp.Error
	stopIO     func()
	confirmed  *amqppublish.Channel
}

func (worker *consumer) openSession(ctx context.Context) (_ *consumerSession, err error) {
	ctx, cancel := context.WithTimeout(ctx, brokerIOTimeout)
	defer cancel()
	session, err := worker.openTransport(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			session.close()
		}
	}()
	if err = worker.declareTopology(ctx, session.channel); err != nil {
		return nil, err
	}
	if err = worker.subscribe(ctx, session); err != nil {
		return nil, err
	}
	if err = session.finishSetup(ctx); err != nil {
		return nil, err
	}
	logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelInfo, Event: "broker_connected", Message: "consuming from " + worker.cfg.Queue}, "queue", worker.cfg.Queue)
	return session, nil
}

func (worker *consumer) openTransport(ctx context.Context) (_ *consumerSession, err error) {
	session := &consumerSession{}
	defer func() {
		if err != nil {
			session.close()
		}
	}()
	finish := amqplog.Start(ctx, worker.logger, "connect")
	session.connection, err = amqp.DialConfig(worker.cfg.BrokerURL, amqp.Config{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		Dial:            func(network, address string) (net.Conn, error) { return session.dial(ctx, network, address) },
	})
	finish(err)
	if err != nil {
		return nil, err
	}
	// v1.15.0 clears handshake deadlines at openComplete; bound channel/RPC setup too.
	deadline, _ := ctx.Deadline()
	if err = session.socket.SetDeadline(deadline); err != nil {
		return nil, err
	}
	finish = amqplog.Start(ctx, worker.logger, "channel_open")
	session.channel, err = session.connection.Channel()
	finish(err)
	if err != nil {
		return nil, err
	}
	session.closed = session.channel.NotifyClose(make(chan *amqp.Error, 1))
	return session, nil
}

func (session *consumerSession) dial(ctx context.Context, network, address string) (net.Conn, error) {
	socket, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	session.socket = socket
	session.stopIO = amqppublish.Interrupt(ctx, socket)
	deadline, _ := ctx.Deadline()
	if err = socket.SetDeadline(deadline); err != nil {
		return nil, err
	}
	return socket, nil
}

func (session *consumerSession) finishSetup(ctx context.Context) error {
	session.stopIO()
	session.stopIO = nil
	if err := ctx.Err(); err != nil {
		return err
	}
	return session.socket.SetDeadline(time.Time{})
}

func (worker *consumer) subscribe(ctx context.Context, session *consumerSession) error {
	finish := amqplog.Start(ctx, worker.logger, "qos")
	err := session.channel.Qos(prefetchCount, 0, false)
	finish(err)
	if err != nil {
		return err
	}
	finish = amqplog.Start(ctx, worker.logger, "consume")
	session.deliveries, err = session.channel.Consume(worker.cfg.Queue, "", false, false, false, false, nil)
	finish(err)
	return err
}

// Raw close bounds cleanup even if the AMQP close handshake cannot progress.
func (session *consumerSession) close() {
	if session.socket != nil {
		_ = session.socket.Close()
	}
	if session.stopIO != nil {
		session.stopIO()
		session.stopIO = nil
	}
	if session.connection != nil {
		_ = session.connection.CloseDeadline(time.Now())
	}
}

func (worker *consumer) declareTopology(ctx context.Context, channel *amqp.Channel) error {
	quorum := amqp.Table{"x-queue-type": "quorum"}
	for _, name := range []string{worker.cfg.Queue, worker.cfg.Queue + ".failed"} {
		finish := amqplog.Start(ctx, worker.logger, "queue_declare")
		_, err := channel.QueueDeclare(name, true, false, false, false, quorum)
		finish(err)
		if err != nil {
			return err
		}
	}
	finish := amqplog.Start(ctx, worker.logger, "queue_bind")
	err := channel.QueueBind(worker.cfg.Queue, "", exchange, false, nil)
	finish(err)
	return err
}

func (worker *consumer) acknowledge(ctx context.Context, session *consumerSession, delivery amqp.Delivery) error {
	return worker.settle(ctx, session, "ack", func() error { return delivery.Ack(false) })
}
func (worker *consumer) requeue(ctx context.Context, session *consumerSession, delivery amqp.Delivery) error {
	return worker.settle(ctx, session, "nack", func() error { return delivery.Nack(false, true) })
}

func (worker *consumer) settle(ctx context.Context, session *consumerSession, operation string, write func() error) (err error) {
	ctx, cancel := context.WithTimeout(ctx, brokerIOTimeout)
	defer cancel()
	stop := amqppublish.Interrupt(ctx, session.socket)
	finish := amqplog.Start(ctx, worker.logger, operation)
	defer func() {
		stop()
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil {
			_ = session.socket.Close()
		}
		finish(err)
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if err = session.socket.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err = write(); err != nil {
		return err
	}
	return session.socket.SetWriteDeadline(time.Time{})
}
