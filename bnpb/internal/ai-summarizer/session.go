package aisummarizer

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
}

func (worker *consumer) openSession(ctx context.Context) (_ *consumerSession, err error) {
	ctx, cancel := context.WithTimeout(ctx, brokerIOTimeout)
	defer cancel()
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
	// The handshake clears its own deadlines; bound channel and RPC setup too.
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
	if err = worker.declareTopology(ctx, session.channel); err != nil {
		return nil, err
	}
	finish = amqplog.Start(ctx, worker.logger, "qos")
	err = session.channel.Qos(prefetchCount, 0, false)
	finish(err)
	if err != nil {
		return nil, err
	}
	finish = amqplog.Start(ctx, worker.logger, "consume")
	session.deliveries, err = session.channel.Consume(worker.cfg.Queue, "", false, false, false, false, nil)
	finish(err)
	if err != nil {
		return nil, err
	}
	session.stopIO()
	session.stopIO = nil
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = session.socket.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelInfo, Event: "broker_connected", Message: "consuming from " + worker.cfg.Queue}, "queue", worker.cfg.Queue)
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
	finish := amqplog.Start(ctx, worker.logger, "queue_declare")
	_, err := channel.QueueDeclare(worker.cfg.Queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	finish(err)
	if err != nil {
		return err
	}
	finish = amqplog.Start(ctx, worker.logger, "queue_bind")
	err = channel.QueueBind(worker.cfg.Queue, "", exchange, false, nil)
	finish(err)
	return err
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
