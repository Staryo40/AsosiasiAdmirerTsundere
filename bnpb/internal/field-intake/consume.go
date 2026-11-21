package fieldintake

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"log/slog"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/amqplog"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/amqppublish"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

const (
	prefetchCount   = 16
	brokerIOTimeout = 5 * time.Second
	maxReportBytes  = 64 << 10
)

var reconnectBackoff = []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second}

var reportOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bnpb_field_reports_total",
	Help: "Field reports settled from the queue, by outcome.",
}, []string{"outcome"})

// Must match the arguments broker-init declares, or the declaration is refused.
var queueArguments = amqp.Table{"x-queue-type": "quorum"}

type consumer struct {
	cfg    Config
	store  *Store
	logger *slog.Logger
}

type session struct {
	socket     net.Conn
	connection *amqp.Connection
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
	closed     <-chan *amqp.Error
}

func run(ctx context.Context, cfg Config, store *Store, logger *slog.Logger) {
	worker := &consumer{cfg: cfg, store: store, logger: logger}
	for _, outcome := range []string{string(Stored), string(Duplicate), string(Conflict), "rejected"} {
		reportOutcomes.WithLabelValues(outcome)
	}
	for attempt := 0; ctx.Err() == nil; attempt++ {
		if err := worker.consumeOnce(ctx); err != nil && ctx.Err() == nil {
			logging.Event(ctx, logger, logging.Record{Level: slog.LevelError, Event: "broker_connection_lost", Message: "AMQP connection or channel closed"}, "category", "broker")
		}
		delay := reconnectBackoff[min(attempt, len(reconnectBackoff)-1)]
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (worker *consumer) consumeOnce(ctx context.Context) error {
	ctx = correlation.WithID(ctx, correlation.NewID())
	current, err := worker.open(ctx)
	if err != nil {
		return err
	}
	defer current.close()
	stop := amqppublish.Interrupt(ctx, current.socket)
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case amqpErr := <-current.closed:
			if amqpErr != nil {
				return amqpErr
			}
			return nil
		case delivery, ok := <-current.deliveries:
			if !ok {
				return nil
			}
			if err := worker.handle(ctx, current, delivery); err != nil {
				return err
			}
		}
	}
}

// An error here drops the connection unacked, so the broker redelivers the report.
func (worker *consumer) handle(ctx context.Context, current *session, delivery amqp.Delivery) error {
	ctx = correlation.MessageContext(ctx, delivery.CorrelationId, "")
	var report Report
	decoder := json.NewDecoder(bytes.NewReader(delivery.Body))
	decoder.DisallowUnknownFields()
	switch {
	case len(delivery.Body) > maxReportBytes:
		return worker.reject(ctx, current, delivery, "oversized")
	case decoder.Decode(&report) != nil || !report.valid():
		return worker.reject(ctx, current, delivery, "invalid_report")
	}
	outcome, err := worker.store.Apply(ctx, delivery.MessageId, report, delivery.Body)
	if err != nil {
		return err
	}
	reportOutcomes.WithLabelValues(string(outcome)).Inc()
	level := slog.LevelInfo
	if outcome == Conflict {
		level = slog.LevelWarn
	}
	logging.Event(ctx, worker.logger, logging.Record{Level: level, Event: "field_report_" + string(outcome), Message: "field report processed"},
		"message_id", delivery.MessageId, "report_id", report.ReportID, "client_id", report.ClientID, "redelivered", delivery.Redelivered)
	return worker.settle(ctx, current, delivery)
}

func (worker *consumer) reject(ctx context.Context, current *session, delivery amqp.Delivery, reason string) error {
	payload := delivery.Body
	if len(payload) > maxReportBytes {
		payload = payload[:maxReportBytes]
	}
	if err := worker.store.Reject(ctx, delivery.MessageId, reason, payload); err != nil {
		return err
	}
	reportOutcomes.WithLabelValues("rejected").Inc()
	logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelWarn, Event: "field_report_rejected", Message: "field report could not be read"},
		"message_id", delivery.MessageId, "reason", reason)
	return worker.settle(ctx, current, delivery)
}

func (worker *consumer) settle(ctx context.Context, current *session, delivery amqp.Delivery) (err error) {
	ctx, cancel := context.WithTimeout(ctx, brokerIOTimeout)
	defer cancel()
	finish := amqplog.Start(ctx, worker.logger, "ack")
	defer func() { finish(err) }()
	deadline, _ := ctx.Deadline()
	if err = current.socket.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err = delivery.Ack(false); err != nil {
		return err
	}
	return current.socket.SetWriteDeadline(time.Time{})
}

func (worker *consumer) open(ctx context.Context) (_ *session, err error) {
	ctx, cancel := context.WithTimeout(ctx, brokerIOTimeout)
	defer cancel()
	current := &session{}
	var stopIO func()
	defer func() {
		if stopIO != nil {
			stopIO()
		}
		if err != nil {
			current.close()
		}
	}()
	finish := amqplog.Start(ctx, worker.logger, "connect")
	current.connection, err = amqp.DialConfig(worker.cfg.BrokerURL, amqp.Config{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		Dial: func(network, address string) (net.Conn, error) {
			socket, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			current.socket = socket
			stopIO = amqppublish.Interrupt(ctx, socket)
			deadline, _ := ctx.Deadline()
			return socket, socket.SetDeadline(deadline)
		}})
	finish(err)
	if err != nil {
		return nil, err
	}
	deadline, _ := ctx.Deadline()
	if err = current.socket.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if current.channel, err = current.connection.Channel(); err != nil {
		return nil, err
	}
	current.closed = current.channel.NotifyClose(make(chan *amqp.Error, 1))
	finish = amqplog.Start(ctx, worker.logger, "queue_declare")
	_, err = current.channel.QueueDeclare(reportQueue, true, false, false, false, queueArguments)
	finish(err)
	if err != nil {
		return nil, err
	}
	if err = current.channel.Qos(prefetchCount, 0, false); err != nil {
		return nil, err
	}
	if current.deliveries, err = current.channel.Consume(reportQueue, "", false, false, false, false, nil); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = current.socket.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	logging.Event(ctx, worker.logger, logging.Record{Level: slog.LevelInfo, Event: "broker_connected", Message: "consuming from " + reportQueue}, "queue", reportQueue)
	return current, nil
}

// Raw close bounds cleanup even if the AMQP close handshake cannot progress.
func (current *session) close() {
	if current.socket != nil {
		_ = current.socket.Close()
	}
	if current.connection != nil {
		_ = current.connection.CloseDeadline(time.Now())
	}
}
