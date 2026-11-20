package consumer2

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDedupeSurvivesRestartAndAgedMessageRetry(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "consumer-2.db")
	first, err := OpenStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	worker := &consumer{store: first}
	created := enumEnvelope("SATELIT", "WILDFIRE")
	if applied, err := worker.applyEnvelope(ctx, "created-message", created); err != nil || !applied.AlertSent {
		t.Fatalf("initial notification: applied=%+v err=%v", applied, err)
	}
	if _, err := first.db.ExecContext(ctx,
		`UPDATE processed_messages SET processed_at = ? WHERE message_id = ?`,
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano), "created-message"); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	worker.store = reopened
	if _, err := worker.applyEnvelope(ctx, "created-message", created); !errors.Is(err, errDuplicateMessage) {
		t.Fatalf("aged retry was not deduplicated: %v", err)
	}

	updated := created
	updated.Revision, updated.EventType = 2, "hazard.updated"
	if applied, err := worker.applyEnvelope(ctx, "updated-message", updated); err != nil || !applied.AlertSent {
		t.Fatalf("later revision: applied=%+v err=%v", applied, err)
	}
	if _, err := worker.applyEnvelope(ctx, "updated-message", updated); !errors.Is(err, errDuplicateMessage) {
		t.Fatalf("later revision retry was not deduplicated: %v", err)
	}
	if _, err := worker.applyEnvelope(ctx, "created-message", created); !errors.Is(err, errDuplicateMessage) {
		t.Fatalf("old revision retry was not deduplicated: %v", err)
	}

	stats, err := reopened.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.AlertsSent != 2 || stats.MessagesSeen != 2 {
		t.Fatalf("unexpected durable effects after retries: %+v", stats)
	}

	worker.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	body, err := json.Marshal(created)
	if err != nil {
		t.Fatal(err)
	}
	socket, peer := net.Pipe()
	defer socket.Close()
	defer peer.Close()
	acknowledger := &duplicateDeliveryAcknowledger{}
	delivery := amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 17, MessageId: "created-message", Body: body}
	if err := worker.handleDelivery(ctx, &consumerSession{socket: socket}, delivery); err != nil {
		t.Fatalf("aged duplicate entered failure handling: %v", err)
	}
	if acknowledger.ackedTag != delivery.DeliveryTag {
		t.Fatal("aged duplicate was not acknowledged")
	}
	var attempts int
	if err := reopened.db.QueryRowContext(ctx, "SELECT count(*) FROM delivery_attempts").Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("aged duplicate consumed a retry: %d, %v", attempts, err)
	}
}

type duplicateDeliveryAcknowledger struct {
	ackedTag uint64
}

func (acknowledger *duplicateDeliveryAcknowledger) Ack(tag uint64, _ bool) error {
	acknowledger.ackedTag = tag
	return nil
}

func (*duplicateDeliveryAcknowledger) Nack(uint64, bool, bool) error {
	return errors.New("duplicate delivery must not be requeued")
}

func (*duplicateDeliveryAcknowledger) Reject(uint64, bool) error {
	return errors.New("duplicate delivery must not be rejected")
}
