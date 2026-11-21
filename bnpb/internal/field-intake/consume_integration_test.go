//go:build integration

package fieldintake

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func startIntake(t *testing.T, cfg Config, store *Store) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(ctx, cfg, store, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}()
	return func() { cancel(); <-done }
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestIntakeStoresEachReportOnceAcrossDuplicatesAndARestart(t *testing.T) {
	address := os.Getenv("BNPB_TEST_AMQP_URL")
	if address == "" {
		t.Skip("set BNPB_TEST_AMQP_URL to run broker integration tests")
	}
	connection, err := amqp.Dial(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	admin, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = admin.QueueDelete(reportQueue, false, false, false)
	if _, err := admin.QueueDeclare(reportQueue, true, false, false, false, queueArguments); err != nil {
		t.Fatal(err)
	}
	if err := admin.Confirm(false); err != nil {
		t.Fatal(err)
	}
	const unique = 200
	publish := func(messageID string, body []byte) {
		confirmation, err := admin.PublishWithDeferredConfirmWithContext(context.Background(), "", reportQueue, true, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, MessageId: messageID, Body: body})
		if err != nil || !confirmation.Wait() {
			t.Fatalf("publish %s: %v", messageID, err)
		}
	}
	for index := range unique {
		report := Report{ClientID: "field-client", ReportID: "fr-" + strconv.Itoa(index), ReportedAt: time.Now().UTC(),
			Latitude: -2.5, Longitude: 113.8, HazardType: "WILDFIRE", Description: "laporan " + strconv.Itoa(index), AcceptedAt: time.Now().UTC()}
		body, _ := json.Marshal(report)
		publish("fr-msg-"+strconv.Itoa(index), body)
		if index%2 == 0 {
			// A client retry: same report, accepted again later.
			report.AcceptedAt = report.AcceptedAt.Add(time.Second)
			retry, _ := json.Marshal(report)
			publish("fr-msg-"+strconv.Itoa(index), retry)
		}
	}
	for _, garbage := range []string{`{"report_id":`, `[]`, `{"client_id":"x"}`} {
		publish("garbage", []byte(garbage))
	}

	store, err := OpenStore(context.Background(), filepath.Join(t.TempDir(), "intake.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	cfg := Config{BrokerURL: address}
	page := func() ReportPage {
		current, err := store.Page(context.Background(), 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		return current
	}

	stop := startIntake(t, cfg, store)
	waitFor(t, 30*time.Second, func() bool { return page().Stored >= unique/4 })
	stop() // killed mid-stream with prefetched, unacknowledged deliveries
	stopped := page().Stored
	if stopped >= unique {
		t.Skip("the intake finished before it could be stopped mid-stream")
	}

	stop = startIntake(t, cfg, store)
	defer stop()
	waitFor(t, 30*time.Second, func() bool { current := page(); return current.Stored == unique && current.Rejected == 3 })
	time.Sleep(300 * time.Millisecond)
	final := page()
	var deliveries int
	if err := store.db.QueryRow("SELECT sum(deliveries) FROM field_reports").Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if final.Stored != unique || final.Conflicts != 0 || final.Rejected != 3 || deliveries < unique+unique/2 {
		t.Fatalf("stored=%d conflicts=%d rejected=%d deliveries=%d (stopped at %d)", final.Stored, final.Conflicts, final.Rejected, deliveries, stopped)
	}
	queue, err := admin.QueueDeclarePassive(reportQueue, true, false, false, false, queueArguments)
	if err != nil || queue.Messages != 0 {
		t.Fatalf("queue still holds %d messages, err=%v", queue.Messages, err)
	}
}
