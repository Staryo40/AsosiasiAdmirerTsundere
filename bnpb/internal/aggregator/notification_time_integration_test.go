//go:build integration

package aggregator

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNotificationProducedAtDoesNotPrecedeFirstReceipt(t *testing.T) {
	store := m2OpenStore(t, m2IsolatedConfig(t))
	identity := "notification-time-skew-" + newReceipt(time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)).HazardID
	raw := p1WithField(t, p1Fixtures(t)[1].Input, "event_id", p1Encode(t, identity))
	changed := p1WithField(t, raw, "magnitude", json.RawMessage("7.2"))
	receivedAt := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	firstReceiptAt := receivedAt.Add(45 * time.Second)
	changedReceiptAt := receivedAt.Add(-30 * time.Second)

	first := IngestChunk{Endpoint: SeismicEndpoint, Records: []json.RawMessage{raw}, ReceivedAt: firstReceiptAt, InputID: "time-skew-first"}
	if err := store.Ingest(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	var initialID string
	if err := store.read.QueryRow(t.Context(), "SELECT hazard_id FROM hazard_events WHERE source_ref_id=$1", identity).Scan(&initialID); err != nil {
		t.Fatal(err)
	}

	changedReceipt := IngestChunk{Endpoint: SeismicEndpoint, Records: []json.RawMessage{changed}, ReceivedAt: changedReceiptAt, InputID: "time-skew-changed"}
	if err := store.Ingest(t.Context(), changedReceipt); err != nil {
		t.Fatal(err)
	}
	var hazardID string
	var ingestedAt time.Time
	var revision int64
	if err := store.read.QueryRow(t.Context(), `SELECT hazard_id, ingested_at, revision FROM hazard_events WHERE source_ref_id=$1`, identity).Scan(&hazardID, &ingestedAt, &revision); err != nil {
		t.Fatal(err)
	}
	if hazardID != initialID || revision != 2 || !ingestedAt.Equal(firstReceiptAt) {
		t.Fatalf("hazard identity/revision/first receipt = %q/%d/%v, want %q/2/%v", hazardID, revision, ingestedAt, initialID, firstReceiptAt)
	}

	messages, err := store.pendingOutbox(t.Context(), 10)
	if err != nil || len(messages) != 2 {
		t.Fatalf("notifications=%d err=%v, want two revisions", len(messages), err)
	}
	var notification hazardNotification
	if err := json.Unmarshal(messages[1].Payload, &notification); err != nil {
		t.Fatal(err)
	}
	if notification.SchemaVersion != 1 || notification.HazardID != hazardID || notification.Revision != 2 {
		t.Fatalf("changed notification envelope = schema %d hazard %q revision %d", notification.SchemaVersion, notification.HazardID, notification.Revision)
	}
	if !notification.HazardEvent.IngestedAt.Equal(ingestedAt) || notification.ProducedAt.Before(notification.HazardEvent.IngestedAt) {
		t.Fatalf("notification timestamps produced=%v ingested=%v; want produced_at >= first ingested_at %v", notification.ProducedAt, notification.HazardEvent.IngestedAt, ingestedAt)
	}

	if err := store.Ingest(t.Context(), changedReceipt); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.pendingOutbox(t.Context(), 10)
	if err != nil || len(replayed) != len(messages) {
		t.Fatalf("replayed notifications=%d err=%v, want unchanged count %d", len(replayed), err, len(messages))
	}
	for index := range messages {
		if messages[index].HazardID != replayed[index].HazardID || messages[index].Revision != replayed[index].Revision || string(messages[index].Payload) != string(replayed[index].Payload) {
			t.Fatalf("receipt replay changed notification %d identity or body", index)
		}
	}
}
