package consumer2

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "consumer-2.db")
	store, err := OpenStore(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func sampleAlert(messageID, severity string) HazardAlert {
	return HazardAlert{
		MessageID: messageID, HazardID: "hz-1", Severity: severity, AreaName: "Test Area",
		OccurredAt: time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC),
	}
}

func TestApplyRecordsAnAlertForSiagaAndAwas(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	for _, severity := range []string{"SIAGA", "AWAS"} {
		result, err := store.Apply(ctx, sampleAlert("m-"+severity, severity))
		if err != nil {
			t.Fatal(err)
		}
		if !result.AlertSent {
			t.Fatalf("%s did not produce an alert", severity)
		}
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.AlertsSent != 2 || stats.MessagesSeen != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestApplyIgnoresNormalAndWaspada(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	for _, severity := range []string{"NORMAL", "WASPADA"} {
		result, err := store.Apply(ctx, sampleAlert("m-"+severity, severity))
		if err != nil {
			t.Fatal(err)
		}
		if result.AlertSent {
			t.Fatalf("%s unexpectedly produced an alert", severity)
		}
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.AlertsSent != 0 || stats.MessagesSeen != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestApplySkipsADuplicateMessageID(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	if _, err := store.Apply(ctx, sampleAlert("m-1", "AWAS")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply(ctx, sampleAlert("m-1", "AWAS")); !errors.Is(err, errDuplicateMessage) {
		t.Fatalf("duplicate message_id was not detected: %v", err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.AlertsSent != 1 {
		t.Fatalf("a duplicate produced a second alert: %+v", stats)
	}
}

func TestIncrementAttemptsCountsUpPerMessage(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	for expected := int64(1); expected <= 3; expected++ {
		attempts, err := store.IncrementAttempts(ctx, "m-1")
		if err != nil {
			t.Fatal(err)
		}
		if attempts != expected {
			t.Fatalf("attempt %d: got count %d", expected, attempts)
		}
	}
	first, err := store.IncrementAttempts(ctx, "m-2")
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 {
		t.Fatalf("a different message_id shares a counter: got %d", first)
	}
}

func TestOpenStoreIsReusableAcrossReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer-2.db")
	ctx := context.Background()
	first, err := OpenStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Apply(ctx, sampleAlert("m-1", "AWAS")); err != nil {
		t.Fatal(err)
	}
	first.Close()

	second, err := OpenStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	stats, err := second.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.AlertsSent != 1 {
		t.Fatal("state did not survive closing and reopening the database file")
	}
}
