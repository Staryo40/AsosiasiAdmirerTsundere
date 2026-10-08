package consumer1

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "consumer-1.db")
	store, err := OpenStore(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func sampleUpdate(messageID string, revision int64) HazardUpdate {
	return HazardUpdate{
		MessageID: messageID, HazardID: "hz-1", Source: "BMKG", HazardType: "SEISMIC",
		Severity: "WASPADA", AreaName: "Test Area",
		OccurredAt: time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC),
		IngestedAt: time.Date(2026, 10, 8, 10, 0, 1, 0, time.UTC),
		Revision:   revision,
	}
}

func TestApplyRecordsAFreshHazard(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	result, err := store.Apply(ctx, sampleUpdate("m-1", 1))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied {
		t.Fatal("a fresh hazard was not reported as applied")
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.HazardsTracked != 1 || stats.MessagesSeen != 1 {
		t.Fatalf("unexpected stats after one fresh message: %+v", stats)
	}
}

func TestApplyOverwritesOnlyWithANewerRevision(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	update := sampleUpdate("m-1", 3)
	update.Severity = "AWAS"
	if result, err := store.Apply(ctx, update); err != nil || !result.Applied {
		t.Fatalf("applied=%v err=%v", result.Applied, err)
	}

	older := sampleUpdate("m-2", 2)
	older.Severity = "WASPADA"
	result, err := store.Apply(ctx, older)
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied {
		t.Fatal("an older revision was reported as applied")
	}

	var severity string
	if err := store.db.QueryRowContext(ctx, `SELECT severity FROM hazard_state WHERE hazard_id = ?`, "hz-1").Scan(&severity); err != nil {
		t.Fatal(err)
	}
	if severity != "AWAS" {
		t.Fatalf("an older revision rolled severity back to %s", severity)
	}
}

func TestApplySkipsADuplicateMessageID(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	if _, err := store.Apply(ctx, sampleUpdate("m-1", 1)); err != nil {
		t.Fatal(err)
	}
	duplicate := sampleUpdate("m-1", 99)
	duplicate.Severity = "AWAS"
	if _, err := store.Apply(ctx, duplicate); !errors.Is(err, errDuplicateMessage) {
		t.Fatalf("duplicate message_id was not detected: %v", err)
	}

	var severity string
	if err := store.db.QueryRowContext(ctx, `SELECT severity FROM hazard_state WHERE hazard_id = ?`, "hz-1").Scan(&severity); err != nil {
		t.Fatal(err)
	}
	if severity == "AWAS" {
		t.Fatal("a rolled-back duplicate still wrote hazard_state")
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

func TestPruneOldRecordsRemovesOnlyStaleRows(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	if _, err := store.Apply(ctx, sampleUpdate("fresh", 1)); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-5 * time.Hour).UTC().Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO processed_messages (message_id, processed_at) VALUES (?, ?)`, "stale", stale); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO delivery_attempts (message_id, attempts, updated_at) VALUES (?, 1, ?)`, "stale", stale); err != nil {
		t.Fatal(err)
	}

	if err := store.PruneOldRecords(ctx); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM processed_messages WHERE message_id = ?`, "stale").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("a stale processed_messages row survived pruning")
	}
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM processed_messages WHERE message_id = ?`, "fresh").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("a fresh processed_messages row was pruned")
	}
}

func TestOpenStoreIsReusableAcrossReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer-1.db")
	ctx := context.Background()
	first, err := OpenStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Apply(ctx, sampleUpdate("m-1", 1)); err != nil {
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
	if stats.HazardsTracked != 1 {
		t.Fatal("state did not survive closing and reopening the database file")
	}
}
