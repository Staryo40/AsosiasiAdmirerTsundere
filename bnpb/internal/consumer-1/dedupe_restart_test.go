package consumer1

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestProcessedMessagesSurviveRestartAndAge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer-1.db")
	ctx := context.Background()
	store, err := OpenStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	initial := sampleUpdate("stable-message", 3)
	initial.Severity = "AWAS"
	if result, err := store.Apply(ctx, initial); err != nil || !result.Applied {
		t.Fatalf("initial apply=%+v err=%v", result, err)
	}

	aged := time.Now().UTC().Add(-5 * time.Hour).Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(ctx, `UPDATE processed_messages SET processed_at = ? WHERE message_id = ?`, aged, initial.MessageID); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	duplicate := sampleUpdate(initial.MessageID, 4)
	duplicate.Severity = "BAHAYA"
	if _, err := store.Apply(ctx, duplicate); !errors.Is(err, errDuplicateMessage) {
		t.Fatalf("aged message identity was not deduplicated after restart: %v", err)
	}

	older := sampleUpdate("new-message-old-revision", 2)
	older.Severity = "WASPADA"
	if result, err := store.Apply(ctx, older); err != nil || result.Applied {
		t.Fatalf("older revision apply=%+v err=%v", result, err)
	}

	var severity string
	var revision int64
	if err := store.db.QueryRowContext(ctx, `SELECT severity, revision FROM hazard_state WHERE hazard_id = ?`, initial.HazardID).Scan(&severity, &revision); err != nil {
		t.Fatal(err)
	}
	if severity != "AWAS" || revision != 3 {
		t.Fatalf("duplicate or older replay changed business state: severity=%q revision=%d", severity, revision)
	}

	var identityCount int
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM processed_messages WHERE message_id IN (?, ?)`, initial.MessageID, older.MessageID).Scan(&identityCount); err != nil {
		t.Fatal(err)
	}
	if identityCount != 2 {
		t.Fatalf("durable identities=%d, want both original and old-revision message", identityCount)
	}
	var retainedProcessedAt string
	if err := store.db.QueryRowContext(ctx, `SELECT processed_at FROM processed_messages WHERE message_id = ?`, initial.MessageID).Scan(&retainedProcessedAt); err != nil {
		t.Fatal(err)
	}
	if retainedProcessedAt != aged {
		t.Fatalf("aged dedupe record was modified: got %q, want %q", retainedProcessedAt, aged)
	}
}
