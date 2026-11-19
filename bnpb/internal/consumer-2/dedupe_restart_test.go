package consumer2

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
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
}
