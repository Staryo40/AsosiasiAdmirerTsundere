package consumer3

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestApplyKeepsAgedMessageIdentityAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer-3.db")
	ctx := t.Context()
	store, err := OpenStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, sampleEntry("aged-message")); err != nil {
		t.Fatal(err)
	}
	agedAt := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	if _, err := store.db.ExecContext(ctx,
		`UPDATE processed_messages SET processed_at = ? WHERE message_id = ?`, agedAt, "aged-message"); err != nil {
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

	if err := store.Apply(ctx, sampleEntry("aged-message")); !errors.Is(err, errDuplicateMessage) {
		t.Fatalf("restarted store accepted an aged duplicate: %v", err)
	}
	if err := store.Apply(ctx, sampleEntry("different-message")); err != nil {
		t.Fatalf("different message was not processible after restart: %v", err)
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.FeedEntries != 2 || stats.MessagesSeen != 2 {
		t.Fatalf("unexpected durable state after duplicate and distinct deliveries: %+v", stats)
	}
}
