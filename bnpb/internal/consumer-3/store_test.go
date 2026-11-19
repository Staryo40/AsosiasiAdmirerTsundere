package consumer3

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "consumer-3.db")
	store, err := OpenStore(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func sampleEntry(messageID string) FeedEntry {
	return FeedEntry{
		MessageID: messageID, HazardID: "hz-1", Source: "BMKG", HazardType: "SEISMIC",
		Severity: "WASPADA", AreaName: "Test Area",
		OccurredAt: time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC),
	}
}

func TestApplyAppendsAFeedEntry(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	if err := store.Apply(ctx, sampleEntry("m-1")); err != nil {
		t.Fatal(err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.FeedEntries != 1 || stats.MessagesSeen != 1 {
		t.Fatalf("unexpected stats after one fresh message: %+v", stats)
	}
}

func TestApplyAppendsEveryDistinctMessageEvenForTheSameHazard(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	first := sampleEntry("m-1")
	first.Severity = "WASPADA"
	second := sampleEntry("m-2")
	second.Severity = "AWAS"
	if err := store.Apply(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, second); err != nil {
		t.Fatal(err)
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.FeedEntries != 2 {
		t.Fatalf("a feed must append, not overwrite, got %d entries", stats.FeedEntries)
	}
}

func TestApplySkipsADuplicateMessageID(t *testing.T) {
	store := openTestStore(t)
	ctx := t.Context()
	if err := store.Apply(ctx, sampleEntry("m-1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(ctx, sampleEntry("m-1")); !errors.Is(err, errDuplicateMessage) {
		t.Fatalf("duplicate message_id was not detected: %v", err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.FeedEntries != 1 {
		t.Fatalf("a duplicate produced a second entry: %+v", stats)
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
	path := filepath.Join(t.TempDir(), "consumer-3.db")
	ctx := context.Background()
	first, err := OpenStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Apply(ctx, sampleEntry("m-1")); err != nil {
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
	if stats.FeedEntries != 1 {
		t.Fatal("state did not survive closing and reopening the database file")
	}
}
