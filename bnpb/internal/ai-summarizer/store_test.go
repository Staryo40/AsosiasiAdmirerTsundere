package aisummarizer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreKeepsTheNewestRevisionAndDropsOldEvents(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStore(ctx, filepath.Join(t.TempDir(), "summary.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now()
	apply := func(message, hazard string, revision int64, severity string, at time.Time) error {
		return store.Apply(ctx, Event{MessageID: message, HazardID: hazard, Revision: revision, HazardType: "SEISMIC",
			Severity: severity, AreaName: "Palu", ReceivedAt: at})
	}
	if err := apply("m1", "h1", 1, "WASPADA", now); err != nil {
		t.Fatal(err)
	}
	if err := apply("m1", "h1", 1, "WASPADA", now); !errors.Is(err, errDuplicateMessage) {
		t.Fatalf("duplicate err = %v", err)
	}
	if err := apply("m2", "h1", 2, "AWAS", now); err != nil {
		t.Fatal(err)
	}
	if err := apply("m3", "h1", 1, "NORMAL", now); err != nil {
		t.Fatal(err)
	}
	if err := apply("m4", "h2", 1, "NORMAL", now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	events, err := store.Recent(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Severity != "AWAS" {
		t.Fatalf("events = %+v, want only h1 at revision 2", events)
	}
}

func TestStoreReturnsTheLatestSummary(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStore(ctx, filepath.Join(t.TempDir(), "summary.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, found, _ := store.Latest(ctx); found {
		t.Fatal("found a summary in an empty store")
	}
	for _, provider := range []string{templateName, "anthropic"} {
		if err := store.SaveSummary(ctx, Summary{Text: "x", Provider: provider, GeneratedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	latest, found, err := store.Latest(ctx)
	if err != nil || !found || latest.Provider != "anthropic" {
		t.Fatalf("latest = %+v, found = %v, err = %v", latest, found, err)
	}
}
