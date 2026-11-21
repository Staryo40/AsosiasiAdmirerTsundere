//go:build integration

package aggregator

import (
	"context"
	"errors"
	"testing"
	"time"
)

func pollFenceStore(t *testing.T) *Store {
	t.Helper()
	store, _ := p1TestStore(t)
	if _, err := store.write.Exec(t.Context(), "DELETE FROM poll_fence"); err != nil {
		t.Fatal(err)
	}
	return store
}

func fencedWrite(ctx context.Context, store *Store) error {
	tx, err := store.beginWriteMutation(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rollbackTransaction(ctx, tx) }()
	return tx.Commit(ctx)
}

func TestPollFenceRejectsAnOlderTermAfterATakeover(t *testing.T) {
	store := pollFenceStore(t)
	first := pollLeaseToken{leaseUID: "uid-1", generation: 3, holder: "aggregator-a"}
	second := pollLeaseToken{leaseUID: "uid-1", generation: 4, holder: "aggregator-b"}
	firstCtx, secondCtx := withPollLease(t.Context(), first), withPollLease(t.Context(), second)

	if err := fencedWrite(firstCtx, store); !errors.Is(err, errStalePollLease) {
		t.Fatalf("a term that never claimed the store must not write: %v", err)
	}
	if err := store.claimPollFence(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := fencedWrite(firstCtx, store); err != nil {
		t.Fatalf("the current term writes: %v", err)
	}
	if err := store.claimPollFence(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if err := fencedWrite(firstCtx, store); !errors.Is(err, errStalePollLease) {
		t.Fatalf("a replica that woke after a takeover must not write: %v", err)
	}
	if err := store.claimPollFence(t.Context(), first); !errors.Is(err, errStalePollLease) {
		t.Fatalf("an older term must not reclaim the store: %v", err)
	}
	if err := fencedWrite(secondCtx, store); err != nil {
		t.Fatalf("the new term writes: %v", err)
	}
	if err := store.claimPollFence(t.Context(), second); err != nil {
		t.Fatalf("a restarted holder of the same term reclaims: %v", err)
	}
	if err := fencedWrite(t.Context(), store); err != nil {
		t.Fatalf("writes outside polling stay unfenced: %v", err)
	}
	if pollCategory(errStalePollLease) != "lease_lost" {
		t.Fatal("a stale term must be logged as lease_lost")
	}
}

func TestPollFenceTakeoverWaitsForAnInFlightWrite(t *testing.T) {
	store := pollFenceStore(t)
	first := pollLeaseToken{leaseUID: "uid-1", generation: 1, holder: "aggregator-a"}
	second := pollLeaseToken{leaseUID: "uid-1", generation: 2, holder: "aggregator-b"}
	if err := store.claimPollFence(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	firstCtx := withPollLease(t.Context(), first)
	inFlight, err := store.beginWriteMutation(firstCtx)
	if err != nil {
		t.Fatal(err)
	}
	claimed := make(chan error, 1)
	go func() { claimed <- store.claimPollFence(t.Context(), second) }()
	select {
	case err := <-claimed:
		t.Fatalf("the takeover claim must wait for the in-flight write, got %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := inFlight.Commit(firstCtx); err != nil {
		t.Fatalf("a write that started under the current term commits: %v", err)
	}
	if err := <-claimed; err != nil {
		t.Fatal(err)
	}
	if err := fencedWrite(firstCtx, store); !errors.Is(err, errStalePollLease) {
		t.Fatalf("after the claim the old term is fenced: %v", err)
	}
}

func TestPollFenceAcceptsARecreatedLease(t *testing.T) {
	store := pollFenceStore(t)
	old := pollLeaseToken{leaseUID: "uid-1", generation: 9, holder: "aggregator-a"}
	recreated := pollLeaseToken{leaseUID: "uid-2", generation: 0, holder: "aggregator-b"}
	if err := store.claimPollFence(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	if err := store.claimPollFence(t.Context(), recreated); err != nil {
		t.Fatalf("a recreated Lease restarts its counter and must still claim: %v", err)
	}
	if err := fencedWrite(withPollLease(t.Context(), old), store); !errors.Is(err, errStalePollLease) {
		t.Fatalf("the term from the deleted Lease is fenced: %v", err)
	}
}

func TestPollFenceGuardsTheCoastalCursor(t *testing.T) {
	store := pollFenceStore(t)
	current := pollLeaseToken{leaseUID: "uid-1", generation: 1, holder: "aggregator-a"}
	stale := pollLeaseToken{leaseUID: "uid-1", generation: 0, holder: "aggregator-b"}
	if err := store.claimPollFence(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC()
	if err := store.completeCoastalPoll(withPollLease(t.Context(), stale), CoastalEndpoint, started, "stale-cursor"); !errors.Is(err, errStalePollLease) {
		t.Fatalf("a stale term must not move the coastal cursor: %v", err)
	}
	if err := store.completeCoastalPoll(withPollLease(t.Context(), current), CoastalEndpoint, started, "cursor-1"); err != nil {
		t.Fatal(err)
	}
}
