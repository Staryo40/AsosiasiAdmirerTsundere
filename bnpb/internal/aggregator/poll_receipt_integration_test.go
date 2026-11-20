//go:build integration

package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestPollReceiptIntegrationReplacementResumesCommittedPrefix(t *testing.T) {
	cfg := routedM2Config(t)
	cfg.IngestChunkRecords = 1
	store := openRoutedM2Store(t, cfg)
	activateM2ShardFixtureOwner(t, cfg, store)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	// The first sorted chunk can commit, but the next cannot bind its shard guard.
	first := fixtureMutationContext(t, ctx, fixtureMutationOwner, fixtureMutationEpoch, store, store.shards[0])
	started := time.Date(2026, 11, 18, 4, 0, 0, 0, time.UTC)
	identities := routedShardIdentities(t)
	records := make([]json.RawMessage, len(identities))
	for index, identity := range identities {
		compact := routedRecord(t, identity, started.Add(time.Duration(index)*time.Second))
		records[index] = append(json.RawMessage("{ \t "), compact[1:]...)
	}
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte("["))
		_, _ = w.Write(bytes.Join([][]byte{records[2], records[0], records[1]}, []byte(",")))
		_, _ = w.Write([]byte("]"))
	}))
	t.Cleanup(upstream.Close)
	cfg.BMKG.BaseURL = upstream.URL
	logger := quietLogger()
	sources := newSourceClients(cfg, logger)
	t.Cleanup(sources.closeIdleConnections)
	initialPoller := &poller{cfg: cfg, store: store, sources: sources, logger: logger}
	if err := initialPoller.pollOnce(first, SeismicEndpoint, started); !errors.Is(err, errStaleMutationAuthority) {
		t.Fatalf("initial poll error = %v, want rejection at second chunk", err)
	}
	assertRoutedCheckpoint(t, ctx, store, SeismicEndpoint, &started)
	assertRoutedOutcome(t, store.shards[0], identities[0], 1)
	assertRoutedOutcome(t, store.shards[1], identities[1], 0)
	assertRoutedOutcome(t, store.shards[2], identities[2], 0)
	pending, err := store.pendingPollReceipt(first, SeismicEndpoint)
	if err != nil || pending.response.InputID == "" || !pending.started.Equal(started) || len(pending.response.Records) != len(records) {
		t.Fatalf("expected pending original response: id=%q started=%v records=%d err=%v",
			pending.response.InputID, pending.started, len(pending.response.Records), err)
	}
	for index, raw := range records {
		if !bytes.Equal(pending.response.Records[index], raw) {
			t.Fatalf("pending receipt record %d lost its original bytes or ingest order", index)
		}
	}

	for _, address := range append([]string{cfg.DatabaseURL}, cfg.ShardURLs[:]...) {
		closeFixtureMutationGuard(t, address)
	}
	owner := activateFixtureOwner(t, cfg, store, "poll-receipt-replacement", fixtureMutationEpoch+1)
	owner, cancelOwner := context.WithTimeout(owner, 10*time.Second)
	defer cancelOwner()
	if err := store.clearPollReceipt(first, pending.response); !errors.Is(err, errStaleMutationAuthority) {
		t.Fatalf("clearing with stale owner error = %v, want stale authority rejection", err)
	}
	if err := store.stagePollReceipt(first, pending); !errors.Is(err, errStaleMutationAuthority) {
		t.Fatalf("staging with stale owner error = %v, want stale authority rejection", err)
	}
	replacement := &poller{cfg: cfg, store: store, sources: sources, logger: logger}
	if err := replacement.pollOnce(owner, SeismicEndpoint, started.Add(time.Minute)); err != nil {
		t.Fatalf("resuming pending poll: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("upstream requests = %d, want only the original fetch", got)
	}
	wantCheckpoint := started.Add(2 * time.Second)
	assertRoutedCheckpoint(t, owner, store, SeismicEndpoint, &wantCheckpoint)
	assertCompletedPollReceipt(t, owner, store, SeismicEndpoint, started)
	for index, identity := range identities {
		shard := store.shards[index]
		assertRoutedOutcome(t, shard, identity, 1)
		chunk := pending.response
		chunk.InputOffset = index
		var raw []byte
		var received time.Time
		if err := shard.read.QueryRow(owner, `SELECT raw_payload, received_at FROM handled_inputs
			WHERE endpoint = $1 AND input_hash = $2`, string(chunk.Endpoint), inputHash(chunk)).Scan(&raw, &received); err != nil {
			t.Fatalf("original receipt ledger row %d: %v", index, err)
		}
		if !bytes.Equal(raw, records[index]) || !received.Equal(chunk.ReceivedAt) {
			t.Fatalf("receipt ledger row %d lost its original bytes or receipt time", index)
		}
	}
}

func TestPollReceiptIntegrationReplaysEmptyWarningReconciliation(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	owner := activateM2ShardFixtureOwner(t, cfg, store)
	started := time.Date(2026, 11, 18, 5, 0, 0, 0, time.UTC)
	if err := m2ExecFixtureMutation(owner, store, `UPDATE poll_state SET checkpoint = $1 WHERE endpoint = 'warnings'`, started.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	staged := pollReceipt{
		response: IngestChunk{Endpoint: WarningEndpoint, InputID: "empty-full-warning", ReceivedAt: started.Add(time.Second)},
		started:  started, reconcilesWarnings: true,
	}
	if err := store.stagePollReceipt(owner, staged); err != nil {
		t.Fatal(err)
	}
	pending, err := store.pendingPollReceipt(owner, WarningEndpoint)
	if err != nil || pending.response.InputID != staged.response.InputID || len(pending.response.Records) != 0 || !pending.reconcilesWarnings {
		t.Fatalf("expected an empty pending response: id=%q records=%d full=%t err=%v",
			pending.response.InputID, len(pending.response.Records), pending.reconcilesWarnings, err)
	}
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(upstream.Close)
	cfg.BMKG.BaseURL = upstream.URL
	logger := quietLogger()
	sources := newSourceClients(cfg, logger)
	t.Cleanup(sources.closeIdleConnections)
	replacement := &poller{cfg: cfg, store: store, sources: sources, logger: logger}
	if err := replacement.pollOnce(owner, WarningEndpoint, started.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("upstream requests = %d, want zero for empty-response replay", got)
	}
	if !replacement.lastWarningReconcile.Equal(started) {
		t.Fatalf("warning reconciliation was recorded at %v, want original %v", replacement.lastWarningReconcile, started)
	}
	assertRoutedCheckpoint(t, owner, store, WarningEndpoint, &started)
	assertCompletedPollReceipt(t, owner, store, WarningEndpoint, started)
}

func assertCompletedPollReceipt(t *testing.T, ctx context.Context, store *Store, endpoint Endpoint, started time.Time) {
	t.Helper()
	pending, err := store.pendingPollReceipt(ctx, endpoint)
	if err != nil || pending.response.InputID != "" {
		t.Fatalf("receipt still pending after success: id=%q err=%v", pending.response.InputID, err)
	}
	position, err := store.pollPosition(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	observation := position.observation
	if observation.LastSuccessfulPoll == nil || observation.LastAttemptAt == nil || !observation.LastAttemptAt.Equal(started) ||
		observation.StaleSince != nil || observation.ErrorCategory != nil {
		t.Fatalf("completed observation did not preserve the original attempt time %v: %+v", started, observation)
	}
}

func TestPollWorkerHandoffQuietInterval(t *testing.T) {
	const interval = 3 * time.Second
	for _, test := range []struct {
		name           string
		routed         bool
		cancelEarly    bool
		firstPollDelay time.Duration
	}{
		{name: "legacy polls immediately"},
		{name: "routed waits one interval", routed: true, firstPollDelay: interval},
		{name: "cancelled handoff never polls", routed: true, cancelEarly: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				store := &Store{}
				if test.routed {
					store.shards[0] = &Store{}
				}
				started := time.Now()
				var firstPoll time.Time
				worker := (&poller{store: store}).endpointWorker(interval, func(context.Context) error {
					firstPoll = time.Now()
					cancel()
					return nil
				})
				done := make(chan struct{})
				go func() {
					defer close(done)
					worker(ctx)
				}()
				if test.cancelEarly {
					time.Sleep(interval - time.Nanosecond)
					cancel()
				}
				<-done
				if test.cancelEarly {
					if !firstPoll.IsZero() {
						t.Fatal("poll was dispatched before the quiet interval elapsed")
					}
				} else if got := firstPoll.Sub(started); got != test.firstPollDelay {
					t.Fatalf("first poll at %s, want %s", got, test.firstPollDelay)
				}
			})
		})
	}
}
