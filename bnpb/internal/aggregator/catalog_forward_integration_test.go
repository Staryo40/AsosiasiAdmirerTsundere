//go:build integration

package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/httpserver"
	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/logging"
)

func TestCatalogFollowerForwardsOriginalReceiptAndChunksAtOwner(t *testing.T) {
	cfg := m2IsolatedConfig(t)
	cfg.Catalog.Token = "catalog-forward-test-token"
	cfg.IngestChunkRecords = 2
	store := m2OpenStore(t, cfg)
	followerStore := m2OpenStore(t, cfg)
	before, err := store.pollPosition(t.Context(), SeismicEndpoint)
	if err != nil {
		t.Fatal(err)
	}

	type forwardedReceipt struct {
		body    []byte
		headers http.Header
		traceID string
	}
	forwarded := make(chan forwardedReceipt, 1)
	logger := logging.New("catalog-forward-test", slog.LevelInfo, io.Discard)
	ownerMux := http.NewServeMux()
	registerCatalogRoutes(ownerMux, store, cfg, nil)
	ownerHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read forwarded receipt: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		forwarded <- forwardedReceipt{body: body, headers: r.Header.Clone(), traceID: correlation.TraceID(r.Context())}
		r.Body = io.NopCloser(bytes.NewReader(body))
		ownerMux.ServeHTTP(w, r)
	})
	owner := httptest.NewServer(httpserver.Handler(ownerHandler, logger, 1))
	defer owner.Close()

	kube, _ := liveAuthorityFixture("catalog-owner", 1)
	kube.record.OwnerEndpoint = owner.URL
	followerAuthority := &PollAuthority{owner: "catalog-follower", kubernetes: kube}
	followerMux := http.NewServeMux()
	registerCatalogRoutes(followerMux, followerStore, cfg, followerAuthority)
	follower := httptest.NewServer(httpserver.Handler(followerMux, logger, 1))
	defer follower.Close()

	const inputID = "catalog-forward-retry"
	received := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	records := []map[string]any{
		{"event_id": "forward-event-1", "magnitude": 5.1, "depth_km": 10.0, "epicenter_lat": -7.8, "epicenter_lon": 110.4, "region_name": "Fixture", "occurred_at": "2026-11-18T11:00:00Z", "potential_tsunami": false},
		{"event_id": "forward-event-2", "magnitude": 5.2, "depth_km": 11.0, "epicenter_lat": -7.9, "epicenter_lon": 110.5, "region_name": "Fixture", "occurred_at": "2026-11-18T11:01:00Z", "potential_tsunami": false},
		{"event_id": "forward-event-3", "magnitude": 5.3, "depth_km": 12.0, "epicenter_lat": -8.0, "epicenter_lon": 110.6, "region_name": "Fixture", "occurred_at": "2026-11-18T11:02:00Z", "potential_tsunami": false},
	}
	payload, err := json.Marshal(struct {
		InputID     string           `json:"input_id"`
		InputOffset int              `json:"input_offset"`
		ReceivedAt  time.Time        `json:"received_at"`
		Records     []map[string]any `json:"records"`
	}{inputID, 40, received, records})
	if err != nil {
		t.Fatal(err)
	}
	payload = append(payload, '\n')
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const parent = "00-" + traceID + "-00f067aa0ba902b7-03"
	for _, attempt := range []struct {
		name, parent, wantTraceID string
		wantParents               []string
	}{
		{name: "valid", parent: parent, wantTraceID: traceID, wantParents: []string{parent}},
		{name: "invalid retry", parent: "00-" + traceID + "-0000000000000000-03"},
	} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, follower.URL+"/internal/catalog/seismic", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+cfg.Catalog.Token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(correlation.Header, "catalog-forward-test")
		request.Header.Set("Traceparent", attempt.parent)
		request.Header.Set("Tracestate", "vendor=untrusted")
		request.Header.Set("Baggage", "key=untrusted")
		response, err := follower.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var accepted struct {
			Accepted    int    `json:"accepted"`
			InputID     string `json:"input_id"`
			InputOffset int    `json:"input_offset"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&accepted)
		_ = response.Body.Close()
		if decodeErr != nil || response.StatusCode != http.StatusOK || accepted.Accepted != 3 || accepted.InputID != inputID || accepted.InputOffset != 40 {
			t.Fatalf("%s attempt returned status=%d response=%+v decodeErr=%v", attempt.name, response.StatusCode, accepted, decodeErr)
		}
		var got forwardedReceipt
		select {
		case got = <-forwarded:
		default:
			t.Fatalf("%s attempt was accepted without reaching the owner", attempt.name)
		}
		if !bytes.Equal(got.body, payload) {
			t.Fatalf("%s attempt did not preserve the original request bytes", attempt.name)
		}
		if got.headers.Get("Authorization") != "Bearer "+cfg.Catalog.Token ||
			got.headers.Get("Content-Type") != "application/json" ||
			got.headers.Get(correlation.Header) != "catalog-forward-test" ||
			got.headers.Get(catalogForwardedHeader) != "1" {
			t.Fatalf("%s attempt did not preserve forwarded headers: %v", attempt.name, got.headers)
		}
		if parents := got.headers.Values("Traceparent"); !slices.Equal(parents, attempt.wantParents) || got.traceID != attempt.wantTraceID {
			t.Fatalf("%s attempt: owner traceparent=%q trace ID=%q, want %q and %q",
				attempt.name, parents, got.traceID, attempt.wantParents, attempt.wantTraceID)
		}
		if len(got.headers.Values("Tracestate")) != 0 || len(got.headers.Values("Baggage")) != 0 {
			t.Fatalf("%s attempt forwarded tracestate or baggage", attempt.name)
		}
	}
	var count int
	for _, query := range []string{
		`SELECT count(*) FROM hazard_events WHERE source='BMKG' AND source_ref_id LIKE 'forward-event-%'`,
		`SELECT count(*) FROM source_seismic WHERE event_id LIKE 'forward-event-%'`,
		`SELECT count(*) FROM outbox o JOIN hazard_events h USING (hazard_id) WHERE h.source='BMKG' AND h.source_ref_id LIKE 'forward-event-%'`,
		`SELECT count(*) FROM handled_inputs WHERE endpoint='seismic' AND convert_from(raw_payload,'UTF8')::jsonb->>'event_id' LIKE 'forward-event-%'`,
	} {
		if err := store.read.QueryRow(t.Context(), query).Scan(&count); err != nil || count != 3 {
			t.Fatalf("durable count for %q = %d, err=%v; want exactly three", query, count, err)
		}
	}
	after, err := store.pollPosition(t.Context(), SeismicEndpoint)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("catalog forwarding changed poll state: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestCatalogForwardingStopsAfterOneHop(t *testing.T) {
	cfg := Config{Catalog: CatalogConfig{Token: "catalog-loop-test-token"}}
	kube, _ := liveAuthorityFixture("catalog-owner", 1)
	var requests atomic.Int32
	var server *httptest.Server
	mux := http.NewServeMux()
	authority := &PollAuthority{owner: "catalog-follower", kubernetes: kube}
	registerCatalogRoutes(mux, &Store{}, cfg, authority)
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		mux.ServeHTTP(w, r)
	}))
	defer server.Close()
	kube.record.OwnerEndpoint = server.URL
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/internal/catalog/seismic", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+cfg.Catalog.Token)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || requests.Load() != 2 {
		t.Fatalf("loop response status=%d, requests=%d; want one forwarded hop then 503", response.StatusCode, requests.Load())
	}
}
