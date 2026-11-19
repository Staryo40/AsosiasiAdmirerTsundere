//go:build integration

package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Staryo40/AsosiasiAdmirerTsundere/bnpb/internal/platform/correlation"
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

	var forwardedBody []byte
	var forwardedHeaders http.Header
	ownerMux := http.NewServeMux()
	registerCatalogRoutes(ownerMux, store, cfg, nil)
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwardedHeaders = r.Header.Clone()
		forwardedBody, _ = io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(forwardedBody))
		ownerMux.ServeHTTP(w, r)
	}))
	defer owner.Close()

	kube, _ := liveAuthorityFixture("catalog-owner", 1)
	kube.record.OwnerEndpoint = owner.URL
	followerAuthority := &PollAuthority{owner: "catalog-follower", kubernetes: kube}
	followerMux := http.NewServeMux()
	registerCatalogRoutes(followerMux, followerStore, cfg, followerAuthority)
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := correlation.WithID(r.Context(), r.Header.Get(correlation.Header))
		followerMux.ServeHTTP(w, r.WithContext(ctx))
	}))
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
	for attempt := 0; attempt < 2; attempt++ {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, follower.URL+"/internal/catalog/seismic", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+cfg.Catalog.Token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Correlation-ID", "catalog-forward-test")
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
			t.Fatalf("attempt %d returned status=%d response=%+v decodeErr=%v", attempt, response.StatusCode, accepted, decodeErr)
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
	if !bytes.Equal(forwardedBody, payload) {
		t.Fatal("follower did not preserve the original request bytes")
	}
	if forwardedHeaders.Get("Authorization") != "Bearer "+cfg.Catalog.Token ||
		forwardedHeaders.Get("Content-Type") != "application/json" ||
		forwardedHeaders.Get("X-Correlation-ID") != "catalog-forward-test" ||
		forwardedHeaders.Get(catalogForwardedHeader) != "1" {
		t.Fatalf("forwarded headers were not preserved: %v", forwardedHeaders)
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
