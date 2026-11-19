//go:build integration

package aggregator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestCatalogImportDoesNotAdvancePollState(t *testing.T) {
	cfg := m2IsolatedConfig(t)
	cfg.Catalog = CatalogConfig{Token: "catalog-import-test-token"}
	store := m2OpenStore(t, cfg)

	checkpoint := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	lastAttempt := checkpoint.Add(time.Hour)
	lastSuccess := checkpoint.Add(-time.Hour)
	staleSince := checkpoint.Add(2 * time.Hour)
	if _, err := store.write.Exec(t.Context(), `UPDATE poll_state SET
		checkpoint=$1, last_attempt_at=$2, last_successful_poll=$3,
		stale_since=$4, error_category='upstream_timeout'
		WHERE endpoint='seismic'`, checkpoint, lastAttempt, lastSuccess, staleSince); err != nil {
		t.Fatal(err)
	}
	before, err := store.pollPosition(t.Context(), SeismicEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	freshnessTime := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	freshnessBefore := endpointObservation(endpointSnapshot{
		endpoint: SeismicEndpoint, pollObservation: before.observation,
	}, freshnessTime, cfg)

	mux := http.NewServeMux()
	registerCatalogRoutes(mux, store, cfg, nil)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	const eventID = "catalog-seismic-event"
	payload := []byte(`{"input_id":"catalog-receipt-1","input_offset":0,"received_at":"2026-11-18T12:00:00Z","records":[{"event_id":"catalog-seismic-event","magnitude":6.1,"depth_km":12.5,"epicenter_lat":-7.8,"epicenter_lon":110.4,"region_name":"Fixture","occurred_at":"2026-11-18T11:00:00Z","potential_tsunami":false}]}`)
	for attempt := 1; attempt <= 2; attempt++ {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/internal/catalog/seismic", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+cfg.Catalog.Token)
		request.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var accepted struct {
			Accepted int    `json:"accepted"`
			InputID  string `json:"input_id"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&accepted)
		closeErr := response.Body.Close()
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if response.StatusCode != http.StatusOK || accepted.Accepted != 1 || accepted.InputID != "catalog-receipt-1" {
			t.Fatalf("attempt %d: status=%d response=%+v, want successful acceptance of one record", attempt, response.StatusCode, accepted)
		}
	}

	var canonical, source, notifications, receipts int
	for _, check := range []struct {
		query string
		count *int
	}{
		{`SELECT count(*) FROM hazard_events WHERE source='BMKG' AND source_ref_id=$1`, &canonical},
		{`SELECT count(*) FROM source_seismic WHERE event_id=$1`, &source},
		{`SELECT count(*) FROM outbox o JOIN hazard_events h USING (hazard_id) WHERE h.source='BMKG' AND h.source_ref_id=$1`, &notifications},
		{`SELECT count(*) FROM handled_inputs WHERE endpoint='seismic' AND convert_from(raw_payload,'UTF8')::jsonb->>'event_id'=$1`, &receipts},
	} {
		if err := store.read.QueryRow(t.Context(), check.query, eventID).Scan(check.count); err != nil {
			t.Fatal(err)
		}
	}
	if canonical != 1 || source != 1 || notifications != 1 || receipts != 1 {
		t.Fatalf("durable rows canonical/source/outbox/handled=%d/%d/%d/%d, want 1 each", canonical, source, notifications, receipts)
	}
	after, err := store.pollPosition(t.Context(), SeismicEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("catalog import changed poll state: before=%+v after=%+v", before, after)
	}
	freshnessAfter := endpointObservation(endpointSnapshot{
		endpoint: SeismicEndpoint, pollObservation: after.observation,
	}, freshnessTime, cfg)
	if !reflect.DeepEqual(freshnessAfter, freshnessBefore) {
		t.Fatalf("catalog import changed endpoint freshness: before=%+v after=%+v", freshnessBefore, freshnessAfter)
	}
}
