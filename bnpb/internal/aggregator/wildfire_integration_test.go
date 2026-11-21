//go:build integration

package aggregator

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func wildfireRecordJSON(t *testing.T, id string, revision int64, active bool, count int, occurred time.Time) json.RawMessage {
	t.Helper()
	keys := make([]string, count)
	for index := range keys {
		keys[index] = "N20|2026-11-28|23" + string(rune('0'+index%10)) + "|-2.51|113.8" + string(rune('0'+index))
	}
	raw, err := json.Marshal(map[string]any{"source_ref_id": id, "feed_revision": revision, "active": active, "severity": "SIAGA",
		"area_name": "Kalimantan Tengah", "latitude": -2.51234, "longitude": 113.8045, "occurred_at": occurred.Format(time.RFC3339),
		"attributes": map[string]any{"detection_count": count, "satellites": []string{"N20", "N21"}, "max_confidence": "h",
			"total_frp_mw": 41.45, "detection_keys": keys}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type storedFire struct {
	hazardID, severity string
	revision           int64
	occurredAt         time.Time
	attributes         map[string]json.RawMessage
	outbox             int
	withdrawn          bool
}

func readStoredFire(t *testing.T, ctx context.Context, store *Store, id string) (storedFire, bool) {
	t.Helper()
	candidates := []*Store{store}
	if store.shards[0] != nil {
		shard, err := logicalShard(Satelit, id)
		if err != nil {
			t.Fatal(err)
		}
		candidates = []*Store{store.shards[shard]}
	}
	var fire storedFire
	var attributes []byte
	err := candidates[0].read.QueryRow(ctx, `SELECT h.hazard_id, h.severity, h.revision, h.occurred_at, h.attributes,
            (SELECT count(*) FROM outbox o WHERE o.hazard_id = h.hazard_id), h.withdrawn_at IS NOT NULL
        FROM hazard_events h WHERE h.source = 'SATELIT' AND h.source_ref_id = $1`, id).
		Scan(&fire.hazardID, &fire.severity, &fire.revision, &fire.occurredAt, &attributes, &fire.outbox, &fire.withdrawn)
	if err != nil {
		return fire, false
	}
	if err := json.Unmarshal(attributes, &fire.attributes); err != nil {
		t.Fatal(err)
	}
	return fire, true
}

func sendFires(t *testing.T, ctx context.Context, api wildfireAPI, records ...json.RawMessage) {
	t.Helper()
	receipt := catalogReceipt{InputID: "sat-" + rand.Text(), ReceivedAt: time.Now().UTC(), Records: records}
	if err := api.ingestReceipt(ctx, receipt); err != nil {
		t.Fatal(err)
	}
}

func assertWildfireLifecycle(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()
	api := wildfireAPI{store: store, cfg: testConfig(t)}
	id := "fire-" + strings.ToLower(rand.Text()[:16])
	first := time.Date(2026, 11, 28, 23, 0, 0, 0, time.UTC)

	sendFires(t, ctx, api, wildfireRecordJSON(t, id, 10, true, 2, first))
	created, found := readStoredFire(t, ctx, store, id)
	if !found || created.revision != 1 || created.severity != "SIAGA" || len(created.attributes) != 5 || created.outbox != 1 {
		t.Fatalf("created fire %+v found=%v", created, found)
	}

	sendFires(t, ctx, api, wildfireRecordJSON(t, id, 10, true, 2, first))
	if again, _ := readStoredFire(t, ctx, store, id); again.revision != 1 || again.outbox != 1 {
		t.Fatalf("a resent revision must be a no-op: %+v", again)
	}

	earlier := first.Add(-2 * time.Hour)
	sendFires(t, ctx, api, wildfireRecordJSON(t, id, 20, true, 3, earlier))
	moved, _ := readStoredFire(t, ctx, store, id)
	if moved.hazardID != created.hazardID || moved.revision != 2 || !moved.occurredAt.Equal(earlier) || moved.outbox != 2 {
		t.Fatalf("a late earlier detection must move occurred_at on the same event: %+v", moved)
	}

	sendFires(t, ctx, api, wildfireRecordJSON(t, id, 15, true, 9, first))
	if stale, _ := readStoredFire(t, ctx, store, id); stale.revision != 2 || string(stale.attributes["detection_count"]) != "3" {
		t.Fatalf("an older revision must not overwrite a newer one: %+v", stale)
	}

	sendFires(t, ctx, api, wildfireRecordJSON(t, id, 30, false, 3, earlier))
	withdrawn, _ := readStoredFire(t, ctx, store, id)
	if withdrawn.hazardID != created.hazardID || withdrawn.severity != "NORMAL" || string(withdrawn.attributes["status"]) != `"withdrawn"` || !withdrawn.withdrawn {
		t.Fatalf("a withdrawn fire updates the same event: %+v", withdrawn)
	}

	sendFires(t, ctx, api, wildfireRecordJSON(t, id, 40, true, 4, earlier))
	revived, _ := readStoredFire(t, ctx, store, id)
	if revived.hazardID != created.hazardID || revived.severity != "SIAGA" || revived.attributes["status"] != nil || revived.revision != 4 || revived.withdrawn {
		t.Fatalf("a revived fire keeps its event: %+v", revived)
	}

	broken := wildfireRecordJSON(t, id+"-broken", 1, true, 2, first)
	var fields map[string]any
	_ = json.Unmarshal(broken, &fields)
	delete(fields["attributes"].(map[string]any), "detection_count")
	broken, _ = json.Marshal(fields)
	sendFires(t, ctx, api, broken)
	if row, found := readFailedRow(t, ctx, store, WildfireEndpoint, "record", broken); !found || row.status != "parked" ||
		row.reason != "missing_field: attributes.detection_count" {
		t.Fatalf("an invalid fire record must be parked: found=%v %+v", found, row)
	}
}

func TestWildfireLifecycle(t *testing.T) {
	assertWildfireLifecycle(t, t.Context(), legacyFailedStore(t, pvmbgAdaptation{}))
}

func TestWildfireLifecycleWhenRouted(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ctx, cancel := context.WithTimeout(activateM2ShardFixtureOwner(t, cfg, store), time.Minute)
	defer cancel()
	assertWildfireLifecycle(t, ctx, store)
}

func TestWildfireRouteNeedsItsOwnToken(t *testing.T) {
	store := legacyFailedStore(t, pvmbgAdaptation{})
	cfg := testConfig(t)
	cfg.SatelliteIngestToken = "satellite-ingest-token-" + strings.Repeat("x", 16)
	mux := http.NewServeMux()
	registerWildfireRoute(mux, store, cfg, nil)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	id := "fire-" + strings.ToLower(rand.Text()[:16])
	body, _ := json.Marshal(map[string]any{"input_id": "sat-" + rand.Text(), "input_offset": 0, "received_at": time.Now().UTC(),
		"records": []json.RawMessage{wildfireRecordJSON(t, id, 1, true, 2, time.Date(2026, 11, 28, 0, 0, 0, 0, time.UTC))}})
	for token, want := range map[string]int{"": http.StatusUnauthorized, "wrong": http.StatusForbidden, cfg.InternalKey: http.StatusForbidden,
		cfg.SatelliteIngestToken: http.StatusOK} {
		request, _ := http.NewRequest(http.MethodPost, server.URL+"/internal/satellite/fires", strings.NewReader(string(body)))
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != want {
			t.Errorf("token %q: HTTP %d, want %d", token, response.StatusCode, want)
		}
	}
	if _, found := readStoredFire(t, t.Context(), store, id); !found {
		t.Fatal("the accepted request must store the fire")
	}
}

func auditSatelitCount(t *testing.T, server *httptest.Server, token string) int {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/internal/audit/checksum?source=SATELIT", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body struct {
		Count int `json:"count"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&body) != nil {
		t.Fatalf("audit checksum HTTP %d", response.StatusCode)
	}
	return body.Count
}

func TestWithdrawnFiresLeaveTheAuditPopulation(t *testing.T) {
	ctx := t.Context()
	store := legacyFailedStore(t, pvmbgAdaptation{})
	cfg := testConfig(t)
	cfg.Audit.Token = "audit-token-" + strings.Repeat("a", 24)
	mux := http.NewServeMux()
	registerAuditRoutes4(mux, store, cfg, 10*time.Second)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	api := wildfireAPI{store: store, cfg: cfg}
	id := "fire-" + strings.ToLower(rand.Text()[:16])
	at := time.Date(2026, 11, 28, 1, 0, 0, 0, time.UTC)

	before := auditSatelitCount(t, server, cfg.Audit.Token)
	sendFires(t, ctx, api, wildfireRecordJSON(t, id, 1, true, 2, at))
	if got := auditSatelitCount(t, server, cfg.Audit.Token); got != before+1 {
		t.Fatalf("an active fire is counted: %d -> %d", before, got)
	}
	sendFires(t, ctx, api, wildfireRecordJSON(t, id, 2, false, 2, at))
	if got := auditSatelitCount(t, server, cfg.Audit.Token); got != before {
		t.Fatalf("a withdrawn fire must leave the audit population: %d -> %d", before, got)
	}
	sendFires(t, ctx, api, wildfireRecordJSON(t, id, 3, true, 3, at))
	if got := auditSatelitCount(t, server, cfg.Audit.Token); got != before+1 {
		t.Fatalf("a revived fire is counted again: %d -> %d", before, got)
	}
}
