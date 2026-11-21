//go:build integration

package aggregator

import (
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"
)

func TestFallbackTimeStaysStableWhenPVMBGResendsTheRecord(t *testing.T) {
	cfg := testConfig(t)
	cfg.DatabaseURL = p1TestDatabaseURL(t)
	cfg.PVMBGAdaptation = pvmbgAdaptation{timeFallback: true}
	store, err := OpenStore(t.Context(), cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	reportID := "rpt-fallback-" + rand.Text()
	raw, _ := json.Marshal(map[string]any{"report_id": reportID, "volcano_id": "merapi", "alert_level": "Siaga",
		"eruption_count_24h": 3, "ash_column_height_m": 1200.5})
	first := time.Date(2026, 11, 21, 4, 5, 6, 0, time.UTC)
	for _, received := range []time.Time{first, first.Add(10 * time.Second), first.Add(time.Minute)} {
		if err := store.Ingest(t.Context(), IngestChunk{Endpoint: VolcanicEndpoint, Records: []json.RawMessage{raw}, ReceivedAt: received}); err != nil {
			t.Fatal(err)
		}
	}

	var count int
	var occurredAt time.Time
	var inferred string
	err = store.read.QueryRow(t.Context(), `SELECT count(*), min(occurred_at), min(attributes->>'occurred_at_inferred')
        FROM hazard_events WHERE source = 'PVMBG' AND source_ref_id = $1`, reportID).Scan(&count, &occurredAt, &inferred)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || !occurredAt.Equal(first) || inferred != "true" {
		t.Fatalf("count=%d occurred_at=%s inferred=%q", count, occurredAt, inferred)
	}
	var rejects int
	if err := store.read.QueryRow(t.Context(), "SELECT count(*) FROM ingest_rejects WHERE source_ref_id = $1", reportID).Scan(&rejects); err != nil {
		t.Fatal(err)
	}
	if rejects != 0 {
		t.Fatalf("re-sent records must not be rejected, got %d rejects", rejects)
	}
}
