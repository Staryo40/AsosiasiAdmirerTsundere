package aggregator

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
)

type shardVector struct {
	Source    Source `json:"source"`
	Reference string `json:"source_ref_id"`
	Shard     int16  `json:"shard"`
}

func shardVectors(t *testing.T) []shardVector {
	t.Helper()
	raw, err := os.ReadFile("testdata/shards.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []shardVector
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	return vectors
}

func TestM2ShardGoldenVectors(t *testing.T) {
	for _, vector := range shardVectors(t) {
		t.Run(string(vector.Source)+"/"+vector.Reference, func(t *testing.T) {
			got, err := logicalShard(vector.Source, vector.Reference)
			if err != nil || got != vector.Shard {
				t.Fatalf("got shard %d, %v; want %d", got, err, vector.Shard)
			}
		})
	}
}

func TestM2WarningColocation(t *testing.T) {
	raw := json.RawMessage(`{"warning_id":"warning-not-the-event-key","related_event_id":"evt-1","issued_at":"2026-11-12T00:00:00Z","estimated_arrival":"2026-11-12T01:00:00Z","threat_level":"Awas","affected_zones":[]}`)
	warning, err := parseWarning(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := logicalShard(BMKG, warning.eventID)
	if err != nil || got != 2 {
		t.Fatalf("warning routed to %d, %v; want seismic shard 2", got, err)
	}
}

func TestM2ShardIdentityBoundaries(t *testing.T) {
	for _, reference := range []string{"", " \t", "bad\x00key", "bad\nkey", string([]byte{0xff}), strings.Repeat("x", 2049)} {
		t.Run(reference, func(t *testing.T) {
			if _, err := logicalShard(BMKG, reference); err == nil {
				t.Fatal("invalid identity accepted")
			}
		})
	}
	if _, err := logicalShard(SensorPantai, strings.Repeat("x", 2048)); err != nil {
		t.Fatal(err)
	}
	if _, err := logicalShard(Source("UNKNOWN"), "evt-1"); err == nil {
		t.Fatal("unknown source accepted")
	}
}

func TestM2ExpandedFilters(t *testing.T) {
	cfg := testConfig(t)
	sources := []Source{BMKG, PVMBG, Satelit, SensorPantai}
	types := []HazardType{Seismic, Volcanic, Wildfire, Coastal}
	for _, source := range sources {
		for _, kind := range types {
			t.Run(string(source)+"/"+string(kind), func(t *testing.T) {
				_, err := parseHazardQuery("source="+string(source)+"&hazard_type="+string(kind), cfg)
				if (err == nil) != (hazardTypeForSource(source) == kind) {
					t.Fatalf("pair validation: %v", err)
				}
			})
		}
		kind := hazardTypeForSource(source)
		query, err := parseHazardQuery("hazard_type="+string(kind), cfg)
		if err != nil || !slices.Equal(query.filters.selectedSources(), []Source{source}) {
			t.Fatalf("type-only source selection for %s: %+v, %v", source, query, err)
		}
	}
	query, err := parseHazardQuery("", cfg)
	if err != nil || !slices.Equal(query.filters.selectedSources(), sources) {
		t.Fatalf("all-source selection: %+v, %v", query, err)
	}
	for _, raw := range []string{"source=UNKNOWN", "hazard_type=UNKNOWN", "hazard_type=TSUNAMI"} {
		if _, err := parseHazardQuery(raw, cfg); err == nil {
			t.Fatalf("invalid query %q accepted", raw)
		}
	}
}

func rotatingPlacement() []shardPlacement {
	return []shardPlacement{
		{0, "s0-a", "node-a", "primary", "ready"}, {0, "s0-b", "node-b", "replica", "ready"},
		{1, "s1-b", "node-b", "primary", "ready"}, {1, "s1-c", "node-c", "replica", "ready"},
		{2, "s2-c", "node-c", "primary", "ready"}, {2, "s2-a", "node-a", "replica", "ready"},
	}
}

func TestM2PlacementAdmissionCountsTransientCopies(t *testing.T) {
	initial := rotatingPlacement()
	if err := validatePlacement(initial); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"ready", "copying", "retiring"} {
		t.Run(state, func(t *testing.T) {
			full := append(slices.Clone(initial), shardPlacement{1, "s1-a", "node-a", "replica", state})
			if err := validatePlacement(full); err == nil {
				t.Fatal("third shard on node accepted")
			}
		})
	}
	repair := append(slices.Clone(initial), shardPlacement{0, "s0-d", "node-d", "replica", "copying"}, shardPlacement{2, "s2-d", "node-d", "replica", "copying"})
	if err := validatePlacement(repair); err != nil {
		t.Fatal(err)
	}
	if retainsPhysicalCopies(initial, initial[1:]) {
		t.Fatal("forgotten physical copy accepted")
	}
	moved := slices.Clone(initial)
	moved[0].Node = "node-d"
	if retainsPhysicalCopies(initial, moved) {
		t.Fatal("same unit moved without new copy identity")
	}
}
