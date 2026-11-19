//go:build integration

package aggregator

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connection settings make the fixture identity explicit; this is synthetic SQL probe coverage, not Kubernetes node-identity proof.
func TestAuditLocationUsesLivePhysicalDatabaseAndServerIdentity(t *testing.T) {
	cfg := m2IsolatedConfig(t)
	want := auditLocation{Node: "fixture-node", Unit: "fixture-unit", Role: "primary"}
	baseURL := cfg.DatabaseURL
	cfg.DatabaseURL = auditLocationFixtureURL(t, baseURL, want.Node, want.Unit)
	store := m2OpenStore(t, cfg)
	const hazardID = "location-hazard"
	_, err := store.write.Exec(t.Context(), `INSERT INTO hazard_events
		(hazard_id, source, source_ref_id, hazard_type, severity, area_name, latitude, longitude,
		 occurred_at, ingested_at, attributes, revision, updated_at)
		VALUES ($1, 'BMKG', $2, 'SEISMIC', 'NORMAL', 'fixture', 0, 0, now(), now(), '{}'::jsonb, 1, now())`, hazardID, "ref-"+hazardID)
	if err != nil {
		t.Fatal(err)
	}
	api := auditAPI{store: store, cfg: cfg}
	assertAuditLocations(t, api, hazardID, false, want)
	assertAuditError(t, requestAuditLocation(t, api, "not-present"), http.StatusNotFound, "not_found")

	t.Run("blank_identity", func(t *testing.T) {
		pool, err := openPool(t.Context(), auditLocationFixtureURL(t, baseURL, "", want.Unit), 1, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		unnamed := auditAPI{store: &Store{read: pool}, cfg: cfg}
		assertAuditLocations(t, unnamed, hazardID, true)
		assertAuditLocations(t, unnamed, "not-present", true)
	})

	t.Run("legacy_schema", func(t *testing.T) {
		legacyCfg := m2IsolatedConfig(t)
		legacyCfg.DatabaseURL = auditLocationFixtureURL(t, legacyCfg.DatabaseURL, want.Node, want.Unit)
		legacy := auditAPI{store: &Store{read: m2LegacyPool(t, legacyCfg)}, cfg: legacyCfg}
		assertAuditLocations(t, legacy, "legacy-bmkg", false, want)
	})
}

// The routed fixture uses separate schemas on one PostgreSQL primary, not real standby or Kubernetes topology.
func TestAuditLocationChecksLiveRoutedIdentityAndPlacement(t *testing.T) {
	cfg := routedM2Config(t)
	placement := placementSnapshot{Copies: make([]shardPlacement, 0, logicalShardCount)}
	for id := range cfg.ShardURLs {
		node, unit := fmt.Sprintf("fixture-node-%d", id), fmt.Sprintf("fixture-unit-%d", id)
		placement.Copies = append(placement.Copies, shardPlacement{ShardID: int16(id), Unit: unit, Node: node, Role: "primary", State: "ready"})
		cfg.ShardURLs[id] = auditLocationFixtureURL(t, cfg.ShardURLs[id], node, unit)
		cfg.ShardWriterURLs[id] = auditLocationFixtureURL(t, cfg.ShardWriterURLs[id], node, unit)
	}
	store := openRoutedM2Store(t, cfg)
	ownerCtx := activateM2ShardFixtureOwner(t, cfg, store)
	const targetShard = 1
	reference := globalReadReference(t, targetShard, "location")
	hazardID := "fixture-" + reference
	target := store.shards[targetShard]
	if err := m2InsertHazardWithOwner(t, ownerCtx, target, BMKG, Seismic, reference); err != nil {
		t.Fatalf("seed routed location hazard: %v", err)
	}
	api := auditAPI{store: store, cfg: cfg}
	expectedCopy := placement.Copies[targetShard]
	want := auditLocation{Node: expectedCopy.Node, Unit: expectedCopy.Unit, Role: expectedCopy.Role}
	actualIdentity := target.storeIdentity
	wrongShard := int16(0)
	wrongIdentity := storeIdentity{role: "shard", shardID: &wrongShard}

	t.Run("startup_verification", func(t *testing.T) {
		target.storeIdentity = wrongIdentity
		defer func() {
			target.storeIdentity = actualIdentity
			target.startupUnverified.Store(false)
		}()
		target.startupUnverified.Store(true)
		assertAuditLocations(t, api, hazardID, true)
		if !target.startupUnverified.Load() {
			t.Fatal("unexpectedly verified a recovered database with the wrong identity")
		}
		target.storeIdentity = actualIdentity
		assertAuditLocations(t, api, hazardID, true, want)
		if target.startupUnverified.Load() {
			t.Fatal("correct recovered primary did not verify")
		}
	})
	t.Run("absent_placement", func(t *testing.T) {
		assertAuditLocations(t, api, hazardID, true, want)
		assertAuditLocations(t, api, "not-present", true)
	})
	if err := store.recordPlacement(t.Context(), placement); err != nil {
		t.Fatal(err)
	}
	t.Run("complete_inventory", func(t *testing.T) {
		assertAuditLocations(t, api, hazardID, false, want)
		assertAuditError(t, requestAuditLocation(t, api, "not-present"), http.StatusNotFound, "not_found")
	})
	t.Run("identity_changes_after_startup", func(t *testing.T) {
		setFixtureStoreIdentity(t, target, wrongIdentity)
		defer setFixtureStoreIdentity(t, target, actualIdentity)
		assertAuditLocations(t, api, hazardID, true)
		assertAuditLocations(t, api, "not-present", true)
	})
	t.Run("duplicate_and_mismatched_physical_probes", func(t *testing.T) {
		address := auditLocationFixtureURL(t, cfg.ShardURLs[targetShard], "wrong-node", expectedCopy.Unit)
		conflict, err := openPool(t.Context(), address, 1, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(conflict.Close)
		store.physicalCopies = []*pgxpool.Pool{target.read}
		t.Cleanup(func() { store.physicalCopies = nil })
		assertAuditLocations(t, api, hazardID, false, want)
		store.physicalCopies = append(store.physicalCopies, conflict)
		assertAuditLocations(t, api, hazardID, true, want)
		store.physicalCopies = store.physicalCopies[:1]
		primaryPool := target.read
		target.read = nil
		defer func() { target.read = primaryPool }()
		assertAuditLocations(t, api, hazardID, true, want)
	})
}

func auditLocationFixtureURL(t *testing.T, address, node, unit string) string {
	t.Helper()
	parsed, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	options := parsed.Query()
	options.Set("options", "-c bnpb.node_name="+node+" -c bnpb.unit_name="+unit)
	parsed.RawQuery = options.Encode()
	return parsed.String()
}

func requestAuditLocation(t *testing.T, api auditAPI, hazardID string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	api.location(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/internal/audit/location?hazard_id="+url.QueryEscape(hazardID), nil))
	return recorder
}

func assertAuditLocations(t *testing.T, api auditAPI, hazardID string, partial bool, want ...auditLocation) {
	t.Helper()
	recorder := requestAuditLocation(t, api, hazardID)
	mediaType, _, typeErr := mime.ParseMediaType(recorder.Header().Get("Content-Type"))
	if recorder.Code != http.StatusOK || typeErr != nil || mediaType != "application/json" {
		t.Fatalf("status=%d content-type=%q body=%s", recorder.Code, mediaType, recorder.Body)
	}
	var response struct {
		HazardID  string          `json:"hazard_id"`
		Locations []auditLocation `json:"locations"`
		Partial   *bool           `json:"partial"`
	}
	decoder := json.NewDecoder(recorder.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.HazardID != hazardID || response.Partial == nil || *response.Partial != partial ||
		response.Locations == nil || !slices.Equal(response.Locations, want) {
		t.Fatalf("location response=%+v, want hazard=%q partial=%t locations=%+v", response, hazardID, partial, want)
	}
}
