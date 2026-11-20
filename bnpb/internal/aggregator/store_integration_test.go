//go:build integration

package aggregator

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This opt-in fixture cannot target the configured canonical or Compose database.
func m2IsolatedConfig(t *testing.T) Config {
	t.Helper()
	address := os.Getenv("BNPB_M2_LOCAL_TEST_DATABASE_URL")
	parsed, err := url.Parse(address)
	if err != nil || parsed.Hostname() != "127.0.0.1" || parsed.Path != "/bnpb_m2_step4_test" || parsed.Port() == "" {
		t.Fatal("BNPB_M2_LOCAL_TEST_DATABASE_URL must target an isolated loopback port/bnpb_m2_step4_test")
	}
	admin, err := pgx.Connect(t.Context(), address)
	if err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"m2_" + rand.Text()}.Sanitize()
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
		if err := admin.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	cfg := testConfig(t)
	cfg.DatabaseURL = parsed.String()
	return cfg
}

func m2OpenStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	store, err := OpenStore(t.Context(), cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}

func m2LegacyPool(t *testing.T, cfg Config) *pgxpool.Pool {
	t.Helper()
	pool, err := openPool(t.Context(), cfg.DatabaseURL, 2, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTransaction(t.Context(), tx)
	if _, err := tx.Exec(t.Context(), "CREATE TABLE schema_migrations (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		t.Fatal(err)
	}
	if err := applyInitialSchema(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{outboxIdentitySchema, snapshotIndexSchema, outboxSequenceSchema} {
		if _, err := tx.Exec(t.Context(), sql); err != nil {
			t.Fatal(err)
		}
	}
	stamp := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	for _, event := range []HazardEvent{
		{HazardID: "legacy-bmkg", Source: BMKG, SourceRefID: "evt-1", HazardType: Seismic, Severity: Siaga, AreaName: "Fixture", OccurredAt: stamp, IngestedAt: stamp, Attributes: map[string]json.RawMessage{"magnitude": json.RawMessage(`7`)}},
		{HazardID: "legacy-volcanic", Source: PVMBG, SourceRefID: "rpt-1", HazardType: Volcanic, Severity: Waspada, AreaName: "Fixture", OccurredAt: stamp, IngestedAt: stamp, Attributes: map[string]json.RawMessage{"confidence_level": json.RawMessage(`0`)}},
	} {
		if err := writeHazard(t.Context(), tx, event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(t.Context(), `INSERT INTO source_seismic VALUES ('evt-1', '2026-11-12T00:00:00Z', '{}');
        INSERT INTO source_warnings VALUES ('warning-different-key','evt-1','2026-11-12T00:00:00Z','{}');
        INSERT INTO ingest_rejects (endpoint,category,field,payload_sha256,payload_bytes,first_seen_at,last_seen_at)
        VALUES ('volcanic','wrong_type','alert_level',repeat('0',64),3,'2026-11-12T00:00:00Z','2026-11-12T00:00:00Z');
        UPDATE poll_state SET checkpoint='2026-11-12T00:00:00Z',last_successful_poll='2026-11-12T00:00:00Z'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	return pool
}

func m2Rows(t *testing.T, pool *pgxpool.Pool, table string) string {
	t.Helper()
	var contents string
	err := pool.QueryRow(t.Context(), "SELECT COALESCE(jsonb_agg(to_jsonb(t) - 'logical_shard_id' ORDER BY (to_jsonb(t) - 'logical_shard_id')::text), '[]')::text FROM "+pgx.Identifier{table}.Sanitize()+" t").Scan(&contents)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

func TestM2MigrationPreservesLegacyRowsAndOutbox(t *testing.T) {
	cfg := m2IsolatedConfig(t)
	legacy := m2LegacyPool(t, cfg)
	before := map[string]string{}
	for _, table := range []string{"hazard_events", "source_seismic", "source_warnings", "outbox", "poll_state", "ingest_rejects", "volcano_reference"} {
		before[table] = m2Rows(t, legacy, table)
	}
	store := m2OpenStore(t, cfg)
	for table, expected := range before {
		if got := m2Rows(t, store.read, table); got != expected {
			t.Fatalf("migration changed %s rows", table)
		}
	}
	if err := store.Ready(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, vector := range shardVectors(t) {
		var shard int16
		if err := store.read.QueryRow(t.Context(), "SELECT canonical_shard($1,$2)", string(vector.Source), vector.Reference).Scan(&shard); err != nil || shard != vector.Shard {
			t.Fatalf("SQL golden %s/%s: %d, %v; want %d", vector.Source, vector.Reference, shard, err, vector.Shard)
		}
	}
	var seismic, warning, hazard int16
	if err := store.read.QueryRow(t.Context(), `SELECT s.logical_shard_id,w.logical_shard_id,h.logical_shard_id
        FROM source_seismic s JOIN source_warnings w ON w.related_event_id=s.event_id
        JOIN hazard_events h ON h.source='BMKG' AND h.source_ref_id=s.event_id`).Scan(&seismic, &warning, &hazard); err != nil {
		t.Fatal(err)
	}
	if seismic != 2 || warning != seismic || hazard != seismic {
		t.Fatalf("warning/event/backfill differ: %d/%d/%d", seismic, warning, hazard)
	}
	_ = m2OpenStore(t, cfg)
	assertSchemaRepairMetadata(t, store.read, true)
}

func TestM2RepairMetadataOnExistingVersion12(t *testing.T) {
	shardID := int16(0)
	for _, identity := range []storeIdentity{{role: "control"}, {role: "shard", shardID: &shardID}} {
		t.Run(identity.role, func(t *testing.T) {
			cfg := m2IsolatedConfig(t)
			ctx := t.Context()
			pool, err := openPool(ctx, cfg.DatabaseURL, 2, quietLogger())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackTransaction(ctx, tx)
			if err := migrateSchema(ctx, tx); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			assertSchemaRepairMetadata(t, pool, false)

			if err := (&Store{}).migrate(ctx, pool, nil, identity); err != nil {
				t.Fatal(err)
			}
			assertSchemaRepairMetadata(t, pool, identity.role == "control")
		})
	}
}

func assertSchemaRepairMetadata(t *testing.T, pool *pgxpool.Pool, wantRepair bool) {
	t.Helper()
	var version, count, minimum int
	if err := pool.QueryRow(t.Context(), schemaHistoryQuery).Scan(&version, &count, &minimum); err != nil || version != currentSchemaVersion || count != currentSchemaVersion || minimum != 1 {
		t.Fatalf("schema history: version=%d count=%d minimum=%d, %v; want 1 through %d", version, count, minimum, err, currentSchemaVersion)
	}
	var repairTable bool
	var columnType string
	err := pool.QueryRow(t.Context(), `SELECT to_regclass('shard_repair_jobs') IS NOT NULL, COALESCE((
		SELECT data_type FROM information_schema.columns WHERE table_schema = current_schema()
		AND table_name = 'placement_revision' AND column_name = 'bootstrap_config'
	), '')`).Scan(&repairTable, &columnType)
	if err != nil || repairTable != wantRepair || (wantRepair && columnType != "jsonb") || (!wantRepair && columnType != "") {
		t.Fatalf("repair metadata (want present=%t): table=%t bootstrap_config type=%q, %v", wantRepair, repairTable, columnType, err)
	}
}

func m2InsertHazard(t *testing.T, store *Store, source Source, kind HazardType, reference string) error {
	t.Helper()
	_, err := store.write.Exec(t.Context(), `INSERT INTO hazard_events
        (hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,revision,updated_at)
        VALUES ($1,$2,$3,$4,'SIAGA','Fixture',0,0,'2026-11-12T00:00:00Z','2026-11-12T00:00:00Z','{}',1,'2026-11-12T00:00:00Z')`,
		"fixture-"+reference, string(source), reference, string(kind))
	return err
}

func TestM2DatabasePairsAndNewSourceNetworkReads(t *testing.T) {
	cfg := m2IsolatedConfig(t)
	store := m2OpenStore(t, cfg)
	sources := []Source{BMKG, PVMBG, Satelit, SensorPantai}
	types := []HazardType{Seismic, Volcanic, Wildfire, Coastal, "TSUNAMI"}
	for _, source := range append(slices.Clone(sources), Source("UNKNOWN")) {
		for _, kind := range types {
			t.Run(string(source)+"/"+string(kind), func(t *testing.T) {
				err := m2InsertHazard(t, store, source, kind, string(source)+"-"+string(kind))
				valid := hazardTypeForSource(source) != "" && hazardTypeForSource(source) == kind
				if (err == nil) != valid {
					t.Fatalf("DB pair validation: %v", err)
				}
			})
		}
	}
	server := p1ReadServer(t, store, cfg)
	for _, source := range []Source{Satelit, SensorPantai} {
		items := p1ReadPage(t, server, cfg, source)
		if len(items) != 1 || items[0].Source != source {
			t.Fatalf("new-source list: %+v", items)
		}
		for _, path := range []string{"/internal/hazards?source=" + string(source), "/internal/hazards/" + items[0].HazardID} {
			request, err := http.NewRequest("GET", server.URL+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set(internalKeyHeader, cfg.InternalKey)
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			var envelope struct {
				Sources map[Source]sourceState `json:"sources"`
			}
			err = json.NewDecoder(response.Body).Decode(&envelope)
			response.Body.Close()
			if err != nil || response.StatusCode != 200 {
				t.Fatalf("new-source response: %d, %v", response.StatusCode, err)
			}
			state, exists := envelope.Sources[source]
			if source == SensorPantai {
				// Sensor Pantai has a real coastal poll endpoint; only Satelit stays unconfigured.
				if !exists || (state.ErrorCategory != nil && *state.ErrorCategory == "source_not_configured") {
					t.Fatalf("coastal freshness must come from poll state: %+v", state)
				}
				continue
			}
			if !exists || !state.HasCachedData || state.Status != "stale" || state.ErrorCategory == nil || *state.ErrorCategory != "source_not_configured" || state.LastSuccessfulPoll != nil || len(state.Endpoints) != 0 {
				t.Fatalf("invented/missing freshness: %+v", state)
			}
		}
	}
}

func TestM2PersistentPlacementCASAndRepairAdmission(t *testing.T) {
	cfg := m2IsolatedConfig(t)
	store := m2OpenStore(t, cfg)
	initial, err := store.placement(t.Context())
	if err != nil || initial.Revision != 0 || len(initial.Copies) != 0 {
		t.Fatalf("unconfigured placement: %+v, %v", initial, err)
	}
	if err := store.recordPlacement(t.Context(), placementSnapshot{Copies: rotatingPlacement()}); err != nil {
		t.Fatal(err)
	}
	if err := store.recordPlacement(t.Context(), placementSnapshot{Copies: rotatingPlacement()}); !errors.Is(err, errPlacementChanged) {
		t.Fatalf("stale CAS: %v", err)
	}
	current, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	copying := append(rotatingPlacement(), shardPlacement{0, "s0-d", "node-d", "replica", "copying"}, shardPlacement{2, "s2-d", "node-d", "replica", "copying"})
	if err := store.recordPlacement(t.Context(), placementSnapshot{Revision: current.Revision, Copies: copying}); err != nil {
		t.Fatal(err)
	}
	before, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.recordPlacement(t.Context(), placementSnapshot{Revision: before.Revision, Copies: copying[1:]}); !errors.Is(err, errInvalidPlacement) {
		t.Fatalf("forgot old physical copy: %v", err)
	}
	for _, state := range []string{"copying", "retiring", "ready"} {
		_, err := store.write.Exec(t.Context(), `INSERT INTO shard_placements VALUES ('forbidden',1,'node-d','replica',$1)`, state)
		if err == nil {
			t.Fatalf("DB allowed third shard in state %s", state)
		}
	}
	reopened := m2OpenStore(t, cfg)
	after, err := reopened.placement(t.Context())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("placement lost across reopen or rejected mutation: %+v, %v", after, err)
	}
	for _, vector := range shardVectors(t) {
		got, err := logicalShard(vector.Source, vector.Reference)
		if err != nil || got != vector.Shard {
			t.Fatalf("physical node addition changed logical shard: %d, %v", got, err)
		}
	}
	start := make(chan struct{})
	outcomes := make(chan error, 2)
	for range 2 {
		go func() { <-start; outcomes <- store.recordPlacement(t.Context(), before) }()
	}
	close(start)
	first, second := <-outcomes, <-outcomes
	if !((first == nil && errors.Is(second, errPlacementChanged)) || (second == nil && errors.Is(first, errPlacementChanged))) {
		t.Fatalf("CAS concurrent outcomes: %v / %v", first, second)
	}
}
