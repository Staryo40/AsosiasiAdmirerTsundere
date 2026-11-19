//go:build integration


package aggregator

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestGlobalReadIntegrationTieOrderAndWalk(t *testing.T) {
	cfg := globalReadM2Config(t)
	cfg.ListDefaultLimit = 2
	store := openGlobalReadM2Store(t, cfg)
	ownerCtx := activateFixtureOwner(t, cfg, store, fixtureMutationOwner, fixtureMutationEpoch)
	at := time.Date(2026, 11, 12, 0, 0, 0, 123456000, time.UTC)
	ids := []string{"hazard-a", "hazard-b", "hazard-c", "hazard-d", "hazard-e", "hazard-f"}
	for index, id := range ids {
		shard := store.shards[index%len(store.shards)]
		insertGlobalReadFixture(t, ownerCtx, shard, id, globalReadReference(t, index%len(store.shards), id), at)
	}
	query := hazardQuery{limit: 2, filters: hazardFilters{}}
	var walked []string
	for {
		page, err := store.listHazards(t.Context(), query, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, hazard := range page.Items {
			walked = append(walked, hazard.HazardID)
		}
		if page.NextCursor == nil {
			break
		}
		query.cursor, err = parseHazardCursor(*page.NextCursor, query.filters)
		if err != nil {
			t.Fatal(err)
		}
		query.cursorToken = *page.NextCursor
	}
	want := []string{"hazard-f", "hazard-e", "hazard-d", "hazard-c", "hazard-b", "hazard-a"}
	if !reflect.DeepEqual(walked, want) {
		t.Fatalf("walked %v, want %v", walked, want)
	}
}

func TestGlobalReadIntegrationPartialRetryAndRouteReplacement(t *testing.T) {
	cfg := globalReadM2Config(t)
	cfg.ListDefaultLimit = 2
	store := openGlobalReadM2Store(t, cfg)
	ownerCtx := activateFixtureOwner(t, cfg, store, fixtureMutationOwner, fixtureMutationEpoch)
	at := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	for shardID, id := range []string{"retry-a", "retry-b", "retry-c"} {
		insertGlobalReadFixture(t, ownerCtx, store.shards[shardID], id, globalReadReference(t, shardID, id), at)
	}
	broken := store.shards[1]
	t.Cleanup(broken.Close)
	broken.read.Close()
	first, err := store.listHazards(t.Context(), hazardQuery{limit: 2}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Partial || !reflect.DeepEqual(first.UnavailableUnits, []string{"shard-1"}) {
		t.Fatalf("partial response = %+v", first)
	}
	if first.NextCursor == nil {
		t.Fatal("partial first page omitted retry boundary")
	}
	start, err := parseHazardCursor(*first.NextCursor, hazardFilters{})
	if err != nil || !start.Start {
		t.Fatalf("retry cursor = %+v, err %v", start, err)
	}

	store.shards[1] = replacementReader(t, cfg.ShardWriterURLs[1], broken.storeIdentity)
	retry, err := store.listHazards(t.Context(), hazardQuery{limit: 2, cursor: start, cursorToken: *first.NextCursor}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Partial {
		t.Fatalf("recovered retry remained partial: %+v", retry)
	}
	if retry.NextCursor == nil {
		t.Fatal("recovered nonterminal retry lost continuation cursor")
	}
	if retry.Items[0].HazardID != "retry-c" {
		t.Fatalf("recovered order starts with %q", retry.Items[0].HazardID)
	}
	continuedCursor, err := parseHazardCursor(*retry.NextCursor, hazardFilters{})
	if err != nil {
		t.Fatal(err)
	}

	// A normal keyset token remains valid when the physical Store object for the
	// same logical shard is replaced after the token was issued.
	store.shards[1] = replacementReader(t, cfg.ShardWriterURLs[1], broken.storeIdentity)
	continued, err := store.listHazards(t.Context(), hazardQuery{limit: 2, cursor: continuedCursor, cursorToken: *retry.NextCursor}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(continued.Items) != 1 || continued.Items[0].HazardID != "retry-a" {
		t.Fatalf("continued page = %+v", continued.Items)
	}
}

func TestGlobalReadIntegrationChecksIdentityAfterStartup(t *testing.T) {
	cfg := globalReadM2Config(t)
	cfg.DBReadTimeout = 2 * time.Second
	cfg.DBReadMaxConns = 1
	cfg.ReadMaxInFlight = 1
	store := openGlobalReadM2Store(t, cfg)
	ownerCtx := activateFixtureOwner(t, cfg, store, fixtureMutationOwner, fixtureMutationEpoch)
	at := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	for shardID, id := range []string{"identity-a", "identity-b"} {
		insertGlobalReadFixture(t, ownerCtx, store.shards[shardID], id, globalReadReference(t, shardID, id), at)
	}
	query := hazardQuery{limit: 2}
	fetch := func() hazardList {
		t.Helper()
		page, err := store.listHazards(t.Context(), query, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	if page := fetch(); page.Partial || len(page.Items) != 2 {
		t.Fatalf("initial read = %+v", page)
	}

	target := store.shards[1]
	t.Cleanup(target.Close)
	setFixtureStoreIdentity(t, target, store.shards[2].storeIdentity)
	partial := fetch()
	if !partial.Partial || !reflect.DeepEqual(partial.UnavailableUnits, []string{"shard-1"}) ||
		len(partial.Items) != 1 || partial.Items[0].HazardID != "identity-a" || partial.NextCursor == nil {
		t.Fatalf("cached read pool accepted a different shard: %+v", partial)
	}
	start, err := parseHazardCursor(*partial.NextCursor, query.filters)
	if err != nil || !start.Start {
		t.Fatalf("partial page did not preserve the start boundary: cursor=%+v error=%v", start, err)
	}
	query.cursor, query.cursorToken = start, *partial.NextCursor
	detail, err := store.hazardDetail(t.Context(), "identity-b", cfg)
	if err != errHazardUnavailable || !detail.Partial || !reflect.DeepEqual(detail.UnavailableUnits, []string{"shard-1"}) {
		t.Fatalf("detail from mismatched shard: %+v, error=%v", detail, err)
	}

	setFixtureStoreIdentity(t, target, target.storeIdentity)
	store.shards[1] = replacementReader(t, cfg.ShardWriterURLs[2], target.storeIdentity)
	if page := fetch(); !page.Partial || !reflect.DeepEqual(page.UnavailableUnits, []string{"shard-1"}) ||
		page.NextCursor == nil || *page.NextCursor != query.cursorToken {
		t.Fatalf("wrong replacement route was not rejected: %+v", page)
	}
	store.shards[1] = replacementReader(t, cfg.ShardWriterURLs[1], target.storeIdentity)
	if page := fetch(); page.Partial || len(page.Items) != 2 || page.Items[0].HazardID != "identity-b" || page.NextCursor != nil {
		t.Fatalf("correct replacement route did not restore the complete page: %+v", page)
	}
}

func TestGlobalReadIntegrationUsesOneSharedDeadline(t *testing.T) {
	cfg := globalReadM2Config(t)
	cfg.DBReadTimeout = 150 * time.Millisecond
	store := openGlobalReadM2Store(t, cfg)
	pool := store.shards[0].read
	held := make([]*pgxpool.Conn, 0, pool.Config().MaxConns)
	for range pool.Config().MaxConns {
		connection, err := pool.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, connection)
	}
	started := time.Now()
	page, err := store.listHazards(t.Context(), hazardQuery{limit: 1}, cfg)
	elapsed := time.Since(started)
	for _, connection := range held {
		connection.Release()
	}
	if err != nil {
		t.Fatal(err)
	}
	if !page.Partial || !reflect.DeepEqual(page.UnavailableUnits, []string{"shard-0"}) {
		t.Fatalf("timed-out read = %+v", page)
	}
	if elapsed > time.Second {
		t.Fatalf("shared deadline took %s", elapsed)
	}
}

func TestGlobalReadIntegrationPartialDetailIsUnavailable(t *testing.T) {
	cfg := globalReadM2Config(t)
	store := openGlobalReadM2Store(t, cfg)
	store.shards[2].read.Close()
	_, err := store.hazardDetail(t.Context(), "not-present", cfg)
	if err != errHazardUnavailable {
		t.Fatalf("detail error = %v, want unavailable", err)
	}
}

func TestGlobalReadIntegrationControlOutageKeepsCanonicalRows(t *testing.T) {
	cfg := globalReadM2Config(t)
	store := openGlobalReadM2Store(t, cfg)
	ownerCtx := activateFixtureOwner(t, cfg, store, fixtureMutationOwner, fixtureMutationEpoch)
	insertGlobalReadFixture(t, ownerCtx, store.shards[0], "metadata-outage", globalReadReference(t, 0, "metadata-outage"), time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC))
	store.read.Close()
	page, err := store.listHazards(t.Context(), hazardQuery{limit: 10}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if page.Partial || len(page.Items) != 1 || page.Items[0].HazardID != "metadata-outage" {
		t.Fatalf("reachable canonical data was lost during metadata outage: %+v", page)
	}
	state := page.Sources[BMKG]
	if !page.MetadataUnavailable || state.Status != "unavailable" || !state.HasCachedData {
		t.Fatalf("metadata/cache status = metadataUnavailable:%t source:%+v", page.MetadataUnavailable, state)
	}
}

func TestGlobalReadIntegrationReadyWithDataDespiteControlOutage(t *testing.T) {
	cfg := globalReadM2Config(t)
	store := openGlobalReadM2Store(t, cfg)
	ownerCtx := activateFixtureOwner(t, cfg, store, fixtureMutationOwner, fixtureMutationEpoch)
	insertGlobalReadFixture(t, ownerCtx, store.shards[0], "readiness-data", globalReadReference(t, 0, "readiness-data"), time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC))
	store.read.Close()
	if err := store.Ready(t.Context()); err != nil {
		t.Fatalf("reachable logical data did not keep readiness healthy during control metadata outage: %v", err)
	}
}

func globalReadM2Config(t *testing.T) Config {
	t.Helper()
	return routedM2Config(t)
}

func openGlobalReadM2Store(t *testing.T, cfg Config) *Store {
	t.Helper()
	return openRoutedM2Store(t, cfg)
}

func replacementReader(t *testing.T, address string, identity storeIdentity) *Store {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), address)
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{read: pool, storeIdentity: identity}
	t.Cleanup(store.Close)
	return store
}

// The schema is disposable. Override just the fixture identity atomically so
// existing read connections see a different identity while the rows remain.
func setFixtureStoreIdentity(t *testing.T, target *Store, identity storeIdentity) {
	t.Helper()
	ctx := t.Context()
	tx, err := target.authorityPools[0].Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rollbackTransaction(ctx, tx); err != nil {
			t.Error(err)
		}
	}()
	if _, err := tx.Exec(ctx, `ALTER TABLE store_identity DISABLE TRIGGER store_identity_immutable`); err != nil {
		t.Fatal(err)
	}
	tag, err := tx.Exec(ctx, `UPDATE store_identity SET role = $1, shard_id = $2 WHERE singleton`, identity.role, identity.shardID)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("switch fixture identity: affected=%d error=%v", tag.RowsAffected(), err)
	}
	if _, err := tx.Exec(ctx, `ALTER TABLE store_identity ENABLE TRIGGER store_identity_immutable`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func globalReadReference(t *testing.T, shardID int, hazardID string) string {
	t.Helper()
	for suffix := 0; ; suffix++ {
		reference := fmt.Sprintf("global-read-%s-%d", hazardID, suffix)
		routedShard, err := logicalShard(BMKG, reference)
		if err != nil {
			t.Fatal(err)
		}
		if int(routedShard) == shardID {
			return reference
		}
	}
}

func insertGlobalReadFixture(t *testing.T, ctx context.Context, store *Store, id, reference string, occurred time.Time) {
	t.Helper()
	if err := m2ExecFixtureMutation(ctx, store, `INSERT INTO hazard_events
		(hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,revision,updated_at)
		VALUES ($1,'BMKG',$2,'SEISMIC','SIAGA','Fixture',0,0,$3,$3,'{}',1,$3)`, id, reference, occurred); err != nil {
		t.Fatal(err)
	}
}
