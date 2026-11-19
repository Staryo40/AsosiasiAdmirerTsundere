//go:build integration

package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func routedM2Config(t *testing.T) Config {
	t.Helper()
	cfg := m2IsolatedShardConfig(t, m2IsolatedConfig(t))
	for shardID := range cfg.ShardURLs {
		shardCfg := m2IsolatedShardConfig(t, cfg)
		cfg.ShardURLs[shardID] = shardCfg.DatabaseURL
		cfg.ShardWriterURLs[shardID] = shardCfg.WriterURL
	}
	return cfg
}

func openRoutedM2Store(t *testing.T, cfg Config) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store, err := OpenStore(ctx, cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	grantM2WriterPrivileges(t, cfg, store)
	t.Cleanup(store.Close)
	return store
}

func routedRecord(t *testing.T, identity string, at time.Time) json.RawMessage {
	t.Helper()
	raw := p1UniqueRecord(t, p1Fixtures(t)[1])
	raw = p1WithField(t, raw, "event_id", p1Encode(t, identity))
	return p1WithField(t, raw, "occurred_at", p1Encode(t, at.UTC().Format(time.RFC3339Nano)))
}

func routedShardIdentities(t *testing.T) [3]string {
	t.Helper()
	var identities [3]string
	for shardID := range identities {
		for suffix := 0; ; suffix++ {
			identity := fmt.Sprintf("routed-step5-%d-%d", shardID, suffix)
			actual, err := logicalShard(BMKG, identity)
			if err != nil {
				t.Fatal(err)
			}
			if int(actual) == shardID {
				identities[shardID] = identity
				break
			}
		}
	}
	return identities
}

func routedCounts(t *testing.T, store *Store, eventID string) (canonical, source, notifications, receipts int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, query := range []struct {
		sql  string
		into *int
	}{
		{"SELECT count(*) FROM hazard_events WHERE source='BMKG' AND source_ref_id=$1", &canonical},
		{"SELECT count(*) FROM source_seismic WHERE event_id=$1", &source},
		{"SELECT count(*) FROM outbox o JOIN hazard_events h USING (hazard_id) WHERE h.source='BMKG' AND h.source_ref_id=$1", &notifications},
		{"SELECT count(*) FROM handled_inputs WHERE endpoint='seismic' AND convert_from(raw_payload, 'UTF8')::jsonb->>'event_id'=$1", &receipts},
	} {
		if err := store.read.QueryRow(ctx, query.sql, eventID).Scan(query.into); err != nil {
			t.Fatal(err)
		}
	}
	return canonical, source, notifications, receipts
}

func TestRoutedWriteIntegrationWarningAndDataStayOnOwningShard(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	activateM2ShardFixtureOwner(t, cfg, store)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	ownerCtx := fixtureMutationContext(t, ctx, fixtureMutationOwner, fixtureMutationEpoch, allFixtureStores(store)...)
	identity := routedShardIdentities(t)[2]
	stamp := time.Date(2026, 11, 18, 1, 0, 0, 0, time.UTC)
	seismic := routedRecord(t, identity, stamp)
	if err := store.Ingest(ownerCtx, IngestChunk{Endpoint: SeismicEndpoint, Records: []json.RawMessage{seismic}, ReceivedAt: stamp.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	warning := p1WithField(t, p1Fixtures(t)[1].Warnings[0], "related_event_id", p1Encode(t, identity))
	warning = p1WithField(t, warning, "warning_id", p1Encode(t, identity+"-warning"))
	warning = p1WithField(t, warning, "issued_at", p1Encode(t, stamp.Add(time.Second).Format(time.RFC3339Nano)))
	if err := store.Ingest(ownerCtx, IngestChunk{Endpoint: WarningEndpoint, Records: []json.RawMessage{warning}, ReceivedAt: stamp.Add(2 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	for shardID, shard := range store.shards {
		canonical, source, notifications, receipts := routedCounts(t, shard, identity)
		want := 0
		if shardID == 2 {
			want = 1
		}
		wantNotifications := want
		if shardID == 2 {
			wantNotifications++
		}
		if canonical != want || source != want || notifications != wantNotifications || receipts != want {
			t.Fatalf("shard %d event rows canonical/source/outbox/receipt=%d/%d/%d/%d, want %d/%d/%d/%d", shardID, canonical, source, notifications, receipts, want, want, wantNotifications, want)
		}
		var warnings, warningReceipts int
		if err := shard.read.QueryRow(ctx, "SELECT count(*) FROM source_warnings WHERE related_event_id=$1", identity).Scan(&warnings); err != nil {
			t.Fatal(err)
		}
		if err := shard.read.QueryRow(ctx, "SELECT count(*) FROM handled_inputs WHERE endpoint='warnings' AND convert_from(raw_payload, 'UTF8')::jsonb->>'related_event_id'=$1", identity).Scan(&warningReceipts); err != nil {
			t.Fatal(err)
		}
		if warnings != want || warningReceipts != want {
			t.Fatalf("shard %d related warning/source receipt=%d/%d, want %d each", shardID, warnings, warningReceipts, want)
		}
	}
	var controlWarnings, controlReceipts int
	if err := store.read.QueryRow(ctx, "SELECT count(*) FROM source_warnings WHERE related_event_id=$1", identity).Scan(&controlWarnings); err != nil {
		t.Fatal(err)
	}
	if err := store.read.QueryRow(ctx, "SELECT count(*) FROM handled_inputs WHERE endpoint='warnings' AND convert_from(raw_payload, 'UTF8')::jsonb->>'related_event_id'=$1", identity).Scan(&controlReceipts); err != nil {
		t.Fatal(err)
	}
	if canonical, source, notifications, receipts := routedCounts(t, store, identity); canonical+source+notifications+receipts+controlWarnings+controlReceipts != 0 {
		t.Fatalf("control database contains routed rows canonical/source/outbox/receipt/warning/warning receipt=%d/%d/%d/%d/%d/%d", canonical, source, notifications, receipts, controlWarnings, controlReceipts)
	}
}

func TestRoutedWriteIntegrationStopsAtFirstFailingShardAndReplaysGap(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ownerCtx := activateM2ShardFixtureOwner(t, cfg, store)
	ctx, cancel := context.WithTimeout(ownerCtx, 30*time.Second)
	defer cancel()
	identities := routedShardIdentities(t)
	stamp := time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC)
	records := []json.RawMessage{
		routedRecord(t, identities[0], stamp),
		routedRecord(t, identities[1], stamp.Add(time.Minute)),
		routedRecord(t, identities[2], stamp.Add(2*time.Minute)),
	}
	admin, err := pgx.Connect(ctx, cfg.ShardURLs[1])
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	if _, err := admin.Exec(ctx, `CREATE FUNCTION fail_handled_input() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected isolated shard failure'; END $$; CREATE TRIGGER fail_handled_input BEFORE INSERT ON handled_inputs FOR EACH ROW EXECUTE FUNCTION fail_handled_input()`); err != nil {
		t.Fatal(err)
	}
	chunk := IngestChunk{Endpoint: SeismicEndpoint, Records: records, ReceivedAt: stamp.Add(3 * time.Minute), InputID: "ordered-routed-gap"}
	if err := store.Ingest(ctx, chunk); err == nil {
		t.Fatal("ingest succeeded despite isolated failing-shard trigger")
	}
	assertRoutedCheckpoint(t, ctx, store, SeismicEndpoint, nil)
	for shardID, shard := range store.shards {
		want := 0
		if shardID == 0 {
			want = 1
		}
		assertRoutedOutcome(t, shard, identities[shardID], want)
	}
	store.Close()
	for _, address := range append([]string{cfg.DatabaseURL}, cfg.ShardURLs[:]...) {
		closeFixtureMutationGuard(t, address)
	}
	store = openRoutedM2Store(t, cfg)
	if _, err := admin.Exec(ctx, "DROP TRIGGER fail_handled_input ON handled_inputs; DROP FUNCTION fail_handled_input()"); err != nil {
		t.Fatal(err)
	}
	activateM2ShardFixtureOwner(t, cfg, store)
	ctx = fixtureMutationContext(t, ctx, fixtureMutationOwner, fixtureMutationEpoch, allFixtureStores(store)...)
	if err := store.Ingest(ctx, chunk); err != nil {
		t.Fatalf("replay same receipt: %v", err)
	}
	recoveredCheckpoint := stamp.Add(2 * time.Minute)
	assertRoutedCheckpoint(t, ctx, store, SeismicEndpoint, &recoveredCheckpoint)
	for shardID, shard := range store.shards {
		assertRoutedOutcome(t, shard, identities[shardID], 1)
	}
}

func TestRoutedWriteIntegrationCheckpointFailureReplaysDurableOutcomes(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ownerCtx := activateM2ShardFixtureOwner(t, cfg, store)
	ctx, cancel := context.WithTimeout(ownerCtx, 30*time.Second)
	defer cancel()
	identities := routedShardIdentities(t)
	stamp := time.Date(2026, 11, 18, 3, 0, 0, 0, time.UTC)
	records := make([]json.RawMessage, len(identities))
	for shardID, identity := range identities {
		records[shardID] = routedRecord(t, identity, stamp.Add(time.Duration(shardID)*time.Minute))
	}
	admin, err := pgx.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	if _, err := admin.Exec(ctx, `CREATE FUNCTION fail_poll_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected isolated checkpoint failure'; END $$; CREATE TRIGGER fail_poll_checkpoint BEFORE UPDATE ON poll_state FOR EACH ROW EXECUTE FUNCTION fail_poll_checkpoint()`); err != nil {
		t.Fatal(err)
	}
	chunk := IngestChunk{Endpoint: SeismicEndpoint, Records: records, ReceivedAt: stamp.Add(3 * time.Minute), InputID: "checkpoint-routed-replay"}
	if err := store.Ingest(ctx, chunk); err == nil {
		t.Fatal("ingest succeeded despite isolated checkpoint trigger")
	}
	assertRoutedCheckpoint(t, ctx, store, SeismicEndpoint, nil)
	for shardID, shard := range store.shards {
		assertRoutedOutcome(t, shard, identities[shardID], 1)
	}
	store.Close()
	for _, address := range append([]string{cfg.DatabaseURL}, cfg.ShardURLs[:]...) {
		closeFixtureMutationGuard(t, address)
	}
	store = openRoutedM2Store(t, cfg)
	if _, err := admin.Exec(ctx, "DROP TRIGGER fail_poll_checkpoint ON poll_state; DROP FUNCTION fail_poll_checkpoint()"); err != nil {
		t.Fatal(err)
	}
	activateM2ShardFixtureOwner(t, cfg, store)
	ctx = fixtureMutationContext(t, ctx, fixtureMutationOwner, fixtureMutationEpoch, allFixtureStores(store)...)
	if err := store.Ingest(ctx, chunk); err != nil {
		t.Fatalf("replay after checkpoint failure: %v", err)
	}
	recoveredCheckpoint := stamp.Add(2 * time.Minute)
	assertRoutedCheckpoint(t, ctx, store, SeismicEndpoint, &recoveredCheckpoint)
	for shardID, shard := range store.shards {
		assertRoutedOutcome(t, shard, identities[shardID], 1)
	}
}

func assertRoutedOutcome(t *testing.T, store *Store, identity string, want int) {
	t.Helper()
	canonical, source, notifications, receipts := routedCounts(t, store, identity)
	if canonical != want || source != want || notifications != want || receipts != want {
		t.Fatalf("event %q rows canonical/source/outbox/receipt=%d/%d/%d/%d, want %d each", identity, canonical, source, notifications, receipts, want)
	}
}

func assertRoutedCheckpoint(t *testing.T, ctx context.Context, store *Store, endpoint Endpoint, want *time.Time) {
	t.Helper()
	var checkpoint *time.Time
	if err := store.read.QueryRow(ctx, "SELECT checkpoint FROM poll_state WHERE endpoint=$1", string(endpoint)).Scan(&checkpoint); err != nil {
		t.Fatal(err)
	}
	if want == nil {
		if checkpoint != nil {
			t.Fatalf("checkpoint=%s, want unchanged NULL", checkpoint)
		}
		return
	}
	if checkpoint == nil || !checkpoint.Equal(*want) {
		t.Fatalf("checkpoint=%v, want %s", checkpoint, *want)
	}
}
