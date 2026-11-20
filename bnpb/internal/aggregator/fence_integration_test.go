//go:build integration

package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const fixtureMutationOwner = "m2-fixture-owner"
const fixtureMutationEpoch int64 = 41

type fixtureMutationGuard struct {
	resource   string
	generation int64
}

// This fixture deliberately uses privileged connections only to initialize test
// state. Runtime writes always use Store's non-owner writer connection.
func activateFixtureMutationGuard(t *testing.T, address, owner string, epoch int64) {
	t.Helper()
	admin, err := pgx.Connect(t.Context(), address)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(t.Context())
	if _, err := admin.Exec(t.Context(), "SELECT prepare_mutation_generation('mutation_guard'::regclass)"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(t.Context(), "UPDATE mutation_guard SET owner=$1, epoch=$2, state='active'", owner, epoch); err != nil {
		t.Fatal(err)
	}
}

func closeFixtureMutationGuard(t *testing.T, address string) {
	t.Helper()
	admin, err := pgx.Connect(t.Context(), address)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(t.Context())
	if _, err := admin.Exec(t.Context(), "UPDATE mutation_guard SET state='closed', owner=''"); err != nil {
		t.Fatal(err)
	}
}

func fixtureMutationContext(t *testing.T, ctx context.Context, owner string, epoch int64, stores ...*Store) context.Context {
	t.Helper()
	guards := make([]mutationGuardToken, 0, len(stores))
	for _, store := range stores {
		var guard fixtureMutationGuard
		if err := store.read.QueryRow(ctx, "SELECT resource_identity, pg_generation FROM mutation_guard").Scan(&guard.resource, &guard.generation); err != nil {
			t.Fatal(err)
		}
		guards = append(guards, mutationGuardToken{store: store, resource: guard.resource, generation: guard.generation})
	}
	return context.WithValue(ctx, mutationContextKey{}, mutationToken{owner: owner, epoch: epoch, guards: guards})
}

func allFixtureStores(store *Store) []*Store {
	stores := []*Store{store}
	for _, shard := range store.shards {
		if shard != nil {
			stores = append(stores, shard)
		}
	}
	return stores
}

func activateFixtureOwner(t *testing.T, cfg Config, store *Store, owner string, epoch int64) context.Context {
	t.Helper()
	addresses := []string{cfg.DatabaseURL}
	for _, address := range cfg.ShardURLs {
		if address != "" {
			addresses = append(addresses, address)
		}
	}
	for _, address := range addresses {
		activateFixtureMutationGuard(t, address, owner, epoch)
	}
	return fixtureMutationContext(t, t.Context(), owner, epoch, allFixtureStores(store)...)
}

func TestMutationGuardIntegrationSerializesRevokeWithProtectedWrite(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	activateFixtureMutationGuard(t, cfg.DatabaseURL, fixtureMutationOwner, fixtureMutationEpoch)
	ownerCtx := fixtureMutationContext(t, t.Context(), fixtureMutationOwner, fixtureMutationEpoch, store)
	tx, err := store.beginMutation(ownerCtx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ownerCtx, "UPDATE poll_state SET last_attempt_at=now() WHERE endpoint=$1", string(SeismicEndpoint)); err != nil {
		t.Fatalf("protected write under active guard: %v", err)
	}

	admin, err := pgx.Connect(t.Context(), cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(t.Context())
	revokeTx, err := admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := revokeTx.Exec(t.Context(), "SET LOCAL lock_timeout='150ms'"); err != nil {
		_ = revokeTx.Rollback(t.Context())
		t.Fatal(err)
	}
	_, err = revokeTx.Exec(t.Context(), "UPDATE mutation_guard SET state='closed', owner=''")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		_ = revokeTx.Rollback(t.Context())
		t.Fatalf("guard revoke did not wait for active protected transaction (got %v)", err)
	}
	_ = revokeTx.Rollback(t.Context())
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(t.Context(), "UPDATE mutation_guard SET state='closed', owner=''"); err != nil {
		t.Fatalf("guard revoke after protected commit: %v", err)
	}
	if _, err := store.beginMutation(ownerCtx); err == nil {
		t.Fatal("write with revoked owner token succeeded")
	}
}

func TestMutationGuardIntegrationRejectsStalePostgreSQLGeneration(t *testing.T) {
	cfg := m2IsolatedShardConfig(t, m2IsolatedConfig(t))
	store := openRoutedM2Store(t, cfg)
	activateFixtureMutationGuard(t, cfg.DatabaseURL, fixtureMutationOwner, fixtureMutationEpoch)
	staleCtx := fixtureMutationContext(t, t.Context(), fixtureMutationOwner, fixtureMutationEpoch, store)
	closeFixtureMutationGuard(t, cfg.DatabaseURL)
	admin, err := pgx.Connect(t.Context(), cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(t.Context(), "UPDATE mutation_guard SET pg_generation=pg_generation+1"); err != nil {
		_ = admin.Close(t.Context())
		t.Fatal(err)
	}
	if err := admin.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	activateFixtureMutationGuard(t, cfg.DatabaseURL, fixtureMutationOwner, fixtureMutationEpoch)
	currentCtx := fixtureMutationContext(t, t.Context(), fixtureMutationOwner, fixtureMutationEpoch, store)
	if _, err := store.beginMutation(staleCtx); err == nil {
		t.Fatal("mutation accepted a token from the prior PostgreSQL generation")
	}
	tx, err := store.beginMutation(currentCtx)
	if err != nil {
		t.Fatalf("mutation rejected current PostgreSQL generation: %v", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestMutationGuardIntegrationRejectsCopiedTimeline(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	activateFixtureMutationGuard(t, cfg.DatabaseURL, fixtureMutationOwner, fixtureMutationEpoch)
	admin, err := pgx.Connect(t.Context(), cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(t.Context())

	// Set up a copied guard row without changing the database's live timeline.
	if _, err := admin.Exec(t.Context(), `UPDATE mutation_guard
		SET resource_identity=regexp_replace(resource_identity, ':timeline:[0-9]+$',
		    CASE WHEN resource_identity LIKE '%:timeline:1' THEN ':timeline:2' ELSE ':timeline:1' END)
		WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	staleCtx := fixtureMutationContext(t, t.Context(), fixtureMutationOwner, fixtureMutationEpoch, store)
	started := time.Date(2026, 11, 18, 5, 0, 0, 0, time.UTC)
	if err := store.recordPollAttempt(staleCtx, SeismicEndpoint, started, pollObservation{}); !errors.Is(err, errStaleMutationAuthority) {
		t.Fatalf("mutation with matching row but stale PostgreSQL timeline: got %v, want stale authority", err)
	}

	closeFixtureMutationGuard(t, cfg.DatabaseURL)
	activateFixtureMutationGuard(t, cfg.DatabaseURL, fixtureMutationOwner, fixtureMutationEpoch)
	reboundCtx := fixtureMutationContext(t, t.Context(), fixtureMutationOwner, fixtureMutationEpoch, store)
	if err := store.recordPollAttempt(reboundCtx, SeismicEndpoint, started, pollObservation{}); err != nil {
		t.Fatalf("mutation after reconciling PostgreSQL generation: %v", err)
	}
	var lastAttempt time.Time
	if err := store.read.QueryRow(t.Context(), "SELECT last_attempt_at FROM poll_state WHERE endpoint='seismic'").Scan(&lastAttempt); err != nil {
		t.Fatal(err)
	}
	if !lastAttempt.Equal(started) {
		t.Fatalf("last poll attempt after rebinding: got %v, want %v", lastAttempt, started)
	}
}

func TestMutationGuardIntegrationRejectsStaleOwnerAcrossAllMutationPaths(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	addresses := append([]string{cfg.DatabaseURL}, cfg.ShardURLs[:]...)
	for _, address := range addresses {
		activateFixtureMutationGuard(t, address, fixtureMutationOwner, fixtureMutationEpoch)
	}
	staleCtx := fixtureMutationContext(t, t.Context(), fixtureMutationOwner, fixtureMutationEpoch, allFixtureStores(store)...)
	for _, address := range addresses {
		closeFixtureMutationGuard(t, address)
		activateFixtureMutationGuard(t, address, "new-fixture-owner", fixtureMutationEpoch+1)
	}

	identity := routedShardIdentities(t)[0]
	stamp := time.Date(2026, 11, 18, 5, 0, 0, 0, time.UTC)
	chunk := IngestChunk{Endpoint: SeismicEndpoint, Records: []json.RawMessage{routedRecord(t, identity, stamp)}, ReceivedAt: stamp.Add(time.Minute), InputID: "stale-owner-rejection"}
	if err := store.Ingest(t.Context(), chunk); err == nil {
		t.Fatal("routed input without an owner token succeeded")
	}
	if err := store.Ingest(staleCtx, chunk); err == nil {
		t.Fatal("routed input accepted stale owner context")
	}
	started := stamp.Add(2 * time.Minute)
	if err := store.recordPollAttempt(staleCtx, SeismicEndpoint, started, pollObservation{}); err == nil {
		t.Fatal("control poll attempt accepted stale owner context")
	}
	if err := store.recordPollFailure(staleCtx, SeismicEndpoint, started, "fixture"); err == nil {
		t.Fatal("control poll failure accepted stale owner context")
	}
	if err := store.writeShardCheckpoint(staleCtx, chunk, checkpointPolicy{completePoll: true}); err == nil {
		t.Fatal("control checkpoint accepted stale owner context")
	}
	for _, shard := range store.shards {
		canonical, source, notifications, receipts := routedCounts(t, shard, identity)
		if canonical+source+notifications+receipts != 0 {
			t.Fatalf("stale input left business rows canonical/source/outbox/handled=%d/%d/%d/%d", canonical, source, notifications, receipts)
		}
	}
	assertRoutedCheckpoint(t, t.Context(), store, SeismicEndpoint, nil)
}

func TestMutationGuardIntegrationNewOwnerFirstWriteAndCommitRollback(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	addresses := append([]string{cfg.DatabaseURL}, cfg.ShardURLs[:]...)
	for _, address := range addresses {
		activateFixtureMutationGuard(t, address, fixtureMutationOwner, fixtureMutationEpoch)
	}
	staleOwnerCtx := fixtureMutationContext(t, t.Context(), fixtureMutationOwner, fixtureMutationEpoch, allFixtureStores(store)...)
	for _, address := range addresses {
		closeFixtureMutationGuard(t, address)
		activateFixtureMutationGuard(t, address, "new-fixture-owner", fixtureMutationEpoch+1)
	}
	staleEpochCtx := fixtureMutationContext(t, t.Context(), "new-fixture-owner", fixtureMutationEpoch, allFixtureStores(store)...)
	ownerCtx := fixtureMutationContext(t, t.Context(), "new-fixture-owner", fixtureMutationEpoch+1, allFixtureStores(store)...)

	identity := "new-owner-first-write"
	stamp := time.Date(2026, 11, 18, 6, 0, 0, 0, time.UTC)
	chunk := IngestChunk{Endpoint: SeismicEndpoint, Records: []json.RawMessage{routedRecord(t, identity, stamp)}, ReceivedAt: stamp.Add(time.Minute), InputID: "new-owner-first-write"}
	for _, stale := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "old owner", ctx: staleOwnerCtx},
		{name: "old epoch", ctx: staleEpochCtx},
	} {
		t.Run(stale.name, func(t *testing.T) {
			if err := store.Ingest(stale.ctx, chunk); err == nil {
				t.Fatalf("%s accepted after owner transition", stale.name)
			}
		})
	}
	if err := store.Ingest(ownerCtx, chunk); err != nil {
		t.Fatalf("new owner's first write: %v", err)
	}
	shardID, err := logicalShard(BMKG, identity)
	if err != nil {
		t.Fatal(err)
	}
	canonical, source, notifications, receipts := routedCounts(t, store.shards[shardID], identity)
	if canonical != 1 || source != 1 || notifications != 1 || receipts != 1 {
		t.Fatalf("new-owner rows canonical/source/outbox/handled=%d/%d/%d/%d", canonical, source, notifications, receipts)
	}
	var checkpointBefore *time.Time
	if err := store.read.QueryRow(t.Context(), "SELECT checkpoint FROM poll_state WHERE endpoint=$1", string(SeismicEndpoint)).Scan(&checkpointBefore); err != nil {
		t.Fatal(err)
	}
	if checkpointBefore == nil || !checkpointBefore.Equal(stamp) {
		t.Fatalf("new owner's checkpoint=%v; want %s", checkpointBefore, stamp)
	}

	failedID := "commit-reject-no-partial-outcomes"
	failedShardID, err := logicalShard(BMKG, failedID)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.Connect(t.Context(), cfg.ShardURLs[failedShardID])
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(t.Context())
	if _, err := admin.Exec(t.Context(), `CREATE FUNCTION reject_fixture_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture deferred commit rejection'; END $$;
		CREATE CONSTRAINT TRIGGER reject_fixture_receipt AFTER INSERT ON handled_inputs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_fixture_receipt()`); err != nil {
		t.Fatal(err)
	}
	failedChunk := IngestChunk{Endpoint: SeismicEndpoint, Records: []json.RawMessage{routedRecord(t, failedID, stamp.Add(2*time.Minute))}, ReceivedAt: stamp.Add(3 * time.Minute), InputID: failedID}
	if err := store.Ingest(ownerCtx, failedChunk); err == nil {
		t.Fatal("deferred rejection unexpectedly committed")
	}
	canonical, source, notifications, receipts = routedCounts(t, store.shards[failedShardID], failedID)
	if canonical+source+notifications+receipts != 0 {
		t.Fatalf("rejected commit left partial rows canonical/source/outbox/handled=%d/%d/%d/%d", canonical, source, notifications, receipts)
	}
	var rejects int
	if err := store.shards[failedShardID].read.QueryRow(t.Context(), "SELECT count(*) FROM ingest_rejects").Scan(&rejects); err != nil {
		t.Fatal(err)
	}
	if rejects != 0 {
		t.Fatalf("rejected commit left %d ingest rejects", rejects)
	}
	var checkpointAfter *time.Time
	if err := store.read.QueryRow(t.Context(), "SELECT checkpoint FROM poll_state WHERE endpoint=$1", string(SeismicEndpoint)).Scan(&checkpointAfter); err != nil {
		t.Fatal(err)
	}
	if checkpointAfter == nil || !checkpointAfter.Equal(*checkpointBefore) {
		t.Fatalf("rejected commit changed poll checkpoint from %v to %v", checkpointBefore, checkpointAfter)
	}
	if _, err := admin.Exec(t.Context(), "DROP TRIGGER reject_fixture_receipt ON handled_inputs; DROP FUNCTION reject_fixture_receipt()"); err != nil {
		t.Fatal(err)
	}
}

func TestMutationGuardIntegrationAppWriterRejectsMissingTokenAndTruncate(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	if _, err := store.write.Exec(t.Context(), "UPDATE poll_state SET last_attempt_at=now() WHERE endpoint='seismic'"); err == nil {
		t.Fatal("app writer direct poll_state DML without token succeeded")
	}
	if _, err := store.write.Exec(t.Context(), "TRUNCATE hazard_events"); err == nil {
		t.Fatal("app writer can truncate protected business data")
	}
}
