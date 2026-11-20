//go:build integration

package aggregator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestReceiptBindingIntegrationDatabaseFence(t *testing.T) {
	cfg := routedM2Config(t)
	store := m2OpenStore(t, cfg)
	var restricted bool
	if err := store.write.QueryRow(t.Context(), `SELECT
		has_table_privilege(current_user, 'receipt_bindings', 'SELECT')
		AND has_table_privilege(current_user, 'receipt_bindings', 'INSERT')
		AND NOT has_table_privilege(current_user, 'receipt_bindings', 'UPDATE,DELETE,TRUNCATE')`).Scan(&restricted); err != nil || !restricted {
		t.Fatalf("control writer SELECT+INSERT-only grant=%v, err=%v", restricted, err)
	}
	seed := receiptBindingFixture(t, 0)
	digest := sha256.Sum256(seed.Records[0])
	_, err := store.write.Exec(t.Context(), `INSERT INTO receipt_bindings (endpoint,input_hash,raw_digest,destination)
		VALUES ($1,$2,$3,0)`, string(seed.Endpoint), inputHash(seed), digest[:])
	assertReceiptSQLState(t, err, "42501")

	grantM2WriterPrivileges(t, cfg, store)
	ctx := activateM2ShardFixtureOwner(t, cfg, store)
	if err := store.Ingest(ctx, seed); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"UPDATE receipt_bindings SET destination=1",
		"DELETE FROM receipt_bindings",
	} {
		assertReceiptSQLState(t, m2ExecFixtureMutation(ctx, store, statement), "23514")
	}
	_, err = store.authorityPools[0].Exec(ctx, "TRUNCATE receipt_bindings")
	assertReceiptSQLState(t, err, "23514")
	assertReceiptBinding(t, store, seed, 0)
}

func assertReceiptSQLState(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != want {
		t.Fatalf("receipt binding SQL error=%v, want SQLSTATE %s", err, want)
	}
}

func TestReceiptBindingIntegrationRejectsConflictingCatalogBatch(t *testing.T) {
	for _, tc := range []struct {
		name        string
		firstTarget int
		retryTarget int
	}{
		{"shard to shard", 0, 1},
		{"shard to control", 0, 3},
		{"same shard different bytes", 0, 0},
		{"control to shard", 3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := routedM2Config(t)
			cfg.IngestChunkRecords, cfg.Catalog.Token = 1, "receipt-binding-fixture"
			store := openRoutedM2Store(t, cfg)
			ctx := activateM2ShardFixtureOwner(t, cfg, store)
			mux := http.NewServeMux()
			registerCatalogRoutes(mux, store, cfg, nil)
			seed := receiptBindingFixture(t, tc.firstTarget)
			receipt := catalogReceipt{
				InputID: seed.InputID, InputOffset: seed.InputOffset,
				ReceivedAt: seed.ReceivedAt, Records: seed.Records,
			}
			wantStatus := http.StatusOK
			if tc.firstTarget == 3 {
				wantStatus = http.StatusConflict
			}
			postReceiptBindingFixture(t, ctx, mux, receipt, wantStatus)
			before := receiptEffectCounts(t, store)
			postReceiptBindingFixture(t, ctx, mux, receipt, wantStatus)
			assertReceiptEffects(t, store, before)

			// The conflicting ordinal is in a later configured chunk; the fresh prefix must not commit.
			prefix := receiptBindingFixture(t, 2).Records[0]
			conflicting := receiptBindingFixture(t, tc.retryTarget).Records[0]
			if tc.firstTarget == tc.retryTarget {
				conflicting = p1WithField(t, conflicting, "region_name", p1Encode(t, "Different exact bytes"))
			}
			receipt.InputOffset--
			receipt.Records = []json.RawMessage{prefix, conflicting}
			body := postReceiptBindingFixture(t, ctx, mux, receipt, http.StatusConflict)
			var failure struct {
				Error struct{ Code, Message string }
			}
			if err := json.Unmarshal([]byte(body), &failure); err != nil ||
				failure.Error.Code != "catalogue_input_conflict" || failure.Error.Message == "" {
				t.Fatalf("invalid catalogue conflict error: %s (%v)", body, err)
			}
			assertReceiptEffects(t, store, before)

			receipt.InputOffset = math.MaxInt
			postReceiptBindingFixture(t, ctx, mux, receipt, http.StatusBadRequest)
			assertReceiptEffects(t, store, before)
		})
	}
}

func TestReceiptBindingIntegrationConcurrentConflict(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ctx := activateM2ShardFixtureOwner(t, cfg, store)
	start, outcomes := make(chan struct{}), make(chan error, 2)
	for shard := range 2 {
		chunk := receiptBindingFixture(t, shard)
		go func() {
			<-start
			outcomes <- store.Ingest(ctx, chunk)
		}()
	}
	close(start)
	first, second := <-outcomes, <-outcomes
	if first != nil {
		first, second = second, first
	}
	if first != nil || !errors.Is(second, errReceiptConflict) {
		t.Fatalf("concurrent ingest errors=(%v, %v), want one success and one receipt conflict", first, second)
	}
	assertReceiptEffects(t, store, [5]int{1, 1, 1, 1, 0})
}

func TestReceiptBindingIntegrationResumesAfterBinding(t *testing.T) {
	cfg := routedM2Config(t)
	store := openRoutedM2Store(t, cfg)
	ctx := activateM2ShardFixtureOwner(t, cfg, store)
	seed := receiptBindingFixture(t, 0)
	admin, err := pgx.Connect(ctx, cfg.ShardURLs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, `CREATE FUNCTION interrupt_bound_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'interrupted after control binding'; END $$;
		CREATE TRIGGER interrupt_bound_receipt BEFORE INSERT ON handled_inputs
		FOR EACH ROW EXECUTE FUNCTION interrupt_bound_receipt()`); err != nil {
		t.Fatal(err)
	}
	if err := store.Ingest(ctx, seed); err == nil {
		t.Fatal("ingest succeeded despite interruption after binding")
	}
	assertReceiptEffects(t, store, [5]int{})
	assertRoutedCheckpoint(t, ctx, store, SeismicEndpoint, nil)
	assertReceiptBinding(t, store, seed, 0)
	if err := store.Ingest(ctx, receiptBindingFixture(t, 1)); !errors.Is(err, errReceiptConflict) {
		t.Fatalf("changing bound ordinal after interruption: %v", err)
	}
	assertReceiptEffects(t, store, [5]int{})
	if _, err := admin.Exec(ctx, "DROP TRIGGER interrupt_bound_receipt ON handled_inputs"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := store.Ingest(ctx, seed); err != nil {
			t.Fatalf("identical replay: %v", err)
		}
		assertReceiptEffects(t, store, [5]int{1, 1, 1, 1, 0})
	}
}

func TestReceiptBindingIntegrationProtectsLegacyOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		destination int16
		retryTarget int
	}{
		{"legacy shard", 0, 1},
		{"legacy control", -1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := routedM2Config(t)
			store := openRoutedM2Store(t, cfg)
			ctx := activateM2ShardFixtureOwner(t, cfg, store)
			seed := receiptBindingFixture(t, 0)
			if tc.destination == -1 {
				seed = receiptBindingFixture(t, 3)
				if err := store.writeUnroutable(ctx, seed, seed.Records[0], errors.New("legacy unroutable identity")); err != nil {
					t.Fatal(err)
				}
			} else if err := store.shards[tc.destination].writeShardInput(ctx, seed, seed.Records[0]); err != nil {
				t.Fatal(err)
			}
			before := receiptEffectCounts(t, store)
			if err := store.Ingest(ctx, receiptBindingFixture(t, tc.retryTarget)); !errors.Is(err, errReceiptConflict) {
				t.Fatalf("legacy conflict was not rejected: %v", err)
			}
			assertReceiptEffects(t, store, before)
			var bound bool
			if err := store.read.QueryRow(ctx, "SELECT EXISTS (SELECT FROM receipt_bindings WHERE endpoint=$1 AND input_hash=$2)", string(seed.Endpoint), inputHash(seed)).Scan(&bound); err != nil || bound {
				t.Fatalf("conflicting legacy attempt left binding=%v, err=%v", bound, err)
			}
			if err := store.Ingest(ctx, seed); err != nil {
				t.Fatalf("identical legacy replay: %v", err)
			}
			assertReceiptBinding(t, store, seed, tc.destination)
			assertReceiptEffects(t, store, before)
		})
	}
}

func receiptBindingFixture(t *testing.T, target int) IngestChunk {
	t.Helper()
	stamp := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	raw := json.RawMessage(`{"region_name":"Unroutable fixture"}`)
	if target < logicalShardCount {
		raw = routedRecord(t, routedShardIdentities(t)[target], stamp)
	}
	return IngestChunk{
		Endpoint: SeismicEndpoint, Records: []json.RawMessage{raw}, ReceivedAt: stamp.Add(time.Minute),
		InputID: "receipt-binding-fixture", InputOffset: 21,
	}
}

func postReceiptBindingFixture(t *testing.T, ctx context.Context, mux http.Handler, receipt catalogReceipt, want int) string {
	t.Helper()
	payload, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/internal/catalog/seismic", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer receipt-binding-fixture")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, req)
	if response.Code != want {
		t.Fatalf("catalogue status=%d body=%s, want %d", response.Code, response.Body, want)
	}
	return response.Body.String()
}

func receiptEffectCounts(t *testing.T, store *Store) [5]int {
	t.Helper()
	var sum [5]int
	for _, unit := range allFixtureStores(store) {
		var got [5]int
		if err := unit.read.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM hazard_events),
			(SELECT count(*) FROM source_seismic), (SELECT count(*) FROM outbox),
			(SELECT count(*) FROM handled_inputs), (SELECT count(*) FROM ingest_rejects)`).Scan(&got[0], &got[1], &got[2], &got[3], &got[4]); err != nil {
			t.Fatal(err)
		}
		for i, count := range got {
			sum[i] += count
		}
	}
	return sum
}

func assertReceiptEffects(t *testing.T, store *Store, want [5]int) {
	t.Helper()
	if got := receiptEffectCounts(t, store); got != want {
		t.Fatalf("aggregate canonical/source/outbox/handled/rejected counts=%v, want %v", got, want)
	}
}

func assertReceiptBinding(t *testing.T, store *Store, chunk IngestChunk, wantDestination int16) {
	t.Helper()
	var gotDigest []byte
	var gotDestination int16
	if err := store.read.QueryRow(t.Context(), `SELECT raw_digest,destination FROM receipt_bindings
		WHERE endpoint=$1 AND input_hash=$2`, string(chunk.Endpoint), inputHash(chunk)).Scan(&gotDigest, &gotDestination); err != nil {
		t.Fatal(err)
	}
	wantDigest := sha256.Sum256(chunk.Records[0])
	if !bytes.Equal(gotDigest, wantDigest[:]) || gotDestination != wantDestination {
		t.Fatalf("receipt binding digest matches=%v, destination=%d want %d", bytes.Equal(gotDigest, wantDigest[:]), gotDestination, wantDestination)
	}
}
