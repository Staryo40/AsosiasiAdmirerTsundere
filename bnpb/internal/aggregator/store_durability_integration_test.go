//go:build integration

package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestReplicatedMutationIntegration(t *testing.T) {
	fixture := newReplicationFixture(t)
	cfg := fixture.routedConfig(t)
	store := openRoutedM2Store(t, cfg)
	owner := activateM2ShardFixtureOwner(t, cfg, store)
	identities := routedShardIdentities(t)
	stamp := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	chunk := IngestChunk{Endpoint: SeismicEndpoint, InputID: "replicated-receipt", ReceivedAt: stamp.Add(time.Hour)}
	for index, identity := range identities {
		chunk.Records = append(chunk.Records, routedRecord(t, identity, stamp.Add(time.Duration(index)*time.Minute)))
	}

	fixture.sync(t, "")
	if err := store.Ingest(owner, chunk); err == nil {
		t.Fatal("routed input acknowledged without synchronous replication")
	}
	assertReceiptEffects(t, store, [5]int{})
	assertRoutedCheckpoint(t, owner, store, SeismicEndpoint, nil)
	fixture.sync(t, `FIRST 1 ("durability-standby")`)

	receipt := pollReceipt{response: chunk, started: stamp}
	if err := store.stagePollReceipt(owner, receipt); err != nil {
		t.Fatal(err)
	}
	if err := store.Ingest(owner, chunk); err != nil {
		t.Fatal(err)
	}
	if err := store.clearPollReceipt(owner, chunk); err != nil {
		t.Fatal(err)
	}
	before := durabilityChecksum(t, store, cfg)
	sorted := slices.Clone(identities[:])
	slices.Sort(sorted)
	expected := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	if before.Partial || before.Count != 3 || before.Checksum != hex.EncodeToString(expected[:]) {
		t.Fatalf("acknowledged population is not independent expected population: %+v", before)
	}

	for _, target := range []*Store{store, store.shards[0]} {
		tx, err := target.beginMutation(owner)
		if err != nil {
			t.Fatal(err)
		}
		// A WAL-producing write makes native synchronous COMMIT wait. Canceling
		// that wait may leave visible local rows; it must never become success.
		if _, err := tx.Exec(owner, "UPDATE poll_state SET last_attempt_at=now() WHERE endpoint='seismic'"); err != nil {
			_ = rollbackTransaction(owner, tx)
			t.Fatal(err)
		}
		fixture.command(t, "pg_ctl", "-D", fixture.standby, "-m", "immediate", "-w", "stop")
		commitCtx, cancel := context.WithTimeout(owner, 250*time.Millisecond)
		err = tx.Commit(commitCtx)
		cancel()
		if err == nil {
			t.Fatal("single-copy control/canonical commit was acknowledged")
		}
		if _, err := target.beginMutation(owner); !errors.Is(err, errReplicatedDurability) {
			t.Fatalf("replica loss did not reject a new mutation: %v", err)
		}
		fixture.start(t, fixture.standby)
		fixture.sync(t, `FIRST 1 ("durability-standby")`)
		if err := store.Ingest(owner, chunk); err != nil {
			t.Fatalf("idempotent replay after replication recovery: %v", err)
		}
	}

	// Cover a no-WAL retry and a synchronous configuration change after begin.
	// Native COMMIT alone is allowed to succeed in this case; the post-commit
	// barrier must still reject success until a physical standby flushes it.
	tx, err := store.beginMutation(owner)
	if err != nil {
		t.Fatal(err)
	}
	fixture.sync(t, "")
	commitCtx, cancel := context.WithTimeout(owner, 250*time.Millisecond)
	err = tx.Commit(commitCtx)
	cancel()
	if err == nil {
		t.Fatal("no-WAL retry acknowledged after synchronous policy drift")
	}
	fixture.sync(t, `FIRST 1 ("durability-standby")`)
	if err := store.Ingest(owner, chunk); err != nil {
		t.Fatal(err)
	}
	checkpoint := stamp.Add(2 * time.Minute)
	assertRoutedCheckpoint(t, owner, store, SeismicEndpoint, &checkpoint)

	// This is a fixture-owned disk loss, not a deployment fencing test.
	store.Close()
	fixture.command(t, "pg_ctl", "-D", fixture.primary, "-m", "immediate", "-w", "stop")
	if err := os.RemoveAll(fixture.primary); err != nil {
		t.Fatal(err)
	}
	standby := fixture.connect(t, fixture.standbyURL)
	var promoted bool
	if err := standby.QueryRow(t.Context(), "SELECT pg_promote(true, 10)").Scan(&promoted); err != nil || !promoted {
		_ = standby.Close(t.Context())
		t.Fatalf("promote surviving fixture disk: %t, %v", promoted, err)
	}
	_ = standby.Close(t.Context())
	store.read = replacementReader(t, fixtureReplicaURL(t, cfg.WriterURL, fixture.standbyURL), store.storeIdentity).read
	for index, old := range store.shards {
		store.shards[index] = replacementReader(t, fixtureReplicaURL(t, cfg.ShardWriterURLs[index], fixture.standbyURL), old.storeIdentity)
	}
	after := durabilityChecksum(t, store, cfg)
	if after.Partial || after.Count != before.Count || after.Checksum != before.Checksum {
		t.Fatalf("primary disk loss changed acknowledged Audit population: before=%+v after=%+v", before, after)
	}
	assertRoutedCheckpoint(t, t.Context(), store, SeismicEndpoint, &checkpoint)
	var pending, bindings int
	if err := store.read.QueryRow(t.Context(), "SELECT (SELECT count(*) FROM pending_poll_receipts), (SELECT count(*) FROM receipt_bindings)").Scan(&pending, &bindings); err != nil {
		t.Fatal(err)
	}
	if pending != 0 || bindings != 3 {
		t.Fatalf("control receipt state lost after primary disk loss: pending=%d bindings=%d", pending, bindings)
	}
}

func durabilityChecksum(t *testing.T, store *Store, cfg Config) auditChecksumResponse {
	t.Helper()
	request := httptest.NewRequest("GET", "/internal/audit/checksum?source=BMKG", nil).WithContext(t.Context())
	response := httptest.NewRecorder()
	auditAPI{store: store, cfg: cfg}.checksum(response, request)
	var checksum auditChecksumResponse
	if err := json.Unmarshal(response.Body.Bytes(), &checksum); err != nil || response.Code != 200 {
		t.Fatalf("Audit checksum: status=%d err=%v", response.Code, err)
	}
	return checksum
}

func fixtureReplicaURL(t *testing.T, original, replica string) string {
	t.Helper()
	address, err := url.Parse(original)
	if err != nil {
		t.Fatal(err)
	}
	target, err := url.Parse(replica)
	if err != nil {
		t.Fatal(err)
	}
	address.Host = target.Host
	return address.String()
}

var _ pgx.Tx = (*replicatedMutation)(nil)
