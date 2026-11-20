//go:build integration

package aggregator

import (
	"errors"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPlacementRejectsStaleSnapshot(t *testing.T) {
	store := m2OpenStore(t, m2IsolatedConfig(t))
	if _, err := store.write.Exec(t.Context(), `INSERT INTO shard_placements VALUES ('s0-target',0,'target','replica','copying')`); err != nil {
		t.Fatal(err)
	}
	stale, err := store.write.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTransaction(t.Context(), stale)
	var count int
	if err := stale.QueryRow(t.Context(), "SELECT count(*) FROM shard_placements").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.Exec(t.Context(), `INSERT INTO shard_placements VALUES ('s1-target',1,'target','replica','copying')`); err != nil {
		t.Fatal(err)
	}
	_, err = stale.Exec(t.Context(), `INSERT INTO shard_placements VALUES ('s2-target',2,'target','replica','copying')`)
	if err == nil {
		if err := stale.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := store.read.QueryRow(t.Context(), "SELECT count(DISTINCT shard_id) FROM shard_placements WHERE node='target'").Scan(&count); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("stale snapshot committed %d distinct shards onto one node", count)
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || (postgresError.Code != "40001" && postgresError.Code != "23514") {
		t.Fatalf("unexpected storage rejection: %v", err)
	}
}

func TestPlacementStorageRetainsCopyInventory(t *testing.T) {
	for _, statement := range []string{"DELETE FROM shard_placements WHERE unit='s0-a'", "TRUNCATE shard_placements"} {
		t.Run(statement, func(t *testing.T) {
			store := m2OpenStore(t, m2IsolatedConfig(t))
			if err := store.recordPlacement(t.Context(), placementSnapshot{Copies: rotatingPlacement()}); err != nil {
				t.Fatal(err)
			}
			before, err := store.placement(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.write.Exec(t.Context(), statement); err == nil {
				t.Fatal("storage forgot physical copies without retirement proof")
			}
			after, err := store.placement(t.Context())
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected mutation changed inventory: %+v, %v", after, err)
			}
		})
	}
}

func TestPlacementStorageRevisionInvalidatesCAS(t *testing.T) {
	store := m2OpenStore(t, m2IsolatedConfig(t))
	if err := store.recordPlacement(t.Context(), placementSnapshot{Copies: rotatingPlacement()}); err != nil {
		t.Fatal(err)
	}
	before, err := store.placement(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.write.Exec(t.Context(), "UPDATE shard_placements SET state='retiring' WHERE unit='s0-b'"); err != nil {
		t.Fatal(err)
	}
	if err := store.recordPlacement(t.Context(), before); !errors.Is(err, errPlacementChanged) {
		t.Fatalf("stale proposal overwrote a durable placement change: %v", err)
	}
}

func TestPlacementVersionSixUpgradePreservesInventory(t *testing.T) {
	cfg := m2IsolatedConfig(t)
	legacy := m2LegacyPool(t, cfg)
	for _, migration := range []string{coastalSchema, m2EnumSchema, shardSchema} {
		if _, err := legacy.Exec(t.Context(), migration); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := legacy.Exec(t.Context(), `INSERT INTO shard_placements VALUES
        ('s0-a',0,'node-a','primary','ready'), ('s0-b',0,'node-b','replica','ready'),
        ('s1-b',1,'node-b','primary','ready'), ('s1-c',1,'node-c','replica','ready'),
        ('s2-c',2,'node-c','primary','ready'), ('s2-a',2,'node-a','replica','retiring');
        UPDATE placement_revision SET revision=11`); err != nil {
		t.Fatal(err)
	}
	before := m2Rows(t, legacy, "shard_placements")
	store := m2OpenStore(t, cfg)
	if after := m2Rows(t, store.read, "shard_placements"); after != before {
		t.Fatal("version-six migration changed the copy inventory")
	}
	placement, err := store.placement(t.Context())
	if err != nil || placement.Revision != 11 {
		t.Fatalf("upgrade reset the existing CAS revision: %+v, %v", placement, err)
	}
	if err := store.Ready(t.Context()); err != nil {
		t.Fatal(err)
	}
}
