//go:build integration

package aggregator

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func m2IsolatedShardConfig(t *testing.T, base Config) Config {
	t.Helper()
	parsed, err := url.Parse(base.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgx.Connect(t.Context(), base.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schemaName := "m2_" + rand.Text()
	schema := pgx.Identifier{schemaName}.Sanitize()
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+schema); err != nil {
		_ = admin.Close(t.Context())
		t.Fatal(err)
	}
	roleName, password := "m2_writer_"+rand.Text(), rand.Text()
	role := pgx.Identifier{roleName}.Sanitize()
	var createRole string
	if err := admin.QueryRow(t.Context(), "SELECT format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', $1::text, $2::text)", roleName, password).Scan(&createRole); err != nil {
		_, _ = admin.Exec(t.Context(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(t.Context())
		t.Fatal(err)
	}
	if _, err := admin.Exec(t.Context(), createRole); err != nil {
		_, _ = admin.Exec(t.Context(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(t.Context())
		t.Fatal(err)
	}
	// LIFO cleanup: Stores close first, then schema, then the disposable role.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP ROLE "+role); err != nil {
			t.Error(err)
		}
		if err := admin.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	if _, err := admin.Exec(t.Context(), "GRANT USAGE ON SCHEMA "+schema+" TO "+role); err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	base.DatabaseURL = parsed.String()
	writerURL, err := url.Parse(base.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	writerURL.User = url.UserPassword(roleName, password)
	base.WriterURL = writerURL.String()
	return base
}

func grantM2WriterPrivileges(t *testing.T, cfg Config, store *Store) {
	t.Helper()
	targets := []struct{ adminURL, writerURL string }{{cfg.DatabaseURL, cfg.WriterURL}}
	for shardID, adminURL := range cfg.ShardURLs {
		if adminURL == "" {
			continue
		}
		targets = append(targets, struct{ adminURL, writerURL string }{adminURL, cfg.ShardWriterURLs[shardID]})
	}
	for _, target := range targets {
		admin, err := pgx.Connect(t.Context(), target.adminURL)
		if err != nil {
			t.Fatal(err)
		}
		var schemaName string
		if err := admin.QueryRow(t.Context(), "SELECT current_schema()").Scan(&schemaName); err != nil {
			_ = admin.Close(t.Context())
			t.Fatal(err)
		}
		writerURL, err := url.Parse(target.writerURL)
		if err != nil || writerURL.User == nil {
			_ = admin.Close(t.Context())
			t.Fatalf("invalid disposable writer URL: %v", err)
		}
		roleName := writerURL.User.Username()
		role := pgx.Identifier{roleName}.Sanitize()
		schema := pgx.Identifier{schemaName}.Sanitize()
		guard := pgx.Identifier{schemaName, "mutation_guard"}.Sanitize()
		for _, statement := range []string{
			"GRANT pg_read_all_stats TO " + role,
			"GRANT USAGE ON SCHEMA " + schema + " TO " + role,
			"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA " + schema + " TO " + role,
			"REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON " + guard + " FROM " + role,
			"GRANT SELECT ON " + guard + " TO " + role,
			"GRANT EXECUTE ON FUNCTION " + schema + ".begin_mutation_guard(text,bigint,bigint,text) TO " + role,
			"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA " + schema + " TO " + role,
		} {
			if _, err := admin.Exec(t.Context(), statement); err != nil {
				_ = admin.Close(t.Context())
				t.Fatalf("grant writer privilege %q in schema %q: %v", statement, schemaName, err)
			}
		}
		if err := admin.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	_ = store // grants deliberately follow migration and precede runtime writes
}

func activateM2ShardFixtureOwner(t *testing.T, cfg Config, store *Store) context.Context {
	t.Helper()
	return activateFixtureOwner(t, cfg, store, fixtureMutationOwner, fixtureMutationEpoch)
}

func m2BeginFixtureMutation(t *testing.T, ctx context.Context, store *Store) pgx.Tx {
	t.Helper()
	tx, err := store.beginMutation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func m2InsertHazardWithOwner(t *testing.T, ctx context.Context, store *Store, source Source, hazardType HazardType, reference string) error {
	t.Helper()
	tx := m2BeginFixtureMutation(t, ctx, store)
	if _, err := tx.Exec(ctx, `INSERT INTO hazard_events
		(hazard_id,source,source_ref_id,hazard_type,severity,area_name,latitude,longitude,occurred_at,ingested_at,attributes,revision,updated_at)
		VALUES ($1,$2,$3,$4,'SIAGA','Fixture',0,0,'2026-11-12T00:00:00Z','2026-11-12T00:00:00Z','{}',1,'2026-11-12T00:00:00Z')`,
		"fixture-"+reference, string(source), reference, string(hazardType)); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func m2ExecFixtureMutation(ctx context.Context, store *Store, statement string, args ...any) error {
	tx, err := store.beginMutation(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, statement, args...); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func TestM2ShardStoresRouteAndEnforceIdentity(t *testing.T) {
	cfg := m2IsolatedShardConfig(t, m2IsolatedConfig(t))
	for shardID := range cfg.ShardURLs {
		shardConfig := m2IsolatedShardConfig(t, cfg)
		cfg.ShardURLs[shardID] = shardConfig.DatabaseURL
		cfg.ShardWriterURLs[shardID] = shardConfig.WriterURL
	}
	store, err := OpenStore(t.Context(), cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			store.Close()
		}
	}()
	grantM2WriterPrivileges(t, cfg, store)
	ownerCtx := activateM2ShardFixtureOwner(t, cfg, store)
	findReference := func(want int16, prefix string) string {
		t.Helper()
		for n := range 10000 {
			reference := fmt.Sprintf("%s-%d", prefix, n)
			id, err := logicalShard(BMKG, reference)
			if err != nil {
				t.Fatal(err)
			}
			if id == want {
				return reference
			}
		}
		t.Fatalf("could not find reference for shard %d", want)
		return ""
	}
	ref := findReference(1, "routed")
	child, err := store.shardStore(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if child == store {
		t.Fatal("routed shard resolved to control store")
	}
	if err := m2InsertHazardWithOwner(t, ownerCtx, child, BMKG, Seismic, ref); err != nil {
		t.Fatalf("insert into owning shard: %v", err)
	}
	wrongShard := store.shards[0]
	if err := m2InsertHazardWithOwner(t, ownerCtx, wrongShard, BMKG, Seismic, ref); err == nil {
		t.Fatal("misrouted hazard insert succeeded")
	}
	if err := m2InsertHazardWithOwner(t, ownerCtx, store, BMKG, Seismic, findReference(0, "control")); err == nil {
		t.Fatal("control-store canonical insert succeeded")
	}
	stamp := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	if err := m2ExecFixtureMutation(ownerCtx, wrongShard, `INSERT INTO source_seismic (event_id, occurred_at, payload)
		VALUES ($1, $2, '{}'::jsonb)`, ref, stamp); err == nil {
		t.Fatal("misrouted seismic insert succeeded")
	}
	if err := m2ExecFixtureMutation(ownerCtx, wrongShard, `INSERT INTO source_warnings (warning_id, related_event_id, issued_at, payload)
		VALUES ('warning-fixture', $1, $2, '{}'::jsonb)`, ref, stamp); err == nil {
		t.Fatal("misrouted warning insert succeeded")
	}
	var parentRole, childRole string
	var childShard int16
	if err := store.read.QueryRow(t.Context(), "SELECT role FROM store_identity WHERE singleton").Scan(&parentRole); err != nil {
		t.Fatal(err)
	}
	if err := child.read.QueryRow(t.Context(), "SELECT role, shard_id FROM store_identity WHERE singleton").Scan(&childRole, &childShard); err != nil {
		t.Fatal(err)
	}
	if parentRole != "control" || childRole != "shard" || childShard != 1 {
		t.Fatalf("store identities parent=%q child=%q/%d", parentRole, childRole, childShard)
	}
	var parentRows, shardRows int
	if err := store.read.QueryRow(t.Context(), "SELECT count(*) FROM hazard_events").Scan(&parentRows); err != nil {
		t.Fatal(err)
	}
	if err := child.read.QueryRow(t.Context(), "SELECT count(*) FROM hazard_events").Scan(&shardRows); err != nil {
		t.Fatal(err)
	}
	if parentRows != 0 || shardRows != 1 {
		t.Fatalf("canonical rows parent=%d shard=%d; want parent=0 shard=1", parentRows, shardRows)
	}
	store.Close()
	store = nil
	for _, address := range append([]string{cfg.DatabaseURL}, cfg.ShardURLs[:]...) {
		closeFixtureMutationGuard(t, address)
	}
	store, err = OpenStore(t.Context(), cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	grantM2WriterPrivileges(t, cfg, store)
	ownerCtx = activateM2ShardFixtureOwner(t, cfg, store)
	child, err = store.shardStore(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := m2ExecFixtureMutation(ownerCtx, child, "UPDATE poll_state SET last_attempt_at=last_attempt_at WHERE endpoint='seismic'"); err != nil {
		t.Fatalf("reopened writer guarded update: %v", err)
	}
	var count, version int
	if err := child.read.QueryRow(t.Context(), "SELECT count(*) FROM hazard_events WHERE source_ref_id = $1", ref).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("reopened shard row count=%d, want 1", count)
	}
	if err := child.read.QueryRow(t.Context(), "SELECT count(*) FROM schema_migrations").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != currentSchemaVersion {
		t.Fatalf("schema version=%d, want %d", version, currentSchemaVersion)
	}
}

func TestM2MigrationEightPreservesLegacyRows(t *testing.T) {
	cfg := m2IsolatedConfig(t)
	_ = m2LegacyPool(t, cfg)
	store := m2OpenStore(t, cfg)
	var count, version int
	if err := store.read.QueryRow(t.Context(), "SELECT count(*) FROM hazard_events").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := store.read.QueryRow(t.Context(), "SELECT count(*) FROM schema_migrations").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if count != 2 || version != currentSchemaVersion {
		t.Fatalf("upgrade retained rows=%d schema version=%d; want rows=2 version=%d", count, version, currentSchemaVersion)
	}
}
