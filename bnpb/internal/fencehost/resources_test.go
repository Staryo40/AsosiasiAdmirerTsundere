//go:build integration

package fencehost

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const testGuardOwner = "resource-owner"

func isolatedGuardDatabase(t *testing.T) string {
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
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := admin.Close(ctx); err != nil {
			t.Errorf("close isolated database connection: %v", err)
		}
	})
	return parsed.String()
}

func createGuardSchema(t *testing.T, databaseURL string, state, owner string, epoch int64) string {
	t.Helper()
	admin, err := pgx.Connect(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(t.Context())
	schema := "m2_" + strings.ToLower(randomSchemaToken())
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		cleanup, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Errorf("connect to clean isolated guard schema: %v", err)
			return
		}
		defer cleanup.Close(ctx)
		if _, err := cleanup.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("drop isolated guard schema: %v", err)
		}
	})
	if _, err := admin.Exec(t.Context(), "CREATE TABLE "+pgx.Identifier{schema, "mutation_guard"}.Sanitize()+" (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), state text NOT NULL, owner text NOT NULL, epoch bigint NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(t.Context(), "INSERT INTO "+pgx.Identifier{schema, "mutation_guard"}.Sanitize()+" (singleton,state,owner,epoch) VALUES (true,$1,$2,$3)", state, owner, epoch); err != nil {
		t.Fatal(err)
	}
	return schema
}

func randomSchemaToken() string { return rand.Text() }

func target(databaseURL, schema string) GuardTarget {
	return GuardTarget{DatabaseURL: databaseURL, Schema: schema}
}

func resourceTestHost(t *testing.T, bindings ResourceBindings) (*Host, *fakeOps) {
	t.Helper()
	ops := &fakeOps{boot: "resource-test-boot", now: int64(time.Second)}
	dir := t.TempDir()
	if err := ops.persistState(dir, diskState{BootID: ops.boot, Domains: map[string]domainState{}}); err != nil {
		t.Fatal(err)
	}
	host, err := openWithOps(dir, "secret", []string{testDomain, testStorageDomain}, ops, bindings)
	if err != nil {
		t.Fatal(err)
	}
	return host, ops
}

func grantResourceTest(t *testing.T, host *Host, domain, ownerDomain string, epoch uint64) Identity {
	t.Helper()
	identity := Identity{DomainUUID: domain, Epoch: epoch, RenewalSeq: 1, Owner: testGuardOwner, OwnerDomainUUID: ownerDomain}
	body, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	response := send(host, "POST", "/v1/grant", string(body))
	if response.Code != 200 {
		t.Fatalf("grant failed: %d %s", response.Code, response.Body)
	}
	return identity
}

func readGuard(t *testing.T, databaseURL, schema string) (string, string, int64) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(t.Context())
	var state, owner string
	var epoch int64
	if err := conn.QueryRow(t.Context(), "SELECT state,owner,epoch FROM "+pgx.Identifier{schema, "mutation_guard"}.Sanitize()+" WHERE singleton").Scan(&state, &owner, &epoch); err != nil {
		t.Fatal(err)
	}
	return state, owner, epoch
}

func TestHostResourceRevocationSafety(t *testing.T) {
	databaseURL := isolatedGuardDatabase(t)
	t.Run("graceful owner close verifies all guards without fencing", func(t *testing.T) {
		schemaA := createGuardSchema(t, databaseURL, "active", testGuardOwner, 11)
		schemaB := createGuardSchema(t, databaseURL, "active", testGuardOwner, 11)
		bindings := ResourceBindings{Domains: []DomainResources{
			{DomainUUID: testDomain, Guards: []GuardTarget{target(databaseURL, schemaA), target(databaseURL, schemaB)}},
			{DomainUUID: testStorageDomain, AppOnly: true},
		}}
		host, ops := resourceTestHost(t, bindings)
		identity := grantResourceTest(t, host, testDomain, testDomain, 11)
		if err := host.Revoke(t.Context(), identity, RevokeGraceful); err != nil {
			t.Fatal(err)
		}
		for _, schema := range []string{schemaA, schemaB} {
			state, owner, epoch := readGuard(t, databaseURL, schema)
			if state != "closed" || owner != "" || epoch != 11 {
				t.Fatalf("guard %s not closed for exact identity: %s %s %d", schema, state, owner, epoch)
			}
		}
		if len(ops.fenced) != 0 {
			t.Fatalf("confirmed graceful owner close fenced a VM: %v", ops.fenced)
		}
		if err := host.Revoke(t.Context(), identity, RevokeGraceful); err != nil {
			t.Fatalf("closed identity revoke was not idempotent: %v", err)
		}
		if len(ops.fenced) != 0 {
			t.Fatalf("idempotent close fenced a VM: %v", ops.fenced)
		}
	})

	t.Run("takeover closes follower guards and fences old owner domain only", func(t *testing.T) {
		schemaA := createGuardSchema(t, databaseURL, "active", testGuardOwner, 12)
		schemaB := createGuardSchema(t, databaseURL, "active", testGuardOwner, 12)
		bindings := ResourceBindings{Domains: []DomainResources{
			{DomainUUID: testDomain, AppOnly: true},
			{DomainUUID: testStorageDomain, Guards: []GuardTarget{target(databaseURL, schemaA), target(databaseURL, schemaB)}},
		}}
		host, ops := resourceTestHost(t, bindings)
		identity := grantResourceTest(t, host, testStorageDomain, testDomain, 12)
		if err := host.Revoke(t.Context(), identity, RevokeTakeover); err != nil {
			t.Fatal(err)
		}
		for _, schema := range []string{schemaA, schemaB} {
			state, _, _ := readGuard(t, databaseURL, schema)
			if state != "closed" {
				t.Fatalf("follower guard %s remains %q", schema, state)
			}
		}
		if len(ops.fenced) != 1 || ops.fenced[0] != testDomain {
			t.Fatalf("takeover must fence old owner execution domain only, got %v", ops.fenced)
		}
	})

	t.Run("unreachable target forces its resource domain fence", func(t *testing.T) {
		schema := createGuardSchema(t, databaseURL, "active", testGuardOwner, 13)
		badURL, err := url.Parse(databaseURL)
		if err != nil {
			t.Fatal(err)
		}
		badURL.Host = "127.0.0.1:1"
		bindings := ResourceBindings{Domains: []DomainResources{
			{DomainUUID: testDomain, AppOnly: true},
			{DomainUUID: testStorageDomain, Guards: []GuardTarget{target(databaseURL, schema), target(badURL.String(), schema)}},
		}}
		host, ops := resourceTestHost(t, bindings)
		identity := grantResourceTest(t, host, testStorageDomain, testDomain, 13)
		if err := host.Revoke(t.Context(), identity, RevokeGraceful); err != nil {
			t.Fatalf("verified domain fence did not complete revoke: %v", err)
		}
		if len(ops.fenced) != 1 || ops.fenced[0] != testStorageDomain {
			t.Fatalf("uncertain resource closure did not fence its domain: %v", ops.fenced)
		}
	})

	t.Run("expiry closes follower guards and fences old execution domain", func(t *testing.T) {
		schemaA := createGuardSchema(t, databaseURL, "active", testGuardOwner, 15)
		schemaB := createGuardSchema(t, databaseURL, "active", testGuardOwner, 15)
		bindings := ResourceBindings{Domains: []DomainResources{
			{DomainUUID: testDomain, AppOnly: true},
			{DomainUUID: testStorageDomain, Guards: []GuardTarget{target(databaseURL, schemaA), target(databaseURL, schemaB)}},
		}}
		host, ops := resourceTestHost(t, bindings)
		identity := grantResourceTest(t, host, testStorageDomain, testDomain, 15)
		grant := host.state.Domains[testStorageDomain].Grant
		ops.now = grant.DeadlineBootNS - int64(FenceLead)
		if err := host.Expire(t.Context(), testStorageDomain, identity.Epoch, identity.RenewalSeq); err != nil {
			t.Fatal(err)
		}
		for _, schema := range []string{schemaA, schemaB} {
			state, _, _ := readGuard(t, databaseURL, schema)
			if state != "closed" {
				t.Fatalf("expired follower guard %s remains %q", schema, state)
			}
		}
		if len(ops.fenced) != 1 || ops.fenced[0] != testDomain {
			t.Fatalf("expiry must fence old execution domain, not healthy storage domain: %v", ops.fenced)
		}
	})

	t.Run("unclosed higher epoch target forces resource domain fence", func(t *testing.T) {
		schema := createGuardSchema(t, databaseURL, "active", "new-owner", 14)
		bindings := ResourceBindings{Domains: []DomainResources{
			{DomainUUID: testDomain, AppOnly: true},
			{DomainUUID: testStorageDomain, Guards: []GuardTarget{target(databaseURL, schema)}},
		}}
		host, ops := resourceTestHost(t, bindings)
		identity := grantResourceTest(t, host, testStorageDomain, testDomain, 13)
		if err := host.Revoke(t.Context(), identity, RevokeGraceful); err != nil {
			t.Fatalf("verified domain fence did not complete revoke: %v", err)
		}
		state, owner, epoch := readGuard(t, databaseURL, schema)
		if state != "active" || owner != "new-owner" || epoch != 14 {
			t.Fatalf("higher guard was mutated: %s %s %d", state, owner, epoch)
		}
		if len(ops.fenced) != 1 || ops.fenced[0] != testStorageDomain {
			t.Fatalf("unclosed physical target did not fence its domain: %v", ops.fenced)
		}
	})

	t.Run("higher epoch guard remains untouched", func(t *testing.T) {
		schema := createGuardSchema(t, databaseURL, "active", "new-owner", 14)
		err := closeGuard(t.Context(), target(databaseURL, schema), Identity{Owner: testGuardOwner, Epoch: 13})
		if err == nil {
			t.Fatal("higher-epoch guard was reported closed")
		}
		state, owner, epoch := readGuard(t, databaseURL, schema)
		if state != "active" || owner != "new-owner" || epoch != 14 {
			t.Fatalf("higher guard was mutated: %s %s %d", state, owner, epoch)
		}
	})
}
