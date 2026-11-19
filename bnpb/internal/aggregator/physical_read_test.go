package aggregator

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStartupFallbackClassifiesOnlyConnectionOutages(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "network failure", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, want: true},
		{name: "postgres connection failure", err: &pgconn.PgError{Code: "08006"}, want: true},
		{name: "postgres starting up", err: &pgconn.PgError{Code: "57P03"}, want: true},
		{name: "bad credentials", err: &pgconn.PgError{Code: "28P01"}},
		{name: "context cancellation", err: context.Canceled},
		{name: "context deadline", err: context.DeadlineExceeded},
		{name: "schema failure", err: errors.New("relation schema_migrations does not exist")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isStartupConnectionUnavailable(test.err); got != test.want {
				t.Fatalf("isStartupConnectionUnavailable(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}

func TestValidatePhysicalCopyURLs(t *testing.T) {
	tests := []struct {
		name     string
		copyURLs []string
		primary  []string
		wantErr  string
	}{
		{name: "valid", copyURLs: []string{"postgres://reader:secret@copy.example/db"}},
		{name: "too many", copyURLs: []string{
			"postgres://a/db", "postgres://b/db", "postgres://c/db", "postgres://d/db",
			"postgres://e/db", "postgres://f/db", "postgres://g/db",
		}, wantErr: "at most 6"},
		{name: "invalid scheme", copyURLs: []string{"https://reader:secret@copy.example/db"}, wantErr: "invalid"},
		{name: "duplicate copy", copyURLs: []string{"postgres://copy/db", "postgres://copy/db"}, wantErr: "distinct"},
		{name: "primary duplicate", copyURLs: []string{"postgres://primary/db"}, primary: []string{"postgres://primary/db"}, wantErr: "duplicates a primary"},
		{name: "primary duplicate with distinct credentials", copyURLs: []string{"postgres://replica:secret@primary/db"}, primary: []string{"postgres://owner:secret@primary/db"}, wantErr: "duplicates a primary"},
		{name: "whitespace", copyURLs: []string{" postgres://copy/db"}, wantErr: "invalid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePhysicalCopyURLs(test.copyURLs, test.primary)
			if test.wantErr == "" && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("validate error = %v, want %q", err, test.wantErr)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatalf("validation error leaked URL credentials: %v", err)
			}
		})
	}
}

func TestPhysicalCopyPoolsAreLazyBoundedAndClosedWithStore(t *testing.T) {
	pools, err := newPhysicalCopyPools([]string{"postgres://reader:secret@127.0.0.1:1/unreachable?connect_timeout=1"}, nil, nil)
	if err != nil {
		t.Fatalf("create lazy pool: %v", err)
	}
	pool := pools[0]
	if pool.Config().MaxConns != 1 || pool.Config().MinConns != 0 || pool.Config().MinIdleConns != 0 {
		t.Fatalf("unexpected pool bounds: max=%d min=%d idle=%d", pool.Config().MaxConns, pool.Config().MinConns, pool.Config().MinIdleConns)
	}
	if pool.Stat().AcquireCount() != 0 {
		t.Fatalf("pool creation connected before first use: acquire count %d", pool.Stat().AcquireCount())
	}
	store := &Store{physicalCopies: pools}
	store.Close()
	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Fatal("closed Store left an audit-copy pool usable")
	}
}

func TestUnavailableCopyStaysConfiguredAndFailsOnUse(t *testing.T) {
	pools, err := newPhysicalCopyPools(
		[]string{"postgres://reader:secret@127.0.0.1:1/unreachable?connect_timeout=1"}, nil, slog.Default())
	if err != nil {
		t.Fatalf("unreachable copy prevented lazy pool initialization: %v", err)
	}
	defer closePools(pools)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := pools[0].Acquire(ctx); err == nil {
		t.Fatal("unreachable physical copy unexpectedly acquired a connection")
	} else if strings.Contains(err.Error(), "secret") {
		t.Fatalf("connection error leaked copy credentials: %v", err)
	}
}

func TestPhysicalReadPoolsUseLogicalPrimariesAndConfiguredCopies(t *testing.T) {
	pools, err := newPhysicalCopyPools([]string{"postgres://copy-0/db", "postgres://copy-1/db"}, nil, nil)
	if err != nil {
		t.Fatalf("create copy pools: %v", err)
	}
	defer closePools(pools)
	control := mustTestPool(t, "postgres://control/db")
	primary0 := mustTestPool(t, "postgres://primary-0/db")
	primary1 := mustTestPool(t, "postgres://primary-1/db")
	primary2 := mustTestPool(t, "postgres://primary-2/db")
	defer control.Close()
	defer primary0.Close()
	defer primary1.Close()
	defer primary2.Close()
	store := &Store{read: control, physicalCopies: pools}
	store.shards[0] = &Store{read: primary0}
	store.shards[1] = &Store{read: primary1}
	store.shards[2] = &Store{read: primary2}
	got := store.physicalReadPools()
	if len(got) != 5 {
		t.Fatalf("physical pool count = %d, want three primaries plus two copies", len(got))
	}
	want := []*pgxpool.Pool{primary0, primary1, primary2, pools[0], pools[1]}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pool %d = %p, want %p", i, got[i], want[i])
		}
	}
	local := (&Store{read: control}).physicalReadPools()
	if len(local) != 1 || local[0] != control {
		t.Fatalf("local physical pools = %v, want only parent primary", local)
	}
}

func mustTestPool(t *testing.T, address string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), address)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	return pool
}

func TestUnavailableShardStoreRetainsLazyPoolHandles(t *testing.T) {
	shardID := int16(0)
	child, err := openUnavailableShardStore(Config{
		DatabaseURL:    "postgres://admin:secret@127.0.0.1:1/shard",
		WriterURL:      "postgres://runtime:secret@127.0.0.1:1/shard",
		DBReadMaxConns: 2, DBWriteMaxConns: 3,
	}, slog.Default(), storeIdentity{role: "shard", shardID: &shardID})
	if err != nil {
		t.Fatalf("create degraded child: %v", err)
	}
	defer child.Close()
	if !child.startupUnverified.Load() || child.read == nil || child.write == nil || len(child.authorityPools) != 1 {
		t.Fatal("degraded child did not retain lazy read/write/authority handles")
	}
	if child.read.Stat().AcquireCount() != 0 || child.write.Stat().AcquireCount() != 0 ||
		child.authorityPools[0].Stat().AcquireCount() != 0 {
		t.Fatal("degraded child connected during initialization")
	}
}
