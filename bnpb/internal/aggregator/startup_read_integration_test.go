//go:build integration

package aggregator

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestStartupReadIntegrationRecoversUnavailableShardWithoutRestart(t *testing.T) {
	cfg := routedM2Config(t)
	setup, err := OpenStore(t.Context(), cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	grantM2WriterPrivileges(t, cfg, setup)
	ownerCtx := activateFixtureOwner(t, cfg, setup, fixtureMutationOwner, fixtureMutationEpoch)
	stamp := time.Date(2026, 11, 18, 4, 0, 0, 0, time.UTC)
	ids := [logicalShardCount]string{"startup-shard-0", "startup-shard-1", "startup-shard-2"}
	for shardID, shard := range setup.shards {
		insertGlobalReadFixture(t, ownerCtx, shard, ids[shardID], globalReadReference(t, shardID, ids[shardID]), stamp.Add(time.Duration(shardID)*time.Minute))
	}
	setup.Close()

	const unavailableShard = 1
	proxy := newStartupReadProxy(t, cfg.ShardURLs[unavailableShard])
	cfg.ShardURLs[unavailableShard] = startupProxyURL(t, cfg.ShardURLs[unavailableShard], proxy.address())
	cfg.ShardWriterURLs[unavailableShard] = startupProxyURL(t, cfg.ShardWriterURLs[unavailableShard], proxy.address())
	store := openStartupReadStore(t, cfg)

	partial, err := store.listHazards(t.Context(), hazardQuery{limit: 10, filters: hazardFilters{}}, cfg)
	if err != nil {
		t.Fatalf("read reachable shards after startup: %v", err)
	}
	if !partial.Partial || !reflect.DeepEqual(partial.UnavailableUnits, []string{"shard-1"}) {
		t.Fatalf("startup read partial=%v unavailable=%v, want true/[shard-1]", partial.Partial, partial.UnavailableUnits)
	}
	if got, want := startupHazardIDs(partial.Items), []string{ids[2], ids[0]}; !reflect.DeepEqual(got, want) {
		t.Fatalf("initial reachable rows %v, want %v", got, want)
	}

	// Recovery verifies the configured schema and identity through this same
	// child endpoint; it does not require reopening the parent Store.
	proxy.enable()
	if err := waitStartupReadRecovery(t, store, cfg, ids[:]); err != nil {
		t.Fatal(err)
	}

	// Data recovery does not grant mutation authority. A caller without a
	// fenced owner context must remain unable to write after successful reads.
	mutationCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := store.Ingest(mutationCtx, IngestChunk{Endpoint: SeismicEndpoint, Records: nil, ReceivedAt: stamp}); err == nil {
		t.Fatal("mutation succeeded without fixture authority after startup read recovery")
	}
}

func TestStartupReadIntegrationRejectsRecoveredWrongIdentity(t *testing.T) {
	cfg := routedM2Config(t)
	setup, err := OpenStore(t.Context(), cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	grantM2WriterPrivileges(t, cfg, setup)
	ownerCtx := activateFixtureOwner(t, cfg, setup, fixtureMutationOwner, fixtureMutationEpoch)
	stamp := time.Date(2026, 11, 18, 5, 0, 0, 0, time.UTC)
	ids := [logicalShardCount]string{"identity-shard-0", "identity-shard-1", "identity-shard-2"}
	for shardID, shard := range setup.shards {
		insertGlobalReadFixture(t, ownerCtx, shard, ids[shardID], globalReadReference(t, shardID, ids[shardID]), stamp.Add(time.Duration(shardID)*time.Minute))
	}
	setup.Close()

	// Prepare a migrated, separately isolated schema whose persisted identity
	// is shard 0, then route it behind the expected shard-1 endpoint.
	wrongCfg := m2IsolatedShardConfig(t, cfg)
	zero := int16(0)
	wrongStore, err := openStore(t.Context(), wrongCfg, quietLogger(), storeIdentity{role: "shard", shardID: &zero})
	if err != nil {
		t.Fatal(err)
	}
	grantStartupReadWriter(t, wrongCfg)
	wrongID := "wrong-identity-row"
	activateFixtureMutationGuard(t, wrongCfg.DatabaseURL, fixtureMutationOwner, fixtureMutationEpoch)
	wrongOwnerCtx := fixtureMutationContext(t, t.Context(), fixtureMutationOwner, fixtureMutationEpoch, wrongStore)
	insertGlobalReadFixture(t, wrongOwnerCtx, wrongStore, wrongID, globalReadReference(t, 0, wrongID), stamp.Add(time.Minute))
	wrongStore.Close()

	proxy := newStartupReadProxy(t, wrongCfg.DatabaseURL)
	cfg.ShardURLs[1] = startupProxyURL(t, wrongCfg.DatabaseURL, proxy.address())
	cfg.ShardWriterURLs[1] = startupProxyURL(t, wrongCfg.WriterURL, proxy.address())
	store := openStartupReadStore(t, cfg)
	proxy.enable()

	deadline, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	var last error
	for {
		result, readErr := store.listHazards(deadline, hazardQuery{limit: 10, filters: hazardFilters{}}, cfg)
		if readErr == nil && result.Partial && reflect.DeepEqual(result.UnavailableUnits, []string{"shard-1"}) {
			if strings.Contains(strings.Join(startupHazardIDs(result.Items), ","), wrongID) {
				t.Fatalf("wrong-identity row emitted in partial read: %v", startupHazardIDs(result.Items))
			}
			if got, want := startupHazardIDs(result.Items), []string{ids[2], ids[0]}; !reflect.DeepEqual(got, want) {
				t.Fatalf("rows from reachable stores %v, want %v", got, want)
			}
			break
		}
		if readErr != nil {
			last = readErr
		} else {
			last = fmt.Errorf("wrong-identity child became available: partial=%v unavailable=%v rows=%v", result.Partial, result.UnavailableUnits, startupHazardIDs(result.Items))
		}
		if deadline.Err() != nil {
			t.Fatalf("recovered wrong identity was not kept unavailable (last result: %v): %v", last, deadline.Err())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func openStartupReadStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	store, err := OpenStore(ctx, cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}

func waitStartupReadRecovery(t *testing.T, store *Store, cfg Config, wantIDs []string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	var last string
	for {
		var all []string
		query := hazardQuery{limit: 2, filters: hazardFilters{}}
		for {
			page, err := store.listHazards(ctx, query, cfg)
			if err != nil {
				last = err.Error()
				break
			}
			if page.Partial || len(page.UnavailableUnits) != 0 {
				last = fmt.Sprintf("partial=%v unavailable=%v", page.Partial, page.UnavailableUnits)
				break
			}
			all = append(all, startupHazardIDs(page.Items)...)
			if page.NextCursor == nil {
				got := startupHazardIDsFromIDs(all)
				if reflect.DeepEqual(got, wantIDs) {
					return nil
				}
				last = fmt.Sprintf("walked rows %v, want %v", got, wantIDs)
				break
			}
			token := *page.NextCursor
			cursor, err := parseHazardCursor(token, query.filters)
			if err != nil {
				return fmt.Errorf("parse recovery cursor: %w", err)
			}
			query.cursor, query.cursorToken = cursor, token
		}
		if ctx.Err() != nil {
			return fmt.Errorf("full traversal did not recover (last result: %s): %w", last, ctx.Err())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func startupHazardIDs(items []HazardEvent) []string {
	ids := make([]string, len(items))
	for index, item := range items {
		ids[index] = item.HazardID
	}
	return ids
}

func startupHazardIDsFromIDs(ids []string) []string {
	result := append([]string(nil), ids...)
	sort.Strings(result)
	return result
}

func grantStartupReadWriter(t *testing.T, cfg Config) {
	t.Helper()
	admin, err := pgx.Connect(t.Context(), cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(t.Context())
	var schemaName string
	if err := admin.QueryRow(t.Context(), "SELECT current_schema()").Scan(&schemaName); err != nil {
		t.Fatal(err)
	}
	writerURL, err := url.Parse(cfg.WriterURL)
	if err != nil || writerURL.User == nil {
		t.Fatalf("invalid isolated writer URL: %v", err)
	}
	role := pgx.Identifier{writerURL.User.Username()}.Sanitize()
	schema := pgx.Identifier{schemaName}.Sanitize()
	for _, statement := range []string{
		"GRANT USAGE ON SCHEMA " + schema + " TO " + role,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA " + schema + " TO " + role,
		"REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON mutation_guard FROM " + role,
		"GRANT SELECT ON mutation_guard TO " + role,
		"GRANT EXECUTE ON FUNCTION " + schema + ".begin_mutation_guard(text,bigint,bigint,text) TO " + role,
		"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA " + schema + " TO " + role,
	} {
		if _, err := admin.Exec(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
}

type startupReadProxy struct {
	listener net.Listener
	upstream string
	active   atomic.Bool
	accept   sync.WaitGroup
	handlers sync.WaitGroup
	mu       sync.Mutex
	open     map[net.Conn]struct{}
	closed   atomic.Bool
}

func newStartupReadProxy(t *testing.T, databaseURL string) *startupReadProxy {
	t.Helper()
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &startupReadProxy{listener: listener, upstream: parsed.Host, open: make(map[net.Conn]struct{})}
	proxy.accept.Add(1)
	go proxy.serve()
	t.Cleanup(proxy.close)
	return proxy
}

func (proxy *startupReadProxy) address() string { return proxy.listener.Addr().String() }
func (proxy *startupReadProxy) enable()         { proxy.active.Store(true) }

func (proxy *startupReadProxy) serve() {
	defer proxy.accept.Done()
	for {
		connection, err := proxy.listener.Accept()
		if err != nil {
			return
		}
		proxy.handlers.Add(1)
		go proxy.handle(connection)
	}
}

func (proxy *startupReadProxy) handle(client net.Conn) {
	defer proxy.handlers.Done()
	defer client.Close()
	if !proxy.active.Load() {
		return
	}
	upstream, err := (&net.Dialer{Timeout: time.Second}).Dial("tcp", proxy.upstream)
	if err != nil {
		return
	}
	proxy.track(client, true)
	proxy.track(upstream, true)
	defer proxy.track(client, false)
	defer proxy.track(upstream, false)
	defer upstream.Close()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, upstream); done <- struct{}{} }()
	<-done
	_ = client.Close()
	_ = upstream.Close()
	<-done
}

func (proxy *startupReadProxy) track(connection net.Conn, add bool) {
	proxy.mu.Lock()
	if add && !proxy.closed.Load() {
		proxy.open[connection] = struct{}{}
		proxy.mu.Unlock()
		return
	}
	if add {
		proxy.mu.Unlock()
		_ = connection.Close()
		return
	}
	delete(proxy.open, connection)
	proxy.mu.Unlock()
}

func (proxy *startupReadProxy) close() {
	proxy.closed.Store(true)
	_ = proxy.listener.Close()
	proxy.accept.Wait()
	proxy.mu.Lock()
	for connection := range proxy.open {
		_ = connection.Close()
	}
	proxy.mu.Unlock()
	proxy.handlers.Wait()
}

func startupProxyURL(t *testing.T, address, proxyAddress string) string {
	t.Helper()
	parsed, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Host = proxyAddress
	return parsed.String()
}
