package satellite

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A one-object Lease API with the same optimistic concurrency as the Kubernetes API server.
type fakeLeaseAPI struct {
	mu          sync.Mutex
	lease       *leaseObject
	version     int
	partitioned map[string]bool
}

func (api *fakeLeaseAPI) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	api.mu.Lock()
	defer api.mu.Unlock()
	caller := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	if api.partitioned[caller] {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch request.Method {
	case http.MethodGet:
		if api.lease == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(api.lease)
	case http.MethodPost:
		if api.lease != nil {
			w.WriteHeader(http.StatusConflict)
			return
		}
		api.store(w, request, http.StatusCreated)
	case http.MethodPut:
		var incoming leaseObject
		raw, _ := io.ReadAll(request.Body)
		_ = json.Unmarshal(raw, &incoming)
		if api.lease == nil || incoming.Metadata.ResourceVersion != api.lease.Metadata.ResourceVersion {
			w.WriteHeader(http.StatusConflict)
			return
		}
		request.Body = io.NopCloser(strings.NewReader(string(raw)))
		api.store(w, request, http.StatusOK)
	}
}

func (api *fakeLeaseAPI) store(w http.ResponseWriter, request *http.Request, status int) {
	var lease leaseObject
	_ = json.NewDecoder(request.Body).Decode(&lease)
	api.version++
	lease.Metadata.ResourceVersion = strconv.Itoa(api.version)
	api.lease = &lease
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(lease)
}

func (api *fakeLeaseAPI) partition(identity string, cut bool) {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.partitioned[identity] = cut
}

type leadershipLog struct {
	current, maximum atomic.Int32
	mu               sync.Mutex
	holders          []string
}

func (log *leadershipLog) lead(identity string) func(context.Context) {
	return func(ctx context.Context) {
		now := log.current.Add(1)
		for {
			seen := log.maximum.Load()
			if now <= seen || log.maximum.CompareAndSwap(seen, now) {
				break
			}
		}
		log.mu.Lock()
		log.holders = append(log.holders, identity)
		log.mu.Unlock()
		<-ctx.Done()
		log.current.Add(-1)
	}
}

func (log *leadershipLog) lastHolder() string {
	log.mu.Lock()
	defer log.mu.Unlock()
	if len(log.holders) == 0 {
		return ""
	}
	return log.holders[len(log.holders)-1]
}

func testElector(t *testing.T, server *httptest.Server, identity string) *leaseElector {
	t.Helper()
	// Lease durations are whole seconds on the wire, so one second is the shortest usable lease.
	cfg := LeaseConfig{Name: "satellite-feed", Namespace: "bnpb", Identity: identity,
		Duration: time.Second, RenewEvery: 100 * time.Millisecond, StepDownAfter: 700 * time.Millisecond}
	return newLeaseElector(cfg, server.URL, server.Client(), func() (string, error) { return identity, nil },
		slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestOnlyOneReplicaLeadsAndTheOtherTakesOver(t *testing.T) {
	api := &fakeLeaseAPI{partitioned: map[string]bool{}}
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	log := &leadershipLog{}
	first, second := testElector(t, server, "replica-a"), testElector(t, server, "replica-b")
	firstCtx, stopFirst := context.WithCancel(context.Background())
	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	go first.Run(firstCtx, log.lead("replica-a"))
	waitUntil(t, 2*time.Second, first.Leading)
	go second.Run(secondCtx, log.lead("replica-b"))
	time.Sleep(500 * time.Millisecond)
	if second.Leading() {
		t.Fatal("the standby must not lead while the lease is renewed")
	}

	stopFirst()
	waitUntil(t, 3*time.Second, second.Leading)
	if log.maximum.Load() != 1 || log.lastHolder() != "replica-b" {
		t.Fatalf("max concurrent leaders %d, last holder %s", log.maximum.Load(), log.lastHolder())
	}
}

func TestAPartitionedLeaderStepsDownBeforeTheStandbyTakesOver(t *testing.T) {
	api := &fakeLeaseAPI{partitioned: map[string]bool{}}
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	log := &leadershipLog{}
	first, second := testElector(t, server, "replica-a"), testElector(t, server, "replica-b")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go first.Run(ctx, log.lead("replica-a"))
	waitUntil(t, 2*time.Second, first.Leading)
	go second.Run(ctx, log.lead("replica-b"))

	api.partition("replica-a", true)
	waitUntil(t, 3*time.Second, second.Leading)
	if first.Leading() || log.maximum.Load() != 1 {
		t.Fatalf("leaders overlapped: first=%v max=%d", first.Leading(), log.maximum.Load())
	}
	api.partition("replica-a", false)
	time.Sleep(500 * time.Millisecond)
	if first.Leading() || !second.Leading() {
		t.Fatal("a healed replica must not take the lease back while it is renewed")
	}
}

func TestLeaseExpiry(t *testing.T) {
	now := time.Date(2026, 11, 21, 5, 0, 0, 0, time.UTC)
	holder, seconds, renewed := "replica-a", int32(15), now.Add(-10*time.Second).Format(microTime)
	spec := leaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &seconds, RenewTime: &renewed}
	if leaseExpired(spec, now) {
		t.Fatal("a lease renewed 10s ago with 15s duration is still held")
	}
	if !leaseExpired(spec, now.Add(6*time.Second)) {
		t.Fatal("the lease must expire after its duration")
	}
	if !leaseExpired(leaseSpec{}, now) {
		t.Fatal("an empty lease is free")
	}
}
