package aggregator

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

// A one-object Lease API with the same optimistic concurrency and UID as the Kubernetes API server.
type fakePollLeaseAPI struct {
	mu      sync.Mutex
	lease   *leaseObject
	version int
	created int
}

func (api *fakePollLeaseAPI) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	api.mu.Lock()
	defer api.mu.Unlock()
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
		api.created++
		api.store(w, request, http.StatusCreated, "uid-"+strconv.Itoa(api.created))
	case http.MethodPut:
		var incoming leaseObject
		raw, _ := io.ReadAll(request.Body)
		_ = json.Unmarshal(raw, &incoming)
		if api.lease == nil || incoming.Metadata.ResourceVersion != api.lease.Metadata.ResourceVersion {
			w.WriteHeader(http.StatusConflict)
			return
		}
		request.Body = io.NopCloser(strings.NewReader(string(raw)))
		api.store(w, request, http.StatusOK, api.lease.Metadata.UID)
	}
}

func (api *fakePollLeaseAPI) store(w http.ResponseWriter, request *http.Request, status int, uid string) {
	var lease leaseObject
	_ = json.NewDecoder(request.Body).Decode(&lease)
	api.version++
	lease.Metadata.ResourceVersion, lease.Metadata.UID = strconv.Itoa(api.version), uid
	api.lease = &lease
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(lease)
}

func (api *fakePollLeaseAPI) remove() {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.lease = nil
}

type pollTerms struct {
	current, maximum atomic.Int32
	mu               sync.Mutex
	tokens           []pollLeaseToken
}

func (terms *pollTerms) lead(ctx context.Context, token pollLeaseToken) {
	now := terms.current.Add(1)
	for {
		seen := terms.maximum.Load()
		if now <= seen || terms.maximum.CompareAndSwap(seen, now) {
			break
		}
	}
	terms.mu.Lock()
	terms.tokens = append(terms.tokens, token)
	terms.mu.Unlock()
	<-ctx.Done()
	terms.current.Add(-1)
}

func (terms *pollTerms) all() []pollLeaseToken {
	terms.mu.Lock()
	defer terms.mu.Unlock()
	return append([]pollLeaseToken(nil), terms.tokens...)
}

func testPollElector(server *httptest.Server, identity string, skew time.Duration) *pollLeaseElector {
	// Lease durations are whole seconds on the wire, so one second is the shortest usable lease.
	cfg := PollLeaseConfig{Name: "aggregator-poller", Namespace: "bnpb", Identity: identity,
		Duration: time.Second, RenewEvery: 100 * time.Millisecond, StepDownAfter: 700 * time.Millisecond}
	elector := newPollLeaseElector(cfg, server.URL, server.Client(), func() (string, error) { return identity, nil },
		slog.New(slog.NewJSONHandler(io.Discard, nil)))
	elector.now = func() time.Time { return time.Now().Add(skew) }
	return elector
}

func waitForPoll(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPollLeaseTakeoverStartsANewerTerm(t *testing.T) {
	server := httptest.NewServer(&fakePollLeaseAPI{})
	t.Cleanup(server.Close)
	terms := &pollTerms{}
	first, second := testPollElector(server, "aggregator-a", 0), testPollElector(server, "aggregator-b", 0)
	firstCtx, stopFirst := context.WithCancel(context.Background())
	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	go first.Run(firstCtx, terms.lead)
	waitForPoll(t, 2*time.Second, first.Leading)
	go second.Run(secondCtx, terms.lead)
	time.Sleep(500 * time.Millisecond)
	if second.Leading() {
		t.Fatal("the standby must not poll while the lease is renewed")
	}

	stopFirst()
	waitForPoll(t, 3*time.Second, second.Leading)
	tokens := terms.all()
	if terms.maximum.Load() != 1 || len(tokens) != 2 {
		t.Fatalf("max concurrent pollers %d, terms %+v", terms.maximum.Load(), tokens)
	}
	if tokens[0].holder != "aggregator-a" || tokens[1].holder != "aggregator-b" ||
		tokens[1].generation <= tokens[0].generation || tokens[0].leaseUID == "" || tokens[0].leaseUID != tokens[1].leaseUID {
		t.Fatalf("a takeover must carry a higher generation of the same lease: %+v", tokens)
	}
}

// A clock 45 s ahead sees a renewed lease as expired and takes it; the fence relies on its term being newer.
func TestPollLeaseClockSkewTakeoverStillYieldsANewerTerm(t *testing.T) {
	server := httptest.NewServer(&fakePollLeaseAPI{})
	t.Cleanup(server.Close)
	terms := &pollTerms{}
	honest, skewed := testPollElector(server, "aggregator-a", 0), testPollElector(server, "aggregator-b", 45*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go honest.Run(ctx, terms.lead)
	waitForPoll(t, 2*time.Second, honest.Leading)
	go skewed.Run(ctx, terms.lead)
	waitForPoll(t, 2*time.Second, skewed.Leading)
	waitForPoll(t, 2*time.Second, func() bool { return !honest.Leading() })

	tokens := terms.all()
	if len(tokens) < 2 || tokens[len(tokens)-1].holder != "aggregator-b" || tokens[len(tokens)-1].generation <= tokens[0].generation {
		t.Fatalf("the skewed takeover must be a newer term: %+v", tokens)
	}
}

func TestPollLeaseRecreatedObjectGetsANewUID(t *testing.T) {
	api := &fakePollLeaseAPI{}
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	elector := testPollElector(server, "aggregator-a", 0)
	before, ok := elector.tryAcquireOrRenew(context.Background())
	if !ok || before.generation != 0 || before.leaseUID == "" {
		t.Fatalf("first acquire: %+v ok=%v", before, ok)
	}
	api.remove()
	after, ok := elector.tryAcquireOrRenew(context.Background())
	if !ok || after.leaseUID == before.leaseUID {
		t.Fatalf("a recreated lease must report a new UID: before %+v after %+v", before, after)
	}
}

func TestPollLeaseConfigOnlyForASingleStore(t *testing.T) {
	values := map[string]string{"BNPB_AGGREGATOR_LEASE_NAME": "aggregator-poller",
		"BNPB_AGGREGATOR_LEASE_NAMESPACE": "bnpb", "BNPB_AGGREGATOR_IDENTITY": "aggregator-0"}
	lookup := func(name string) (string, bool) { value, ok := values[name]; return value, ok }
	cfg, err := loadPollLeaseConfig(lookup, false)
	if err != nil || cfg.Identity != "aggregator-0" || cfg.StepDownAfter >= cfg.Duration {
		t.Fatalf("cfg %+v err %v", cfg, err)
	}
	if _, err := loadPollLeaseConfig(lookup, true); err == nil {
		t.Fatal("a sharded store must refuse the poll lease")
	}
	if cfg, err := loadPollLeaseConfig(func(string) (string, bool) { return "", false }, true); err != nil || cfg.Name != "" {
		t.Fatalf("an empty lease name keeps the old behaviour: %+v %v", cfg, err)
	}
}
