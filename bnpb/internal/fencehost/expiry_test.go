package fencehost

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	firstOwnerDomain  = "32345678-1234-1234-1234-123456789abc"
	secondOwnerDomain = "42345678-1234-1234-1234-123456789abc"
)

type blockedGuardOps struct {
	*fakeOps
	attempt chan struct{}
	entered chan struct{}
	release chan struct{}
	fenceMu sync.Mutex
}

func (ops *blockedGuardOps) lockState(ctx context.Context, path string) (func(), error) {
	if strings.HasSuffix(path, "domain-"+testDomain+".lock") {
		select {
		case ops.attempt <- struct{}{}:
		default:
		}
	}
	return ops.fakeOps.lockState(ctx, path)
}

func (ops *blockedGuardOps) closeGuard(ctx context.Context, _ GuardTarget, identity Identity) error {
	if identity.DomainUUID != testDomain {
		return nil
	}
	close(ops.entered)
	select {
	case <-ops.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ops *blockedGuardOps) fence(ctx context.Context, domain string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ops.fenceMu.Lock()
	defer ops.fenceMu.Unlock()
	return ops.fakeOps.fence(ctx, domain)
}

func grantBody(t *testing.T, domain, ownerDomain string, epoch, seq uint64) string {
	t.Helper()
	body, err := json.Marshal(Identity{DomainUUID: domain, Epoch: epoch, RenewalSeq: seq, Owner: "owner", OwnerDomainUUID: ownerDomain})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func requireGrant(t *testing.T, host *Host, body string) {
	t.Helper()
	if response := send(host, http.MethodPost, "/v1/grant", body); response.Code != http.StatusOK {
		t.Fatalf("grant status %d: %s", response.Code, response.Body)
	}
}

func startBlockedExpiry(t *testing.T) (*Host, *blockedGuardOps, context.CancelFunc, <-chan error) {
	t.Helper()
	ops := &blockedGuardOps{
		fakeOps: &fakeOps{boot: "boot-1", now: 100},
		attempt: make(chan struct{}, 1),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	host := initializedHost(t, ops.fakeOps)
	host.ops = ops
	for _, domain := range []string{testStorageDomain, firstOwnerDomain, secondOwnerDomain} {
		host.domains[domain] = struct{}{}
		host.bindings[domain] = DomainResources{DomainUUID: domain, AppOnly: true}
	}
	host.bindings[testDomain] = DomainResources{DomainUUID: testDomain, Guards: []GuardTarget{{Schema: "guard"}}}
	host.bindings[testStorageDomain] = DomainResources{DomainUUID: testStorageDomain, Guards: []GuardTarget{{Schema: "guard"}}}
	requireGrant(t, host, grantBody(t, testDomain, firstOwnerDomain, 9, 1))
	requireGrant(t, host, grantBody(t, testStorageDomain, secondOwnerDomain, 11, 1))
	ops.now += int64(LeaseTTL - FenceLead)
	ctx, cancel := context.WithCancel(t.Context())
	worker := &Host{stateDir: host.stateDir, ops: ops, bindings: host.bindings}
	done := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		defer close(done)
		finished <- worker.Expire(ctx, testDomain, 9, 1)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-ops.release:
		default:
			close(ops.release)
		}
		<-done
	})
	select {
	case <-ops.entered:
	case err := <-finished:
		t.Fatalf("expiry returned before reaching the guard: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("expiry did not reach the guard")
	}
	return host, ops, cancel, finished
}

func requireExpiryFinished(t *testing.T, finished <-chan error) {
	t.Helper()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("expiry failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("expiry did not finish")
	}
}

func requireDurableClosed(t *testing.T, host *Host, domain string, epoch uint64) {
	t.Helper()
	state, err := readState(filepath.Join(host.stateDir, "lease-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if record := state.Domains[domain]; !record.Closed || record.Grant != nil || record.HighEpoch != epoch || record.HighSeq != 1 {
		t.Fatalf("domain %s lacks the closed high-water record: %+v", domain, record)
	}
}

func TestExpirySlowGuardDoesNotFenceSiblingOrLoseHighWater(t *testing.T) {
	host, ops, _, firstFinished := startBlockedExpiry(t)
	if err := host.Expire(t.Context(), testStorageDomain, 11, 1); err != nil {
		t.Fatalf("sibling expiry failed while another guard was blocked: %v", err)
	}
	requireDurableClosed(t, host, testStorageDomain, 11)
	close(ops.release)
	requireExpiryFinished(t, firstFinished)
	requireDurableClosed(t, host, testDomain, 9)
	requireDurableClosed(t, host, testStorageDomain, 11)
	if want := []string{secondOwnerDomain, firstOwnerDomain}; !reflect.DeepEqual(ops.fenced, want) {
		t.Fatalf("resource fenced despite verified guard closure: got %v, want only owners %v", ops.fenced, want)
	}
}

func TestExpiryClosingCannotBeRenewedAndSurvivesCancellation(t *testing.T) {
	host, ops, cancel, finished := startBlockedExpiry(t)
	state, err := readState(filepath.Join(host.stateDir, "lease-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if record := state.Domains[testDomain]; record.Closing != RevokeTakeover || record.HighEpoch != 9 || record.HighSeq != 1 {
		t.Fatalf("guard closure began without an exact durable closing marker: %+v", record)
	}
	cancel()
	select {
	case err := <-finished:
		t.Fatalf("cancellation abandoned the committed expiry before protection completed: %v", err)
	default:
	}

	renewal := grantBody(t, testDomain, firstOwnerDomain, 9, 2)
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	cancelRequest()
	if err := host.Expire(requestCtx, testDomain, 8, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled stale timer was not rejected before protection: %v", err)
	}

	<-ops.attempt // Discard the expiry's own acquisition.
	renewalCtx, cancelRenewal := context.WithCancel(t.Context())
	request := httptest.NewRequestWithContext(renewalCtx, http.MethodPost, "/v1/grant", strings.NewReader(renewal))
	request.Header.Set("Authorization", "Bearer secret")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	renewalFinished := make(chan struct{})
	go func() {
		defer close(renewalFinished)
		host.ServeHTTP(response, request)
	}()
	t.Cleanup(func() {
		cancelRenewal()
		<-renewalFinished
	})
	select {
	case <-ops.attempt:
	case <-time.After(time.Second):
		t.Fatal("renewal did not reach the domain lock during closure")
	}
	close(ops.release)
	requireExpiryFinished(t, finished)
	select {
	case <-renewalFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("renewal did not finish after closure")
	}
	if response.Code != http.StatusConflict {
		t.Fatalf("closing epoch was reopened: %d %s", response.Code, response.Body)
	}
	inactive := send(host, http.MethodGet, "/v1/grant?domain_uuid="+testDomain, "")
	var tombstone inactiveGrantResponse
	if inactive.Code != http.StatusNotFound || json.Unmarshal(inactive.Body.Bytes(), &tombstone) != nil || tombstone.Epoch != 9 || tombstone.RenewalSeq != 1 {
		t.Fatalf("closed grant lost its identity: %d %s", inactive.Code, inactive.Body)
	}
	requireGrant(t, host, grantBody(t, testDomain, firstOwnerDomain, 10, 1))
	if err := host.Expire(t.Context(), testDomain, 9, 1); err == nil {
		t.Fatal("stale timer matched the newer epoch")
	}
	if !reflect.DeepEqual(ops.fenced, []string{firstOwnerDomain}) {
		t.Fatalf("stale timer fenced a healthy epoch: %v", ops.fenced)
	}
	if response := send(host, http.MethodGet, "/v1/grant?domain_uuid="+testDomain, ""); response.Code != http.StatusOK {
		t.Fatalf("new grant not current: %d %s", response.Code, response.Body)
	}
}

func TestExpiryRetryClosesExactGrantAfterFenceFailure(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: 100, fenceFail: testDomain}
	host := initializedHost(t, ops)
	requireGrant(t, host, grantBody(t, testDomain, testDomain, 12, 1))
	ops.now += int64(LeaseTTL - FenceLead)
	if err := host.Expire(t.Context(), testDomain, 12, 1); err == nil {
		t.Fatal("expiry succeeded without a verified owner fence")
	}
	if response := send(host, http.MethodGet, "/v1/grant?domain_uuid="+testDomain, ""); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfirmed closure was advertised as active or verified closed: %d %s", response.Code, response.Body)
	}
	if response := send(host, http.MethodPost, "/v1/grant", grantBody(t, testDomain, testDomain, 13, 1)); response.Code != http.StatusConflict {
		t.Fatalf("new epoch was admitted while the old owner was not fenced: %d %s", response.Code, response.Body)
	}
	ops.fenceFail = ""
	if err := host.Expire(t.Context(), testDomain, 12, 1); err != nil {
		t.Fatalf("exact retry could not finish the closing grant: %v", err)
	}
	requireDurableClosed(t, host, testDomain, 12)
	if len(ops.fenced) != 2 {
		t.Fatalf("physical owner fence was not retried: %v", ops.fenced)
	}
}

func TestStaleTimerContendingWithRenewedGrantDoesNotFence(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: 100}
	host := initializedHost(t, ops)
	requireGrant(t, host, grantBody(t, testDomain, testDomain, 12, 1))
	ops.now += int64(time.Second)
	requireGrant(t, host, grantBody(t, testDomain, testDomain, 12, 2))

	// Another operation on the renewed grant owns this domain lock.
	unlock, err := host.lockDomain(t.Context(), testDomain)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	err = host.Expire(ctx, testDomain, 12, 1)
	cancel()
	unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected a lock timeout, got %v", err)
	}
	if err := host.Expire(t.Context(), testDomain, 12, 1); !errors.Is(err, errStaleExpiry) {
		t.Fatalf("stale timer retry did not recognize the renewed sequence: %v", err)
	}
	if len(ops.fenced) != 0 {
		t.Fatalf("stale timer fenced the renewed grant: %v", ops.fenced)
	}
	if response := send(host, http.MethodGet, "/v1/grant?domain_uuid="+testDomain, ""); response.Code != http.StatusOK {
		t.Fatalf("renewed grant not current: %d %s", response.Code, response.Body)
	}
}
