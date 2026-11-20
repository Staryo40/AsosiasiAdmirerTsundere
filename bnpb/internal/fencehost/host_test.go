package fencehost

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testDomain        = "12345678-1234-1234-1234-123456789abc"
	testStorageDomain = "22345678-1234-1234-1234-123456789abc"
)

type fakeOps struct {
	boot         string
	now          int64
	nowErr       error
	armed        []int64
	fenced       []string
	armErr       error
	fenceFail    string
	timerChecks  int
	timerFailAt  int
	timerErrorAt int
	lockMu       sync.Mutex
	locks        map[string]chan struct{}
}

func (f *fakeOps) lockState(ctx context.Context, path string) (func(), error) {
	f.lockMu.Lock()
	if f.locks == nil {
		f.locks = make(map[string]chan struct{})
	}
	lock := f.locks[path]
	if lock == nil {
		lock = make(chan struct{}, 1)
		f.locks[path] = lock
	}
	f.lockMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case lock <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-lock
			return nil, err
		}
		return func() { <-lock }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (f *fakeOps) persistState(dir string, state diskState) error {
	// Keep real file contents for restart tests; this callback does not prove directory fsync durability.
	return persistStateWithDirectorySync(dir, state, func(string) error { return nil })
}

func (f *fakeOps) bootID() (string, error)    { return f.boot, nil }
func (f *fakeOps) boottimeNS() (int64, error) { return f.now, f.nowErr }
func (f *fakeOps) arm(_ context.Context, _, _ string, _, _ uint64, deadline int64) error {
	if f.armErr != nil {
		return f.armErr
	}
	f.armed = append(f.armed, deadline)
	return nil
}
func (f *fakeOps) timerArmed(context.Context, string, uint64, uint64) (bool, error) {
	f.timerChecks++
	if f.timerFailAt == f.timerChecks {
		return false, nil
	}
	if f.timerErrorAt == f.timerChecks {
		return false, errors.New("systemd inspection unavailable")
	}
	return len(f.armed) > 0, nil
}
func (f *fakeOps) fence(_ context.Context, domain string) error {
	f.fenced = append(f.fenced, domain)
	if domain == f.fenceFail {
		return errors.New("fence result is unknown")
	}
	return nil
}

func (*fakeOps) closeGuard(ctx context.Context, target GuardTarget, identity Identity) error {
	return (nativeOps{}).closeGuard(ctx, target, identity)
}

func initializedHost(t *testing.T, ops *fakeOps) *Host {
	t.Helper()
	directory := t.TempDir()
	if err := ops.persistState(directory, diskState{BootID: ops.boot, Domains: map[string]domainState{}}); err != nil {
		t.Fatal(err)
	}
	bindings := ResourceBindings{Domains: []DomainResources{{DomainUUID: testDomain, AppOnly: true}}}
	host, err := openWithOps(directory, "secret", []string{testDomain}, ops, bindings)
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func send(host http.Handler, method, target, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer secret")
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	host.ServeHTTP(response, request)
	return response
}

func TestGrantDuplicateDelayedRenewalAndIdentity(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: int64(time.Second)}
	host := initializedHost(t, ops)
	body := `{"domain_uuid":"` + testDomain + `","epoch":7,"renewal_seq":1,"owner":"poller-a","owner_domain_uuid":"` + testDomain + `"}`
	grantResponse := send(host, http.MethodPost, "/v1/grant", body)
	if grantResponse.Code != http.StatusOK {
		t.Fatalf("grant status %d: %s", grantResponse.Code, grantResponse.Body)
	}
	var initial Grant
	if err := json.Unmarshal(grantResponse.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	if initial.DeadlineBootNS != ops.now+int64(LeaseTTL) || len(ops.armed) != 1 || ops.armed[0] != initial.DeadlineBootNS {
		t.Fatalf("grant deadline/timer mismatch: grant=%d armed=%v", initial.DeadlineBootNS, ops.armed)
	}
	ops.now += 3 * int64(time.Second)
	duplicate := send(host, http.MethodPost, "/v1/grant", body)
	var repeated Grant
	if duplicate.Code != http.StatusOK || json.Unmarshal(duplicate.Body.Bytes(), &repeated) != nil || repeated != initial {
		t.Fatalf("duplicate changed grant: status=%d grant=%+v", duplicate.Code, repeated)
	}
	if len(ops.armed) != 1 {
		t.Fatalf("duplicate armed another timer: %v", ops.armed)
	}
	renewalBody := strings.Replace(body, `"renewal_seq":1`, `"renewal_seq":2`, 1)
	renewed := send(host, http.MethodPost, "/v1/grant", renewalBody)
	var renewal Grant
	if renewed.Code != http.StatusOK || json.Unmarshal(renewed.Body.Bytes(), &renewal) != nil {
		t.Fatalf("valid renewal rejected: status=%d body=%s", renewed.Code, renewed.Body)
	}
	if renewal.RenewalSeq != 2 || renewal.DeadlineBootNS != ops.now+int64(LeaseTTL) {
		t.Fatalf("unexpected renewal: %+v", renewal)
	}
	if err := host.Expire(context.Background(), testDomain, initial.Epoch, initial.RenewalSeq); err == nil {
		t.Fatal("old timer erased the renewed high-water grant")
	}
	if got := host.state.Domains[testDomain].HighSeq; got != 2 {
		t.Fatalf("old timer changed high-water sequence to %d", got)
	}
	ops.now = renewal.DeadlineBootNS - int64(FenceLead)
	afterFenceDecision := strings.Replace(body, `"renewal_seq":1`, `"renewal_seq":3`, 1)
	if response := send(host, http.MethodPost, "/v1/grant", afterFenceDecision); response.Code != http.StatusConflict {
		t.Fatalf("renewal after fence lead was accepted: %d", response.Code)
	}
	outOfOrder := strings.Replace(body, `"renewal_seq":1`, `"renewal_seq":4`, 1)
	if response := send(host, http.MethodPost, "/v1/grant", outOfOrder); response.Code != http.StatusConflict {
		t.Fatalf("out-of-order renewal status %d", response.Code)
	}
	wrongOwner := strings.Replace(renewalBody, "poller-a", "poller-b", 1)
	if response := send(host, http.MethodPost, "/v1/grant", wrongOwner); response.Code != http.StatusConflict {
		t.Fatalf("identity collision status %d", response.Code)
	}
}
func TestGrantExpiryRevokeAndRecoveryState(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: int64(time.Second)}
	host := initializedHost(t, ops)
	request := `{"domain_uuid":"` + testDomain + `","epoch":4,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testDomain + `"}`
	if response := send(host, http.MethodPost, "/v1/grant", request); response.Code != http.StatusOK {
		t.Fatal(response.Code)
	}
	ops.now += int64(LeaseTTL)
	if response := send(host, http.MethodPost, "/v1/grant", strings.Replace(request, `"renewal_seq":1`, `"renewal_seq":2`, 1)); response.Code != http.StatusConflict {
		t.Fatalf("delayed renewal was accepted: %d", response.Code)
	}
	if err := host.Expire(context.Background(), testDomain, 4, 1); err != nil {
		t.Fatal(err)
	}
	if response := send(host, http.MethodPost, "/v1/grant", `{"domain_uuid":"`+testDomain+`","epoch":4,"renewal_seq":2,"owner":"owner","owner_domain_uuid":"`+testDomain+`"}`); response.Code != http.StatusConflict {
		t.Fatalf("closed grant renewal status %d", response.Code)
	}
	if len(ops.fenced) != 1 || ops.fenced[0] != testDomain {
		t.Fatalf("unexpected fences: %v", ops.fenced)
	}
	if response := send(host, http.MethodPost, "/v1/revoke", `{"domain_uuid":"`+testDomain+`","epoch":4,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"`+testDomain+`","mode":"graceful"}`); response.Code != http.StatusNoContent {
		t.Fatalf("exact revoke after expiry status %d", response.Code)
	}
	if response := send(host, http.MethodPost, "/v1/grant", `{"domain_uuid":"`+testDomain+`","epoch":5,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"`+testDomain+`"}`); response.Code != http.StatusOK {
		t.Fatalf("new epoch after verified fence status %d", response.Code)
	}
	if response := send(host, http.MethodPost, "/v1/revoke", `{"domain_uuid":"`+testDomain+`","epoch":4,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"`+testDomain+`","mode":"graceful"}`); response.Code != http.StatusConflict {
		t.Fatalf("stale revoke erased newer grant: %d", response.Code)
	}
	if response := send(host, http.MethodPost, "/v1/revoke", `{"domain_uuid":"`+testDomain+`","epoch":5,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"`+testDomain+`","mode":"graceful"}`); response.Code != http.StatusNoContent {
		t.Fatalf("verified revoke status %d: %s", response.Code, response.Body)
	}
	if response := send(host, http.MethodPost, "/v1/revoke", `{"domain_uuid":"`+testDomain+`","epoch":5,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"`+testDomain+`","mode":"graceful"}`); response.Code != http.StatusNoContent {
		t.Fatalf("duplicate verified revoke status %d: %s", response.Code, response.Body)
	}
	lowerEpoch := `{"domain_uuid":"` + testDomain + `","epoch":4,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testDomain + `"}`
	if response := send(host, http.MethodPost, "/v1/grant", lowerEpoch); response.Code != http.StatusConflict {
		t.Fatalf("grant below closed host epoch was accepted: %d", response.Code)
	}
	state, err := readState(filepath.Join(host.stateDir, "lease-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	record := state.Domains[testDomain]
	if !record.Closed || record.HighEpoch != 5 || record.HighSeq != 1 || record.Grant != nil {
		t.Fatalf("lower ConfigMap epoch reset durable host high-water: %+v", record)
	}
}

func TestTimerArmFailureDoesNotPersistGrant(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: 10, armErr: errors.New("systemd unavailable")}
	host := initializedHost(t, ops)
	body := `{"domain_uuid":"` + testDomain + `","epoch":3,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testDomain + `"}`
	if response := send(host, http.MethodPost, "/v1/grant", body); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("timer arm failure status %d", response.Code)
	}
	if _, exists := host.state.Domains[testDomain]; exists {
		t.Fatal("grant state persisted without an armed timer")
	}
}

func TestTimerVerificationFailureFailsClosedAfterRecord(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: 10, timerFailAt: 2}
	host := initializedHost(t, ops)
	body := `{"domain_uuid":"` + testDomain + `","epoch":3,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testDomain + `"}`
	response := send(host, http.MethodPost, "/v1/grant", body)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("lost timer status %d", response.Code)
	}
	record := host.state.Domains[testDomain]
	if !record.Closed || record.HighEpoch != 3 || record.HighSeq != 1 || len(ops.fenced) != 1 {
		t.Fatalf("lost timer was not fail-closed: record=%+v fences=%v", record, ops.fenced)
	}
}

func TestDurableRestartAndBootChangeFailClosed(t *testing.T) {
	directory := t.TempDir()
	ops := &fakeOps{boot: "boot-1", now: 100}
	if err := ops.persistState(directory, diskState{BootID: ops.boot, Domains: map[string]domainState{}}); err != nil {
		t.Fatal(err)
	}
	bindings := ResourceBindings{Domains: []DomainResources{{DomainUUID: testDomain, AppOnly: true}}}
	host, err := openWithOps(directory, "secret", []string{testDomain}, ops, bindings)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"domain_uuid":"` + testDomain + `","epoch":9,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testDomain + `"}`
	first := send(host, http.MethodPost, "/v1/grant", body)
	if first.Code != http.StatusOK {
		t.Fatal(first.Code)
	}
	restarted, err := openWithOps(directory, "secret", []string{testDomain}, ops, bindings)
	if err != nil {
		t.Fatal(err)
	}
	second := send(restarted, http.MethodPost, "/v1/grant", body)
	if second.Body.String() != first.Body.String() {
		t.Fatalf("restart changed stored grant: %q != %q", second.Body, first.Body)
	}
	ops.boot = "boot-2"
	if _, err := openWithOps(directory, "secret", []string{testDomain}, ops, bindings); err == nil || !strings.Contains(err.Error(), "recovery required") {
		t.Fatalf("changed boot ID did not fail closed: %v", err)
	}
}

func TestTimerExpiresOnlyMatchingDurableGrant(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: 200}
	host := initializedHost(t, ops)
	request := `{"domain_uuid":"` + testDomain + `","epoch":2,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testDomain + `"}`
	if response := send(host, http.MethodPost, "/v1/grant", request); response.Code != http.StatusOK {
		t.Fatal(response.Code)
	}
	if err := host.Expire(context.Background(), testDomain, 3, 1); err == nil {
		t.Fatal("stale timer identity succeeded")
	}
	if err := host.Expire(context.Background(), testDomain, 2, 1); err == nil {
		t.Fatal("early timer succeeded")
	}
	if record := host.state.Domains[testDomain]; len(ops.fenced) != 0 || record.Closed || record.Grant == nil {
		t.Fatalf("early timer changed protection: record=%+v fences=%v", record, ops.fenced)
	}
	ops.now = 200 + int64(LeaseTTL) - int64(FenceLead)
	if err := host.Expire(context.Background(), testDomain, 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(host.stateDir, "lease-state.json")); err != nil {
		t.Fatalf("timer removed durable state: %v", err)
	}
}

func TestExpiryWithUnavailableBoottimeProtectsActiveGrant(t *testing.T) {
	for _, test := range []struct {
		name            string
		missingBinding  bool
		fenceFailDomain string
		wantFences      []string
	}{
		{
			name:       "app-only resource",
			wantFences: []string{testStorageDomain},
		},
		{
			name:           "unverified guard requires resource fence",
			missingBinding: true,
			wantFences:     []string{testDomain, testStorageDomain},
		},
		{
			name:            "resource fence failure still attempts owner",
			missingBinding:  true,
			fenceFailDomain: testDomain,
			wantFences:      []string{testDomain, testStorageDomain},
		},
		{
			name:            "owner fence failure leaves grant active",
			fenceFailDomain: testStorageDomain,
			wantFences:      []string{testStorageDomain},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ops := &fakeOps{boot: "boot-1", now: 200, fenceFail: test.fenceFailDomain}
			host := initializedHost(t, ops)
			host.domains[testStorageDomain] = struct{}{}
			host.bindings[testStorageDomain] = DomainResources{DomainUUID: testStorageDomain, AppOnly: true}
			request := `{"domain_uuid":"` + testDomain + `","epoch":6,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testStorageDomain + `"}`
			if response := send(host, http.MethodPost, "/v1/grant", request); response.Code != http.StatusOK {
				t.Fatalf("grant status %d: %s", response.Code, response.Body)
			}
			if test.missingBinding {
				delete(host.bindings, testDomain)
			}
			ops.now, ops.nowErr = 0, errors.New("CLOCK_BOOTTIME unavailable")
			wantClosed := test.fenceFailDomain == ""
			if err := host.Expire(t.Context(), testDomain, 6, 1); (err == nil) != wantClosed {
				t.Fatalf("expiry returned %v, want closed=%t", err, wantClosed)
			}
			if !reflect.DeepEqual(ops.fenced, test.wantFences) {
				t.Errorf("fenced %v, want %v", ops.fenced, test.wantFences)
			}
			state, err := readState(filepath.Join(host.stateDir, "lease-state.json"))
			if err != nil {
				t.Fatal(err)
			}
			record := state.Domains[testDomain]
			if record.Closed != wantClosed || (record.Grant == nil) != wantClosed ||
				record.HighEpoch != 6 || record.HighSeq != 1 || record.Owner != "owner" || record.OwnerDomainUUID != testStorageDomain {
				t.Fatalf("unexpected persisted state: %+v", record)
			}
		})
	}
}

func TestExpiryWithMissingOrCorruptStateFencesWithoutResettingHighWater(t *testing.T) {
	for _, test := range []struct {
		name    string
		missing bool
	}{
		{name: "missing"},
		{name: "corrupt"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ops := &fakeOps{boot: "boot-1", now: 200}
			host := initializedHost(t, ops)
			request := `{"domain_uuid":"` + testDomain + `","epoch":6,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testDomain + `"}`
			if response := send(host, http.MethodPost, "/v1/grant", request); response.Code != http.StatusOK {
				t.Fatal(response.Code)
			}
			statePath := filepath.Join(host.stateDir, "lease-state.json")
			if test.missing {
				if err := os.Remove(statePath); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(statePath, []byte("{corrupt"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := host.Expire(context.Background(), testDomain, 6, 1); err == nil {
				t.Fatal("expiry accepted unavailable durable state")
			}
			if got := host.state.Domains[testDomain]; got.HighEpoch != 6 || got.HighSeq != 1 || got.Grant == nil {
				t.Fatalf("failed expiry reset in-memory high-water state: %+v", got)
			}
			if len(ops.fenced) != 1 || ops.fenced[0] != testDomain {
				t.Fatalf("unavailable state did not fence validated domain: %v", ops.fenced)
			}
			if test.missing {
				if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed expiry recreated missing state: %v", err)
				}
			} else if contents, err := os.ReadFile(statePath); err != nil || string(contents) != "{corrupt" {
				t.Fatalf("failed expiry rewrote corrupt state: contents=%q err=%v", contents, err)
			}
		})
	}
}
func TestRevokeResourceBindingFailureFencesWithoutResettingHighWater(t *testing.T) {
	for _, test := range []struct {
		name    string
		missing bool
	}{
		{name: "missing binding"},
		{name: "malformed binding"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ops := &fakeOps{boot: "boot-1", now: int64(time.Second)}
			host := initializedHost(t, ops)
			identity := Identity{DomainUUID: testDomain, Epoch: 17, RenewalSeq: 1, Owner: "owner", OwnerDomainUUID: testDomain}
			body, err := json.Marshal(identity)
			if err != nil {
				t.Fatal(err)
			}
			if response := send(host, http.MethodPost, "/v1/grant", string(body)); response.Code != http.StatusOK {
				t.Fatalf("grant status %d: %s", response.Code, response.Body)
			}
			if test.missing {
				delete(host.bindings, testDomain)
			} else {
				host.bindings[testDomain] = DomainResources{
					DomainUUID: testDomain,
					Guards:     []GuardTarget{{DatabaseURL: "not-a-postgres-url", Schema: "guard"}},
				}
			}
			if err := host.Revoke(context.Background(), identity, RevokeGraceful); err != nil {
				t.Fatalf("verified fail-closed fence did not complete revoke: %v", err)
			}
			record := host.state.Domains[testDomain]
			if !record.Closed || record.HighEpoch != identity.Epoch || record.HighSeq != identity.RenewalSeq || record.Grant != nil {
				t.Fatalf("binding failure did not preserve high-water in a closed record: %+v", record)
			}
			durable, err := readState(filepath.Join(host.stateDir, "lease-state.json"))
			if err != nil {
				t.Fatal(err)
			}
			if persisted := durable.Domains[testDomain]; !persisted.Closed || persisted.HighEpoch != identity.Epoch || persisted.HighSeq != identity.RenewalSeq {
				t.Fatalf("binding failure reset durable high-water: %+v", persisted)
			}
			if len(ops.fenced) != 1 || ops.fenced[0] != testDomain {
				t.Fatalf("binding failure did not fence validated domain: %v", ops.fenced)
			}
		})
	}
}

func TestLoadResourceBindingsRejectsMissingOrCorruptConfig(t *testing.T) {
	for _, test := range []struct {
		name    string
		missing bool
	}{
		{name: "missing"},
		{name: "corrupt"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "resource-bindings.json")
			if !test.missing {
				if err := os.WriteFile(path, []byte("{corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := LoadResourceBindings(path, []string{testDomain}); err == nil {
				t.Fatal("resource binding configuration was accepted")
			}
		})
	}
}

func TestHTTPSGrantAuthAndAllowlistBoundaries(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: 1}
	host := initializedHost(t, ops)
	request := httptest.NewRequest(http.MethodGet, "/v1/grant?domain_uuid="+testDomain, nil)
	response := httptest.NewRecorder()
	host.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status %d", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/grant?domain_uuid=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", nil)
	request.Header.Set("Authorization", "Bearer secret")
	response = httptest.NewRecorder()
	host.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("non-allowlisted status %d", response.Code)
	}
}

func TestGetRequiresArmedTimerAndFencesMissingTimer(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: 1}
	host := initializedHost(t, ops)
	body := `{"domain_uuid":"` + testDomain + `","epoch":8,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testDomain + `"}`
	if response := send(host, http.MethodPost, "/v1/grant", body); response.Code != http.StatusOK {
		t.Fatal(response.Code)
	}
	ops.armed = nil
	response := send(host, http.MethodGet, "/v1/grant?domain_uuid="+testDomain, "")
	if response.Code != http.StatusGone {
		t.Fatalf("grant without active timer status %d", response.Code)
	}
	if len(ops.fenced) != 1 {
		t.Fatalf("missing timer did not fence the domain: %v", ops.fenced)
	}
}

func TestTimerInspectionAPIErrorFencesGrant(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: 1, timerErrorAt: 3}
	host := initializedHost(t, ops)
	body := `{"domain_uuid":"` + testDomain + `","epoch":8,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testDomain + `"}`
	if response := send(host, http.MethodPost, "/v1/grant", body); response.Code != http.StatusOK {
		t.Fatal(response.Code)
	}
	response := send(host, http.MethodGet, "/v1/grant?domain_uuid="+testDomain, "")
	if response.Code != http.StatusGone {
		t.Fatalf("timer API error status %d", response.Code)
	}
	record := host.state.Domains[testDomain]
	if !record.Closed || record.HighEpoch != 8 || record.HighSeq != 1 || len(ops.fenced) != 1 {
		t.Fatalf("timer API error did not fail closed: record=%+v fences=%v", record, ops.fenced)
	}
}

func TestTimerArgumentsArmIndependentWakeSystemFence(t *testing.T) {
	// A 20-second lease gets an independent fence timer with a 10-second lead.
	deadline := int64(20 * time.Second)
	got := timerArguments("/usr/local/bin/fence-host", "/var/lib/fence-host", testDomain, 11, 3, deadline)
	want := []string{"--unit=bnpb-fence-12345678123412341234123456789abc-11-3", "--on-boot=10s", "--timer-property=WakeSystem=yes", "--timer-property=AccuracySec=1s", "--timer-property=RandomizedDelaySec=0", "--property=Restart=on-failure", "--", "/usr/local/bin/fence-host", "fence-expire", "--state-dir", "/var/lib/fence-host", "--domain", testDomain, "--epoch", "11", "--seq", "3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected host timer command: %v", got)
	}
}

type unconfirmedFenceOps struct{ *fakeOps }

func (ops *unconfirmedFenceOps) fence(context.Context, string) error {
	return errors.New("fence result is unknown")
}

func TestUnconfirmedTimerFenceDoesNotReportInactiveAuthority(t *testing.T) {
	ops := &fakeOps{boot: "boot-1", now: 1}
	host := initializedHost(t, ops)
	body := `{"domain_uuid":"` + testDomain + `","epoch":8,"renewal_seq":1,"owner":"owner","owner_domain_uuid":"` + testDomain + `"}`
	if response := send(host, http.MethodPost, "/v1/grant", body); response.Code != http.StatusOK {
		t.Fatal(response.Code)
	}
	ops.armed = nil
	host.ops = &unconfirmedFenceOps{fakeOps: ops}
	response := send(host, http.MethodGet, "/v1/grant?domain_uuid="+testDomain, "")
	record := host.state.Domains[testDomain]
	if response.Code != http.StatusServiceUnavailable || record.Closed || record.Grant == nil || record.HighEpoch != 8 {
		t.Fatalf("unconfirmed fence advertised inactive authority: status=%d state=%+v", response.Code, record)
	}
}
