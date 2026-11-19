package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type memoryAuthorityKubernetes struct {
	mu       sync.Mutex
	record   authorityRecord
	version  string
	revision uint64
	readErr  error
	casErr   error
	writes   int
	reads    int
}

func (client *memoryAuthorityKubernetes) Read(context.Context) (authorityRecord, string, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.reads++
	return client.record, client.version, client.readErr
}

func (client *memoryAuthorityKubernetes) readCalls() int {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.reads
}

func (client *memoryAuthorityKubernetes) CompareAndSwap(_ context.Context, version string, record authorityRecord) (string, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.casErr != nil {
		return "", client.casErr
	}
	if version != client.version {
		return "", errAuthorityConflict
	}
	client.record = record
	client.writes++
	client.revision++
	nextVersion := "memory-rv/" + strconv.FormatUint(client.revision, 10)
	if nextVersion == client.version {
		client.revision++
		nextVersion = "memory-rv/" + strconv.FormatUint(client.revision, 10)
	}
	client.version = nextVersion
	return client.version, nil
}

func (client *memoryAuthorityKubernetes) snapshot() (authorityRecord, string, int) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.record, client.version, client.writes
}

type memoryHostFence struct {
	mu         sync.Mutex
	grants     map[string]authorityGrant
	currentErr error
	current    int
	revokes    int
}

func (host *memoryHostFence) Grant(context.Context, hostFenceIdentity) (authorityGrant, error) {
	return authorityGrant{}, errors.New("unexpected Grant call")
}
func (host *memoryHostFence) Current(_ context.Context, domain string) (authorityGrant, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.current++
	grant, ok := host.grants[domain]
	if host.currentErr != nil {
		if errors.Is(host.currentErr, errHostGrantInactive) && ok {
			return grant, host.currentErr
		}
		return authorityGrant{}, host.currentErr
	}
	if !ok {
		return authorityGrant{}, errHostGrantInactive
	}
	return grant, nil
}
func (host *memoryHostFence) Revoke(context.Context, hostFenceIdentity, hostRevokeMode) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.revokes++
	return errors.New("unexpected Revoke call")
}
func (host *memoryHostFence) currentCalls() int {
	host.mu.Lock()
	defer host.mu.Unlock()
	return host.current
}
func (host *memoryHostFence) revokeCalls() int {
	host.mu.Lock()
	defer host.mu.Unlock()
	return host.revokes
}

const authorityTestDomain = "00000000-0000-4000-8000-000000000001"

func liveAuthorityFixture(owner string, epoch int64) (*memoryAuthorityKubernetes, *memoryHostFence) {
	const domain = authorityTestDomain
	grant := authorityGrant{DomainUUID: domain, HostBootID: "boot-1", Epoch: epoch, RenewalSeq: 2, DeadlineBootTimeNS: 99}
	client := &memoryAuthorityKubernetes{
		version: "opaque:initial",
		record:  authorityRecord{Epoch: epoch, Owner: owner, OwnerDomainUUID: domain, State: authorityActive, RenewalSeq: 2, GrantSet: []authorityGrant{grant}},
	}
	host := &memoryHostFence{grants: map[string]authorityGrant{domain: grant}}
	return client, host
}
func TestPollAuthorityRequiresOwnerDomainInConfiguredDomains(t *testing.T) {
	const (
		ownerDomain = "00000000-0000-4000-8000-000000000001"
		otherDomain = "00000000-0000-4000-8000-000000000002"
	)
	_, err := NewPollAuthority(context.Background(), PollAuthorityConfig{
		Owner: "owner", OwnerDomain: ownerDomain, Domains: []string{otherDomain},
		Stores: make([]*Store, 4), AuthorityPools: make([]*pgxpool.Pool, 4),
		Kubernetes: &memoryAuthorityKubernetes{}, Host: &memoryHostFence{},
		RenewalInterval: time.Second,
	})
	if err == nil {
		t.Fatal("NewPollAuthority accepted owner domain outside configured domains")
	}
}

func TestAuthorityRecordRequiresOwnerDomainWhenNotClosed(t *testing.T) {
	record := authorityRecord{
		Epoch: 1, Owner: "owner", State: authorityPending, RenewalSeq: 1,
	}
	if err := validateAuthorityRecord(record); err == nil {
		t.Fatal("pending authority without owner_domain_uuid was accepted; inactive reset could omit the configured owner domain")
	}
}

func TestKubernetesCASUsesOpaqueResourceVersion(t *testing.T) {
	const rv = "opaque:0042/not-numeric"
	var receivedVersion string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(configMapEnvelope{Metadata: struct {
				Name            string `json:"name"`
				Namespace       string `json:"namespace"`
				ResourceVersion string `json:"resourceVersion,omitempty"`
			}{Name: "authority", Namespace: "system", ResourceVersion: rv}, Data: map[string]string{authorityConfigMapKey: `{"epoch":0,"owner":"","state":"closed","renewal_seq":0,"grant_set":[]}`}})
			return
		}
		var envelope configMapEnvelope
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Errorf("decode CAS body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		receivedVersion = envelope.Metadata.ResourceVersion
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &kubernetesAuthorityClient{baseURL: server.URL, namespace: "system", name: "authority", tokenFile: tokenFile, http: server.Client()}
	record, version, err := client.Read(context.Background())
	if err != nil || version != rv || record.Epoch != 0 {
		t.Fatalf("Read = (%+v, %q, %v)", record, version, err)
	}
	_, err = client.CompareAndSwap(context.Background(), version, record)
	if !errors.Is(err, errAuthorityConflict) || receivedVersion != rv {
		t.Fatalf("CAS error=%v resourceVersion=%q", err, receivedVersion)
	}
}

func TestMemoryAuthorityCASAdvancesOpaqueResourceVersion(t *testing.T) {
	client := &memoryAuthorityKubernetes{version: "opaque:initial"}
	_, initialVersion, _ := client.snapshot()
	firstVersion, err := client.CompareAndSwap(context.Background(), initialVersion, authorityRecord{Epoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CompareAndSwap(context.Background(), initialVersion, authorityRecord{Epoch: 2}); !errors.Is(err, errAuthorityConflict) {
		t.Fatalf("stale CAS error = %v, want conflict", err)
	}
	secondVersion, err := client.CompareAndSwap(context.Background(), firstVersion, authorityRecord{Epoch: 2})
	if err != nil {
		t.Fatal(err)
	}
	if firstVersion == initialVersion || secondVersion == firstVersion {
		t.Fatalf("resource versions did not advance uniquely: initial=%q first=%q second=%q", initialVersion, firstVersion, secondVersion)
	}
}

func TestAuthorityOutageDoesNotStartOwnerWork(t *testing.T) {
	workerStarted := atomic.Bool{}
	client := &memoryAuthorityKubernetes{readErr: errors.New("control plane unavailable")}
	authority := &PollAuthority{kubernetes: client}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := authority.run(ctx, func(context.Context) { workerStarted.Store(true) })
	if err == nil {
		t.Fatal("run unexpectedly accepted authority outage")
	}
	if workerStarted.Load() {
		t.Fatal("owner work started without authority")
	}
	if client.readCalls() == 0 {
		t.Fatal("run returned without checking Kubernetes authority")
	}
}

func TestLivePriorOwnerRemainsFollower(t *testing.T) {
	client, host := liveAuthorityFixture("previous-owner", 7)
	authority := &PollAuthority{owner: "new-owner", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}
	workerStarted := atomic.Bool{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := authority.run(ctx, func(context.Context) { workerStarted.Store(true) })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want canceled follower", err)
	}
	_, _, writes := client.snapshot()
	if writes != 0 || workerStarted.Load() || host.currentCalls() != 1 {
		t.Fatalf("CAS writes=%d worker=%v host current calls=%d, want no CAS/business work and one live-grant check", writes, workerStarted.Load(), host.currentCalls())
	}
}

func TestFreshPendingAuthorityRemainsFollower(t *testing.T) {
	for _, tt := range []struct {
		name          string
		publishGrants bool
	}{
		{name: "before grant publication"},
		{name: "after grant publication", publishGrants: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pending := authorityRecord{Epoch: 7, Owner: "candidate", OwnerDomainUUID: authorityTestDomain, State: authorityPending, RenewalSeq: 1}
			grant := authorityGrant{DomainUUID: authorityTestDomain, HostBootID: "boot-1", Epoch: pending.Epoch, RenewalSeq: pending.RenewalSeq, DeadlineBootTimeNS: 99}
			if tt.publishGrants {
				pending.GrantSet = []authorityGrant{grant}
			}
			client := &memoryAuthorityKubernetes{record: pending, version: "opaque:pending"}
			host := &memoryHostFence{grants: map[string]authorityGrant{authorityTestDomain: grant}}
			authority := &PollAuthority{owner: "contender", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}

			if _, _, err := authority.activate(t.Context()); !errors.Is(err, errAuthorityNotClosed) {
				t.Fatalf("activate error = %v, want pending candidate to keep activating", err)
			}
			_, _, writes := client.snapshot()
			if revokes := host.revokeCalls(); writes != 0 || revokes != 0 {
				t.Fatalf("CAS writes=%d host revokes=%d, want no takeover during pending activation", writes, revokes)
			}
		})
	}
}

func TestPendingAuthorityGraceTracksEpoch(t *testing.T) {
	authority := &PollAuthority{}
	start := time.Now()
	pending := authorityRecord{Epoch: 7, Owner: "candidate", OwnerDomainUUID: authorityTestDomain, State: authorityPending, RenewalSeq: 1}
	if !authority.pendingWithinGrace(&pending, start) {
		t.Fatal("first pending observation should start the grace period")
	}
	expires := start.Add(authorityActivationTimeout)
	pending.GrantSet = []authorityGrant{{DomainUUID: authorityTestDomain, HostBootID: "boot-1", Epoch: pending.Epoch, RenewalSeq: pending.RenewalSeq, DeadlineBootTimeNS: 99}}
	if !authority.pendingWithinGrace(&pending, expires.Add(-time.Nanosecond)) {
		t.Fatal("pending activation should remain protected just before the deadline")
	}
	if authority.pendingWithinGrace(&pending, expires) {
		t.Fatal("publishing grants in the same epoch must not extend the grace period")
	}
	nextPending := authorityRecord{Epoch: pending.Epoch + 1, Owner: "next-candidate", OwnerDomainUUID: authorityTestDomain, State: authorityPending, RenewalSeq: 1}
	if !authority.pendingWithinGrace(&nextPending, expires) {
		t.Fatal("a new pending epoch should start a fresh grace period")
	}
	closed := authorityRecord{Epoch: nextPending.Epoch, State: authorityClosed}
	later := expires.Add(authorityActivationTimeout)
	if authority.pendingWithinGrace(&closed, later) || !authority.pendingWithinGrace(&nextPending, later) {
		t.Fatal("a non-pending observation should clear the pending grace state")
	}
}

func TestAuthorityTransportFailureIsNotExpiredAuthority(t *testing.T) {
	t.Run("Kubernetes read outage", func(t *testing.T) {
		client := &memoryAuthorityKubernetes{version: "opaque:initial", readErr: errors.New("control plane unavailable")}
		host := &memoryHostFence{grants: map[string]authorityGrant{}}
		authority := &PollAuthority{owner: "new-owner", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}
		_, _, err := authority.activate(context.Background())
		if !errors.Is(err, errAuthorityUnavailable) {
			t.Fatalf("activate error = %v, want authority unavailable", err)
		}
		_, _, writes := client.snapshot()
		if writes != 0 || host.currentCalls() != 0 {
			t.Fatalf("CAS writes=%d host current calls=%d", writes, host.currentCalls())
		}
	})

	t.Run("host grant transport outage", func(t *testing.T) {
		client, host := liveAuthorityFixture("previous-owner", 7)
		host.mu.Lock()
		host.currentErr = errors.New("host fence unavailable")
		host.mu.Unlock()
		authority := &PollAuthority{owner: "new-owner", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}
		_, _, err := authority.activate(context.Background())
		if !errors.Is(err, errAuthorityUnavailable) || errors.Is(err, errAuthorityNotClosed) {
			t.Fatalf("activate error = %v, want unavailable rather than expired/live classification", err)
		}
		_, _, writes := client.snapshot()
		if writes != 0 || host.currentCalls() != 1 {
			t.Fatalf("CAS writes=%d host current calls=%d", writes, host.currentCalls())
		}
	})
}

func TestMutationContextValidatesLiveAuthority(t *testing.T) {
	t.Run("uninstalled token", func(t *testing.T) {
		client, host := liveAuthorityFixture("owner-1", 8)
		authority := &PollAuthority{owner: "owner-1", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}
		if _, err := authority.currentContext(context.Background()); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("uninstalled token error = %v", err)
		}
		if host.currentCalls() != 0 {
			t.Fatalf("host current calls = %d, want no check without installed token", host.currentCalls())
		}
	})

	t.Run("installed live token", func(t *testing.T) {
		client, host := liveAuthorityFixture("owner-1", 8)
		authority := &PollAuthority{owner: "owner-1", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}
		authority.token = &mutationToken{owner: "owner-1", epoch: 8}
		ctx, err := authority.currentContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		owner, epoch, ok := pollAuthorityToken(ctx)
		if !ok || owner != "owner-1" || epoch != 8 {
			t.Fatalf("token = (%q,%d,%v)", owner, epoch, ok)
		}
		if host.currentCalls() != 1 {
			t.Fatalf("host current calls = %d, want live validation", host.currentCalls())
		}
	})

	t.Run("stale authority record", func(t *testing.T) {
		client, host := liveAuthorityFixture("other-owner", 8)
		authority := &PollAuthority{owner: "owner-1", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}
		authority.token = &mutationToken{owner: "owner-1", epoch: 8}
		if _, err := authority.currentContext(context.Background()); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("stale authority error = %v", err)
		}
		if host.currentCalls() != 0 {
			t.Fatalf("host current calls = %d, want record rejection before host lookup", host.currentCalls())
		}
	})

	t.Run("host grant outage", func(t *testing.T) {
		client, host := liveAuthorityFixture("owner-1", 8)
		host.mu.Lock()
		host.currentErr = errors.New("host fence unavailable")
		host.mu.Unlock()
		authority := &PollAuthority{owner: "owner-1", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}
		authority.token = &mutationToken{owner: "owner-1", epoch: 8}
		if _, err := authority.currentContext(context.Background()); !errors.Is(err, errAuthorityUnavailable) {
			t.Fatalf("host outage error = %v, want unavailable", err)
		}
	})

	t.Run("Kubernetes read outage", func(t *testing.T) {
		client, host := liveAuthorityFixture("owner-1", 8)
		client.mu.Lock()
		client.readErr = errors.New("control plane unavailable")
		client.mu.Unlock()
		authority := &PollAuthority{owner: "owner-1", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}
		authority.token = &mutationToken{owner: "owner-1", epoch: 8}
		if _, err := authority.currentContext(context.Background()); !errors.Is(err, errAuthorityUnavailable) {
			t.Fatalf("Kubernetes outage error = %v, want unavailable", err)
		}
		if host.currentCalls() != 0 {
			t.Fatalf("host current calls = %d, want no check after Kubernetes outage", host.currentCalls())
		}
	})

	t.Run("inactive high-water metadata never grants work", func(t *testing.T) {
		client, host := liveAuthorityFixture("owner-1", 8)
		host.currentErr = errHostGrantInactive
		authority := &PollAuthority{owner: "owner-1", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}
		authority.token = &mutationToken{owner: "owner-1", epoch: 8}
		if _, err := authority.currentContext(context.Background()); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("inactive high-water metadata error = %v, want no work authority", err)
		}
		authority.mu.RLock()
		tokenStillInstalled := authority.token != nil
		authority.mu.RUnlock()
		if tokenStillInstalled {
			t.Fatal("inactive host high-water metadata left the mutation token installed")
		}
	})

	t.Run("cleared token", func(t *testing.T) {
		client, host := liveAuthorityFixture("owner-1", 8)
		authority := &PollAuthority{owner: "owner-1", ownerDomain: authorityTestDomain, domains: []string{authorityTestDomain}, kubernetes: client, host: host}
		authority.token = &mutationToken{owner: "owner-1", epoch: 8}
		authority.clearToken()
		if _, err := authority.currentContext(context.Background()); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("cleared token error = %v", err)
		}
		if host.currentCalls() != 0 {
			t.Fatalf("host current calls = %d, want no check after token cleared", host.currentCalls())
		}
	})
}

func TestHostCurrentDistinguishesInactiveFromUncertain(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusGone, http.StatusServiceUnavailable} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if status == http.StatusNotFound || status == http.StatusGone {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(status)
					_, _ = w.Write([]byte(`{"domain_uuid":"` + authorityTestDomain + `","host_boot_id":"boot-7","epoch":9,"renewal_seq":4,"deadline_boottime_ns":0}`))
					return
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			client := &hostFenceClient{baseURL: server.URL, token: "token", http: server.Client()}
			grant, err := client.Current(context.Background(), authorityTestDomain)
			switch status {
			case http.StatusNotFound, http.StatusGone:
				if !errors.Is(err, errHostGrantInactive) {
					t.Fatalf("Current error = %v, want inactive", err)
				}
				if grant != (authorityGrant{DomainUUID: authorityTestDomain, HostBootID: "boot-7", Epoch: 9, RenewalSeq: 4, DeadlineBootTimeNS: 0}) {
					t.Fatalf("inactive high-water metadata = %+v", grant)
				}
			case http.StatusServiceUnavailable:
				if err == nil || errors.Is(err, errHostGrantInactive) {
					t.Fatalf("Current error = %v, want uncertain service failure", err)
				}
				if grant != (authorityGrant{}) {
					t.Fatalf("uncertain Current returned metadata: %+v", grant)
				}
			}
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close()
	client := &hostFenceClient{baseURL: server.URL, token: "token", http: http.DefaultClient}
	if grant, err := client.Current(context.Background(), authorityTestDomain); err == nil || errors.Is(err, errHostGrantInactive) || grant != (authorityGrant{}) {
		t.Fatalf("network failure = (%+v, %v), want uncertainty without metadata", grant, err)
	}
}
