//go:build integration

package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errRecoveryHostUnavailable = errors.New("recovery fixture host unavailable")

type recoveryHostRevoke struct {
	identity  hostFenceIdentity
	requested hostRevokeMode
	effective hostRevokeMode
}

type recoveryHostFence struct {
	mu                     sync.Mutex
	grants                 map[string]authorityGrant
	identities             map[string]hostFenceIdentity
	inactive               map[string]bool
	failRead               bool
	revocations            []recoveryHostRevoke
	guardsClosedForRelease func() bool
}

func newRecoveryHostFence() *recoveryHostFence {
	return &recoveryHostFence{grants: make(map[string]authorityGrant), identities: make(map[string]hostFenceIdentity), inactive: make(map[string]bool)}
}

func (host *recoveryHostFence) Grant(_ context.Context, identity hostFenceIdentity) (authorityGrant, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	current, exists := host.grants[identity.DomainUUID]
	currentIdentity := host.identities[identity.DomainUUID]
	if exists && (current.Epoch > int64(identity.Epoch) || current.Epoch == int64(identity.Epoch) && current.RenewalSeq > int64(identity.RenewalSeq) ||
		current.Epoch == int64(identity.Epoch) && current.RenewalSeq == int64(identity.RenewalSeq) && currentIdentity != identity) {
		return authorityGrant{}, errAuthorityConflict
	}
	grant := authorityGrant{DomainUUID: identity.DomainUUID, HostBootID: "fixture-boot-" + identity.DomainUUID,
		Epoch: int64(identity.Epoch), RenewalSeq: int64(identity.RenewalSeq),
		DeadlineBootTimeNS: int64(identity.Epoch*1_000_000 + identity.RenewalSeq)}
	host.grants[identity.DomainUUID] = grant
	host.identities[identity.DomainUUID] = identity
	host.inactive[identity.DomainUUID] = false
	return grant, nil
}

func (host *recoveryHostFence) Current(_ context.Context, domain string) (authorityGrant, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.failRead {
		return authorityGrant{}, errRecoveryHostUnavailable
	}
	grant, ok := host.grants[domain]
	if !ok {
		return authorityGrant{}, errHostGrantInactive
	}
	if host.inactive[domain] {
		return grant, errHostGrantInactive
	}
	return grant, nil
}

func (host *recoveryHostFence) Revoke(_ context.Context, identity hostFenceIdentity, mode hostRevokeMode) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	currentIdentity, exists := host.identities[identity.DomainUUID]
	grant, grantExists := host.grants[identity.DomainUUID]
	if !exists || !grantExists || currentIdentity != identity ||
		grant.Epoch != int64(identity.Epoch) || grant.RenewalSeq != int64(identity.RenewalSeq) {
		return errAuthorityConflict
	}
	effective := mode
	if mode == hostRevokeTakeover && identity.DomainUUID != identity.OwnerDomainUUID {
		effective = hostRevokeGraceful
	}
	if effective == hostRevokeGraceful && (host.guardsClosedForRelease == nil || !host.guardsClosedForRelease()) {
		return errors.New("healthy domain released before SQL guards closed")
	}
	host.inactive[identity.DomainUUID] = true
	host.revocations = append(host.revocations, recoveryHostRevoke{identity: identity, requested: mode, effective: effective})
	return nil
}

func (host *recoveryHostFence) expireAll() {
	host.mu.Lock()
	defer host.mu.Unlock()
	for domain := range host.grants {
		host.inactive[domain] = true
	}
}

type recoveryAuthorityKubernetes struct {
	mu                   sync.Mutex
	record               authorityRecord
	version              string
	readsErr             error
	casCalls             int
	readArrivals         chan struct{}
	releaseReads         <-chan struct{}
	readBarrierRemaining int
}

func newRecoveryAuthorityKubernetes(record authorityRecord) *recoveryAuthorityKubernetes {
	return &recoveryAuthorityKubernetes{record: record, version: "rv/initial:not-numeric"}
}

func (client *recoveryAuthorityKubernetes) Read(context.Context) (authorityRecord, string, error) {
	client.mu.Lock()
	record := client.record
	record.GrantSet = append([]authorityGrant(nil), record.GrantSet...)
	version, readErr := client.version, client.readsErr
	arrivals, release := client.readArrivals, client.releaseReads
	barrier := arrivals != nil && client.readBarrierRemaining > 0
	if barrier {
		client.readBarrierRemaining--
	}
	client.mu.Unlock()
	if barrier {
		arrivals <- struct{}{}
		<-release
	}
	return record, version, readErr
}

func (client *recoveryAuthorityKubernetes) CompareAndSwap(_ context.Context, version string, record authorityRecord) (string, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.casCalls++
	if version != client.version {
		return "", errAuthorityConflict
	}
	record.GrantSet = append([]authorityGrant(nil), record.GrantSet...)
	client.record = record
	client.version = fmt.Sprintf("rv/%d", client.casCalls)
	return client.version, nil
}

func (client *recoveryAuthorityKubernetes) snapshot() (authorityRecord, string) {
	client.mu.Lock()
	defer client.mu.Unlock()
	record := client.record
	record.GrantSet = append([]authorityGrant(nil), record.GrantSet...)
	return record, client.version
}

func (client *recoveryAuthorityKubernetes) seed(record authorityRecord) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.record = record
	client.version = "rv/seeded"
}

func recoveryClosedRecord(epoch int64) authorityRecord {
	return authorityRecord{Epoch: epoch, State: authorityClosed}
}

func recoveryDomains() []string {
	return []string{
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
	}
}

func recoveryOwnerDomain(owner string) string {
	switch owner {
	case "recovered-owner", "pending-recovery-owner", "live-contender", "race-owner-b", "uncertainty-contender":
		return recoveryDomains()[2]
	default:
		return recoveryDomains()[0]
	}
}

func openRecoveryAuthority(t *testing.T, cfg Config, store *Store, kube authorityKubernetesClient, host HostFenceExecutor, owner string) *PollAuthority {
	t.Helper()
	stores := allFixtureStores(store)
	addresses := []string{cfg.DatabaseURL, cfg.ShardURLs[0], cfg.ShardURLs[1], cfg.ShardURLs[2]}
	pools := make([]*pgxpool.Pool, 0, len(addresses))
	for _, address := range addresses {
		pool, err := openPool(t.Context(), address, 2, quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		pools = append(pools, pool)
		t.Cleanup(pool.Close)
	}
	if memoryHost, ok := host.(*recoveryHostFence); ok {
		memoryHost.mu.Lock()
		memoryHost.guardsClosedForRelease = func() bool {
			return recoveryGuardsClosed(t.Context(), cfg)
		}
		memoryHost.mu.Unlock()
	}
	authority, err := NewPollAuthority(t.Context(), PollAuthorityConfig{
		Owner: owner, OwnerDomain: recoveryOwnerDomain(owner), Domains: recoveryDomains(), Stores: stores, AuthorityPools: pools,
		Kubernetes: kube, Host: host, RenewalInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return authority
}

func openRecoveryFixture(t *testing.T) (Config, *Store) {
	t.Helper()
	cfg := routedM2Config(t)
	return cfg, openRoutedM2Store(t, cfg)
}

func assertRecoveryGuards(t *testing.T, ctx context.Context, cfg Config, expected []mutationGuardToken, wantState, wantOwner string, wantEpoch int64) {
	t.Helper()
	addresses := []string{cfg.DatabaseURL, cfg.ShardURLs[0], cfg.ShardURLs[1], cfg.ShardURLs[2]}
	if wantState == "active" && len(expected) != len(addresses) {
		t.Fatalf("active guard token count = %d, want %d", len(expected), len(addresses))
	}
	seen := make(map[string]bool, len(expected))
	for _, address := range addresses {
		admin, err := pgx.Connect(ctx, address)
		if err != nil {
			t.Fatal(err)
		}
		var state, owner, resource string
		var epoch, generation int64
		err = admin.QueryRow(ctx, "SELECT state, owner, epoch, pg_generation, resource_identity FROM mutation_guard WHERE singleton").Scan(&state, &owner, &epoch, &generation, &resource)
		closeErr := admin.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if state != wantState || owner != wantOwner || epoch != wantEpoch {
			t.Errorf("guard at %s = (%q,%q,%d), want (%q,%q,%d)", address, state, owner, epoch, wantState, wantOwner, wantEpoch)
		}
		if wantState == "active" {
			matched := false
			for _, token := range expected {
				if token.resource == resource {
					matched = true
					seen[resource] = true
					if token.generation != generation {
						t.Errorf("guard at %s has generation %d, authority token has %d", address, generation, token.generation)
					}
				}
			}
			if !matched || generation < 1 || resource == "" {
				t.Errorf("active guard at %s has no matching authority token: generation=%d resource=%q", address, generation, resource)
			}
		}
	}
	if len(seen) != len(expected) {
		t.Errorf("matched %d SQL guard tokens, want %d", len(seen), len(expected))
	}
}

func recoveryGuardsClosed(ctx context.Context, cfg Config) bool {
	for _, address := range []string{cfg.DatabaseURL, cfg.ShardURLs[0], cfg.ShardURLs[1], cfg.ShardURLs[2]} {
		admin, err := pgx.Connect(ctx, address)
		if err != nil {
			return false
		}
		var state, owner string
		err = admin.QueryRow(ctx, "SELECT state, owner FROM mutation_guard WHERE singleton").Scan(&state, &owner)
		closeErr := admin.Close(ctx)
		if err != nil || closeErr != nil || state != "closed" || owner != "" {
			return false
		}
	}
	return true
}

func recoveryGuardTokens(authority *PollAuthority) []mutationGuardToken {
	authority.mu.RLock()
	defer authority.mu.RUnlock()
	if authority.token == nil {
		return nil
	}
	return append([]mutationGuardToken(nil), authority.token.guards...)
}

func recoveryIngest(t *testing.T, store *Store, ctx context.Context, identity string) error {
	t.Helper()
	return store.Ingest(ctx, IngestChunk{
		Endpoint: SeismicEndpoint, Records: []json.RawMessage{routedRecord(t, identity, time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC))},
		ReceivedAt: time.Date(2026, 11, 18, 0, 1, 0, 0, time.UTC), InputID: "authority-recovery-" + identity,
	})
}

func TestPollAuthorityRecoveryIntegrationActivationAndFailover(t *testing.T) {
	t.Run("fresh closed epoch zero arms every guard before first work", func(t *testing.T) {
		cfg, store := openRecoveryFixture(t)
		kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
		authority := openRecoveryAuthority(t, cfg, store, kube, host, "fresh-owner")
		active, _, err := authority.activate(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if active.State != authorityActive || active.Owner != "fresh-owner" || active.Epoch != 1 || len(active.GrantSet) != len(recoveryDomains()) {
			t.Fatalf("activated record = %+v", active)
		}
		if active.OwnerDomainUUID != recoveryOwnerDomain(active.Owner) {
			t.Fatalf("active owner domain = %q, want %q", active.OwnerDomainUUID, recoveryOwnerDomain(active.Owner))
		}
		assertRecoveryGuards(t, t.Context(), cfg, recoveryGuardTokens(authority), "active", active.Owner, active.Epoch)
		ctx, err := authority.currentContext(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if owner, epoch, ok := pollAuthorityToken(ctx); !ok || owner != active.Owner || epoch != active.Epoch {
			t.Fatalf("mutation token = (%q,%d,%v)", owner, epoch, ok)
		}
		if err := recoveryIngest(t, store, ctx, "fresh-first-effect"); err != nil {
			t.Fatalf("first guarded business mutation: %v", err)
		}
	})

	t.Run("live old owner remains active and is not revoked", func(t *testing.T) {
		cfg, store := openRecoveryFixture(t)
		kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
		old := openRecoveryAuthority(t, cfg, store, kube, host, "live-old-owner")
		active, _, err := old.activate(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if active.OwnerDomainUUID != recoveryOwnerDomain(active.Owner) {
			t.Fatalf("active owner domain = %q, want %q", active.OwnerDomainUUID, recoveryOwnerDomain(active.Owner))
		}
		oldContext, err := old.currentContext(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		contender := openRecoveryAuthority(t, cfg, store, kube, host, "live-contender")
		if _, _, err := contender.activate(t.Context()); !errors.Is(err, errAuthorityNotClosed) {
			t.Fatalf("contender activation error = %v, want live-owner follower", err)
		}
		got, err := host.Current(t.Context(), active.GrantSet[0].DomainUUID)
		if err != nil || got != active.GrantSet[0] {
			t.Fatalf("old host grant changed: %+v, err=%v; want %+v", got, err, active.GrantSet[0])
		}
		host.mu.Lock()
		revocations := append([]recoveryHostRevoke(nil), host.revocations...)
		host.mu.Unlock()
		if len(revocations) != 0 {
			t.Fatalf("live old owner received %d revocations", len(revocations))
		}
		assertRecoveryGuards(t, t.Context(), cfg, recoveryGuardTokens(old), "active", active.Owner, active.Epoch)
		if err := recoveryIngest(t, store, oldContext, "live-owner-effect"); err != nil {
			t.Fatalf("live old owner lost write authority: %v", err)
		}
	})

	t.Run("expired old owner closes guards and advances epoch before new work", func(t *testing.T) {
		cfg, store := openRecoveryFixture(t)
		kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
		old := openRecoveryAuthority(t, cfg, store, kube, host, "expired-old-owner")
		oldRecord, _, err := old.activate(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if oldRecord.OwnerDomainUUID != recoveryDomains()[0] {
			t.Fatalf("old owner domain = %q, want %q", oldRecord.OwnerDomainUUID, recoveryDomains()[0])
		}
		oldContext, err := old.currentContext(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		host.expireAll()
		newOwner := openRecoveryAuthority(t, cfg, store, kube, host, "recovered-owner")
		active, _, err := newOwner.activate(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if active.Epoch != oldRecord.Epoch+1 || active.Owner != "recovered-owner" {
			t.Fatalf("recovered authority = %+v, want epoch %d owner recovered-owner", active, oldRecord.Epoch+1)
		}
		if active.OwnerDomainUUID != recoveryOwnerDomain(active.Owner) {
			t.Fatalf("new owner domain = %q, want %q", active.OwnerDomainUUID, recoveryOwnerDomain(active.Owner))
		}
		host.mu.Lock()
		revocations := append([]recoveryHostRevoke(nil), host.revocations...)
		host.mu.Unlock()
		if len(revocations) != len(recoveryDomains()) {
			t.Fatalf("takeover revoked %d domains, want %d", len(revocations), len(recoveryDomains()))
		}
		seenRevocations := make(map[string]hostRevokeMode, len(revocations))
		for _, revoke := range revocations {
			seenRevocations[revoke.identity.DomainUUID] = revoke.effective
			if revoke.requested != hostRevokeTakeover {
				t.Errorf("domain %s was revoked with mode %q, want takeover request", revoke.identity.DomainUUID, revoke.requested)
			}
			if revoke.identity.Owner != oldRecord.Owner || revoke.identity.OwnerDomainUUID != oldRecord.OwnerDomainUUID ||
				revoke.identity.Epoch != uint64(oldRecord.Epoch) || revoke.identity.RenewalSeq != uint64(oldRecord.RenewalSeq) {
				t.Errorf("revoke identity = %+v, want owner=%q owner-domain=%q epoch=%d seq=%d",
					revoke.identity, oldRecord.Owner, oldRecord.OwnerDomainUUID, oldRecord.Epoch, oldRecord.RenewalSeq)
			}
		}
		for _, domain := range recoveryDomains() {
			wantMode := hostRevokeGraceful
			if domain == oldRecord.OwnerDomainUUID {
				wantMode = hostRevokeTakeover
			}
			if got := seenRevocations[domain]; got != wantMode {
				t.Errorf("domain %s revoke mode = %q, want %q", domain, got, wantMode)
			}
		}
		assertRecoveryGuards(t, t.Context(), cfg, recoveryGuardTokens(newOwner), "active", active.Owner, active.Epoch)
		if err := recoveryIngest(t, store, oldContext, "expired-stale-effect"); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("immutable old context write error = %v, want stale authority", err)
		}
		if err := recoveryIngest(t, store, mustRecoveryContext(t, newOwner), "recovered-first-effect"); err != nil {
			t.Fatalf("new owner's first mutation: %v", err)
		}
	})

	t.Run("partial renewal and closed host preserve each actual high-water sequence", func(t *testing.T) {
		cfg, store := openRecoveryFixture(t)
		kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
		old := openRecoveryAuthority(t, cfg, store, kube, host, "crashed-renewer")
		oldRecord, _, err := old.activate(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		highWaterDomain, expiredDomain := recoveryDomains()[0], recoveryDomains()[1]
		_, err = host.Grant(t.Context(), hostFenceIdentity{
			DomainUUID: highWaterDomain, Epoch: uint64(oldRecord.Epoch), RenewalSeq: uint64(oldRecord.RenewalSeq + 1),
			Owner: oldRecord.Owner, OwnerDomainUUID: oldRecord.OwnerDomainUUID,
		})
		if err != nil {
			t.Fatalf("persist partial renewal: %v", err)
		}
		host.mu.Lock()
		host.inactive[expiredDomain] = true
		host.mu.Unlock()
		kube.mu.Lock()
		beforeRecoveryCAS := kube.casCalls
		kube.mu.Unlock()
		replacement := openRecoveryAuthority(t, cfg, store, kube, host, "replacement-after-crash")
		active, _, err := replacement.activate(t.Context())
		if err != nil {
			t.Fatalf("recover partially advanced grants: %v", err)
		}
		if active.State != authorityActive || active.Epoch != oldRecord.Epoch+1 || active.RenewalSeq != 1 || active.Owner != replacement.owner {
			t.Fatalf("recovered authority = %+v, want one activation at epoch %d", active, oldRecord.Epoch+1)
		}
		host.mu.Lock()
		revocations := append([]recoveryHostRevoke(nil), host.revocations...)
		host.mu.Unlock()
		if len(revocations) != len(recoveryDomains()) {
			t.Fatalf("takeover revoked %d domains, want %d", len(revocations), len(recoveryDomains()))
		}
		wantSeq := map[string]uint64{
			recoveryDomains()[0]: uint64(oldRecord.RenewalSeq + 1),
			recoveryDomains()[1]: uint64(oldRecord.RenewalSeq),
			recoveryDomains()[2]: uint64(oldRecord.RenewalSeq),
		}
		seen := make(map[string]bool, len(revocations))
		for _, revoke := range revocations {
			domain := revoke.identity.DomainUUID
			if seen[domain] {
				t.Errorf("domain %s revoked more than once", domain)
			}
			seen[domain] = true
			if revoke.identity.Owner != oldRecord.Owner || revoke.identity.OwnerDomainUUID != oldRecord.OwnerDomainUUID ||
				revoke.identity.Epoch != uint64(oldRecord.Epoch) || revoke.identity.RenewalSeq != wantSeq[domain] {
				t.Errorf("revoke identity = %+v, want owner=%q epoch=%d actual host sequence=%d",
					revoke.identity, oldRecord.Owner, oldRecord.Epoch, wantSeq[domain])
			}
			if revoke.requested != hostRevokeTakeover {
				t.Errorf("domain %s revoke mode = %q, want takeover request", domain, revoke.requested)
			}
			wantMode := hostRevokeGraceful
			if domain == oldRecord.OwnerDomainUUID {
				wantMode = hostRevokeTakeover
			}
			if revoke.effective != wantMode {
				t.Errorf("domain %s effective revoke mode = %q, want %q", domain, revoke.effective, wantMode)
			}
		}
		for domain := range wantSeq {
			if !seen[domain] {
				t.Errorf("domain %s actual high-water sequence was not revoked", domain)
			}
			current, err := host.Current(t.Context(), domain)
			if err != nil || current.Epoch != active.Epoch || current.RenewalSeq != active.RenewalSeq {
				t.Errorf("domain %s post-recovery host high-water = %+v, err=%v; want epoch %d sequence %d",
					domain, current, err, active.Epoch, active.RenewalSeq)
			}
		}
		kube.mu.Lock()
		recoveryCAS := kube.casCalls - beforeRecoveryCAS
		kube.mu.Unlock()
		if recoveryCAS != 5 {
			t.Fatalf("recovery used %d ConfigMap CAS operations, want closing, closed and three-step activation (5)", recoveryCAS)
		}
		finalRecord, _ := kube.snapshot()
		if finalRecord.State != authorityActive || finalRecord.Epoch != oldRecord.Epoch+1 || finalRecord.Owner != replacement.owner {
			t.Fatalf("replacement activation CAS result = %+v", finalRecord)
		}
		assertRecoveryGuards(t, t.Context(), cfg, recoveryGuardTokens(replacement), "active", active.Owner, active.Epoch)
		if err := recoveryIngest(t, store, mustRecoveryContext(t, replacement), "partial-renewal-recovered-effect"); err != nil {
			t.Fatalf("replacement mutation after recovery: %v", err)
		}
	})

	t.Run("host epoch above ConfigMap high-water refuses reset", func(t *testing.T) {
		cfg, store := openRecoveryFixture(t)
		kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
		old := openRecoveryAuthority(t, cfg, store, kube, host, "old-config-owner")
		oldRecord, _, err := old.activate(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		domain := recoveryDomains()[0]
		if _, err := host.Grant(t.Context(), hostFenceIdentity{
			DomainUUID: domain, Epoch: uint64(oldRecord.Epoch + 1), RenewalSeq: 1,
			Owner: "host-ahead-owner", OwnerDomainUUID: domain,
		}); err != nil {
			t.Fatalf("seed higher host epoch: %v", err)
		}
		host.mu.Lock()
		host.inactive[domain] = true
		host.mu.Unlock()
		replacement := openRecoveryAuthority(t, cfg, store, kube, host, "lower-config-replacement")
		if _, _, err := replacement.activate(t.Context()); err == nil {
			t.Fatal("activation reset a host epoch above the ConfigMap high-water")
		}
		record, _ := kube.snapshot()
		if record.Epoch != oldRecord.Epoch || record.Owner != oldRecord.Owner ||
			record.State != authorityActive && record.State != authorityClosing {
			t.Fatalf("refused recovery reset or advanced ConfigMap authority: %+v", record)
		}
		if record.State == authorityClosing {
			assertRecoveryGuards(t, t.Context(), cfg, nil, "closed", "", oldRecord.Epoch)
		} else {
			assertRecoveryGuards(t, t.Context(), cfg, recoveryGuardTokens(old), "active", oldRecord.Owner, oldRecord.Epoch)
		}
		hostGrant, err := host.Current(t.Context(), domain)
		if !errors.Is(err, errHostGrantInactive) || hostGrant.Epoch != oldRecord.Epoch+1 || hostGrant.RenewalSeq != 1 {
			t.Fatalf("higher host high-water = %+v, err=%v; want unchanged inactive epoch %d", hostGrant, err, oldRecord.Epoch+1)
		}
		if err := recoveryIngest(t, store, t.Context(), "host-high-water-refusal-effect"); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("host high-water above ConfigMap remained usable: %v", err)
		}
	})

	t.Run("pending before or after grant publication is recovered without reset", func(t *testing.T) {
		for _, afterPublication := range []bool{false, true} {
			name := "before-host-grants-published"
			if afterPublication {
				name = "after-host-grants-published"
			}
			t.Run(name, func(t *testing.T) {
				cfg, store := openRecoveryFixture(t)
				kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
				pending := authorityRecord{Epoch: 1, Owner: "crashed-owner", OwnerDomainUUID: recoveryDomains()[0], State: authorityPending, RenewalSeq: 1}
				grants := make([]authorityGrant, 0, len(recoveryDomains()))
				for _, domain := range recoveryDomains() {
					grant, err := host.Grant(t.Context(), hostFenceIdentity{
						DomainUUID: domain, Epoch: uint64(pending.Epoch), RenewalSeq: uint64(pending.RenewalSeq),
						Owner: pending.Owner, OwnerDomainUUID: pending.OwnerDomainUUID,
					})
					if err != nil {
						t.Fatal(err)
					}
					grants = append(grants, grant)
				}
				if afterPublication {
					pending.GrantSet = grants
				}
				kube.seed(pending)
				owner := openRecoveryAuthority(t, cfg, store, kube, host, "pending-recovery-owner")
				// Model a contender that already observed this abandoned epoch for the full grace period.
				owner.pendingWithinGrace(&pending, time.Now().Add(-authorityActivationTimeout))
				active, _, err := owner.activate(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if active.Epoch != 2 || active.Owner != "pending-recovery-owner" {
					t.Fatalf("pending recovery record = %+v, want new owner at epoch 2", active)
				}
				assertRecoveryGuards(t, t.Context(), cfg, recoveryGuardTokens(owner), "active", active.Owner, active.Epoch)
				finalRecord, _ := kube.snapshot()
				if finalRecord.State != authorityActive || finalRecord.Epoch != 2 || finalRecord.Owner != active.Owner {
					t.Fatalf("recovered ConfigMap record = %+v", finalRecord)
				}
				if finalRecord.OwnerDomainUUID != recoveryOwnerDomain(active.Owner) {
					t.Fatalf("recovered owner domain = %q, want %q", finalRecord.OwnerDomainUUID, recoveryOwnerDomain(active.Owner))
				}
				host.mu.Lock()
				revocations := append([]recoveryHostRevoke(nil), host.revocations...)
				host.mu.Unlock()
				if len(revocations) != len(recoveryDomains()) {
					t.Fatalf("pending host grants were not revoked: got %d revocations", len(revocations))
				}
			})
		}
	})

	t.Run("same ConfigMap version has one activation winner", func(t *testing.T) {
		cfg, store := openRecoveryFixture(t)
		kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
		readArrivals := make(chan struct{}, 2)
		releaseReads := make(chan struct{})
		kube.readArrivals, kube.releaseReads, kube.readBarrierRemaining = readArrivals, releaseReads, 2
		owners := []*PollAuthority{
			openRecoveryAuthority(t, cfg, store, kube, host, "race-owner-a"),
			openRecoveryAuthority(t, cfg, store, kube, host, "race-owner-b"),
		}
		type result struct {
			authority *PollAuthority
			err       error
		}
		results := make(chan result, len(owners))
		start := make(chan struct{})
		for _, owner := range owners {
			go func(owner *PollAuthority) {
				<-start
				_, _, err := owner.activate(t.Context())
				results <- result{authority: owner, err: err}
			}(owner)
		}
		close(start)
		for range owners {
			<-readArrivals
		}
		close(releaseReads)
		winners := 0
		var winner, loser *PollAuthority
		for range owners {
			result := <-results
			if result.err == nil {
				winners++
				winner = result.authority
			} else {
				loser = result.authority
			}
		}
		if winners != 1 || winner == nil || loser == nil {
			t.Fatalf("same ConfigMap version produced %d activation winners", winners)
		}
		active, _ := kube.snapshot()
		if active.State != authorityActive || active.Owner != winner.owner || active.Epoch != 1 {
			t.Fatalf("published winner = %+v", active)
		}
		if _, err := loser.currentContext(t.Context()); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("loser's authority context error = %v", err)
		}
		if err := recoveryIngest(t, store, t.Context(), "losing-owner-effect"); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("loser's first business mutation error = %v", err)
		}
		if err := recoveryIngest(t, store, mustRecoveryContext(t, winner), "single-winner-effect"); err != nil {
			t.Fatalf("winning owner's first business mutation: %v", err)
		}
	})

	t.Run("host and Kubernetes uncertainty never authorizes writes", func(t *testing.T) {
		cfg, store := openRecoveryFixture(t)
		kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(0)), newRecoveryHostFence()
		kube.readsErr = errRecoveryHostUnavailable
		owner := openRecoveryAuthority(t, cfg, store, kube, host, "uncertain-owner")
		if _, _, err := owner.activate(t.Context()); !errors.Is(err, errAuthorityUnavailable) {
			t.Fatalf("ConfigMap read uncertainty error = %v", err)
		}
		if err := recoveryIngest(t, store, t.Context(), "api-uncertainty-effect"); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("missing activation write error = %v", err)
		}
		kube.readsErr = nil
		if _, _, err := owner.activate(t.Context()); err != nil {
			t.Fatal(err)
		}
		host.mu.Lock()
		host.failRead = true
		host.mu.Unlock()
		if _, err := owner.currentContext(t.Context()); !errors.Is(err, errAuthorityUnavailable) {
			t.Fatalf("host uncertainty error = %v", err)
		}
		if err := recoveryIngest(t, store, t.Context(), "host-uncertainty-effect"); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("host uncertainty mutation error = %v", err)
		}
		if _, _, err := openRecoveryAuthority(t, cfg, store, kube, host, "uncertainty-contender").activate(t.Context()); !errors.Is(err, errAuthorityUnavailable) {
			t.Fatalf("contender under host uncertainty error = %v", err)
		}
		assertRecoveryGuards(t, t.Context(), cfg, recoveryGuardTokens(owner), "active", "uncertain-owner", 1)
	})

	t.Run("higher SQL epoch refuses ConfigMap reset", func(t *testing.T) {
		cfg, store := openRecoveryFixture(t)
		for _, address := range []string{cfg.DatabaseURL, cfg.ShardURLs[0], cfg.ShardURLs[1], cfg.ShardURLs[2]} {
			admin, err := pgx.Connect(t.Context(), address)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := admin.Exec(t.Context(), "UPDATE mutation_guard SET owner='', epoch=2, state='closed'"); err != nil {
				_ = admin.Close(t.Context())
				t.Fatal(err)
			}
			if err := admin.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		kube, host := newRecoveryAuthorityKubernetes(recoveryClosedRecord(1)), newRecoveryHostFence()
		owner := openRecoveryAuthority(t, cfg, store, kube, host, "reset-must-fail")
		if _, _, err := owner.activate(t.Context()); err == nil || !containsRecoveryError(err, "exceeds ConfigMap high-water") {
			t.Fatalf("higher SQL epoch error = %v, want explicit reconciliation refusal", err)
		}
		if err := recoveryIngest(t, store, t.Context(), "higher-sql-epoch-effect"); !errors.Is(err, errStaleMutationAuthority) {
			t.Fatalf("refused activation write error = %v", err)
		}
		assertRecoveryGuards(t, t.Context(), cfg, nil, "closed", "", 2)
	})
}

func mustRecoveryContext(t *testing.T, authority *PollAuthority) context.Context {
	t.Helper()
	ctx, err := authority.currentContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func containsRecoveryError(err error, fragment string) bool {
	return err != nil && strings.Contains(err.Error(), fragment)
}
