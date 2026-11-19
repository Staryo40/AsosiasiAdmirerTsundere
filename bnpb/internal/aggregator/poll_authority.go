package aggregator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var errAuthorityUnavailable = errors.New("poll authority unavailable")
var errStaleMutationAuthority = errors.New("mutation context is stale or absent")
var errAuthorityNotClosed = errors.New("poll authority is not closed for activation")

const authorityActivationTimeout = 20 * time.Second

type authorityState string

const (
	authorityPending authorityState = "pending"
	authorityActive  authorityState = "active"
	authorityClosing authorityState = "closing"
	authorityClosed  authorityState = "closed"
)

type authorityGrant struct {
	DomainUUID         string `json:"domain_uuid"`
	HostBootID         string `json:"host_boot_id"`
	Epoch              int64  `json:"epoch"`
	RenewalSeq         int64  `json:"renewal_seq"`
	DeadlineBootTimeNS int64  `json:"deadline_boottime_ns"`
}

type authorityRecord struct {
	Epoch           int64            `json:"epoch"`
	Owner           string           `json:"owner"`
	OwnerDomainUUID string           `json:"owner_domain_uuid"`
	OwnerEndpoint   string           `json:"owner_endpoint,omitempty"`
	State           authorityState   `json:"state"`
	RenewalSeq      int64            `json:"renewal_seq"`
	GrantSet        []authorityGrant `json:"grant_set"`
}

type hostFenceIdentity struct {
	DomainUUID      string `json:"domain_uuid"`
	Epoch           uint64 `json:"epoch"`
	RenewalSeq      uint64 `json:"renewal_seq"`
	Owner           string `json:"owner"`
	OwnerDomainUUID string `json:"owner_domain_uuid"`
}
type hostRevokeMode string

const (
	hostRevokeGraceful hostRevokeMode = "graceful"
	hostRevokeTakeover hostRevokeMode = "takeover"
)

type HostFenceExecutor interface {
	Grant(context.Context, hostFenceIdentity) (authorityGrant, error)
	Current(context.Context, string) (authorityGrant, error)
	Revoke(context.Context, hostFenceIdentity, hostRevokeMode) error
}

type PollAuthorityConfig struct {
	Owner           string
	OwnerDomain     string
	OwnerEndpoint   string
	Domains         []string
	Stores          []*Store
	AuthorityPools  []*pgxpool.Pool
	Kubernetes      authorityKubernetesClient
	Host            HostFenceExecutor
	RenewalInterval time.Duration
}

type PollAuthority struct {
	owner           string
	ownerDomain     string
	ownerEndpoint   string
	domains         []string
	stores          []*Store
	authorityPools  []*pgxpool.Pool
	kubernetes      authorityKubernetesClient
	host            HostFenceExecutor
	renewalInterval time.Duration
	mu              sync.RWMutex
	token           *mutationToken
	pendingEpoch    int64
	pendingSince    time.Time
}
type mutationContextKey struct{}
type mutationGuardToken struct {
	store      *Store
	resource   string
	generation int64
}
type mutationToken struct {
	owner  string
	epoch  int64
	guards []mutationGuardToken
}

func NewPollAuthority(ctx context.Context, cfg PollAuthorityConfig) (*PollAuthority, error) {
	if cfg.Owner == "" || !authorityDomainPattern.MatchString(cfg.OwnerDomain) || len(cfg.Domains) == 0 ||
		len(cfg.Stores) != 4 || len(cfg.AuthorityPools) != 4 ||
		cfg.Kubernetes == nil || cfg.Host == nil || cfg.RenewalInterval <= 0 {
		return nil, errors.New("routed poll authority requires owner, owner domain, domains, four stores and authority pools, Kubernetes CAS, host fence and renewal interval")
	}
	if cfg.RenewalInterval >= 10*time.Second {
		return nil, errors.New("authority renewal interval must precede the host's 10s fencing lead")
	}
	if cfg.OwnerEndpoint != "" {
		if err := validateOwnerEndpoint(cfg.OwnerEndpoint); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	domains := append([]string(nil), cfg.Domains...)
	sort.Strings(domains)
	for index, domain := range domains {
		if !authorityDomainPattern.MatchString(domain) || (index > 0 && strings.EqualFold(domains[index-1], domain)) {
			return nil, errors.New("authority domains must be valid, nonempty and unique UUIDs")
		}
	}
	ownerDomain := ""
	for _, domain := range domains {
		if strings.EqualFold(domain, cfg.OwnerDomain) {
			ownerDomain = domain
			break
		}
	}
	if ownerDomain == "" {
		return nil, errors.New("authority owner domain must be included in configured domains")
	}
	for index := range cfg.Stores {
		if cfg.Stores[index] == nil || cfg.Stores[index].write == nil || cfg.AuthorityPools[index] == nil {
			return nil, errors.New("poll authority store or authority pool is unavailable")
		}
	}
	return &PollAuthority{owner: cfg.Owner, ownerDomain: ownerDomain, ownerEndpoint: cfg.OwnerEndpoint, domains: domains, stores: append([]*Store(nil), cfg.Stores...),
		authorityPools: append([]*pgxpool.Pool(nil), cfg.AuthorityPools...), kubernetes: cfg.Kubernetes,
		host: cfg.Host, renewalInterval: cfg.RenewalInterval}, nil
}

func NewAuthorityOwnerID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("create authority owner ID: %w", err)
	}
	return hex.EncodeToString(random[:]), nil
}

func (authority *PollAuthority) currentContext(ctx context.Context) (context.Context, error) {
	authority.mu.RLock()
	token := authority.token
	if token != nil {
		token = &mutationToken{owner: token.owner, epoch: token.epoch, guards: append([]mutationGuardToken(nil), token.guards...)}
	}
	authority.mu.RUnlock()
	if token == nil || token.owner != authority.owner {
		return nil, errStaleMutationAuthority
	}
	record, _, err := authority.kubernetes.Read(ctx)
	if err != nil {
		return nil, errors.Join(errAuthorityUnavailable, err)
	}
	if record.State != authorityActive || record.Owner != token.owner || record.Epoch != token.epoch {
		authority.clearToken()
		return nil, errStaleMutationAuthority
	}
	live, err := authority.hostGrantsCurrent(ctx, record)
	if err != nil {
		return nil, err
	}
	if !live {
		authority.clearToken()
		return nil, errStaleMutationAuthority
	}
	return context.WithValue(ctx, mutationContextKey{}, *token), nil
}
func (authority *PollAuthority) activeOwnerEndpoint(ctx context.Context) (string, error) {
	record, _, err := authority.kubernetes.Read(ctx)
	if err != nil {
		return "", errors.Join(errAuthorityUnavailable, err)
	}
	if err := validateAuthorityRecord(record); err != nil {
		return "", err
	}
	if record.State != authorityActive || record.OwnerEndpoint == "" {
		return "", errAuthorityUnavailable
	}
	if err := validateOwnerEndpoint(record.OwnerEndpoint); err != nil {
		return "", err
	}
	return record.OwnerEndpoint, nil
}

func pollAuthorityToken(ctx context.Context) (string, int64, bool) {
	token, ok := ctx.Value(mutationContextKey{}).(mutationToken)
	return token.owner, token.epoch, ok && token.owner != "" && token.epoch > 0
}

func (authority *PollAuthority) run(ctx context.Context, workers ...func(context.Context)) error {
	if len(workers) == 0 {
		return errors.New("poll authority requires at least one owner worker")
	}
	var active authorityRecord
	var version string
	var err error
	for {
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, authorityActivationTimeout)
		active, version, err = authority.activate(attemptCtx)
		cancelAttempt()
		if err == nil {
			break
		}
		if !errors.Is(err, errAuthorityUnavailable) && !errors.Is(err, errAuthorityNotClosed) && !errors.Is(err, errAuthorityMissing) {
			return err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	ownerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerCtx, err := authority.currentContext(ownerCtx)
	if err != nil {
		return err
	}
	var workersDone sync.WaitGroup
	workerDone := make(chan struct{}, len(workers))
	for _, worker := range workers {
		worker := worker
		workersDone.Add(1)
		go func() { defer workersDone.Done(); worker(workerCtx); workerDone <- struct{}{} }()
	}
	renewDone := make(chan error, 1)
	go func() { renewDone <- authority.renew(ownerCtx, active, version) }()
	select {
	case <-ctx.Done():
		cancel()
		workersDone.Wait()
		renewErr := <-renewDone
		closeErr := authority.closeAfterStop(ctx, active, version, hostRevokeGraceful, ctx.Err())
		if errors.Is(renewErr, context.Canceled) {
			return closeErr
		}
		return errors.Join(renewErr, closeErr)
	case <-workerDone:
		cancel()
		workersDone.Wait()
		<-renewDone
		return authority.closeAfterStop(ctx, active, version, hostRevokeGraceful,
			errors.New("owner worker stopped while authority was active"))
	case err := <-renewDone:
		cancel()
		workersDone.Wait()
		return authority.closeAfterStop(ctx, active, version, hostRevokeTakeover,
			errors.Join(errAuthorityUnavailable, err))

	}
}
func (authority *PollAuthority) closeAfterStop(ctx context.Context, active authorityRecord, version string, mode hostRevokeMode, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := authority.close(cleanupCtx, active, version, mode); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (authority *PollAuthority) activate(ctx context.Context) (authorityRecord, string, error) {
	record, version, err := authority.kubernetes.Read(ctx)
	if err != nil {
		if errors.Is(err, errAuthorityMissing) {
			return authorityRecord{}, "", fmt.Errorf("authority ConfigMap is missing; epoch reset is forbidden: %w", err)
		}
		return authorityRecord{}, "", errors.Join(errAuthorityUnavailable, fmt.Errorf("read authority ConfigMap: %w", err))
	}
	if err := validateAuthorityRecord(record); err != nil {
		return authorityRecord{}, "", err
	}
	if record.State != authorityClosed && !authority.domainConfigured(record.OwnerDomainUUID) {
		return authorityRecord{}, "", errors.New("authority owner domain is outside configured domains")
	}
	if authority.pendingWithinGrace(&record, time.Now()) {
		return authorityRecord{}, "", errAuthorityNotClosed
	}
	if record.State != authorityClosed {
		record, version, err = authority.reconcileRecord(ctx, record, version)
		if err != nil {
			return authorityRecord{}, "", err
		}
		if record.State != authorityClosed {
			return authorityRecord{}, "", errAuthorityNotClosed
		}
	}
	if err := authority.validateDatabaseRoles(ctx, record.Epoch); err != nil {
		return authorityRecord{}, "", err
	}
	if record.Epoch == math.MaxInt64 {
		return authorityRecord{}, "", errors.New("authority epoch exhausted")
	}
	pending := authorityRecord{Epoch: record.Epoch + 1, Owner: authority.owner, OwnerDomainUUID: authority.ownerDomain, OwnerEndpoint: authority.ownerEndpoint, State: authorityPending, RenewalSeq: 1}
	version, err = authority.kubernetes.CompareAndSwap(ctx, version, pending)
	if err != nil {
		return authorityRecord{}, "", errors.Join(errAuthorityUnavailable, fmt.Errorf("reserve pending authority epoch: %w", err))
	}
	grants, err := authority.armHostGrants(ctx, pending)
	if err != nil {
		return authorityRecord{}, "", err
	}
	pending.GrantSet = grants
	version, err = authority.kubernetes.CompareAndSwap(ctx, version, pending)
	if err != nil {
		return authorityRecord{}, "", errors.Join(errAuthorityUnavailable, fmt.Errorf("publish armed host grants: %w", err))
	}
	if err := authority.prepareMutationGenerations(ctx); err != nil {
		return authorityRecord{}, "", err
	}
	if err := authority.activateGuards(ctx, pending); err != nil {
		return authorityRecord{}, "", fmt.Errorf("activate SQL guards: %w", err)
	}
	if err := authority.verifyActiveGuards(ctx, pending); err != nil {
		_ = authority.closeMutationGuards(context.WithoutCancel(ctx), pending.Owner, pending.Epoch)
		return authorityRecord{}, "", err
	}
	guardTokens, err := authority.captureGuardTokens(ctx)
	if err != nil {
		_ = authority.closeMutationGuards(context.WithoutCancel(ctx), pending.Owner, pending.Epoch)
		return authorityRecord{}, "", err
	}
	pending.State = authorityActive
	version, err = authority.kubernetes.CompareAndSwap(ctx, version, pending)
	if err != nil {
		_ = authority.closeMutationGuards(context.WithoutCancel(ctx), pending.Owner, pending.Epoch)
		return authorityRecord{}, "", errors.Join(errAuthorityUnavailable, fmt.Errorf("publish active authority: %w", err))
	}
	authority.mu.Lock()
	authority.token = &mutationToken{owner: pending.Owner, epoch: pending.Epoch, guards: guardTokens}
	authority.mu.Unlock()
	return pending, version, nil
}

// The grace period starts at local first observation and can restart with the
// process. It is not a lease: recovery still requires guard closure and host revoke.
func (authority *PollAuthority) pendingWithinGrace(record *authorityRecord, now time.Time) bool {
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if record.State != authorityPending {
		authority.pendingEpoch, authority.pendingSince = 0, time.Time{}
		return false
	}
	if authority.pendingEpoch != record.Epoch {
		authority.pendingEpoch, authority.pendingSince = record.Epoch, now
		return true
	}
	return now.Sub(authority.pendingSince) < authorityActivationTimeout
}

func (authority *PollAuthority) armHostGrants(ctx context.Context, record authorityRecord) ([]authorityGrant, error) {
	grants := make([]authorityGrant, 0, len(authority.domains))
	for _, domain := range authority.domains {
		grant, err := authority.host.Grant(ctx, hostFenceIdentity{DomainUUID: domain, Epoch: uint64(record.Epoch),
			RenewalSeq: uint64(record.RenewalSeq), Owner: record.Owner, OwnerDomainUUID: record.OwnerDomainUUID})
		if err != nil {
			return nil, errors.Join(errAuthorityUnavailable, fmt.Errorf("arm host grant for %q: %w", domain, err))
		}
		if !validGrant(grant, domain, record.Epoch, record.RenewalSeq, "") {
			return nil, fmt.Errorf("host returned invalid grant for %q", domain)
		}
		grants = append(grants, grant)
	}
	if !grantSetMatches(grants, authority.domains) {
		return nil, errors.New("host grant set does not cover configured execution and storage domains")
	}
	return grants, nil
}

func (authority *PollAuthority) reconcileRecord(ctx context.Context, record authorityRecord, version string) (authorityRecord, string, error) {
	if record.State == authorityActive {
		live, err := authority.hostGrantsCurrent(ctx, record)
		if err != nil {
			return authorityRecord{}, "", err
		}
		if live {
			return record, version, errAuthorityNotClosed
		}
	}
	closed, version, err := authority.closeRecord(ctx, record, version, hostRevokeTakeover)
	if err != nil {
		return authorityRecord{}, "", errors.Join(errAuthorityUnavailable, err)
	}
	return closed, version, nil
}

func (authority *PollAuthority) hostGrantsCurrent(ctx context.Context, record authorityRecord) (bool, error) {
	if !grantSetMatches(record.GrantSet, authority.domains) {
		return false, errAuthorityConflict
	}
	for _, domain := range authority.domains {
		expected, _ := grantForDomain(record.GrantSet, domain)
		current, err := authority.host.Current(ctx, domain)
		if errors.Is(err, errHostGrantInactive) {
			return false, nil
		}
		if err != nil {
			return false, errors.Join(errAuthorityUnavailable, fmt.Errorf("read current host grant for %q: %w", domain, err))
		}
		if current.DomainUUID != domain || current.Epoch != record.Epoch || current.RenewalSeq < record.RenewalSeq ||
			current.HostBootID == "" || current.HostBootID != expected.HostBootID || current.DeadlineBootTimeNS < 1 {
			return false, errAuthorityConflict
		}
	}
	return true, nil
}

func (authority *PollAuthority) closeRecord(ctx context.Context, record authorityRecord, version string, mode hostRevokeMode) (authorityRecord, string, error) {
	canCompleteGrantSet := record.State == authorityPending || (record.State == authorityClosing && len(record.GrantSet) == 0)
	closing, version, err := authority.markRecordClosing(ctx, record, version)
	authority.clearToken()
	if err != nil {
		return authorityRecord{}, "", err
	}
	if err := authority.finishHostClosure(ctx, closing, canCompleteGrantSet, mode); err != nil {
		return authorityRecord{}, "", err
	}
	closing.State, closing.Owner, closing.OwnerDomainUUID, closing.OwnerEndpoint, closing.GrantSet = authorityClosed, "", "", "", nil
	nextVersion, err := authority.kubernetes.CompareAndSwap(ctx, version, closing)
	return closing, nextVersion, err
}

func (authority *PollAuthority) finishHostClosure(ctx context.Context, record authorityRecord, completeGrants bool, mode hostRevokeMode) error {
	if completeGrants {
		if err := authority.completePendingGrants(ctx, record); err != nil {
			return err
		}
	} else if !grantSetMatches(record.GrantSet, authority.domains) {
		return errAuthorityConflict
	}
	initialCtx, cancelInitial := context.WithTimeout(ctx, time.Second)
	initialErr := authority.closeMutationGuards(initialCtx, record.Owner, record.Epoch)
	cancelInitial()
	if err := authority.revokeHostGrants(ctx, record, mode); err != nil {
		return errors.Join(initialErr, err)
	}
	if err := authority.retryCloseMutationGuards(ctx, record.Owner, record.Epoch); err != nil {
		return errors.Join(initialErr, err)
	}
	return nil
}

func (authority *PollAuthority) markRecordClosing(ctx context.Context, record authorityRecord, version string) (authorityRecord, string, error) {
	if record.State == authorityClosing {
		return record, version, nil
	}
	record.State = authorityClosing
	nextVersion, err := authority.kubernetes.CompareAndSwap(ctx, version, record)
	if err != nil {
		return record, version, err
	}
	return record, nextVersion, nil
}

func (authority *PollAuthority) completePendingGrants(ctx context.Context, record authorityRecord) error {
	if record.State != authorityClosing || record.Owner == "" {
		return errAuthorityConflict
	}
	if len(record.GrantSet) > 0 && !grantSetMatches(record.GrantSet, authority.domains) {
		return errAuthorityConflict
	}
	for _, domain := range authority.domains {
		if _, exists := grantForDomain(record.GrantSet, domain); exists {
			continue
		}
		grant, err := authority.host.Grant(ctx, authority.identity(record, domain))
		if err != nil {
			return err
		}
		if !validGrant(grant, domain, record.Epoch, record.RenewalSeq, "") {
			return errAuthorityConflict
		}
	}
	return nil
}

func (authority *PollAuthority) revokeHostGrants(ctx context.Context, record authorityRecord, mode hostRevokeMode) error {
	var revokeErr error
	for _, domain := range authority.domains {
		current, err := authority.currentClosingGrant(ctx, record, domain)
		if err != nil {
			revokeErr = errors.Join(revokeErr, err)
			continue
		}
		identity := authority.identity(record, domain)
		identity.RenewalSeq = uint64(current.RenewalSeq)
		if err := authority.host.Revoke(ctx, identity, mode); err != nil {
			revokeErr = errors.Join(revokeErr, fmt.Errorf("host revoke for %q: %w", domain, err))
			continue
		}
		if err := authority.confirmHostRevoked(ctx, domain, current); err != nil {
			revokeErr = errors.Join(revokeErr, err)
		}
	}
	return revokeErr
}

func (authority *PollAuthority) currentClosingGrant(ctx context.Context, record authorityRecord, domain string) (authorityGrant, error) {
	expected, hasExpected := grantForDomain(record.GrantSet, domain)
	current, err := authority.host.Current(ctx, domain)
	if err != nil && !errors.Is(err, errHostGrantInactive) {
		return authorityGrant{}, fmt.Errorf("read host high-water for %q: %w", domain, err)
	}
	if current.DomainUUID != domain || current.HostBootID == "" || current.Epoch != record.Epoch ||
		current.RenewalSeq < record.RenewalSeq || current.DeadlineBootTimeNS < 0 ||
		hasExpected && current.HostBootID != expected.HostBootID {
		return authorityGrant{}, fmt.Errorf("host high-water conflicts with closing authority for %q: %w", domain, errAuthorityConflict)
	}
	return current, nil
}

func (authority *PollAuthority) confirmHostRevoked(ctx context.Context, domain string, expected authorityGrant) error {
	closed, err := authority.host.Current(ctx, domain)
	if errors.Is(err, errHostGrantInactive) && closed.DomainUUID == domain && closed.HostBootID == expected.HostBootID &&
		closed.Epoch == expected.Epoch && closed.RenewalSeq == expected.RenewalSeq {
		return nil
	}
	return fmt.Errorf("host did not confirm exact fenced high-water for %q: %w", domain, errAuthorityConflict)
}

func (authority *PollAuthority) retryCloseMutationGuards(ctx context.Context, owner string, epoch int64) error {
	for {
		retryCtx, cancelRetry := context.WithTimeout(ctx, 2*time.Second)
		err := authority.closeMutationGuards(retryCtx, owner, epoch)
		cancelRetry()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}

func (authority *PollAuthority) identity(record authorityRecord, domain string) hostFenceIdentity {
	return hostFenceIdentity{DomainUUID: domain, Epoch: uint64(record.Epoch), RenewalSeq: uint64(record.RenewalSeq),
		Owner: record.Owner, OwnerDomainUUID: record.OwnerDomainUUID}
}

func hostBootFor(grants []authorityGrant, domain string) string {
	grant, _ := grantForDomain(grants, domain)
	return grant.HostBootID
}

func (authority *PollAuthority) validateDatabaseRoles(ctx context.Context, highWater int64) error {
	protected := []string{"hazard_events", "source_seismic", "source_warnings", "handled_inputs", "poll_state", "outbox"}
	for index, store := range authority.stores {
		if err := store.verifyStartupResource(ctx); err != nil {
			return err
		}
		var unsafeWriter bool
		err := store.write.QueryRow(ctx, `SELECT r.rolsuper OR has_table_privilege(current_user, 'mutation_guard', 'UPDATE')
			OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
				WHERE n.nspname=current_schema() AND c.relname=ANY($1::text[]) AND pg_has_role(current_user,c.relowner,'MEMBER'))
			FROM pg_roles r WHERE r.rolname=current_user`, protected).Scan(&unsafeWriter)
		if err != nil {
			return storeError("validate routed application database role", err)
		}
		if unsafeWriter {
			return errors.New("routed application role must not be superuser, protected-table owner or mutation_guard writer")
		}
		var canBindGuard bool
		err = store.write.QueryRow(ctx, `SELECT has_function_privilege(current_user,
			'begin_mutation_guard(text,bigint,bigint,text)', 'EXECUTE')`).Scan(&canBindGuard)
		if err != nil {
			return storeError("validate mutation guard binding privilege", err)
		}
		if !canBindGuard {
			return errors.New("routed writer role must be granted mutation guard binding function execution")
		}
		var role string
		var shard int16
		err = store.write.QueryRow(ctx, `SELECT role, COALESCE(shard_id, -1)::smallint FROM store_identity WHERE singleton`).Scan(&role, &shard)
		if err != nil {
			return storeError("validate routed store identity", err)
		}
		if (index == 0 && (role != "control" || shard != -1)) || (index > 0 && (role != "shard" || shard != int16(index-1))) {
			return errors.New("routed stores do not match control plus ordered shard identities")
		}
		var authorityCanFence bool
		err = authority.authorityPools[index].QueryRow(ctx, `SELECT has_table_privilege(current_user, 'mutation_guard', 'UPDATE')`).Scan(&authorityCanFence)
		if err != nil {
			return storeError("validate authority database role", err)
		}
		if !authorityCanFence {
			return errors.New("authority database role cannot update mutation guards")
		}
		var state string
		var epoch int64
		err = authority.authorityPools[index].QueryRow(ctx, `SELECT state, epoch FROM mutation_guard WHERE singleton`).Scan(&state, &epoch)
		if err != nil {
			return storeError("read mutation guard high-water", err)
		}
		if state != "closed" {
			return errors.New("mutation guard is not closed")
		}
		if epoch > highWater {
			return errors.New("SQL app epoch exceeds ConfigMap high-water; explicit reconciliation required")
		}
	}
	return nil
}

func validateAuthorityRecord(record authorityRecord) error {
	if record.Epoch < 0 || record.RenewalSeq < 0 {
		return errors.New("authority ConfigMap contains negative high-water value")
	}
	switch record.State {
	case authorityPending, authorityActive, authorityClosing, authorityClosed:
	default:
		return errors.New("authority ConfigMap contains invalid state")
	}
	if record.Epoch == 0 && (record.Owner != "" || record.State != authorityClosed || record.RenewalSeq != 0 || len(record.GrantSet) != 0) {
		return errors.New("authority initial record must be closed at epoch zero")
	}
	if record.State == authorityClosed && (record.Owner != "" || record.OwnerDomainUUID != "" || record.OwnerEndpoint != "" || len(record.GrantSet) != 0) {
		return errors.New("closed authority must not retain an owner, owner domain, endpoint or grants")
	}
	if record.OwnerEndpoint != "" {
		if err := validateOwnerEndpoint(record.OwnerEndpoint); err != nil {
			return fmt.Errorf("authority ConfigMap contains invalid owner endpoint: %w", err)
		}
	}
	if record.State != authorityClosed && (record.Owner == "" || !authorityDomainPattern.MatchString(record.OwnerDomainUUID)) {
		return errors.New("authority record omitted owner or valid owner domain")
	}
	for _, grant := range record.GrantSet {
		if !validGrant(grant, grant.DomainUUID, record.Epoch, record.RenewalSeq, "") {
			return errors.New("authority record contains invalid grant")
		}
	}
	return nil
}
func (authority *PollAuthority) prepareMutationGenerations(ctx context.Context) error {
	for _, pool := range authority.authorityPools {
		var generation int64
		err := pool.QueryRow(ctx, `SELECT prepare_mutation_generation('mutation_guard'::regclass)`).Scan(&generation)
		if err != nil {
			return storeError("prepare PostgreSQL mutation generation", err)
		}
		if generation < 1 {
			return errors.New("PostgreSQL generation preparation returned an invalid value")
		}
	}
	return nil
}

func (authority *PollAuthority) captureGuardTokens(ctx context.Context) ([]mutationGuardToken, error) {
	tokens := make([]mutationGuardToken, 0, len(authority.authorityPools))
	resources := make(map[string]struct{}, len(authority.authorityPools))
	for index, pool := range authority.authorityPools {
		var state, resource string
		var generation int64
		err := pool.QueryRow(ctx, `SELECT state, pg_generation, resource_identity FROM mutation_guard WHERE singleton`).Scan(&state, &generation, &resource)
		if err != nil {
			return nil, storeError("capture mutation guard generation", err)
		}
		if state != "active" || generation < 1 || resource == "" {
			return nil, errors.New("active mutation guard lacks PostgreSQL generation/resource identity")
		}
		if _, exists := resources[resource]; exists {
			return nil, errors.New("mutation guard resource identities are not unique")
		}
		resources[resource] = struct{}{}
		tokens = append(tokens, mutationGuardToken{store: authority.stores[index], resource: resource, generation: generation})
	}
	return tokens, nil
}

func validGrant(grant authorityGrant, domain string, epoch, seq int64, bootID string) bool {
	return grant.DomainUUID == domain && grant.Epoch == epoch && grant.RenewalSeq == seq && grant.HostBootID != "" &&
		(bootID == "" || grant.HostBootID == bootID) && grant.DeadlineBootTimeNS > 0
}
func (authority *PollAuthority) domainConfigured(domain string) bool {
	for _, configured := range authority.domains {
		if strings.EqualFold(configured, domain) {
			return true
		}
	}
	return false
}

func grantForDomain(grants []authorityGrant, domain string) (authorityGrant, bool) {
	for _, grant := range grants {
		if grant.DomainUUID == domain {
			return grant, true
		}
	}
	return authorityGrant{}, false
}

func grantSetMatches(grants []authorityGrant, domains []string) bool {
	if len(grants) != len(domains) {
		return false
	}
	for _, domain := range domains {
		if _, exists := grantForDomain(grants, domain); !exists {
			return false
		}
	}
	return true
}

func (authority *PollAuthority) renew(ctx context.Context, active authorityRecord, version string) error {
	ticker := time.NewTicker(authority.renewalInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		next, nextVersion, err := authority.renewOnce(ctx, active, version)
		if err != nil {
			authority.clearToken()
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			guardErr := authority.closeMutationGuards(cleanupCtx, active.Owner, active.Epoch)
			cancel()
			return errors.Join(err, guardErr)
		}
		active, version = next, nextVersion
	}
}

func (authority *PollAuthority) renewOnce(ctx context.Context, active authorityRecord, version string) (authorityRecord, string, error) {
	current, actualVersion, err := authority.kubernetes.Read(ctx)
	if err != nil {
		return authorityRecord{}, "", err
	}
	if actualVersion != version || current.State != authorityActive || current.Owner != authority.owner ||
		current.OwnerDomainUUID != authority.ownerDomain || current.Epoch != active.Epoch ||
		current.RenewalSeq != active.RenewalSeq {
		return authorityRecord{}, "", errAuthorityConflict
	}
	if !grantSetMatches(current.GrantSet, authority.domains) {
		return authorityRecord{}, "", errors.New("active grant set does not cover all configured domains")
	}
	if current.RenewalSeq == math.MaxInt64 {
		return authorityRecord{}, "", errors.New("authority renewal sequence exhausted")
	}
	next := current
	next.RenewalSeq++
	next.GrantSet = make([]authorityGrant, 0, len(authority.domains))
	for _, domain := range authority.domains {
		prior, exists := grantForDomain(current.GrantSet, domain)
		if !exists {
			return authorityRecord{}, "", errors.New("active grant set omitted configured domain")
		}
		live, err := authority.host.Current(ctx, domain)
		if err != nil {
			return authorityRecord{}, "", fmt.Errorf("verify current host grant for %q: %w", domain, err)
		}
		if !validGrant(live, domain, current.Epoch, current.RenewalSeq, prior.HostBootID) {
			return authorityRecord{}, "", fmt.Errorf("host grant changed or expired for %q", domain)
		}
		grant, err := authority.host.Grant(ctx, authority.identity(next, domain))
		if err != nil {
			return authorityRecord{}, "", err
		}
		if !validGrant(grant, domain, current.Epoch, next.RenewalSeq, prior.HostBootID) {
			return authorityRecord{}, "", errors.New("host returned invalid renewal grant")
		}
		next.GrantSet = append(next.GrantSet, grant)
	}
	updated, err := authority.kubernetes.CompareAndSwap(ctx, actualVersion, next)
	if err != nil {
		return authorityRecord{}, "", err
	}
	return next, updated, nil
}

func (authority *PollAuthority) activateGuards(ctx context.Context, record authorityRecord) error {
	for _, pool := range authority.authorityPools {
		command, err := pool.Exec(ctx, `UPDATE mutation_guard SET owner=$1, epoch=$2, state='active'
			WHERE singleton AND state='closed' AND epoch < $2 AND pg_generation > 0 AND resource_identity <> ''`, record.Owner, record.Epoch)
		if err != nil {
			return storeError("activate mutation guard", err)
		}
		if command.RowsAffected() == 1 {
			continue
		}
		var state, owner string
		var epoch int64
		err = pool.QueryRow(ctx, `SELECT state, owner, epoch FROM mutation_guard WHERE singleton`).Scan(&state, &owner, &epoch)
		if err != nil {
			return storeError("verify mutation guard activation", err)
		}
		if state != "active" || owner != record.Owner || epoch != record.Epoch {
			return errors.New("mutation guard was not closed at a valid PostgreSQL generation")
		}
	}
	return nil
}
func (authority *PollAuthority) verifyActiveGuards(ctx context.Context, record authorityRecord) error {
	for _, pool := range authority.authorityPools {
		var state, owner, resource string
		var epoch, generation int64
		err := pool.QueryRow(ctx, `SELECT state, owner, epoch, pg_generation, resource_identity
			FROM mutation_guard WHERE singleton`).Scan(&state, &owner, &epoch, &generation, &resource)
		if err != nil {
			return storeError("read activated mutation guard", err)
		}
		if state != "active" || owner != record.Owner || epoch != record.Epoch || generation < 1 || resource == "" {
			return errors.New("mutation guard lacks active authority or initialized PostgreSQL generation/resource identity")
		}
	}
	return nil
}

func (authority *PollAuthority) closeMutationGuards(ctx context.Context, owner string, epoch int64) error {
	var closeErr error
	for _, pool := range authority.authorityPools {
		_, err := pool.Exec(ctx, `UPDATE mutation_guard SET state='closed', owner='', epoch=$2
			WHERE singleton AND ((state='active' AND owner=$1 AND epoch=$2) OR (state='closed' AND epoch <= $2))`, owner, epoch)
		if err != nil {
			closeErr = errors.Join(closeErr, storeError("close mutation guard", err))
			continue
		}
		var state, currentOwner string
		var currentEpoch int64
		err = pool.QueryRow(ctx, `SELECT state, owner, epoch FROM mutation_guard WHERE singleton`).Scan(&state, &currentOwner, &currentEpoch)
		if err != nil {
			closeErr = errors.Join(closeErr, storeError("verify closed mutation guard", err))
			continue
		}
		if state != "closed" || currentOwner != "" || currentEpoch != epoch {
			closeErr = errors.Join(closeErr, errors.New("mutation guard high-water conflicts with revoke epoch"))
		}
	}
	return closeErr
}

func (authority *PollAuthority) close(ctx context.Context, active authorityRecord, _ string, mode hostRevokeMode) error {
	current, version, err := authority.kubernetes.Read(ctx)
	if err == nil && current.Owner == active.Owner && current.OwnerDomainUUID == active.OwnerDomainUUID &&
		current.Epoch == active.Epoch && (current.State == authorityActive || current.State == authorityClosing) {
		active = current
	} else if err == nil {
		err = errAuthorityConflict
	}
	authority.clearToken()
	_, _, closeErr := authority.closeRecord(ctx, active, version, mode)
	return errors.Join(err, closeErr)
}

func (authority *PollAuthority) clearToken() {
	authority.mu.Lock()
	authority.token = nil
	authority.mu.Unlock()
}
