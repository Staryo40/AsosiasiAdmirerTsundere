package fencehost

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

type nativeOps struct{}

func timerName(domain string, epoch, seq uint64) string {
	return fmt.Sprintf("bnpb-fence-%s-%d-%d.timer", strings.ReplaceAll(domain, "-", ""), epoch, seq)
}

func timerArguments(executable, stateDir, domain string, epoch, seq uint64, hardDeadline int64) []string {
	unit := strings.TrimSuffix(timerName(domain, epoch, seq), ".timer")
	fenceAt := hardDeadline - int64(FenceLead)
	return []string{"--unit=" + unit, "--on-boot=" + time.Duration(fenceAt).String(), "--timer-property=WakeSystem=yes", "--timer-property=AccuracySec=1s", "--timer-property=RandomizedDelaySec=0", "--property=Restart=on-failure", "--", executable, "fence-expire", "--state-dir", stateDir, "--domain", domain, "--epoch", fmt.Sprint(epoch), "--seq", fmt.Sprint(seq)}
}

const (
	// Contract README.md §Authority and fencing: begin fencing ten seconds before hard expiry.
	LeaseTTL       = 20 * time.Second
	FenceLead      = 10 * time.Second
	operationLimit = 2 * time.Second
)

var domainPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

var errRevokeIdentityConflict = errors.New("revoke identity does not match active grant")
var errDurableStateUnavailable = errors.New("durable host state is unavailable")
var errDomainPersistence = errors.New("could not persist domain state")
var errGrantExpiring = errors.New("expired grant requires a new epoch")

type Grant struct {
	DomainUUID     string `json:"domain_uuid"`
	HostBootID     string `json:"host_boot_id"`
	Epoch          uint64 `json:"epoch"`
	RenewalSeq     uint64 `json:"renewal_seq"`
	DeadlineBootNS int64  `json:"deadline_boottime_ns"`
	Owner          string `json:"-"`
}

type Identity struct {
	DomainUUID      string `json:"domain_uuid"`
	Epoch           uint64 `json:"epoch"`
	RenewalSeq      uint64 `json:"renewal_seq"`
	Owner           string `json:"owner"`
	OwnerDomainUUID string `json:"owner_domain_uuid"`
}

type RevokeMode string

const (
	RevokeGraceful RevokeMode = "graceful"
	RevokeTakeover RevokeMode = "takeover"
)

type domainState struct {
	HighEpoch       uint64     `json:"high_epoch"`
	HighSeq         uint64     `json:"high_seq"`
	Owner           string     `json:"owner,omitempty"`
	OwnerDomainUUID string     `json:"owner_domain_uuid,omitempty"`
	Closed          bool       `json:"closed"`
	Closing         RevokeMode `json:"closing,omitempty"`
	Grant           *Grant     `json:"grant,omitempty"`
}

type diskState struct {
	BootID  string                 `json:"host_boot_id"`
	Domains map[string]domainState `json:"domains"`
}

type hostOps interface {
	bootID() (string, error)
	boottimeNS() (int64, error)
	arm(context.Context, string, string, uint64, uint64, int64) error
	timerArmed(context.Context, string, uint64, uint64) (bool, error)
	fence(context.Context, string) error
	closeGuard(context.Context, GuardTarget, Identity) error
	lockState(context.Context, string) (func(), error)
	persistState(string, diskState) error
}

type Host struct {
	mu       sync.Mutex // Protects the in-process high-water cache while the state file lock is held.
	stateDir string
	token    []byte
	domains  map[string]struct{}
	bindings map[string]DomainResources
	ops      hostOps
	state    diskState
}

func Open(stateDir, token string, domains []string, bindings ResourceBindings) (*Host, error) {
	if err := bindings.Validate(domains); err != nil {
		_ = FailClosedDomains(domains)
		return nil, err
	}
	if err := persistResourceBindings(filepath.Join(stateDir, "resource-bindings.json"), bindings); err != nil {
		_ = FailClosedDomains(domains)
		return nil, err
	}
	host, err := openWithOps(stateDir, token, domains, nativeOps{}, bindings)
	if err != nil {
		_ = FailClosedDomains(domains)
		return nil, err
	}
	return host, nil
}

func openWithOps(stateDir, token string, domains []string, ops hostOps, configurations ...ResourceBindings) (*Host, error) {
	if token == "" || len(domains) == 0 || len(configurations) > 1 {
		return nil, errors.New("bearer token and allowed domains are required")
	}
	allow := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		if !domainPattern.MatchString(domain) {
			return nil, errors.New("invalid allowed domain UUID")
		}
		allow[strings.ToLower(domain)] = struct{}{}
	}
	bindings := map[string]DomainResources{}
	if len(configurations) > 0 {
		if err := configurations[0].Validate(domains); err != nil {
			return nil, err
		}
		for _, binding := range configurations[0].Domains {
			bindings[strings.ToLower(binding.DomainUUID)] = binding
		}
	} else if _, native := ops.(nativeOps); native {
		return nil, errors.New("resource bindings are required")
	}
	bootID, err := ops.bootID()
	if err != nil {
		return nil, fmt.Errorf("read host boot ID: %w", err)
	}
	h := &Host{stateDir: stateDir, token: []byte(token), domains: allow, bindings: bindings, ops: ops}
	state, err := readState(filepath.Join(stateDir, "lease-state.json"))
	if err != nil {
		return nil, err
	}
	if state.BootID == "" || state.BootID != bootID {
		return nil, errors.New("missing or changed host boot ID; explicit fail-closed recovery required")
	}
	h.state = state
	for domain, record := range state.Domains {
		if _, ok := allow[domain]; !ok {
			return nil, errors.New("state contains a non-allowlisted domain")
		}
		if record.Grant == nil {
			continue
		}
		if err := h.checkStartupGrant(domain); err != nil {
			return nil, err
		}
	}
	return h, nil
}

func readState(path string) (diskState, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return diskState{}, fmt.Errorf("read durable lease state: %w", err)
	}
	var state diskState
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return diskState{}, fmt.Errorf("decode durable lease state: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return diskState{}, errors.New("durable lease state has trailing data")
	}
	if state.BootID == "" || state.Domains == nil {
		return diskState{}, errors.New("durable lease state is incomplete")
	}
	for domain, record := range state.Domains {
		if !domainPattern.MatchString(domain) || domain != strings.ToLower(domain) || record.HighEpoch == 0 && record.HighSeq != 0 || record.HighEpoch != 0 && record.HighSeq == 0 {
			return diskState{}, errors.New("durable lease high-water state is invalid")
		}
		if record.Closed && (record.Closing != "" || record.Grant != nil) ||
			record.Closing != "" && (record.Closing != RevokeGraceful && record.Closing != RevokeTakeover || record.HighEpoch == 0 || record.Grant == nil) ||
			!record.Closed && record.HighEpoch > 0 && record.Grant == nil {
			return diskState{}, errors.New("durable lease state has an invalid closure/grant combination")
		}
		if record.Grant != nil && (record.Grant.DomainUUID != domain || record.Grant.Epoch != record.HighEpoch || record.Grant.RenewalSeq != record.HighSeq || record.Grant.HostBootID != state.BootID || record.Owner == "" || !domainPattern.MatchString(record.OwnerDomainUUID) || record.Grant.DeadlineBootNS <= 0) {
			return diskState{}, errors.New("durable grant identity does not match its high-water state")
		}
	}
	return state, nil
}

func Initialize(stateDir string) error {
	bootID, err := nativeOps{}.bootID()
	if err != nil {
		return fmt.Errorf("read host boot ID: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(stateDir), 0700); err != nil {
		return fmt.Errorf("create state parent directory: %w", err)
	}
	if err := os.Mkdir(stateDir, 0700); err != nil {
		return fmt.Errorf("state directory already exists or cannot be created; lost state requires fail-closed recovery: %w", err)
	}
	return persistState(stateDir, diskState{BootID: bootID, Domains: map[string]domainState{}})
}
func InitializeWithResources(stateDir string, domains []string, bindings ResourceBindings) error {
	if err := bindings.Validate(domains); err != nil {
		return err
	}
	if err := Initialize(stateDir); err != nil {
		return err
	}
	if err := persistResourceBindings(filepath.Join(stateDir, "resource-bindings.json"), bindings); err != nil {
		return err
	}
	return nil
}

func persistState(dir string, state diskState) error {
	return persistStateWithDirectorySync(dir, state, func(dir string) error {
		directory, err := os.Open(dir)
		if err != nil {
			return err
		}
		defer directory.Close()
		return directory.Sync()
	})
}

func persistStateWithDirectorySync(dir string, state diskState, syncDir func(string) error) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	contents, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode lease state: %w", err)
	}
	file, err := os.CreateTemp(dir, ".lease-state-*")
	if err != nil {
		return fmt.Errorf("create state temporary file: %w", err)
	}
	temp := file.Name()
	defer os.Remove(temp)
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(contents); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp, filepath.Join(dir, "lease-state.json")); err != nil {
		return fmt.Errorf("replace durable lease state: %w", err)
	}
	return syncDir(dir)
}

func (h *Host) lockDomain(ctx context.Context, domain string) (func(), error) {
	return h.ops.lockState(ctx, filepath.Join(h.stateDir, "domain-"+domain+".lock"))
}

func (h *Host) lockLeaseState(ctx context.Context) (func(), error) {
	unlock, err := h.ops.lockState(ctx, filepath.Join(h.stateDir, "lease-state.lock"))
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	if err := h.refreshState(); err != nil {
		h.mu.Unlock()
		unlock()
		return nil, err
	}
	return func() {
		h.mu.Unlock()
		unlock()
	}, nil
}

func (h *Host) refreshState() error {
	state, err := readState(filepath.Join(h.stateDir, "lease-state.json"))
	if err != nil {
		return fmt.Errorf("%w: %w", errDurableStateUnavailable, err)
	}
	if h.state.BootID != "" && state.BootID != h.state.BootID {
		return fmt.Errorf("%w: host boot ID changed; fail-closed recovery required", errDurableStateUnavailable)
	}
	for domain, previous := range h.state.Domains {
		current, ok := state.Domains[domain]
		if !ok || current.HighEpoch < previous.HighEpoch || current.HighEpoch == previous.HighEpoch && current.HighSeq < previous.HighSeq {
			return fmt.Errorf("%w: lease high-water state regressed", errDurableStateUnavailable)
		}
		if current.HighEpoch == previous.HighEpoch && (previous.Closed && !current.Closed ||
			previous.Closing != "" && (current.HighSeq != previous.HighSeq || current.Closing == "" && !current.Closed) ||
			previous.Closing == RevokeTakeover && current.Closing == RevokeGraceful) {
			return fmt.Errorf("%w: closing grant was reopened", errDurableStateUnavailable)
		}
	}
	h.state = state
	return nil
}

func (h *Host) readDomain(ctx context.Context, domain string) (string, domainState, error) {
	unlock, err := h.lockLeaseState(ctx)
	if err != nil {
		return "", domainState{}, err
	}
	defer unlock()
	return h.state.BootID, h.state.Domains[domain], nil
}

// The domain lock keeps the target record stable across I/O. Reload siblings
// while holding the state lock so their commits are not overwritten.
func (h *Host) writeDomain(ctx context.Context, domain string, record domainState) error {
	unlock, err := h.lockLeaseState(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	return h.saveDomainLocked(domain, record)
}

func (h *Host) recordGrant(ctx context.Context, domain string, record domainState) error {
	unlock, err := h.lockLeaseState(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	if current := h.state.Domains[domain]; current.Grant != nil {
		now, err := h.ops.boottimeNS()
		if err != nil || current.Grant.DeadlineBootNS-int64(FenceLead) <= now {
			return errGrantExpiring
		}
	}
	return h.saveDomainLocked(domain, record)
}

func (h *Host) saveDomainLocked(domain string, record domainState) error {
	previous, existed := h.state.Domains[domain]
	h.state.Domains[domain] = record
	if err := h.ops.persistState(h.stateDir, h.state); err != nil {
		if existed {
			h.state.Domains[domain] = previous
		} else {
			delete(h.state.Domains, domain)
		}
		return fmt.Errorf("%w: %w", errDomainPersistence, err)
	}
	return nil
}

func (h *Host) markClosing(ctx context.Context, domain string, record domainState, mode RevokeMode) error {
	if record.Closing == RevokeTakeover || record.Closing == mode {
		return nil
	}
	record.Closing = mode
	return h.writeDomain(ctx, domain, record)
}

func (h *Host) finishClosing(ctx context.Context, domain string, record domainState) error {
	record.Closing = ""
	record.Closed = true
	record.Grant = nil
	return h.writeDomain(ctx, domain, record)
}

func (h *Host) failClosedGrant(ctx context.Context, domain string, record domainState) error {
	fenceCtx, cancel := closureContext(ctx)
	defer cancel()
	markErr := h.markClosing(fenceCtx, domain, record, RevokeTakeover)
	if err := h.fenceGrantDomains(fenceCtx, domain, record.OwnerDomainUUID); err != nil {
		return errors.Join(markErr, err)
	}
	if markErr != nil {
		return markErr
	}
	return h.finishClosing(fenceCtx, domain, record)
}

func (h *Host) checkStartupGrant(domain string) error {
	ctx, cancel := context.WithTimeout(context.Background(), expiryBudget)
	defer cancel()
	unlock, err := h.lockDomain(ctx, domain)
	if err != nil {
		return err
	}
	defer unlock()
	_, record, err := h.readDomain(ctx, domain)
	if err != nil || record.Grant == nil {
		return err
	}
	if record.Closing != "" {
		return h.closeExpiredGrant(ctx, domain, record)
	}
	now, err := h.ops.boottimeNS()
	if err != nil {
		return errors.New("host boottime unavailable")
	}
	armed, timerErr := h.ops.timerArmed(ctx, domain, record.HighEpoch, record.HighSeq)
	if record.Grant.DeadlineBootNS > now && timerErr == nil && armed {
		return nil
	}
	return h.failClosedGrant(ctx, domain, record)
}

func (h *Host) timerReady(ctx context.Context, domain string, epoch, seq uint64, deadline int64) error {
	now, err := h.ops.boottimeNS()
	if err != nil {
		return fmt.Errorf("read host boottime: %w", err)
	}
	if deadline <= now {
		return errors.New("grant deadline elapsed before activation")
	}
	armed, err := h.ops.timerArmed(ctx, domain, epoch, seq)
	if err != nil {
		return fmt.Errorf("inspect independent fence timer: %w", err)
	}
	if !armed {
		return errors.New("independent fence timer is not active")
	}
	return nil
}
func (h *Host) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !h.authorized(r) {
		writeError(w, http.StatusUnauthorized, "invalid bearer credential")
		return
	}
	switch {
	case r.URL.Path == "/v1/grant" && r.Method == http.MethodPost:
		h.createGrant(w, r)
	case r.URL.Path == "/v1/grant" && r.Method == http.MethodGet:
		h.getGrant(w, r)
	case r.URL.Path == "/v1/revoke" && r.Method == http.MethodPost:
		h.revoke(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (h *Host) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(header, prefix)), h.token) == 1
}

type grantRequest struct{ Identity }

type revokeRequest struct {
	Identity
	Mode RevokeMode `json:"mode"`
}

func (h *Host) createGrant(w http.ResponseWriter, r *http.Request) {
	var request grantRequest
	if !decodeBody(w, r, &request) || !h.validIdentity(request.Identity) {
		writeError(w, http.StatusBadRequest, "invalid grant request")
		return
	}
	domain := strings.ToLower(request.DomainUUID)
	ctx, cancel := context.WithTimeout(r.Context(), operationLimit)
	defer cancel()
	unlock, err := h.lockDomain(ctx, domain)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "durable host state is unavailable")
		return
	}
	defer unlock()
	bootID, current, err := h.readDomain(ctx, domain)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "durable host state is unavailable")
		return
	}
	if current.Closing != "" {
		writeError(w, http.StatusConflict, "previous grant is closing")
		return
	}
	ownerDomain := strings.ToLower(request.OwnerDomainUUID)
	if request.Epoch < current.HighEpoch || request.Epoch == current.HighEpoch && request.RenewalSeq < current.HighSeq {
		writeError(w, http.StatusConflict, "stale grant")
		return
	}
	if current.Grant != nil && request.Epoch == current.HighEpoch && request.RenewalSeq == current.HighSeq {
		if current.Owner != request.Owner || current.OwnerDomainUUID != ownerDomain || current.Closed {
			writeError(w, http.StatusConflict, "grant identity conflict")
			return
		}
		now, clockErr := h.ops.boottimeNS()
		if clockErr != nil || current.Grant.DeadlineBootNS-int64(FenceLead) <= now {
			writeError(w, http.StatusServiceUnavailable, "grant is expiring")
			return
		}
		armed, timerErr := h.ops.timerArmed(ctx, domain, current.HighEpoch, current.HighSeq)
		if timerErr != nil || !armed {
			if ctx.Err() == nil {
				_ = h.failClosedGrant(ctx, domain, current)
			}
			writeError(w, http.StatusServiceUnavailable, "grant timer is unavailable")
			return
		}
		writeJSON(w, http.StatusOK, current.Grant)
		return
	}
	renewal := current.Grant != nil && request.Epoch == current.HighEpoch && current.HighSeq != ^uint64(0) && request.RenewalSeq == current.HighSeq+1
	if renewal {
		if current.Closed || current.Owner != request.Owner || current.OwnerDomainUUID != ownerDomain {
			writeError(w, http.StatusConflict, "grant identity conflict")
			return
		}
		now, clockErr := h.ops.boottimeNS()
		if clockErr != nil || current.Grant.DeadlineBootNS-int64(FenceLead) <= now {
			writeError(w, http.StatusConflict, "expired grant requires a new epoch")
			return
		}
	} else if current.Grant != nil || request.RenewalSeq != 1 || request.Epoch <= current.HighEpoch || current.HighEpoch > 0 && !current.Closed {
		writeError(w, http.StatusConflict, "previous grant must be closed before a new epoch")
		return
	}
	now, err := h.ops.boottimeNS()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "host clock unavailable")
		return
	}
	deadline := now + int64(LeaseTTL)
	grant := &Grant{DomainUUID: domain, HostBootID: bootID, Epoch: request.Epoch, RenewalSeq: request.RenewalSeq, DeadlineBootNS: deadline, Owner: request.Owner}
	if err := h.ops.arm(ctx, h.stateDir, domain, grant.Epoch, grant.RenewalSeq, deadline); err != nil {
		writeError(w, http.StatusServiceUnavailable, "independent fence timer could not be armed")
		return
	}
	if err := h.timerReady(ctx, domain, grant.Epoch, grant.RenewalSeq, deadline); err != nil {
		writeError(w, http.StatusServiceUnavailable, "independent fence timer is not safely armed")
		return
	}
	next := domainState{HighEpoch: grant.Epoch, HighSeq: grant.RenewalSeq, Owner: request.Owner, OwnerDomainUUID: ownerDomain, Grant: grant}
	if err := h.recordGrant(ctx, domain, next); err != nil {
		if errors.Is(err, errGrantExpiring) {
			writeError(w, http.StatusConflict, errGrantExpiring.Error())
			return
		}
		if errors.Is(err, errDomainPersistence) {
			_ = h.failClosedGrant(ctx, domain, next)
		}
		writeError(w, http.StatusServiceUnavailable, "grant could not be durably recorded")
		return
	}
	if err := h.timerReady(ctx, domain, grant.Epoch, grant.RenewalSeq, deadline); err != nil {
		if ctx.Err() == nil {
			_ = h.failClosedGrant(ctx, domain, next)
		}
		writeError(w, http.StatusServiceUnavailable, "grant timer was lost before activation")
		return
	}
	writeJSON(w, http.StatusOK, grant)
}

func (h *Host) getGrant(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if len(query) != 1 || len(query["domain_uuid"]) != 1 || query.Get("domain_uuid") == "" {
		writeError(w, http.StatusBadRequest, "domain_uuid is required exactly once")
		return
	}
	domain := strings.ToLower(query.Get("domain_uuid"))
	if _, ok := h.domains[domain]; !ok {
		writeError(w, http.StatusBadRequest, "domain is not allowed")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), operationLimit)
	defer cancel()
	unlock, err := h.lockDomain(ctx, domain)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "durable host state is unavailable")
		return
	}
	defer unlock()
	bootID, record, err := h.readDomain(ctx, domain)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "durable host state is unavailable")
		return
	}
	if record.Closing != "" {
		writeError(w, http.StatusServiceUnavailable, "grant closure is uncertain")
		return
	}
	if record.Grant == nil {
		writeJSON(w, http.StatusNotFound, inactiveGrant(bootID, domain, record, "no active grant"))
		return
	}
	now, err := h.ops.boottimeNS()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "host clock unavailable")
		return
	}
	if record.Grant.DeadlineBootNS <= now {
		writeJSON(w, http.StatusGone, inactiveGrant(bootID, domain, record, "grant expired"))
		return
	}
	armed, err := h.ops.timerArmed(ctx, domain, record.HighEpoch, record.HighSeq)
	if err != nil || !armed {
		if ctx.Err() != nil || h.failClosedGrant(ctx, domain, record) != nil {
			writeError(w, http.StatusServiceUnavailable, "grant timer state is unavailable and fail-closed fencing failed")
			return
		}
		writeJSON(w, http.StatusGone, inactiveGrant(bootID, domain, record, "grant timer is not active"))
		return
	}
	writeJSON(w, http.StatusOK, record.Grant)
}

type inactiveGrantResponse struct {
	Error            string `json:"error"`
	DomainUUID       string `json:"domain_uuid,omitempty"`
	HostBootID       string `json:"host_boot_id,omitempty"`
	Epoch            uint64 `json:"epoch,omitempty"`
	RenewalSeq       uint64 `json:"renewal_seq,omitempty"`
	DeadlineBootTime int64  `json:"deadline_boottime_ns"`
}

func inactiveGrant(bootID, domain string, record domainState, message string) inactiveGrantResponse {
	response := inactiveGrantResponse{Error: message}
	if record.HighEpoch > 0 && record.HighSeq > 0 {
		response.DomainUUID = domain
		response.HostBootID = bootID
		response.Epoch = record.HighEpoch
		response.RenewalSeq = record.HighSeq
		if record.Grant != nil {
			response.DeadlineBootTime = record.Grant.DeadlineBootNS
		}
	}
	return response
}

func (h *Host) revoke(w http.ResponseWriter, r *http.Request) {
	var request revokeRequest
	if !decodeBody(w, r, &request) || !h.validIdentity(request.Identity) || (request.Mode != RevokeGraceful && request.Mode != RevokeTakeover) {
		writeError(w, http.StatusBadRequest, "invalid revoke request")
		return
	}
	if err := h.Revoke(r.Context(), request.Identity, request.Mode); err != nil {
		if errors.Is(err, errRevokeIdentityConflict) {
			writeError(w, http.StatusConflict, "revoke identity conflicts with current grant")
		} else {
			writeError(w, http.StatusServiceUnavailable, "grant closure is uncertain")
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Host) Revoke(ctx context.Context, identity Identity, mode RevokeMode) error {
	if !h.validIdentity(identity) || (mode != RevokeGraceful && mode != RevokeTakeover) {
		return errors.New("invalid revoke request")
	}
	domain := strings.ToLower(identity.DomainUUID)
	lockCtx, cancel := context.WithTimeout(ctx, operationLimit)
	defer cancel()
	unlock, err := h.lockDomain(lockCtx, domain)
	if err != nil {
		return err
	}
	defer unlock()
	_, current, err := h.readDomain(lockCtx, domain)
	if errors.Is(err, errDurableStateUnavailable) {
		return failClosedWithoutState(ctx, h.ops, domain, err)
	}
	if err != nil {
		return err
	}
	exact := current.HighEpoch == identity.Epoch && current.HighSeq == identity.RenewalSeq && current.Owner == identity.Owner && current.OwnerDomainUUID == strings.ToLower(identity.OwnerDomainUUID)
	if current.Closed && current.Grant == nil && exact {
		return h.retireClosedGrant(ctx, domain, current)
	}
	if !exact || current.Grant == nil || current.Closed {
		return errRevokeIdentityConflict
	}
	if current.Closing == RevokeTakeover {
		mode = RevokeTakeover
	}
	if err := h.markClosing(lockCtx, domain, current, mode); err != nil {
		return err
	}
	identity.DomainUUID = domain
	actionCtx, cancelAction := closureContext(ctx)
	defer cancelAction()
	return h.revokeClosingGrant(actionCtx, identity, current, mode)
}

func (h *Host) revokeClosingGrant(ctx context.Context, identity Identity, record domainState, mode RevokeMode) error {
	if mode == RevokeTakeover {
		if err := h.protectExpiredGrant(ctx, identity); err != nil {
			return err
		}
	} else if err := h.closeGrantResource(ctx, identity); err != nil {
		return err
	}
	if err := h.finishClosing(ctx, identity.DomainUUID, record); err != nil {
		return err
	}
	return h.retireClosedGrant(ctx, identity.DomainUUID, record)
}

func (h *Host) retireClosedGrant(ctx context.Context, domain string, record domainState) error {
	if record.HighEpoch == 0 || record.Owner == "" || record.OwnerDomainUUID == "" {
		return errors.New("closed grant identity is incomplete")
	}
	if ops, ok := h.ops.(interface {
		retireTimer(context.Context, string, uint64, uint64) error
	}); ok {
		if err := ops.retireTimer(ctx, domain, record.HighEpoch, record.HighSeq); err != nil {
			return errors.New("closed grant timer could not be retired")
		}
	}
	return nil
}

func (h *Host) validIdentity(identity Identity) bool {
	if !domainPattern.MatchString(identity.DomainUUID) || !domainPattern.MatchString(identity.OwnerDomainUUID) || identity.Epoch == 0 || identity.RenewalSeq == 0 || identity.Owner == "" || len(identity.Owner) > 128 {
		return false
	}
	_, allowed := h.domains[strings.ToLower(identity.DomainUUID)]
	_, ownerAllowed := h.domains[strings.ToLower(identity.OwnerDomainUUID)]
	return allowed && ownerAllowed
}

func decodeBody(w http.ResponseWriter, r *http.Request, target any) bool {
	if r.Header.Get("Content-Type") != "application/json" {
		return false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return false
	}
	return decoder.Decode(new(any)) == io.EOF
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
