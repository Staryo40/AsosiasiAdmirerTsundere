package fencehost

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const nativeCommandBudget = 4 * time.Second

func boundedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, nativeCommandBudget)
}

func failClosedWithoutState(ctx context.Context, ops hostOps, domain string, cause error) error {
	fenceCtx, cancelFence := boundedContext(ctx)
	defer cancelFence()
	if err := ops.fence(fenceCtx, domain); err != nil {
		return fmt.Errorf("%v; fail-closed fence failed: %w", cause, err)
	}
	return fmt.Errorf("%v; resource fenced without modifying persisted state", cause)
}
func FailClosedDomains(domains []string) error {
	ops := nativeOps{}
	for _, domain := range domains {
		if !domainPattern.MatchString(domain) {
			continue
		}
		ctx, cancel := boundedContext(context.Background())
		err := ops.fence(ctx, strings.ToLower(domain))
		cancel()
		if err != nil {
			return errors.New("fail-closed fence of a validated domain failed")
		}
	}
	return nil
}

func (h *Host) Expire(ctx context.Context, domain string, epoch, seq uint64) error {
	// Leave room for timer accuracy before the grant's hard deadline.
	ctx, cancelAction := context.WithTimeout(ctx, FenceLead-3*time.Second)
	defer cancelAction()
	if !domainPattern.MatchString(domain) {
		return errors.New("invalid domain UUID")
	}
	domain = strings.ToLower(domain)
	lockCtx, cancelLock := context.WithTimeout(ctx, time.Second)
	defer cancelLock()
	unlock, err := h.ops.lockState(lockCtx, h.stateDir)
	if err != nil {
		return failClosedWithoutState(ctx, h.ops, domain, errors.New("state serialization unavailable"))
	}
	defer unlock()
	state, err := readState(filepath.Join(h.stateDir, "lease-state.json"))
	if err != nil {
		return failClosedWithoutState(ctx, h.ops, domain, errors.New("expiry state unavailable"))
	}
	h.state = state
	record, ok := state.Domains[domain]
	if !ok || record.Closed || record.Grant == nil || record.Grant.Epoch != epoch || record.Grant.RenewalSeq != seq {
		return errors.New("timer identity is no longer active")
	}
	bootID, err := h.ops.bootID()
	if err != nil || bootID != state.BootID {
		return failClosedWithoutState(ctx, h.ops, domain, errors.New("host boot identity is unavailable"))
	}
	now, err := h.ops.boottimeNS()
	// Only a readable clock can prove that fencing is premature.
	if err == nil && now < record.Grant.DeadlineBootNS-int64(FenceLead) {
		return errors.New("fence timer ran before its authorized deadline")
	}
	identity := Identity{DomainUUID: domain, Epoch: epoch, RenewalSeq: seq, Owner: record.Owner, OwnerDomainUUID: record.OwnerDomainUUID}
	if err := h.protectExpiredGrant(ctx, identity); err != nil {
		return err
	}
	next := cloneState(state)
	next.Domains[domain] = domainState{HighEpoch: record.HighEpoch, HighSeq: record.HighSeq, Owner: record.Owner, OwnerDomainUUID: record.OwnerDomainUUID, Closed: true}
	if err := h.ops.persistState(h.stateDir, next); err != nil {
		return errors.New("closed expiry high-water could not be persisted")
	}
	h.state = next
	if ops, ok := h.ops.(interface {
		retireTimer(context.Context, string, uint64, uint64) error
	}); ok {
		if err := ops.retireTimer(ctx, domain, epoch, seq); err != nil {
			return errors.New("expired timer could not be retired")
		}
	}
	return nil
}

func (h *Host) protectExpiredGrant(ctx context.Context, identity Identity) error {
	var resourceFenceErr error
	if err := h.revokeResources(ctx, identity.DomainUUID, identity); err != nil {
		fenceCtx, cancelFence := boundedContext(ctx)
		if err := h.ops.fence(fenceCtx, identity.DomainUUID); err != nil {
			resourceFenceErr = errors.New("guard closure uncertain and domain fence failed")
		}
		cancelFence()
	}
	// The old owner can still poll even if resource fencing failed.
	fenceCtx, cancelFence := boundedContext(ctx)
	defer cancelFence()
	if err := h.ops.fence(fenceCtx, strings.ToLower(identity.OwnerDomainUUID)); err != nil {
		return errors.Join(resourceFenceErr, errors.New("expired owner execution domain could not be fenced"))
	}
	return resourceFenceErr
}
