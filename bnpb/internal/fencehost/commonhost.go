package fencehost

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const nativeCommandBudget = 4 * time.Second
const expiryBudget = FenceLead - 3*time.Second

var errStaleExpiry = errors.New("timer identity is no longer active")

func boundedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, nativeCommandBudget)
}

func closureContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// A committed closing operation cannot depend on the caller staying connected.
	return context.WithTimeout(context.WithoutCancel(ctx), expiryBudget)
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
	deadline := time.Now().Add(expiryBudget)
	waitCtx, cancelWait := context.WithDeadline(ctx, deadline)
	defer cancelWait()
	if !domainPattern.MatchString(domain) {
		return errors.New("invalid domain UUID")
	}
	domain = strings.ToLower(domain)
	unlock, err := h.lockDomain(waitCtx, domain)
	if err != nil {
		return err
	}
	defer unlock()
	diskBootID, record, err := h.readDomain(waitCtx, domain)
	if errors.Is(err, errDurableStateUnavailable) {
		return failClosedWithoutState(waitCtx, h.ops, domain, err)
	}
	if err != nil {
		return err
	}
	if record.Grant == nil || record.HighEpoch != epoch || record.HighSeq != seq {
		return errStaleExpiry
	}
	bootID, err := h.ops.bootID()
	if err != nil || bootID != diskBootID {
		return failClosedWithoutState(waitCtx, h.ops, domain, errors.New("host boot identity is unavailable"))
	}
	now, err := h.ops.boottimeNS()
	// Only a readable clock can prove that fencing is premature.
	if record.Closing == "" && err == nil && now < record.Grant.DeadlineBootNS-int64(FenceLead) {
		return errors.New("fence timer ran before its authorized deadline")
	}
	if err := waitCtx.Err(); err != nil {
		return err
	}
	// Keep the original timer budget, even if the caller cancels during closure.
	actionCtx, cancelAction := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancelAction()
	return h.closeExpiredGrant(actionCtx, domain, record)
}

func (h *Host) closeExpiredGrant(ctx context.Context, domain string, record domainState) error {
	identity := Identity{DomainUUID: domain, Epoch: record.HighEpoch, RenewalSeq: record.HighSeq, Owner: record.Owner, OwnerDomainUUID: record.OwnerDomainUUID}
	if err := h.markClosing(ctx, domain, record, RevokeTakeover); err != nil {
		// The exact grant is still serialized. A failed durable mark rules out
		// graceful closure, but both fences must still be attempted.
		return errors.Join(err, h.fenceGrantDomains(ctx, domain, record.OwnerDomainUUID))
	}
	return h.revokeClosingGrant(ctx, identity, record, RevokeTakeover)
}

func (h *Host) closeGrantResource(ctx context.Context, identity Identity) error {
	if err := h.revokeResources(ctx, identity.DomainUUID, identity); err != nil {
		fenceCtx, cancelFence := boundedContext(ctx)
		defer cancelFence()
		if err := h.ops.fence(fenceCtx, identity.DomainUUID); err != nil {
			return errors.New("guard closure uncertain and domain fence failed")
		}
	}
	return nil
}

func (h *Host) fenceOwnerDomain(ctx context.Context, domain string) error {
	fenceCtx, cancelFence := boundedContext(ctx)
	defer cancelFence()
	if err := h.ops.fence(fenceCtx, strings.ToLower(domain)); err != nil {
		return errors.New("owner execution domain could not be fenced")
	}
	return nil
}

func (h *Host) fenceGrantDomains(ctx context.Context, domain, ownerDomain string) error {
	fenceCtx, cancelFence := boundedContext(ctx)
	resourceErr := h.ops.fence(fenceCtx, domain)
	cancelFence()
	if strings.EqualFold(domain, ownerDomain) {
		return resourceErr
	}
	return errors.Join(resourceErr, h.fenceOwnerDomain(ctx, ownerDomain))
}

func (h *Host) protectExpiredGrant(ctx context.Context, identity Identity) error {
	resourceErr := h.closeGrantResource(ctx, identity)
	// The old owner can still poll even if resource fencing failed.
	return errors.Join(resourceErr, h.fenceOwnerDomain(ctx, identity.OwnerDomainUUID))
}
