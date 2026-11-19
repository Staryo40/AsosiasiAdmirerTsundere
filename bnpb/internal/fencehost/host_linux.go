//go:build linux

package fencehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

func nativeStateLock(ctx context.Context, stateDir string) (func(), error) {
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return nil, fmt.Errorf("create host state directory: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(stateDir, "lease-state.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open host state lock: %w", err)
	}
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure host state lock: %w", err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
				_ = file.Close()
			}, nil
		}
		if err != unix.EWOULDBLOCK && err != unix.EAGAIN {
			_ = file.Close()
			return nil, fmt.Errorf("lock host lease state: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, fmt.Errorf("wait for host lease state lock: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func readLockedState(stateDir string) (diskState, error) {
	return readState(filepath.Join(stateDir, "lease-state.json"))
}

func (nativeOps) lockState(ctx context.Context, stateDir string) (func(), error) {
	return nativeStateLock(ctx, stateDir)
}

func (nativeOps) persistState(stateDir string, state diskState) error {
	return persistState(stateDir, state)
}

func (nativeOps) bootID() (string, error) {
	contents, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	bootID := strings.TrimSpace(string(contents))
	if !domainPattern.MatchString(bootID) {
		return "", errors.New("kernel boot ID is invalid")
	}
	return strings.ToLower(bootID), nil
}

func (nativeOps) boottimeNS() (int64, error) {
	var value unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &value); err != nil {
		return 0, err
	}
	return value.Nano(), nil
}

func (nativeOps) arm(ctx context.Context, stateDir, domain string, epoch, seq uint64, deadline int64) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	args := timerArguments(executable, stateDir, domain, epoch, seq, deadline)
	commandCtx, cancel := boundedContext(ctx)
	defer cancel()
	output, err := exec.CommandContext(commandCtx, "systemd-run", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemd-run failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (nativeOps) timerArmed(ctx context.Context, domain string, epoch, seq uint64) (bool, error) {
	commandCtx, cancel := boundedContext(ctx)
	defer cancel()
	output, err := exec.CommandContext(commandCtx, "systemctl", "show", timerName(domain, epoch, seq), "--property=ActiveState", "--value").Output()
	if err != nil {
		return false, fmt.Errorf("inspect systemd fence timer: %w", err)
	}
	return strings.TrimSpace(string(output)) == "active", nil
}
func (nativeOps) retireTimer(ctx context.Context, domain string, epoch, seq uint64) error {
	commandCtx, cancel := boundedContext(ctx)
	defer cancel()
	unit := timerName(domain, epoch, seq)
	output, err := exec.CommandContext(commandCtx, "systemctl", "stop", unit).CombinedOutput()
	if err != nil && !strings.Contains(string(output), "not loaded") {
		return errors.New("retire systemd fence timer failed")
	}
	return nil
}

func (nativeOps) fence(ctx context.Context, domain string) error {
	const shutOff = "shut off"
	commandCtx, cancel := boundedContext(ctx)
	defer cancel()
	if err := runVirsh(commandCtx, "autostart", "--disable", domain); err != nil {
		return err
	}
	state, err := virshCommand(commandCtx, "domstate", domain).Output()
	if err == nil && string(bytes.TrimSpace(state)) == shutOff {
		return nil
	}

	// A concurrent shutdown can make destroy fail even though the VM is off.
	destroyErr := runVirsh(commandCtx, "destroy", domain)
	state, err = virshCommand(commandCtx, "domstate", domain).Output()
	if err != nil {
		return errors.Join(destroyErr, fmt.Errorf("verify domain state: %w", err))
	}
	state = bytes.TrimSpace(state)
	if string(state) != shutOff {
		return errors.Join(destroyErr, fmt.Errorf("domain is not shut off: %q", state))
	}
	return nil
}

func virshCommand(ctx context.Context, args ...string) *exec.Cmd {
	command := exec.CommandContext(ctx, "virsh", args...)
	command.Env = append(command.Environ(), "LC_ALL=C")
	return command
}

func runVirsh(ctx context.Context, args ...string) error {
	output, err := virshCommand(ctx, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("virsh %s failed: %w: %s", filepath.Base(args[0]), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func Expire(stateDir, domain string, epoch, seq uint64) error {
	if !domainPattern.MatchString(domain) {
		return errors.New("invalid expiry domain UUID")
	}
	failClosed := func(message string) error {
		ctx, cancel := context.WithTimeout(context.Background(), nativeCommandBudget)
		defer cancel()
		if err := (nativeOps{}).fence(ctx, strings.ToLower(domain)); err != nil {
			return errors.New(message + "; validated-domain fence failed")
		}
		return errors.New(message + "; domain fenced without changing high-water state")
	}
	body, err := os.ReadFile(filepath.Join(stateDir, "resource-bindings.json"))
	if err != nil {
		return failClosed("persisted resource bindings are unavailable")
	}
	var bindings ResourceBindings
	if json.Unmarshal(body, &bindings) != nil || len(bindings.Domains) == 0 {
		return failClosed("persisted resource bindings are invalid")
	}
	allowed := make([]string, 0, len(bindings.Domains))
	for _, binding := range bindings.Domains {
		allowed = append(allowed, binding.DomainUUID)
	}
	if err := bindings.Validate(allowed); err != nil {
		return failClosed("persisted resource bindings are invalid")
	}
	host := &Host{stateDir: stateDir, ops: nativeOps{}, bindings: map[string]DomainResources{}, domains: map[string]struct{}{}}
	for _, binding := range bindings.Domains {
		key := strings.ToLower(binding.DomainUUID)
		host.bindings[key] = binding
		host.domains[key] = struct{}{}
	}
	if _, ok := host.domains[strings.ToLower(domain)]; !ok {
		return failClosed("timer domain is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), nativeCommandBudget*2)
	defer cancel()
	return host.Expire(ctx, domain, epoch, seq)
}

func Recover(stateDir string, domains []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), nativeCommandBudget)
	defer cancel()
	unlock, err := nativeStateLock(ctx, stateDir)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := readLockedState(stateDir)
	if err != nil {
		return err
	}
	ops := nativeOps{}
	bootID, err := ops.bootID()
	if err != nil {
		return err
	}
	configured := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		if !domainPattern.MatchString(domain) {
			return fmt.Errorf("invalid domain UUID %q", domain)
		}
		key := strings.ToLower(domain)
		configured[key] = struct{}{}
		if err := ops.fence(ctx, key); err != nil {
			return fmt.Errorf("fail-closed recovery fence %s: %w", key, err)
		}
	}
	for domain := range state.Domains {
		if _, ok := configured[domain]; !ok {
			return fmt.Errorf("recovery allowlist omits persisted domain %s", domain)
		}
	}
	next := cloneState(state)
	for domain := range configured {
		record := next.Domains[domain]
		record.Grant = nil
		record.Closed = true
		next.Domains[domain] = record
	}
	next.BootID = bootID
	return persistState(stateDir, next)
}
