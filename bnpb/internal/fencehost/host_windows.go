//go:build windows

package fencehost

import (
	"context"
	"errors"
)

func (nativeOps) lockState(ctx context.Context, stateDir string) (func(), error) {
	return nativeStateLock(ctx, stateDir)
}

func (nativeOps) persistState(stateDir string, state diskState) error {
	return persistState(stateDir, state)
}

func nativeStateLock(context.Context, string) (func(), error) {
	return nil, errors.New("host state serialization requires Linux flock")
}

func (nativeOps) bootID() (string, error)    { return "", errors.New("host fencing requires Linux") }
func (nativeOps) boottimeNS() (int64, error) { return 0, errors.New("host fencing requires Linux") }
func (nativeOps) arm(context.Context, string, string, uint64, uint64, int64) error {
	return errors.New("host fencing requires Linux systemd and libvirt")
}
func (nativeOps) fence(context.Context, string) error {
	return errors.New("host fencing requires Linux systemd and libvirt")
}
func (nativeOps) timerArmed(context.Context, string, uint64, uint64) (bool, error) {
	return false, errors.New("host fencing requires Linux systemd and libvirt")
}
func Recover(string, []string) error {
	return errors.New("host fencing requires Linux systemd and libvirt")
}
func Expire(string, string, uint64, uint64) error {
	return errors.New("host fencing requires Linux systemd and libvirt")
}
