//go:build !windows

package state

import (
	"errors"
	"os"
	"syscall"
	"time"
)

// lockFile acquires the process-exclusive store lock in non-blocking mode.
func lockFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return ErrBusy
		}
		return err
	}
	return nil
}

// acquireLock blocks (up to timeout) until the process-exclusive store lock is
// available, letting a concurrent trigger wait for an in-flight critical
// section rather than failing on transient lock contention. A timeout returns
// ErrBusy.
func acquireLock(f *os.File, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := lockFile(f)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrBusy) {
			return err
		}
		if time.Now().After(deadline) {
			return ErrBusy
		}
		select {
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
