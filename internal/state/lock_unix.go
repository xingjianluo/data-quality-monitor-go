//go:build !windows

package state

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
)

// tryLockFile attempts a non-blocking exclusive flock. It returns
// locked=false when another open file description already holds the lock.
func tryLockFile(f *os.File) (bool, error) {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return false, nil
	}
	return false, err
}

// lockWithContext waits for an exclusive flock, honoring ctx. The wait polls a
// non-blocking lock so cancellation cannot leave a blocked syscall behind.
func lockWithContext(ctx context.Context, f *os.File) error {
	if locked, err := tryLockFile(f); err != nil {
		return err
	} else if locked {
		return nil
	}
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			locked, err := tryLockFile(f)
			if err != nil {
				return err
			}
			if locked {
				return nil
			}
		}
	}
}

func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
