//go:build !windows

package state

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func lockFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return fmt.Errorf("%w", ErrBusy)
		}
		return fmt.Errorf("acquire state lock: %w", err)
	}
	return nil
}

func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
