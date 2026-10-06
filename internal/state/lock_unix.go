//go:build !windows

package state

import (
	"os"
	"syscall"
)

// lockFile takes a blocking exclusive flock on f. Critical sections guarded by
// it are single-file read-modify-write cycles (microseconds), so a blocking
// lock is appropriate: concurrent processes queue instead of failing, which is
// what lets unrelated batches and read-only queries proceed independently.
func lockFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	return nil
}

func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
