//go:build !windows

package alert

import (
	"os"
	"syscall"
)

// lockFile takes a blocking exclusive flock on f. Dispatch critical sections
// are short, so concurrent processes queue instead of failing.
func lockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
