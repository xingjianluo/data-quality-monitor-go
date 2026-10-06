//go:build windows

package alert

import (
	"context"
	"os"
)

// On Windows the per-batch in-process mutex in alert.go serializes dispatch.
func tryLockFile(f *os.File) (bool, error)                { return true, nil }
func lockWithContext(_ context.Context, _ *os.File) error { return nil }
func unlockFile(f *os.File) error                         { return nil }
