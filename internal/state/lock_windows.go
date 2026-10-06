//go:build windows

package state

import (
	"context"
	"os"
)

// Windows file locking is not required for the local single-process harness;
// the per-batch in-process shard locks in state.go still serialize access
// within one process.
func tryLockFile(f *os.File) (bool, error) { return true, nil }

func lockWithContext(_ context.Context, _ *os.File) error { return nil }

func unlockFile(f *os.File) error { return nil }
