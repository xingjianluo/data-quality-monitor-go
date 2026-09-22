//go:build windows

package state

import "os"

// Windows file locking is not required for the local single-process test
// harness; the in-process mutex still serializes access.
func lockFile(f *os.File) error   { return nil }
func unlockFile(f *os.File) error { return nil }
