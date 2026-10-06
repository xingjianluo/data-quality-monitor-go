//go:build windows

package alert

import "os"

// Windows file locking is not required for the local single-process harness;
// the in-process mutex still serializes dispatchers in one process.
func lockFile(f *os.File) error   { return nil }
func unlockFile(f *os.File) error { return nil }
