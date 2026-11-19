//go:build darwin || linux

package control

import (
	"path/filepath"
	"testing"
)

func TestOwnerLockExclusionAndRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owner.lock")
	first, err := acquireOwnerLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if other, err := acquireOwnerLock(path); err == nil {
		other.Close()
		t.Fatal("second owner acquired lock")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	last, err := acquireOwnerLock(path)
	if err != nil {
		t.Fatal(err)
	}
	last.Close()
}
