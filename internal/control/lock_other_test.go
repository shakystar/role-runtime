//go:build !darwin && !linux

package control

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUnsupportedOwnerLockFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owner.lock")
	f, err := acquireOwnerLock(path)
	if f != nil || err == nil || !strings.Contains(err.Error(), "Linux/macOS") {
		t.Fatalf("unsupported owner must fail closed: file=%v err=%v", f, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("unsupported owner created lock file: %v", err)
	}
}
