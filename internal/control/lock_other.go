//go:build !darwin && !linux

package control

import (
	"errors"
	"os"
)

// Match the Store's supported platforms. Never start an unlocked owner.
func acquireOwnerLock(string) (*os.File, error) {
	return nil, errors.New("control owner currently supports Linux/macOS only")
}
