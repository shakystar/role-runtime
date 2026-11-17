//go:build !darwin && !linux

package engine

import (
	"errors"
	"os"
)

func acquireLock(string) (*os.File, error) {
	return nil, errors.New("state store currently supports Linux/macOS only")
}
