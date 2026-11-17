//go:build !darwin && !linux

package pirpc

import "os/exec"

func configureProcess(cmd *exec.Cmd) {}
func killProcess(cmd *exec.Cmd)      { _ = cmd.Process.Kill() }
