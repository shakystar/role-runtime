//go:build !darwin && !linux

package codexrpc

import "os/exec"

func configureProcess(cmd *exec.Cmd) {}
func killProcess(cmd *exec.Cmd)      { _ = cmd.Process.Kill() }
