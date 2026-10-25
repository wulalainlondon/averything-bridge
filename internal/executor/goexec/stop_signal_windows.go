//go:build windows

package goexec

import (
	"os"
	"os/exec"
	"time"
)

// Preserve the existing Windows interruption/fallback behavior.
func configureOwnedProcessGroup(*exec.Cmd)        {}
func signalOwnedProcessGroup(cmd *exec.Cmd) error { return cmd.Process.Signal(os.Interrupt) }

func forceOwnedProcessGroup(cmd *exec.Cmd)                    { _ = cmd.Process.Kill() }
func waitOwnedProcessGroupExit(*exec.Cmd, time.Duration) bool { return true }
