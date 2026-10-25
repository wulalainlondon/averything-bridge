//go:build !windows

package goexec

import (
	"os/exec"
	"syscall"
	"time"
)

// Every executor-owned Claude process receives a fresh private process group.
// Shared provider daemons are never members of this group or signal targets.
func configureOwnedProcessGroup(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func signalOwnedProcessGroup(cmd *exec.Cmd) error {
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err == nil && pgid == cmd.Process.Pid {
		return syscall.Kill(-pgid, syscall.SIGTERM)
	}
	// No guessed group: only the exact owned PID may be the fallback target.
	return cmd.Process.Signal(syscall.SIGTERM)
}

func forceOwnedProcessGroup(cmd *exec.Cmd) {
	// os.Process.Signal refuses a reaped owned process. Combined with fresh
	// Setpgid and exact-leader validation this excludes stale/unrelated groups.
	if cmd.Process.Signal(syscall.Signal(0)) != nil {
		return
	}
	if pgid, err := syscall.Getpgid(cmd.Process.Pid); err == nil && pgid == cmd.Process.Pid {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
}
func waitOwnedProcessGroupExit(cmd *exec.Cmd, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for {
		err := syscall.Kill(-cmd.Process.Pid, 0)
		if err == syscall.ESRCH {
			return true
		}
		if err != nil {
			return false
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}
