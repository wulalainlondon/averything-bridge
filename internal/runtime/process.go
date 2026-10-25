package runtime

import (
	"context"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"everything-go/internal/protocol"
)

// CollectProcesses lists OS processes, sorted by (cpu, mem) desc, capped at
// limit. Mirrors bridge/handlers/runtime_ops.py _collect_processes_posix_async.
func CollectProcesses(limit int) []protocol.Process {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ps", "-axo", "pid=,pcpu=,rss=,user=,comm=,args=")
	out, err := cmd.Output()
	if err != nil {
		return []protocol.Process{}
	}

	var procs []protocol.Process
	for _, raw := range strings.Split(string(out), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		// Collapse the runs of spaces ps emits between fixed columns.
		fields := splitFields(line, 6)
		if len(fields) < 5 {
			continue
		}
		pidF, err1 := strconv.ParseFloat(fields[0], 64)
		cpu, err2 := strconv.ParseFloat(fields[1], 64)
		memF, err3 := strconv.ParseFloat(fields[2], 64)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		pid := int(pidF)
		if pid <= 0 {
			continue
		}
		user := fields[3]
		comm := fields[4]
		args := comm
		if len(fields) > 5 {
			args = fields[5]
		}
		procs = append(procs, protocol.Process{
			PID: pid, CPUPercent: cpu, MemRSSKB: int(memF),
			User: user, Command: comm, Args: args,
		})
	}

	sort.SliceStable(procs, func(i, j int) bool {
		if procs[i].CPUPercent != procs[j].CPUPercent {
			return procs[i].CPUPercent > procs[j].CPUPercent
		}
		return procs[i].MemRSSKB > procs[j].MemRSSKB
	})
	if limit > 0 && len(procs) > limit {
		procs = procs[:limit]
	}
	if procs == nil {
		procs = []protocol.Process{}
	}
	return procs
}

// splitFields splits on runs of whitespace into at most max fields, mirroring
// Python str.split(None, max-1): the final field keeps its internal spaces.
func splitFields(s string, max int) []string {
	var fields []string
	i := 0
	n := len(s)
	for i < n && len(fields) < max-1 {
		for i < n && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
		if i >= n {
			break
		}
		start := i
		for i < n && s[i] != ' ' && s[i] != '\t' {
			i++
		}
		fields = append(fields, s[start:i])
	}
	// remainder (already whitespace-trimmed at front) is the last field
	for i < n && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	if i < n {
		fields = append(fields, s[i:])
	}
	return fields
}

// KillProcess sends SIGTERM (or SIGKILL when force) to a pid. Returns
// (success, errorCode) where errorCode is a stable token on failure.
func KillProcess(pid int, force bool) (bool, string) {
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	err := syscall.Kill(pid, sig)
	if err == nil {
		return true, ""
	}
	switch err {
	case syscall.ESRCH:
		return false, "process_not_found"
	case syscall.EPERM:
		return false, "permission_denied"
	default:
		return false, "kill_failed: " + err.Error()
	}
}
