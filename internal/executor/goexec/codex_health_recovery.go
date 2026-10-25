package goexec

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Set only on the one designated supervisor. Shared clients must not decide
// ownership from their own (incomplete) view of active work.
func (c *Codex) healthRestartOwner() bool {
	return c.appServerSocket == "" && envBool("EVERYTHING_GO_CODEX_DAEMON_RECOVERY_OWNER", false)
}

func (c *Codex) captureHealthEvidence(status string, result codexProbeResult, detail string) {
	dir := filepath.Join(c.dataDir, "diagnostics")
	if os.MkdirAll(dir, 0700) != nil {
		return
	}
	record := map[string]any{"at": time.Now().UTC(), "status": status, "detail": detail, "probe": result, "runtime": c.RuntimeDiagnostics()}
	// Overwrite one bounded snapshot. Never persist RPC payloads or credentials.
	raw, err := json.MarshalIndent(record, "", "  ")
	if err == nil {
		writeHealthSnapshot(filepath.Join(dir, "codex-health.json"), raw)
	}
}

// Atomic replacement keeps status readers from seeing a partial JSON document.
func writeHealthSnapshot(path string, raw []byte) {
	f, err := os.CreateTemp(filepath.Dir(path), ".health-*")
	if err != nil {
		return
	}
	defer os.Remove(f.Name())
	_, err = f.Write(raw)
	closeErr := f.Close()
	if err == nil && closeErr == nil {
		_ = os.Rename(f.Name(), path)
	}
}

func (c *Codex) recoverUnhealthyDaemon() error {
	if !c.healthRestartOwner() {
		return fmt.Errorf("daemon restart requires a designated recovery owner")
	}
	home := filepath.Dir(c.sessionsRoot)
	dir := filepath.Join(home, "app-server-daemon")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	unlock, err := lockCodexRecovery(filepath.Join(dir, "bridge-recovery.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	stamp := filepath.Join(dir, "bridge-recovery-at")
	if info, err := os.Stat(stamp); err == nil && time.Since(info.ModTime()) < 5*time.Minute {
		return fmt.Errorf("daemon recovery cooldown active; restart suppressed")
	}
	// Recheck under the cross-process lock. Another supervisor may have recovered
	// the daemon between the original probe and acquiring ownership.
	c.health.mu.Lock()
	thread := c.health.threadID
	c.health.mu.Unlock()
	check := c.probeDaemonHealth(thread)
	if check.Reads > 0 {
		return nil
	}
	if !check.Initialized || check.ReadTimeouts < 2 || c.hasActiveWork() {
		return fmt.Errorf("daemon restart suppressed: insufficient independent evidence or local work active")
	}
	c.captureHealthEvidence("restarting", check, "Designated supervisor; no request replay")
	diagnostics := filepath.Join(c.dataDir, "diagnostics")
	if raw, err := os.ReadFile(filepath.Join(diagnostics, "codex-health.json")); err == nil {
		writeHealthSnapshot(filepath.Join(diagnostics, "codex-health-before-restart.json"), raw)
	}
	if f, err := os.Open(filepath.Join(dir, "app-server.stderr.log")); err == nil {
		if info, err := f.Stat(); err == nil {
			offset := info.Size() - 64*1024
			if offset < 0 {
				offset = 0
			}
			if _, err := f.Seek(offset, io.SeekStart); err == nil {
				if raw, err := io.ReadAll(io.LimitReader(f, 64*1024)); err == nil {
					writeHealthSnapshot(filepath.Join(diagnostics, "codex-daemon.stderr-tail.log"), raw)
				}
			}
		}
		_ = f.Close()
	}
	// Capture process stacks before the disruptive action, with a hard deadline.
	var pid struct {
		PID int `json:"pid"`
	}
	if raw, err := os.ReadFile(filepath.Join(dir, "app-server.pid")); err == nil && json.Unmarshal(raw, &pid) == nil && pid.PID > 1 {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if raw, err := exec.CommandContext(ctx, "ps", "-axo", "pid,ppid,etime,stat,comm").Output(); err == nil {
			// Record executable names, not command arguments (which may contain secrets).
			var lines []string
			for _, line := range strings.Split(string(raw), "\n") {
				fields := strings.Fields(line)
				if len(fields) > 1 && (fields[0] == strconv.Itoa(pid.PID) || fields[1] == strconv.Itoa(pid.PID)) {
					lines = append(lines, line)
				}
			}
			writeHealthSnapshot(filepath.Join(diagnostics, "codex-daemon-processes.txt"), []byte(strings.Join(lines, "\n")))
		}
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		output := filepath.Join(c.dataDir, "diagnostics", "codex-daemon.sample.txt")
		// sample is a macOS diagnostic. On other hosts its absence is non-fatal.
		_ = exec.CommandContext(ctx, "sample", fmt.Sprint(pid.PID), "1", "1", "-file", output).Run()
		cancel()
		_ = os.Chmod(output, 0600)
	}
	// Record attempts, including failed ones, to prevent a restart storm.
	if err := os.WriteFile(stamp, []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0600); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.codexBin, "app-server", "daemon", "restart")
	cmd.Env = append(os.Environ(), "CODEX_HOME="+home)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("daemon restart failed: %w", err)
	}
	c.refreshRuntimeDiagnostics(home)
	return nil
}
