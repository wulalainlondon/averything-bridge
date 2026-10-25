package goexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"everything-go/internal/session"
	"everything-go/internal/toolenv"
	"golang.org/x/sync/singleflight"
)

type toolEnvironmentState struct {
	mu     sync.Mutex
	cache  map[string]toolenv.Snapshot
	flight singleflight.Group
	epoch  atomic.Uint64
}

func toolMutationMethod(m string) bool {
	return m == "turn/start" || m == "turn/steer" || m == "thread/start" || m == "thread/resume" || m == "thread/fork" || m == "thread/settings/update" || m == "thread/compact/start" || strings.HasPrefix(m, "thread/goal/") && m != "thread/goal/get"
}

func (c *Codex) toolGeneration() string {
	b := sha256.Sum256([]byte(fmt.Sprintf("%p:%s:%d", c, c.appServerSocket, c.toolEnvironment.epoch.Load())))
	return hex.EncodeToString(b[:12])
}

// Diagnostics must not call ensureServer: that path may start/configure the
// daemon. Only the normal user-authorized attach workflow owns that lifecycle.
func (c *Codex) toolRequest(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if ctx.Err() != nil {
		return nil, toolenv.Error("inspection_timeout")
	}
	if !c.rpc.hasWriter() {
		return nil, toolenv.Error("daemon_not_connected")
	}
	timeout := 15 * time.Second
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < timeout {
		timeout = time.Until(deadline)
	}
	if timeout <= 0 {
		return nil, toolenv.Error("inspection_timeout")
	}
	raw, err := c.rpc.request(method, params, timeout)
	if err == nil {
		return raw, nil
	}
	var timed *rpcTimeoutError
	if errors.As(err, &timed) || errors.Is(err, context.DeadlineExceeded) {
		return nil, toolenv.Error("rpc_timeout")
	}
	// Only expose fixed reason codes. MCP errors can contain provider secrets.
	if strings.Contains(err.Error(), "-32601") {
		return nil, toolenv.Error("method_unsupported")
	}
	return nil, toolenv.Error("rpc_failed")
}

func (c *Codex) InspectToolEnvironment(ctx context.Context, s *session.Session, force bool) (toolenv.Snapshot, error) {
	id := s.ResumeID()
	if id == "" {
		return toolenv.Snapshot{}, toolenv.Error("thread_not_attached")
	}
	if c.appServerMode != "daemon" {
		return toolenv.Snapshot{}, toolenv.Error("backend_unsupported")
	}
	generation := c.toolGeneration()
	key := generation + ":" + id
	c.toolEnvironment.mu.Lock()
	cached, found := c.toolEnvironment.cache[key]
	c.toolEnvironment.mu.Unlock()
	if !force && found && time.Now().UnixMilli()-cached.CheckedAt < 60_000 {
		return cached, nil
	}
	result := c.toolEnvironment.flight.DoChan(key, func() (any, error) {
		probeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		snapshot, err := c.inspectTools(probeCtx, id, generation)
		if err == nil && generation != c.toolGeneration() {
			return nil, toolenv.Error("environment_changed")
		}
		if err == nil {
			c.toolEnvironment.mu.Lock()
			if c.toolEnvironment.cache == nil || len(c.toolEnvironment.cache) > 256 {
				c.toolEnvironment.cache = map[string]toolenv.Snapshot{}
			}
			c.toolEnvironment.cache[key] = snapshot
			c.toolEnvironment.mu.Unlock()
		}
		return snapshot, err
	})
	select {
	case <-ctx.Done():
		return toolenv.Snapshot{}, toolenv.Error("inspection_timeout")
	case r := <-result:
		if r.Err != nil {
			return toolenv.Snapshot{}, r.Err
		}
		return r.Val.(toolenv.Snapshot), nil
	}
}

func (c *Codex) inspectTools(ctx context.Context, id, generation string) (toolenv.Snapshot, error) {
	now := time.Now().UnixMilli()
	snap := toolenv.Snapshot{ThreadID: id, Generation: generation, Revision: now, CheckedAt: now, State: "unknown", Reason: "integrated_tools_not_covered", Coverage: "configured_mcp_only", Evidence: "none", Services: []toolenv.Service{}}
	raw, err := c.toolRequest(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": false})
	if err != nil {
		return snap, err
	}
	var thread struct {
		Thread struct {
			ID     string `json:"id"`
			Status struct {
				Type string `json:"type"`
			} `json:"status"`
		} `json:"thread"`
	}
	if json.Unmarshal(raw, &thread) != nil || thread.Thread.ID != id {
		return snap, toolenv.Error("invalid_thread_response")
	}
	snap.ThreadState = thread.Thread.Status.Type
	if snap.ThreadState == "notLoaded" {
		snap.Reason = "thread_not_loaded"
		return snap, nil
	}
	seen := map[string]bool{}
	cursor := ""
	for page := 0; ; page++ {
		if page >= 16 {
			return snap, toolenv.Error("inventory_limit")
		}
		params := map[string]any{"threadId": id, "limit": 100, "detail": "toolsAndAuthOnly"}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err = c.toolRequest(ctx, "mcpServerStatus/list", params)
		if err != nil {
			return snap, err
		}
		var inventory struct {
			Data []struct {
				Name  string                     `json:"name"`
				Auth  string                     `json:"authStatus"`
				State string                     `json:"runtimeStatus"`
				Tools map[string]json.RawMessage `json:"tools"`
			} `json:"data"`
			Cursor *string `json:"nextCursor"`
		}
		if json.Unmarshal(raw, &inventory) != nil || inventory.Data == nil {
			return snap, toolenv.Error("invalid_inventory")
		}
		for _, server := range inventory.Data {
			if !safeToolName(server.Name) || len(server.Tools) > 1024 || len(snap.Services) >= 256 {
				return snap, toolenv.Error("inventory_limit")
			}
			state := server.State
			switch state {
			case "notStarted", "starting", "connected", "authenticationRequired", "failed", "cancelled", "disabled":
			default:
				state = "unknown"
			}
			auth := server.Auth
			switch auth {
			case "unknown", "unsupported", "notLoggedIn", "bearerToken", "oAuth":
			default:
				auth = "unknown"
			}
			service := toolenv.Service{Name: server.Name, State: state, Auth: auth, Tools: []string{}}
			for name := range server.Tools {
				if safeToolName(name) {
					service.Tools = append(service.Tools, name)
				}
			}
			sort.Strings(service.Tools)
			snap.Services = append(snap.Services, service)
		}
		if inventory.Cursor == nil || *inventory.Cursor == "" {
			break
		}
		cursor = *inventory.Cursor
		if seen[cursor] || len(cursor) > 4096 {
			return snap, toolenv.Error("invalid_inventory_cursor")
		}
		seen[cursor] = true
	}
	sort.Slice(snap.Services, func(i, j int) bool { return snap.Services[i].Name < snap.Services[j].Name })
	snap.Evidence = "inventory"
	for _, server := range snap.Services {
		if server.Name != "node_repl" && server.Name != "cua_repl" && server.Name != "browser" && server.Name != "chrome" {
			continue
		}
		if server.State == "authenticationRequired" {
			snap.State = "auth_required"
			snap.Reason = "authentication_required"
			break
		}
		if server.State == "failed" || server.State == "cancelled" || server.State == "disabled" {
			snap.State = "unavailable"
			snap.Reason = "tool_service_unavailable"
			break
		}
		if len(server.Tools) > 0 {
			snap.State = "listed_unverified"
			snap.Reason = "browser_not_verified"
		}
	}
	if d, ok := c.RuntimeDiagnostics()["codex"].(map[string]any); ok {
		snap.RunningVersion, _ = d["running_version"].(string)
		snap.DiskVersion, _ = d["managed_version"].(string)
	}
	// The host opts in only after arranging a maintenance window with every
	// client of this daemon. This is NOT a remote permission escalation switch.
	snap.ReloadAllowed = envBool("EVERYTHING_GO_CODEX_TOOL_MAINTENANCE", false) && knownToolAPIVersion(snap.RunningVersion)
	snap.ForkAllowed = knownToolAPIVersion(snap.RunningVersion)
	return snap, nil
}

func safeToolName(s string) bool {
	if len(s) == 0 || len(s) > 160 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.-/", r)) {
			return false
		}
	}
	return true
}

func knownToolAPIVersion(v string) bool {
	// Conservative schema compatibility, not a claim that refresh repairs CUA.
	v = strings.TrimPrefix(v, "codex-cli ")
	return v == "0.153.2" || v == "0.153.4"
}

func (c *Codex) loadedToolsIdle(ctx context.Context, action, thread string) error {
	if c.hasActiveWork() {
		return toolenv.Error("waiting_idle")
	}
	c.interMu.Lock()
	interactions := len(c.interactions)
	c.interMu.Unlock()
	if interactions > 0 {
		return toolenv.Error("waiting_idle")
	}
	ids := []string{thread}
	if action == "reload" {
		ids = nil
		cursor := ""
		seen := map[string]bool{}
		for i := 0; ; i++ {
			if i >= 16 {
				return toolenv.Error("idle_state_unknown")
			}
			params := map[string]any{"limit": 100}
			if cursor != "" {
				params["cursor"] = cursor
			}
			raw, err := c.toolRequest(ctx, "thread/loaded/list", params)
			if err != nil {
				return err
			}
			var list struct {
				Data   []string `json:"data"`
				Cursor *string  `json:"nextCursor"`
			}
			if json.Unmarshal(raw, &list) != nil || list.Data == nil {
				return toolenv.Error("idle_state_unknown")
			}
			ids = append(ids, list.Data...)
			if list.Cursor == nil || *list.Cursor == "" {
				break
			}
			cursor = *list.Cursor
			if seen[cursor] {
				return toolenv.Error("idle_state_unknown")
			}
			seen[cursor] = true
		}
	}
	for _, id := range ids {
		raw, err := c.toolRequest(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": false})
		if err != nil {
			return err
		}
		var v struct {
			Thread struct {
				ID     string `json:"id"`
				Status struct {
					Type string `json:"type"`
				} `json:"status"`
			} `json:"thread"`
		}
		if json.Unmarshal(raw, &v) != nil || v.Thread.ID != id {
			return toolenv.Error("idle_state_unknown")
		}
		if v.Thread.Status.Type != "idle" && v.Thread.Status.Type != "notLoaded" {
			return toolenv.Error("waiting_idle")
		}
	}
	return nil
}

func (c *Codex) RepairToolEnvironment(ctx context.Context, s *session.Session, action, generation string) (toolenv.Result, error) {
	if !c.toolRPCGate.TryLock() {
		return toolenv.Result{}, toolenv.Error("waiting_idle")
	}
	defer c.toolRPCGate.Unlock()
	// Coordinate Bridge processes sharing this socket. External clients still
	// require the explicit host maintenance window; this is not a daemon lease.
	unlock, lockErr := lockCodexRecovery(c.daemonSocketPath(filepath.Dir(c.sessionsRoot)) + ".tool-maintenance.lock")
	if lockErr != nil {
		return toolenv.Result{}, toolenv.Error("waiting_idle")
	}
	defer unlock()
	if generation != c.toolGeneration() {
		return toolenv.Result{}, toolenv.Error("environment_changed")
	}
	snap, err := c.inspectTools(ctx, s.ResumeID(), generation)
	if err != nil {
		return toolenv.Result{}, err
	}
	if action == "reload" && !snap.ReloadAllowed {
		return toolenv.Result{}, toolenv.Error("maintenance_window_required")
	}
	if action == "fork" && !snap.ForkAllowed {
		return toolenv.Result{}, toolenv.Error("fork_unsupported")
	}
	if action != "reload" && action != "fork" {
		return toolenv.Result{}, toolenv.Error("unsupported_action")
	}
	if err = c.loadedToolsIdle(ctx, action, s.ResumeID()); err != nil {
		return toolenv.Result{}, err
	}
	if generation != c.toolGeneration() {
		return toolenv.Result{}, toolenv.Error("environment_changed")
	}
	var newThread string
	if action == "reload" {
		_, err = c.toolRequest(ctx, "config/mcpServer/reload", nil)
	} else {
		var raw json.RawMessage
		raw, err = c.toolRequest(ctx, "thread/fork", map[string]any{"threadId": s.ResumeID(), "excludeTurns": true})
		if err == nil {
			var fork struct {
				Thread struct {
					ID     string `json:"id"`
					Parent string `json:"forkedFromId"`
					Cwd    string `json:"cwd"`
				} `json:"thread"`
			}
			if json.Unmarshal(raw, &fork) != nil || fork.Thread.ID == "" || fork.Thread.ID == s.ResumeID() || fork.Thread.Parent != s.ResumeID() || filepath.Clean(fork.Thread.Cwd) != filepath.Clean(s.Snapshot().Cwd) {
				return toolenv.Result{}, toolenv.Error("mutation_outcome_unknown")
			}
			newThread = fork.Thread.ID
		}
	}
	if err != nil {
		if toolenv.Code(err) == "method_unsupported" || toolenv.Code(err) == "inspection_timeout" || toolenv.Code(err) == "daemon_not_connected" {
			return toolenv.Result{}, err
		}
		return toolenv.Result{}, toolenv.Error("mutation_outcome_unknown")
	}
	// Invalidate all configured-MCP snapshots after a global refresh. A new
	// epoch also invalidates prepared plans even when socket/PID stayed the same.
	c.toolEnvironment.epoch.Add(1)
	id := s.ResumeID()
	if newThread != "" {
		id = newThread
	}
	verified, e := c.inspectTools(ctx, id, c.toolGeneration())
	if e != nil {
		return toolenv.Result{NewThread: newThread}, toolenv.Error("verification_incomplete")
	}
	return toolenv.Result{Snapshot: verified, NewThread: newThread}, nil
}

// ProbeToolEnvironment connects read-only to an existing daemon; it never
// starts the daemon, resumes a thread, loads auth files or edits configuration.
func ProbeToolEnvironment(ctx context.Context, socket, thread string) (toolenv.Snapshot, error) {
	c := NewCodex(&caplessSink{}, "codex")
	c.appServerMode = "daemon"
	c.appServerSocket = socket
	c.remoteReconnect = false
	c.startMu.Lock()
	err := c.startRemoteServerLocked(filepath.Dir(c.sessionsRoot))
	c.startMu.Unlock()
	if err != nil {
		return toolenv.Snapshot{}, toolenv.Error("daemon_not_connected")
	}
	conn, cancel := c.remoteConn, c.remoteCancel
	defer func() { cancel(); conn.CloseNow() }()
	s := session.NewRegistry().Create("tool-probe", "tool-probe", "", "codex", "", "", thread)
	return c.InspectToolEnvironment(ctx, s, true)
}

type caplessSink struct{}

func (*caplessSink) Emit(any) {}
