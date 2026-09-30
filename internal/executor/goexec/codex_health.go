package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const codexHealthRetry = 30 * time.Second

// Health checks never share the production response reader or create/resume
// threads. A stuck notification handler therefore cannot hide daemon health.
type codexHealth struct {
	mu        sync.Mutex
	checking  bool
	blocked   bool
	nextCheck time.Time
	failures  int
	method    string
	threadID  string
	// Hooks are assigned before use by deterministic failure-injection tests.
	probe        func(string) codexProbeResult
	recover      func() error
	probeTimeout time.Duration
}

type codexProbeResult struct {
	Initialized  bool               `json:"initialized"`
	Reads        int                `json:"successful_reads"`
	ReadTimeouts int                `json:"read_timeouts"`
	Active       bool               `json:"active_thread_observed"`
	Samples      []codexProbeSample `json:"samples"`
}
type codexProbeSample struct {
	Method       string `json:"method"`
	Milliseconds int64  `json:"duration_ms"`
	Outcome      string `json:"outcome"`
}

func healthGatedMethod(method string) bool {
	// Recovery must never prevent interrupting work or releasing a subscription.
	if method == "turn/interrupt" || method == "thread/unsubscribe" {
		return false
	}
	return strings.HasPrefix(method, "thread/") || strings.HasPrefix(method, "turn/")
}

func (c *Codex) healthAdmission() error {
	c.health.mu.Lock()
	blocked := c.health.blocked
	due := blocked && !c.health.checking && !time.Now().Before(c.health.nextCheck)
	method, thread := c.health.method, c.health.threadID
	c.health.mu.Unlock()
	if due {
		c.scheduleHealthCheck(method, map[string]any{"threadId": thread})
	}
	if blocked {
		return errors.New("Codex health check/recovery pending; request not submitted; try again after recovery")
	}
	return nil
}

func (c *Codex) setHealthDiagnostics(status string, result codexProbeResult, detail string) {
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	if c.runtimeDiagnostics == nil {
		c.runtimeDiagnostics = make(map[string]any)
	}
	fields, _ := c.runtimeDiagnostics["codex"].(map[string]any)
	if fields == nil {
		fields = make(map[string]any)
		c.runtimeDiagnostics["codex"] = fields
	}
	// Preserve version status (e.g. restart_required) separately from readiness.
	fields["health_status"] = status
	fields["health_checked_at_ms"] = time.Now().UnixMilli()
	fields["health_detail"] = detail
	fields["health_reads"] = result.Reads
	fields["health_read_timeouts"] = result.ReadTimeouts
	fields["health_restart_owner"] = c.healthRestartOwner()
}

func (c *Codex) scheduleHealthCheck(method string, params any) {
	c.health.mu.Lock()
	if c.health.checking || time.Now().Before(c.health.nextCheck) {
		c.health.mu.Unlock()
		return
	}
	c.health.checking, c.health.blocked = true, true
	c.health.method = method
	if p, ok := params.(map[string]any); ok {
		if id, ok := p["threadId"].(string); ok {
			c.health.threadID = id
		}
	}
	thread := c.health.threadID
	c.health.mu.Unlock()
	c.setHealthDiagnostics("checking", codexProbeResult{}, "Independent read-only probe; new thread/turn requests paused")
	go c.checkHealth(thread)
}

func (c *Codex) checkHealth(thread string) {
	probe := c.probeDaemonHealth
	if c.health.probe != nil {
		probe = c.health.probe
	}
	result := probe(thread)
	status, detail := "unavailable", "Independent probe failed; no automatic task replay"
	blocked := true
	c.health.mu.Lock()
	if result.Initialized && result.ReadTimeouts >= 2 && result.Reads == 0 {
		c.health.failures++
	} else {
		c.health.failures = 0
	}
	failures := c.health.failures
	c.health.mu.Unlock()

	if result.Reads > 0 {
		status, detail, blocked = "ready", "Independent thread read succeeded", false
		if result.ReadTimeouts > 0 {
			status, detail = "thread_degraded", "Some threads respond; daemon-wide restart is not justified"
		} else if !c.hasActiveWork() && c.rpc.pendingCount() == 0 {
			// Replace only our transport. The daemon and other clients keep running.
			c.startMu.Lock()
			if c.remoteConn != nil {
				err := c.stopServerLocked()
				if err == nil {
					err = c.startRemoteServerLocked(filepath.Dir(c.sessionsRoot))
				}
				if err != nil {
					status, detail, blocked = "connection_unavailable", err.Error(), true
				}
			}
			c.startMu.Unlock()
		}
	} else if failures >= 2 {
		status, detail = "restart_required", "Two independent probe rounds timed out reading two distinct threads"
		if c.healthRestartOwner() && !result.Active && !c.hasActiveWork() {
			recoverDaemon := c.recoverUnhealthyDaemon
			if c.health.recover != nil {
				recoverDaemon = c.health.recover
			}
			if err := recoverDaemon(); err != nil {
				detail = err.Error()
			} else {
				// Command success alone is not readiness. Verify the replacement daemon.
				result = probe(thread)
				if result.Reads > 0 {
					status, detail, blocked = "ready", "Daemon recovered and thread read verified; tasks were not replayed", false
				} else {
					detail = "Restart completed but thread health verification failed"
				}
			}
		}
	}
	c.setHealthDiagnostics(status, result, detail)
	if blocked {
		c.captureHealthEvidence(status, result, detail)
	}
	c.health.mu.Lock()
	c.health.blocked, c.health.checking = blocked, false
	c.health.nextCheck = time.Now().Add(codexHealthRetry)
	if !blocked {
		c.health.failures = 0
		c.health.nextCheck = time.Time{}
	}
	c.health.mu.Unlock()
	// Poll only while degraded. The next request also triggers an overdue probe.
	if blocked {
		time.AfterFunc(codexHealthRetry, func() {
			c.health.mu.Lock()
			stillBlocked := c.health.blocked
			c.health.mu.Unlock()
			if stillBlocked {
				c.scheduleHealthCheck("health/recheck", map[string]any{"threadId": thread})
			}
		})
	}
}

// A probe connection owns one bounded reader; all payloads stay local.
type healthProbeClient struct {
	timeout   time.Duration
	conn      *websocket.Conn
	transport *http.Transport
	id        int
	samples   []codexProbeSample
}

func (c *Codex) openHealthProbe() (*healthProbeClient, error) {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "unix", c.daemonSocketPath(filepath.Dir(c.sessionsRoot)))
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	conn.SetReadLimit(2 * 1024 * 1024)
	p := &healthProbeClient{conn: conn, transport: transport, timeout: 4 * time.Second}
	if c.health.probeTimeout > 0 {
		p.timeout = c.health.probeTimeout
	}
	if _, err = p.call("initialize", map[string]any{"clientInfo": map[string]any{"name": "averything-health", "version": "1"}}); err == nil {
		notifyCtx, stop := context.WithTimeout(context.Background(), p.timeout)
		err = conn.Write(notifyCtx, websocket.MessageText, []byte(`{"method":"initialized","params":{}}`))
		stop()
	}
	if err != nil {
		p.close()
		return nil, err
	}
	return p, nil
}
func (p *healthProbeClient) close() { _ = p.conn.CloseNow(); p.transport.CloseIdleConnections() }
func (p *healthProbeClient) call(method string, params any) (json.RawMessage, error) {
	p.id++
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	payload, _ := json.Marshal(map[string]any{"id": p.id, "method": method, "params": params})
	err := p.conn.Write(ctx, websocket.MessageText, payload)
	var raw json.RawMessage
	for err == nil {
		_, payload, readErr := p.conn.Read(ctx)
		if readErr != nil {
			err = readErr
			break
		}
		var reply struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if json.Unmarshal(payload, &reply) != nil {
			err = errors.New("invalid probe response")
			break
		}
		if reply.ID != p.id {
			continue
		}
		if len(reply.Error) > 0 {
			err = errors.New("RPC rejected")
		} else {
			raw = reply.Result
		}
		break
	}
	outcome := "ok"
	if err != nil {
		outcome = "error"
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
			outcome = "timeout"
			err = context.DeadlineExceeded
		}
	}
	p.samples = append(p.samples, codexProbeSample{Method: method, Milliseconds: time.Since(start).Milliseconds(), Outcome: outcome})
	return raw, err
}

func (c *Codex) probeDaemonHealth(preferredThread string) codexProbeResult {
	result := codexProbeResult{}
	p, err := c.openHealthProbe()
	if err != nil {
		result.Samples = []codexProbeSample{{Method: "connect/initialize", Outcome: "failed"}}
		return result
	}
	defer p.close()
	result.Initialized = true
	ids := []string{}
	add := func(id string) {
		if id == "" || len(ids) >= 2 {
			return
		}
		for _, existing := range ids {
			if existing == id {
				return
			}
		}
		ids = append(ids, id)
	}
	add(preferredThread)
	// Known thread IDs allow corroboration even when thread/list also stalls.
	c.mu.Lock()
	for id := range c.threadToSession {
		add(id)
	}
	c.mu.Unlock()
	if len(ids) < 2 {
		raw, listErr := p.call("thread/list", map[string]any{"limit": 3})
		if listErr == nil {
			var listing struct {
				Data []struct {
					ID string `json:"id"`
				} `json:"data"`
			}
			if json.Unmarshal(raw, &listing) == nil {
				for _, t := range listing.Data {
					add(t.ID)
				}
			}
		}
	}
	result.Samples = append(result.Samples, p.samples...)
	for _, id := range ids {
		// coder/websocket closes its connection after a read timeout. Always use a
		// fresh connection so that two timeouts are independent evidence.
		read, openErr := c.openHealthProbe()
		if openErr != nil {
			result.Samples = append(result.Samples, codexProbeSample{Method: "read/connect", Outcome: "failed"})
			continue
		}
		raw, readErr := read.call("thread/read", map[string]any{"threadId": id, "includeTurns": false})
		if readErr == nil {
			var response struct {
				Thread struct {
					ID     string `json:"id"`
					Status struct {
						Type string `json:"type"`
					} `json:"status"`
				} `json:"thread"`
			}
			if json.Unmarshal(raw, &response) == nil && response.Thread.ID == id {
				result.Reads++
				result.Active = result.Active || response.Thread.Status.Type == "active"
			}
		} else if errors.Is(readErr, context.DeadlineExceeded) {
			result.ReadTimeouts++
		}
		result.Samples = append(result.Samples, read.samples...)
		read.close()
	}
	return result
}
