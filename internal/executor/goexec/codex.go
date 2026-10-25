// Codex executor: drives a single persistent `codex app-server` subprocess via
// newline-delimited JSON-RPC, one thread per session. Fidelity reference:
// bridge/backends/codex_appserver.py.
package goexec

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"everything-go/internal/agentvisibility"
	"everything-go/internal/backend"
	"everything-go/internal/executor"
	"everything-go/internal/protocol"
	"everything-go/internal/recap"
	"everything-go/internal/recovery"
	"everything-go/internal/runtime"
	"everything-go/internal/session"
	"everything-go/internal/sourcepolicy"
)

const (
	codexDefaultModel     = "gpt-5.6-sol"
	codexCompactThreshold = 0.80
	codexStallWarnAfter   = 5 * time.Minute
	// Manual control by default: inactivity warns, never auto-interrupts work.
	codexStallAbortAfter = time.Duration(0)
	codexStallCheckEvery = 30 * time.Second
	// A thread/resume response contains the complete serialized history in one
	// WebSocket message. coder/websocket defaults to 32 KiB, which disconnects
	// healthy daemon sessions as soon as a non-trivial thread is resumed. Keep a
	// finite ceiling for memory safety while matching the hundreds-of-MB replies
	// already supported by the stdio transport below.
	codexRemoteReadLimit = 512 * 1024 * 1024
)

var (
	codexAskUserRE     = regexp.MustCompile("(?s)```(?:json)?\\s*(\\{[^`]*?\"(?:ask_user_question|AskUserQuestion)\"[^`]*?\\})\\s*```|(\\{[^{}]*\"(?:ask_user_question|AskUserQuestion)\"[^{}]*\\})")
	codexInlineImageRE = regexp.MustCompile(`"type"\s*:\s*"input_image"`)
	// Image generation tool results contain an inline input_image data URL plus
	// a compact text block naming the durable file under ~/.codex/generated_images.
	// Extract only that managed path; never forward the multi-megabyte base64
	// payload to the mobile tool card.
	codexGeneratedImagePathRE = regexp.MustCompile(`(/[^\s"'<>]*\.codex/generated_images/[^\s"'<>]+\.(?:png|jpe?g|webp|gif))`)
)

type codexState struct {
	visibility agentvisibility.Channel

	mu       sync.Mutex
	ensureMu sync.Mutex

	threadID                            string
	currentTurnID                       string
	startAdmission                      *codexStartAdmission
	submissionEpoch                     uint64
	nativeBoundary                      uint64
	unconfirmedActivity                 bool
	pendingNotifications                []json.RawMessage
	pendingNotificationBytes            int
	confirmedRequestID, confirmedTurnID string
	turnStartPending                    bool
	pendingTerminals                    map[string]codexTurnTerminal
	turnActive                          bool
	turnErr                             string
	turnErrorCode                       string
	turnFailure                         *codexFailureDetail
	failureNoticeKey                    string
	turnDone                            chan struct{}
	stopping                            bool
	reqID                               string
	observedTurnID                      string
	observedRequestID                   string
	retiredTurns                        []string
	// tempImages is keyed by the owning turn request id. A terminal event can
	// synchronously release the Session actor and start the next turn before the
	// previous runTurn defer executes; a flat Session-wide slice lets that older
	// defer delete the newer turn's images.
	tempImages              map[string][]string
	accumulatedText         string
	askExtracted            bool
	contextUsed             int
	contextMax              int
	compactActive           bool
	compactErr              string
	compactDone             chan struct{}
	compactTurnID           string
	lastEventAt             time.Time
	stallWarned             bool
	inactivityStopRequested bool
	inactivityStopReason    string
	pendingHandoff          string
	pendingRecovery         *codexRecovery
	agents                  map[string]*codexAgent
}

type codexAgent struct {
	id, agentType, description, parentID, output string
	startMS, endMS                               *int64
	tools                                        []protocol.AgentToolCall
}

func newCodexState() *codexState {
	return &codexState{
		agents:     make(map[string]*codexAgent),
		tempImages: make(map[string][]string),
	}
}

func (st *codexState) finishCompact(errStr string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.compactActive {
		return
	}
	st.compactActive = false
	if st.compactErr == "" {
		st.compactErr = errStr
	}
	if st.compactDone != nil {
		close(st.compactDone)
	}
}

// finish completes the in-flight turn exactly once. errStr=="" means success;
// "stopped" is the interrupt sentinel.
func (st *codexState) finish(errStr string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.finishTurnLocked(errStr)
}

func (st *codexState) finishTurnLocked(errStr string) {
	if !st.turnActive {
		return
	}
	st.turnActive = false
	st.retireTurnLocked(st.currentTurnID)
	st.currentTurnID = ""
	st.turnStartPending = false
	st.pendingTerminals = nil
	if st.turnErr == "" {
		st.turnErr = errStr
	}
	if st.turnDone != nil {
		close(st.turnDone)
	}
}

func (st *codexState) touch(now time.Time) {
	st.mu.Lock()
	st.lastEventAt = now
	st.stallWarned = false
	st.mu.Unlock()
}

// Codex implements executor.Executor over the codex app-server.
type Codex struct {
	appSandbox *codexAppSandbox
	// Optional owned-fixture observer. Nil in production; never grants authority.
	nativeFixtureObserver func(json.RawMessage)
	mediaOrigins          mediaOriginState
	lineOrigins           lineOriginState
	voiceControlOrigins   voiceOriginState
	metadataObserverBoot  string

	taskProvider           backend.TaskAPIProvider
	voiceMu                sync.Mutex
	voiceCalls             map[string]*codexVoiceCall
	voicePreparing         map[string]bool
	voicePermissionSignals map[string]chan json.RawMessage
	recapStoreMu           sync.Mutex
	recapStore             *recap.Store
	recapSlots             chan struct{}
	pmProvider             backend.PMProvider
	delegationProvider     backend.DelegationProvider
	controlProvider        backend.SessionControlProvider
	toolEnvironment        toolEnvironmentState
	toolRPCGate            sync.RWMutex
	sink                   executor.Sink
	tools                  *toolEmitter
	codexBin               string
	sessionsRoot           string
	indexPath              string
	rpc                    *rpcPlumber

	startMu               sync.Mutex
	proc                  *exec.Cmd
	stdin                 io.WriteCloser
	procDone              chan struct{}
	remoteConn            *websocket.Conn
	remoteDone            chan struct{}
	remoteCancel          context.CancelFunc
	authPath              string
	serverAuthFingerprint string
	authReloadDeferred    bool
	authWatchOnce         sync.Once

	mu                 sync.Mutex
	states             map[string]*codexState
	threadToSession    map[string]*session.Session
	activeThreadOwner  map[string]*session.Session
	interMu            sync.Mutex
	interactions       map[string]codexInteraction
	asyncMu            sync.Mutex
	asyncResolved      map[string]bool
	asyncSessions      map[string]*session.Session
	asyncScanMu        sync.Mutex
	asyncScanned       map[string]asyncScanStamp
	catalogMu          sync.RWMutex
	catalog            backend.Definition
	collaborationModes map[string]map[string]any
	exactFinalMu       sync.Mutex
	exactFinalScans    map[string]*exactFinalScan
	exactFinalClock    uint64
	rolloutMu          sync.Mutex
	rolloutRoot        string
	rolloutScannedAt   time.Time
	rolloutPaths       []string
	rolloutByID        map[string]string
	stallWarnAfter     time.Duration
	stallAbortAfter    time.Duration
	stallCheckEvery    time.Duration
	dataDir            string
	failureJournalMu   sync.Mutex
	failureJournal     *recovery.Journal
	historyRequestMu   sync.Mutex
	maintenanceMu      sync.Mutex
	maintenance        map[string]backend.Maintenance
	rolloverEnabled    bool
	coldResumeMaxBytes int64
	checkpointMaxBytes int
	appServerMode      string // daemon (default) | stdio (compatibility fallback)
	appServerSocket    string
	runtimeMu          sync.RWMutex
	runtimeDiagnostics map[string]any
	reconnectMu        sync.Mutex
	reconnectRunning   bool
	remoteReconnect    bool
	health             codexHealth
}

type codexInteraction struct {
	payload      backend.UserInputPayload
	rpcID        any
	responseKind string
	reqID        string
	mcpParams    json.RawMessage
	nativeThread string
	submitting   bool
}

func NewCodex(sink executor.Sink, codexBin string) *Codex {
	if codexBin == "" {
		codexBin = "codex"
	}
	codexHome := sourcepolicy.CodexHome()
	home, _ := os.UserHomeDir()
	return &Codex{
		recapSlots:         make(chan struct{}, 2),
		sink:               sink,
		tools:              newToolEmitter(sink),
		codexBin:           codexBin,
		sessionsRoot:       filepath.Join(codexHome, "sessions"),
		indexPath:          filepath.Join(codexHome, "session_index.jsonl"),
		authPath:           filepath.Join(codexHome, "auth.json"),
		rpc:                newRPCPlumber("codex"),
		states:             make(map[string]*codexState),
		threadToSession:    make(map[string]*session.Session),
		activeThreadOwner:  make(map[string]*session.Session),
		interactions:       make(map[string]codexInteraction),
		collaborationModes: make(map[string]map[string]any),
		stallWarnAfter:     codexStallWarnAfter,
		stallAbortAfter:    codexStallAbortAfter,
		stallCheckEvery:    codexStallCheckEvery,
		dataDir:            filepath.Join(home, ".everything-go-runtime"),
		rolloverEnabled:    envBool("EVERYTHING_GO_CODEX_ROLLOVER_ENABLED", false),
		coldResumeMaxBytes: envInt64("EVERYTHING_GO_CODEX_COLD_RESUME_MAX_BYTES", 256*1024*1024),
		checkpointMaxBytes: int(envInt64("EVERYTHING_GO_CODEX_CHECKPOINT_MAX_BYTES", 128*1024)),
		runtimeDiagnostics: map[string]any{"codex": map[string]any{"status": "not_checked"}},
		remoteReconnect:    true,
		appServerMode:      codexAppServerMode(),
		appServerSocket:    strings.TrimSpace(os.Getenv("EVERYTHING_GO_CODEX_APP_SERVER_SOCKET")),
	}
}

func codexAppServerMode() string {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("EVERYTHING_GO_CODEX_APP_SERVER_MODE")))
	if mode == "stdio" || mode == "private" {
		return "stdio"
	}
	return "daemon"
}

type codexDaemonVersion struct {
	Status              string `json:"status"`
	Backend             string `json:"backend"`
	ManagedCodexPath    string `json:"managedCodexPath"`
	ManagedCodexVersion string `json:"managedCodexVersion"`
	SocketPath          string `json:"socketPath"`
	CLIVersion          string `json:"cliVersion"`
	AppServerVersion    string `json:"appServerVersion"`
}

func decodeCodexDaemonVersion(raw []byte) (map[string]any, error) {
	var version codexDaemonVersion
	if err := json.Unmarshal(raw, &version); err != nil {
		return nil, err
	}
	status := "ready"
	if version.Status != "running" || version.AppServerVersion == "" {
		status = "not_running"
	} else if version.ManagedCodexVersion != "" && version.ManagedCodexVersion != version.AppServerVersion {
		status = "restart_required"
	}
	return map[string]any{
		"status":          status,
		"daemon_status":   version.Status,
		"backend":         version.Backend,
		"cli_version":     version.CLIVersion,
		"managed_version": version.ManagedCodexVersion,
		"running_version": version.AppServerVersion,
		"managed_path":    version.ManagedCodexPath,
		"socket_path":     version.SocketPath,
		"checked_at_ms":   time.Now().UnixMilli(),
	}, nil
}

func (c *Codex) refreshRuntimeDiagnostics(codexHome string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.codexBin, "app-server", "daemon", "version")
	cmd.Env = append(os.Environ(), "CODEX_HOME="+codexHome)
	raw, err := cmd.Output()
	diagnostics := map[string]any{"status": "unknown", "checked_at_ms": time.Now().UnixMilli()}
	if err == nil {
		if decoded, decodeErr := decodeCodexDaemonVersion(raw); decodeErr == nil {
			diagnostics = decoded
		} else {
			diagnostics["error"] = "invalid daemon version response: " + decodeErr.Error()
		}
	} else {
		diagnostics["error"] = err.Error()
	}
	c.runtimeMu.Lock()
	c.runtimeDiagnostics = map[string]any{"codex": diagnostics}
	c.runtimeMu.Unlock()
	if diagnostics["status"] == "restart_required" {
		log.Printf("[codex] shared daemon runtime mismatch managed=%v running=%v", diagnostics["managed_version"], diagnostics["running_version"])
	}
}

// RuntimeDiagnostics returns a copy of the last daemon version probe without
// starting a process or blocking the status endpoint.
func (c *Codex) RuntimeDiagnostics() map[string]any {
	c.runtimeMu.RLock()
	defer c.runtimeMu.RUnlock()
	out := make(map[string]any, len(c.runtimeDiagnostics))
	for key, value := range c.runtimeDiagnostics {
		if fields, ok := value.(map[string]any); ok {
			copyFields := make(map[string]any, len(fields))
			for field, fieldValue := range fields {
				copyFields[field] = fieldValue
			}
			out[key] = copyFields
			continue
		}
		out[key] = value
	}
	return out
}

// SetDataDir points generation manifests at the bridge's configured state
// directory. It must be called during startup before the executor is shared.
func (c *Codex) SetDataDir(path string) {
	if strings.TrimSpace(path) != "" {
		c.dataDir = runtime.ExpandPath(path)
		c.loadMaintenance()
	}
}

func (c *Codex) state(id string) *codexState {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.states[id]
	if st == nil {
		st = newCodexState()
		c.states[id] = st
	}
	return st
}

// ensureServer spawns and initializes the singleton app-server if needed. It
// also reloads the process when Codex credentials change on disk. codex
// app-server reads auth.json at startup and otherwise keeps the old account in
// memory, so an external `codex login` cannot take effect without this restart.
func (c *Codex) ensureServer() error {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.serverRunningLocked() {
		// A shared daemon owns credential lifecycle. Restarting it from one Bridge
		// would disrupt desktop TUI and other Bridge clients, so only the legacy
		// private stdio mode performs auth-triggered process replacement.
		if c.appServerMode == "daemon" {
			return nil
		}
		fingerprint, err := codexAuthFingerprint(c.authPath)
		if err != nil {
			log.Printf("[codex] auth credential check failed: %v", err)
			return nil
		}
		if fingerprint == c.serverAuthFingerprint {
			c.authReloadDeferred = false
			return nil
		}
		if c.hasActiveWork() || c.rpc.pendingCount() > 0 {
			if !c.authReloadDeferred {
				log.Printf("[codex] auth credentials changed; reload deferred until Codex is idle")
				c.authReloadDeferred = true
			}
			return nil
		}

		log.Printf("[codex] auth credentials changed; restarting codex app-server")
		if err := c.stopServerLocked(); err != nil {
			return err
		}
	}
	return c.startServerLocked()
}

func (c *Codex) startServerLocked() error {
	log.Printf("[codex] connecting app-server mode=%s", c.appServerMode)
	if c.appSandbox == nil {
		if changed, err := ensureBrowserElicitationRouting(filepath.Dir(c.sessionsRoot)); err != nil {
			log.Printf("[codex] browser elicitation routing config failed: %v", err)
		} else if changed {
			log.Printf("[codex] Browser Use elicitations routed through bridge policy")
		}
	}
	codexHome := filepath.Dir(c.sessionsRoot)
	if c.appServerMode == "daemon" {
		for _, action := range []string{"start", "enable-remote-control"} {
			startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			managed := exec.CommandContext(startCtx, c.codexBin, "app-server", "daemon", action)
			managed.Env = append(os.Environ(), "CODEX_HOME="+codexHome)
			output, err := managed.CombinedOutput()
			cancel()
			if err != nil {
				return fmt.Errorf("codex app-server daemon %s: %w (%s)", action, err, strings.TrimSpace(string(output)))
			}
		}
		c.refreshRuntimeDiagnostics(codexHome)
		return c.startRemoteServerLocked(codexHome)
	}
	args := []string{"app-server"}
	if c.appSandbox != nil {
		args = append(args, "-c", `cli_auth_credentials_store="ephemeral"`, "-c", "project_root_markers=[]")
	}
	cmd := exec.Command(c.codexBin, args...)
	cmd.Env = append(os.Environ(), "CODEX_HOME="+codexHome)
	if c.appSandbox != nil {
		cmd.Env = c.appSandboxEnvironment()
		cmd.Dir = c.appSandbox.workspace
	}
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	authFingerprint := "shared-daemon"
	if c.appServerMode != "daemon" && c.appSandbox == nil {
		var err error
		authFingerprint, err = codexAuthFingerprint(c.authPath)
		if err != nil {
			_ = cmd.Process.Kill()
			return fmt.Errorf("read Codex credentials: %w", err)
		}
	}
	done := make(chan struct{})
	c.proc = cmd
	c.stdin = stdinPipe
	c.procDone = done
	c.serverAuthFingerprint = authFingerprint
	c.authReloadDeferred = false
	// Write raw to the pipe (line-delimited JSON, one syscall per line, serialized
	// by the plumber's write mutex) so no flush step can be skipped.
	c.rpc.setWriter(stdinPipe)

	go c.readLoop(stdoutPipe)
	go drainStderr("codex", stderrPipe)
	go func(started *exec.Cmd, exited chan struct{}) {
		_ = cmd.Wait()
		c.rpc.failAll(errProcDead)
		c.invalidateLiveThreads()
		close(exited)
		c.startMu.Lock()
		if c.proc == started {
			c.proc = nil
			c.stdin = nil
			c.procDone = nil
			c.rpc.setWriter(nil)
		}
		c.startMu.Unlock()
	}(cmd, done)

	if err := c.initializeRPC(); err != nil {
		return err
	}
	if c.appSandbox != nil {
		if err := c.loginExternalChatGPT(); err != nil {
			_ = c.stopServerLocked()
			return err
		}
		return nil
	}
	c.startAuthWatcher()
	return nil
}

func (c *Codex) initializeRPC() error {
	if _, err := c.rpc.request("initialize", map[string]any{
		"clientInfo": map[string]any{
			"name":    "claude-bridge",
			"title":   "Averything Bridge",
			"version": "1.0",
		},
		"capabilities": map[string]any{
			"experimentalApi":                true,
			"requestAttestation":             false,
			"mcpServerOpenaiFormElicitation": true,
		},
	}, 30*time.Second); err != nil {
		return err
	}
	if err := c.rpc.notify("initialized", nil); err != nil {
		return err
	}
	return nil
}

type websocketRPCWriter struct {
	conn *websocket.Conn
}

func (w websocketRPCWriter) Write(p []byte) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.conn.Write(ctx, websocket.MessageText, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *Codex) daemonSocketPath(codexHome string) string {
	if c.appServerSocket != "" {
		return c.appServerSocket
	}
	return filepath.Join(codexHome, "app-server-control", "app-server-control.sock")
}

func (c *Codex) startRemoteServerLocked(codexHome string) error {
	socketPath := c.daemonSocketPath(codexHome)
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	httpClient := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}}
	ctx, cancelDial := context.WithTimeout(context.Background(), 15*time.Second)
	conn, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: httpClient})
	cancelDial()
	if err != nil {
		return fmt.Errorf("connect shared codex app-server at %s: %w", socketPath, err)
	}
	conn.SetReadLimit(codexRemoteReadLimit)
	readCtx, cancelRead := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.remoteConn = conn
	c.remoteDone = done
	c.remoteCancel = cancelRead
	c.serverAuthFingerprint = "shared-daemon"
	c.authReloadDeferred = false
	c.rpc.setWriter(websocketRPCWriter{conn: conn})
	c.toolEnvironment.epoch.Add(1)
	go c.readRemoteLoop(readCtx, conn, done)
	if err := c.initializeRPC(); err != nil {
		cancelRead()
		_ = conn.Close(websocket.StatusInternalError, "initialize failed")
		// readRemoteLoop closes done before taking startMu, so it is safe to wait
		// here even though the caller holds startMu. Clear the half-open transport
		// before a reconnect attempt can mistake it for a healthy connection.
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		if c.remoteConn == conn {
			c.remoteConn = nil
			c.remoteDone = nil
			c.remoteCancel = nil
			c.rpc.setWriter(nil)
		}
		return err
	}
	log.Printf("[codex] connected shared daemon socket=%s", socketPath)
	return nil
}

func (c *Codex) readRemoteLoop(ctx context.Context, conn *websocket.Conn, done chan struct{}) {
	var readErr error
	defer func() {
		failure := error(errProcDead)
		unexpected := readErr != nil && ctx.Err() == nil
		if unexpected {
			failure = fmt.Errorf("app-server connection lost: %w", readErr)
			log.Printf("[codex] remote read loop error: %v", readErr)
		}
		c.rpc.failAll(failure)
		c.failVoiceCalls()
		if unexpected {
			c.failLiveOperations(failure.Error())
			c.expireDisconnectedInteractions()
		}
		c.invalidateLiveThreads()
		close(done)
		c.startMu.Lock()
		if c.remoteConn == conn {
			c.remoteConn = nil
			c.remoteDone = nil
			c.remoteCancel = nil
			c.rpc.setWriter(nil)
		}
		c.startMu.Unlock()
		if unexpected {
			c.scheduleRemoteReconnect()
		}
	}()
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			readErr = err
			return
		}
		if !c.rpc.dispatchResponse(payload) {
			c.dispatch(payload)
		}
	}
}

func (c *Codex) failLiveOperations(message string) {
	c.mu.Lock()
	states := make(map[string]*codexState, len(c.states))
	for sessionID, st := range c.states {
		states[sessionID] = st
	}
	c.mu.Unlock()
	for sessionID, st := range states {
		st.mu.Lock()
		turnActive := st.turnActive
		compactActive := st.compactActive
		observedRequestID := st.observedRequestID
		st.observedTurnID, st.observedRequestID = "", ""
		st.mu.Unlock()
		if observedRequestID != "" {
			// Transport loss is not native completion. Do not retire the native
			// ID: correlated live activity after reattachment can recover it.
			c.sink.Emit(backend.ObservedTurn{SessionID: sessionID, RequestID: observedRequestID, Phase: "interrupted", Message: message})
		}
		if turnActive {
			st.finish(message + "; the submitted turn was not retried automatically")
		}
		if compactActive {
			st.finishCompact(message)
		}
	}
}

func (c *Codex) expireDisconnectedInteractions() {
	c.interMu.Lock()
	pending := make([]codexInteraction, 0, len(c.interactions))
	for _, interaction := range c.interactions {
		if interaction.nativeThread != "" {
			continue // Ordinary async annotations survive an RPC transport loss.
		}
		pending = append(pending, interaction)
	}
	for id, interaction := range c.interactions {
		if interaction.nativeThread == "" {
			delete(c.interactions, id)
		}
	}
	c.interMu.Unlock()
	for _, interaction := range pending {
		c.sink.Emit(backend.NewInteractionResolved(interaction.payload.RequestID, interaction.payload.SessionID, "expired"))
	}
}

func (c *Codex) scheduleRemoteReconnect() {
	if !c.remoteReconnect || c.appServerMode != "daemon" {
		return
	}
	c.reconnectMu.Lock()
	if c.reconnectRunning {
		c.reconnectMu.Unlock()
		return
	}
	c.reconnectRunning = true
	c.reconnectMu.Unlock()
	go func() {
		defer func() {
			c.reconnectMu.Lock()
			c.reconnectRunning = false
			c.reconnectMu.Unlock()
		}()
		delay := 250 * time.Millisecond
		for attempt := 1; attempt <= 6; attempt++ {
			time.Sleep(delay)
			if err := c.ensureServer(); err == nil {
				log.Printf("[codex] shared daemon reconnected after %d attempt(s)", attempt)
				return
			} else {
				log.Printf("[codex] shared daemon reconnect attempt=%d failed: %v", attempt, err)
			}
			if delay < 4*time.Second {
				delay *= 2
			}
		}
		log.Printf("[codex] shared daemon reconnect exhausted; the next request will retry")
	}()
}

func codexAuthFingerprint(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "missing", nil
		}
		return "", err
	}
	sum := sha256.Sum256(b)
	return fmt.Sprintf("sha256:%x", sum), nil
}

func (c *Codex) startAuthWatcher() {
	c.authWatchOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				if c.authCredentialsChanged() {
					if err := c.ensureServer(); err != nil {
						log.Printf("[codex] automatic auth reload failed: %v", err)
					}
				}
			}
		}()
	})
}

func (c *Codex) authCredentialsChanged() bool {
	if c.appSandbox != nil {
		return false
	}
	fingerprint, err := codexAuthFingerprint(c.authPath)
	if err != nil {
		return false
	}
	c.startMu.Lock()
	defer c.startMu.Unlock()
	return c.serverRunningLocked() && fingerprint != c.serverAuthFingerprint
}

func (c *Codex) serverRunningLocked() bool {
	if c.appServerMode == "daemon" {
		if c.remoteConn == nil {
			return false
		}
		if c.remoteDone == nil {
			return true
		}
		select {
		case <-c.remoteDone:
			return false
		default:
			return true
		}
	}
	if c.proc == nil {
		return false
	}
	if c.procDone == nil {
		return true
	}
	select {
	case <-c.procDone:
		return false
	default:
		return true
	}
}

func (c *Codex) hasActiveWork() bool {
	c.mu.Lock()
	states := make([]*codexState, 0, len(c.states))
	for _, st := range c.states {
		states = append(states, st)
	}
	c.mu.Unlock()

	for _, st := range states {
		st.mu.Lock()
		active := st.turnActive || st.compactActive
		st.mu.Unlock()
		if active {
			return true
		}
	}
	return false
}

func (c *Codex) stopServerLocked() error {
	if c.appServerMode == "daemon" {
		conn, done, cancel := c.remoteConn, c.remoteDone, c.remoteCancel
		if cancel != nil {
			cancel()
		}
		if conn != nil {
			_ = conn.Close(websocket.StatusNormalClosure, "bridge client closing")
		}
		if done != nil {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				return fmt.Errorf("timed out closing shared app-server client")
			}
		}
		c.remoteConn = nil
		c.remoteDone = nil
		c.remoteCancel = nil
		c.rpc.setWriter(nil)
		return nil
	}
	cmd := c.proc
	if cmd == nil || !c.serverRunningLocked() {
		c.proc = nil
		c.stdin = nil
		c.procDone = nil
		c.rpc.setWriter(nil)
		return nil
	}

	done := c.procDone
	if c.stdin != nil {
		_ = c.stdin.Close()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				return fmt.Errorf("timed out stopping codex app-server")
			}
		}
	} else {
		_ = cmd.Process.Kill()
	}
	if c.proc == cmd {
		c.proc = nil
		c.stdin = nil
		c.procDone = nil
		c.rpc.setWriter(nil)
	}
	return nil
}

// Catalog reads the app-server catalog instead of duplicating model ids in the
// bridge. The last successful result is retained for transient reconnects.
func (c *Codex) Catalog(ctx context.Context) (backend.Definition, error) {
	if err := c.ensureServer(); err != nil {
		return backend.Definition{}, err
	}
	raw, err := c.rpcCall("model/list", map[string]any{"limit": 100, "includeHidden": false}, 20*time.Second)
	if err != nil {
		return backend.Definition{}, err
	}
	var response struct {
		Data []struct {
			ID          string `json:"id"`
			Model       string `json:"model"`
			DisplayName string `json:"displayName"`
			Description string `json:"description"`
			Hidden      bool   `json:"hidden"`
			Supported   []struct {
				Effort string `json:"reasoningEffort"`
			} `json:"supportedReasoningEfforts"`
			DefaultEffort       string   `json:"defaultReasoningEffort"`
			InputModalities     []string `json:"inputModalities"`
			SupportsPersonality bool     `json:"supportsPersonality"`
			ServiceTiers        []struct {
				ID          string `json:"id"`
				Name        string `json:"name"`
				Description string `json:"description"`
			} `json:"serviceTiers"`
			DefaultServiceTier *string `json:"defaultServiceTier"`
			IsDefault          bool    `json:"isDefault"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return backend.Definition{}, err
	}
	def := backend.Definition{ID: backend.Codex, Label: "Codex", Capabilities: backend.Capabilities{History: true, Usage: true, Interactions: true, Sandbox: true, Images: true, Files: true, Steering: true}}
	for _, m := range response.Data {
		if m.Hidden {
			continue
		}
		id := firstNonEmpty(m.Model, m.ID)
		bm := backend.Model{ID: id, Label: firstNonEmpty(m.DisplayName, id), Description: m.Description, DefaultReasoningEffort: m.DefaultEffort, InputModalities: m.InputModalities, SupportsPersonality: m.SupportsPersonality, IsDefault: m.IsDefault}
		for _, e := range m.Supported {
			bm.SupportedReasoningEfforts = append(bm.SupportedReasoningEfforts, e.Effort)
		}
		for _, t := range m.ServiceTiers {
			bm.ServiceTiers = append(bm.ServiceTiers, backend.ServiceTier{ID: t.ID, Name: t.Name, Description: t.Description})
		}
		if m.DefaultServiceTier != nil {
			bm.DefaultServiceTier = *m.DefaultServiceTier
		}
		def.Models = append(def.Models, bm)
		if m.IsDefault {
			def.DefaultModel = id
		}
	}
	if def.DefaultModel == "" && len(def.Models) > 0 {
		def.DefaultModel = def.Models[0].ID
	}
	if modesRaw, modeErr := c.rpcCall("collaborationMode/list", map[string]any{}, 10*time.Second); modeErr == nil {
		var modes struct {
			Data []struct {
				Name   string  `json:"name"`
				Mode   *string `json:"mode"`
				Model  *string `json:"model"`
				Effort *string `json:"reasoning_effort"`
			} `json:"data"`
		}
		if json.Unmarshal(modesRaw, &modes) == nil {
			modeMap := make(map[string]map[string]any)
			for _, m := range modes.Data {
				mode := "default"
				if m.Mode != nil {
					mode = *m.Mode
				}
				model := def.DefaultModel
				if m.Model != nil {
					model = *m.Model
				}
				effort := any(nil)
				if m.Effort != nil {
					effort = *m.Effort
				}
				def.CollaborationModes = append(def.CollaborationModes, backend.CollaborationMode{Name: m.Name, Mode: mode, Model: model, ReasoningEffort: func() string {
					if m.Effort != nil {
						return *m.Effort
					}
					return ""
				}()})
				modeMap[strings.ToLower(m.Name)] = map[string]any{"mode": mode, "settings": map[string]any{"model": model, "reasoning_effort": effort, "developer_instructions": nil}}
				modeMap[strings.ToLower(mode)] = modeMap[strings.ToLower(m.Name)]
			}
			c.catalogMu.Lock()
			c.collaborationModes = modeMap
			c.catalogMu.Unlock()
		}
	}
	c.catalogMu.Lock()
	c.catalog = def
	c.catalogMu.Unlock()
	return def, nil
}

// defaultModel follows the authenticated app-server catalog when available.
// The static value is only a transport-startup fallback; keeping the runtime
// catalog authoritative lets newly rolled-out models become the default without
// requiring every client to send an explicit model id.
func (c *Codex) defaultModel() string {
	c.catalogMu.RLock()
	model := c.catalog.DefaultModel
	c.catalogMu.RUnlock()
	if model != "" {
		return model
	}
	return codexDefaultModel
}

func (c *Codex) invalidateLiveThreads() {
	c.mu.Lock()
	c.threadToSession = make(map[string]*session.Session)
	c.activeThreadOwner = make(map[string]*session.Session)
	states := make([]*codexState, 0, len(c.states))
	for _, st := range c.states {
		states = append(states, st)
	}
	c.mu.Unlock()

	for _, st := range states {
		st.mu.Lock()
		st.threadID = ""
		st.currentTurnID = ""
		st.mu.Unlock()
	}
}

// rpcCall sends an RPC and waits for the response. Writes go straight to the
// pipe, so there is no flush step.
func (c *Codex) rpcCall(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	return c.rpcCallAtEpoch(method, params, timeout, nil)
}

func (c *Codex) rpcCallAtEpoch(method string, params any, timeout time.Duration, epoch *uint64) (json.RawMessage, error) {
	// A confirmed tool maintenance window serializes our submission RPCs, not
	// interrupt/release or read-only diagnostics. It never restarts the daemon.
	if toolMutationMethod(method) {
		c.toolRPCGate.RLock()
		defer c.toolRPCGate.RUnlock()
	}
	if c.appServerMode == "daemon" && healthGatedMethod(method) {
		if err := c.healthAdmission(); err != nil {
			return nil, err
		}
	}
	raw, err := c.rpc.requestAtEpoch(method, params, timeout, epoch)
	var timeoutErr *rpcTimeoutError
	if c.appServerMode == "daemon" && (errors.As(err, &timeoutErr) || errors.Is(err, context.DeadlineExceeded)) && healthGatedMethod(method) {
		c.scheduleHealthCheck(method, params)
		return raw, fmt.Errorf("%w; server outcome unknown; request was not retried; checking Codex health", err)
	}
	return raw, err
}

// readLoop consumes the app-server's newline-delimited stdout. It uses a
// bufio.Reader rather than a bufio.Scanner because a thread/resume reply can
// serialize an entire thread's history into a single line — hundreds of MB,
// far beyond any fixed buffer cap. Scanner would hit ErrTooLong, silently end
// the loop, and (since the app-server is a singleton) wedge every codex
// session. ReadBytes grows unbounded, matching the Python reference's
// readline().
func (c *Codex) readLoop(stdout io.Reader) {
	r := bufio.NewReaderSize(stdout, 64*1024)
	for {
		line, err := r.ReadBytes('\n')
		if n := trimLineEnd(line); n > 0 {
			raw := make(json.RawMessage, n)
			copy(raw, line[:n])
			if !c.rpc.dispatchResponse(raw) {
				c.dispatch(raw)
			}
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("[codex] read loop error: %v", err)
			}
			break
		}
	}
	log.Printf("[codex] read loop exited")
}

// trimLineEnd returns the content length of a line read by ReadBytes('\n'),
// excluding the trailing \n and an optional preceding \r.
func trimLineEnd(line []byte) int {
	n := len(line)
	if n > 0 && line[n-1] == '\n' {
		n--
		if n > 0 && line[n-1] == '\r' {
			n--
		}
	}
	return n
}

type codexMsg struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type codexTokenUsage struct {
	Last struct {
		TotalTokens  int `json:"totalTokens"`
		TotalTokens2 int `json:"total_tokens"`
	} `json:"last"`
	ModelContextWindow  int `json:"modelContextWindow"`
	ModelContextWindow2 int `json:"model_context_window"`
}

func (c *Codex) dispatch(raw json.RawMessage) {
	if c.nativeFixtureObserver != nil {
		c.nativeFixtureObserver(raw)
	}
	var m codexMsg
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	if m.Method == "thread/settings/updated" {
		c.observeVoicePermissionSettings(m.Params)
	}
	// Server→client request (has id + method, no result/error).
	if len(m.ID) > 0 && string(m.ID) != "null" && m.Method != "" {
		// Keep the request ID as raw JSON so app-server string IDs and numeric
		// IDs are echoed with their original type and value.
		c.handleServerRequest(m.ID, m.Method, m.Params)
		return
	}
	if c.dispatchVoice(m.Method, m.Params) {
		return
	}

	var p struct {
		ThreadID  string            `json:"threadId"`
		TurnID    string            `json:"turnId"`
		RequestID json.RawMessage   `json:"requestId"`
		Diff      string            `json:"diff"`
		Message   string            `json:"message"`
		Changes   []map[string]any  `json:"changes"`
		Delta     string            `json:"delta"`
		Phase     string            `json:"phase"`
		Text      string            `json:"text"`
		Output    json.RawMessage   `json:"output"`
		ItemID    string            `json:"itemId"`
		CallID    string            `json:"callId"`
		Name      string            `json:"name"`
		Command   json.RawMessage   `json:"command"`
		Plan      []json.RawMessage `json:"plan"`
		Turn      struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Error  struct {
				Message string          `json:"message"`
				Info    json.RawMessage `json:"codexErrorInfo"`
			} `json:"error"`
		} `json:"turn"`
		Item struct {
			ID                string          `json:"id"`
			Name              string          `json:"name"`
			Type              string          `json:"type"`
			Command           json.RawMessage `json:"command"`
			Output            json.RawMessage `json:"output"`
			AggregatedOutput  json.RawMessage `json:"aggregatedOutput"`
			Status            string          `json:"status"`
			Tool              string          `json:"tool"`
			SenderThreadID    string          `json:"senderThreadId"`
			ReceiverThreadIDs []string        `json:"receiverThreadIds"`
			Prompt            *string         `json:"prompt"`
			Model             *string         `json:"model"`
			AgentsStates      map[string]struct {
				Status  string  `json:"status"`
				Message *string `json:"message"`
			} `json:"agentsStates"`
		} `json:"item"`
		Thread struct {
			ID             string  `json:"id"`
			ParentThreadID *string `json:"parentThreadId"`
			AgentNickname  *string `json:"agentNickname"`
			AgentRole      *string `json:"agentRole"`
		} `json:"thread"`
		WillRetry *bool `json:"willRetry"`
		Error     struct {
			Message string          `json:"message"`
			Info    json.RawMessage `json:"codexErrorInfo"`
		} `json:"error"`
		TokenUsage codexTokenUsage `json:"tokenUsage"`
		Usage      codexTokenUsage `json:"usage"`
		Goal       backend.Goal    `json:"goal"`
	}
	if json.Unmarshal(m.Params, &p) != nil {
		return
	}
	if p.ThreadID == "" {
		p.ThreadID = p.Thread.ID
	}

	c.mu.Lock()
	s := c.activeThreadOwner[p.ThreadID]
	if s == nil {
		s = c.threadToSession[p.ThreadID]
	}
	if s == nil && p.Thread.ParentThreadID != nil {
		s = c.activeThreadOwner[*p.Thread.ParentThreadID]
		if s == nil {
			s = c.threadToSession[*p.Thread.ParentThreadID]
		}
		if s != nil {
			c.threadToSession[p.ThreadID] = s
		}
	}
	c.mu.Unlock()
	if s == nil {
		// Reconciliation can know a native conversation without claiming or
		// resuming its writer. Route only its async annotations via that map.
		if m.Method == "item/completed" {
			c.asyncMu.Lock()
			recipient := c.asyncSessions[p.ThreadID]
			c.asyncMu.Unlock()
			if recipient != nil {
				c.observeAsyncItem(recipient, p.ThreadID, m.Params)
			}
		}
		return
	}
	// Async annotations are independent of the generating turn's lifetime. A
	// replayed completed item must not be discarded by retired-turn filtering.
	if m.Method == "item/completed" {
		c.observeAsyncItem(s, p.ThreadID, m.Params)
	}
	st := c.state(s.ID)
	st.mu.Lock()
	reqID := st.reqID
	rootThreadID := st.threadID
	st.mu.Unlock()
	isRootThread := p.ThreadID == rootThreadID
	st.mu.Lock()
	if isRootThread && (m.Method == "turn/started" || m.Method == "turn/completed") {
		st.nativeBoundary++
	}
	pendingStart := st.turnActive && st.turnStartPending
	st.mu.Unlock()
	if pendingStart {
		st.mu.Lock()
		if isRootThread && firstNonEmpty(p.TurnID, p.Turn.ID) != "" {
			st.unconfirmedActivity = true
		}
		st.mu.Unlock()
		if isRootThread && firstNonEmpty(p.TurnID, p.Turn.ID) != "" && (strings.HasPrefix(m.Method, "item/") || m.Method == "turn/plan/updated" || m.Method == "turn/diff/updated") {
			st.mu.Lock()
			if st.turnStartPending && len(raw) <= 64<<10 && len(st.pendingNotifications) < 64 && st.pendingNotificationBytes+len(raw) <= 256<<10 {
				st.pendingNotifications = append(st.pendingNotifications, append(json.RawMessage(nil), raw...))
				st.pendingNotificationBytes += len(raw)
			}
			st.mu.Unlock()
		}
		// Notification order does not establish input ownership. Keep only a
		// bounded terminal candidate; publish no stream/tool/child state yet.
		if isRootThread && m.Method == "turn/completed" {
			st.completeOwnedTurn(codexTurnTerminal{ID: p.Turn.ID, Status: p.Turn.Status, Message: p.Turn.Error.Message,
				ErrorCode: codexErrorCode(p.Turn.Error.Info, p.Turn.Error.Message), Failure: recovery.ClassifyCodex(p.Turn.Error.Info, p.Turn.Error.Message)})
		} else if isRootThread && m.Method == "error" && p.TurnID != "" && p.WillRetry != nil && !*p.WillRetry {
			st.completeOwnedTurn(codexTurnTerminal{ID: p.TurnID, Status: "failed", Message: p.Error.Message,
				ErrorCode: codexErrorCode(p.Error.Info, p.Error.Message), Failure: recovery.ClassifyCodex(p.Error.Info, p.Error.Message), NativeWillRetry: p.WillRetry})
		}
		return
	}
	if isRootThread {
		turnID := firstNonEmpty(p.TurnID, p.Turn.ID)
		if m.Method == "error" && turnID == "" {
			return
		}
		var accept bool
		reqID, accept = c.observeNativeTurn(s, st, m.Method, turnID)
		if !accept {
			return
		}
	}
	st.touch(time.Now())
	if isRootThread {
		c.observeMediaNative(s, m.Method, m.Params)
		c.observeLineNative(s, m.Method, m.Params)
		c.observeVoiceControlNative(s, m.Method, m.Params)
	}

	switch m.Method {
	case "mcpServer/startupStatus/updated":
		c.toolEnvironment.epoch.Add(1)
		c.toolEnvironment.mu.Lock()
		c.toolEnvironment.cache = nil
		c.toolEnvironment.mu.Unlock()
	case "serverRequest/resolved":
		c.resolveServerRequest(p.RequestID)

	case "thread/started":
		if p.Thread.ParentThreadID != nil {
			c.ensureCodexAgent(s, p.Thread.ID, *p.Thread.ParentThreadID, firstPtr(p.Thread.AgentRole, p.Thread.AgentNickname, "subagent"), "")
			c.emitCodexAgentTree(s)
		}

	case "turn/started":
		if !isRootThread {
			return
		}
		st.mu.Lock()
		if st.compactActive {
			st.compactTurnID = p.Turn.ID
			st.mu.Unlock()
			c.updateMaintenance(s.ID, "running", "", p.Turn.ID)
			return
		}
		if st.turnActive {
			st.currentTurnID = p.Turn.ID
		}
		st.mu.Unlock()
		c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "thinking", ""))

	case "item/agentMessage/delta":
		if p.Delta == "" {
			return
		}
		if !isRootThread {
			c.appendCodexAgentOutput(st, p.ThreadID, p.Delta)
			c.emitCodexAgentTree(s)
			return
		}
		if p.Phase == "commentary" {
			// Commentary is user-facing progress (the same channel used for
			// intermediate assistant updates), not private model reasoning.  Sending
			// it as thinking_chunk buries every live update in a collapsed thinking
			// card and makes an active Codex turn look as if it is not streaming.
			chunk := backend.NewTextChunk(s.ID, reqID, p.Delta)
			chunk.SourceItemID = p.ItemID
			c.sink.Emit(chunk)
			return
		}
		c.appendCodexAgentOutput(st, p.ThreadID, p.Delta)
		st.mu.Lock()
		st.accumulatedText += p.Delta
		st.mu.Unlock()
		chunk := backend.NewTextChunk(s.ID, reqID, p.Delta)
		chunk.SourceItemID = p.ItemID
		c.sink.Emit(chunk)

	case "item/reasoning/textDelta":
		if !isRootThread {
			return
		}
		d := p.Delta
		if d == "" {
			d = p.Text
		}
		if d != "" {
			c.sink.Emit(backend.NewThinkingChunk(s.ID, reqID, d))
		}

	case "item/commandExecution/outputDelta", "item/fileChange/outputDelta", "item/commandExecution/terminalInteraction":
		if !isRootThread {
			return
		}
		itemID := firstNonEmpty(p.ItemID, p.CallID, "codex_item")
		d := p.Delta
		if d == "" {
			d = p.Text
		}
		if d != "" {
			c.tools.Delta(s.ID, reqID, itemID, d)
		}

	case "item/started":
		if isRootThread && p.Item.Type == "contextCompaction" {
			c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "composing", "正在整理上下文；後續訊息會依序處理"))
			return
		}
		if p.Item.Type == "collabAgentToolCall" {
			c.updateCodexCollabItem(s, st, p.Item.Tool, p.Item.SenderThreadID, p.Item.ReceiverThreadIDs, p.Item.Prompt, p.Item.Model, p.Item.AgentsStates, time.Now().UnixMilli())
			return
		}
		c.recordCodexAgentTool(st, p.ThreadID, p.Item.Type, time.Now().UnixMilli())
		if !isRootThread {
			c.emitCodexAgentTree(s)
			return
		}
		tool, ok := normalizeCodexLiveTool(m.Params)
		if !ok {
			return
		}
		c.tools.Start(s.ID, reqID, tool.ID, tool.Name, tool.Command)

	case "item/completed":
		if isRootThread && p.Item.Type == "contextCompaction" {
			st.mu.Lock()
			managedCompact := st.compactActive
			st.mu.Unlock()
			// Native automatic compaction continues the same model turn. Managed
			// /compact instead clears through its durable maintenance terminal.
			if !managedCompact {
				c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "thinking", ""))
			}
			return
		}
		if p.Item.Type == "collabAgentToolCall" {
			c.updateCodexCollabItem(s, st, p.Item.Tool, p.Item.SenderThreadID, p.Item.ReceiverThreadIDs, p.Item.Prompt, p.Item.Model, p.Item.AgentsStates, time.Now().UnixMilli())
			return
		}
		if !isRootThread {
			c.emitCodexAgentTree(s)
			return
		}
		if _, isTool := normalizeCodexLiveTool(m.Params); !isTool {
			return
		}
		itemID := firstNonEmpty(p.ItemID, p.Item.ID, "codex_item")
		rawOutput := p.Output
		if len(rawOutput) == 0 || string(rawOutput) == "null" {
			rawOutput = p.Item.Output
		}
		if len(rawOutput) == 0 || string(rawOutput) == "null" {
			rawOutput = p.Item.AggregatedOutput
		}
		generatedPaths := codexGeneratedImagePaths(rawOutput)
		output := rawToString(rawOutput)
		if codexHasInlineImage(rawOutput) {
			if len(generatedPaths) > 0 {
				output = "Generated image:\n" + strings.Join(generatedPaths, "\n")
			} else {
				output = "Generated image completed, but no saved file path was returned."
			}
		}
		if output != "" {
			c.tools.Result(s.ID, reqID, itemID, output)
		}
		c.tools.End(s.ID, reqID, itemID)
		for _, path := range generatedPaths {
			c.sink.Emit(protocol.Media{
				Type: "media", SessionID: s.ID, RequestID: reqID,
				MediaType: "image", Path: path,
			})
		}

	case "turn/plan/updated":
		// Codex update_plan → normalized todo panel. Full replace; step→content.
		if todos := normalizeFullList(p.Plan, "step"); len(todos) > 0 {
			c.sink.Emit(backend.NewTodoUpdate(s.ID, reqID, todosValue(todos)))
		}

	case "turn/diff/updated":
		c.sink.Emit(protocol.NewCodexLiveDiff(s.ID, reqID, p.Diff))

	case "item/fileChange/patchUpdated":
		itemID := firstNonEmpty(p.ItemID, "codex_patch")
		c.tools.Delta(s.ID, reqID, itemID, codexJSON(p.Changes))

	case "item/mcpToolCall/progress":
		itemID := firstNonEmpty(p.ItemID, "codex_mcp")
		c.tools.Delta(s.ID, reqID, itemID, p.Message+"\n")

	case "thread/goal/updated":
		if p.Goal.ThreadID != "" {
			c.sink.Emit(backend.NewGoalUpdate(s.ID, p.Goal))
		}

	case "thread/goal/cleared":
		c.sink.Emit(backend.NewGoalCleared(s.ID))

	case "turn/completed":
		if !isRootThread {
			c.finishCodexAgent(st, p.ThreadID, time.Now().UnixMilli())
			c.emitCodexAgentTree(s)
			return
		}
		phase, message := "completed", p.Turn.Error.Message
		if p.Turn.Status == "interrupted" {
			phase = "interrupted"
		} else if p.Turn.Status == "failed" {
			phase = "failed"
			message = firstNonEmpty(message, "turn failed")
		}
		failure := recovery.ClassifyCodex(p.Turn.Error.Info, message)
		if c.finishObservedTurn(s, st, p.Turn.ID, phase, failure.ErrorCode(), failure.PublicMessage(message)) {
			if phase == "failed" {
				c.recordCodexFailure(s.ID, reqID, p.ThreadID, p.Turn.ID, failure, recovery.Context{Acceptance: recovery.Accepted, Terminal: true})
			}
			return
		}
		st.mu.Lock()
		compacting := st.compactActive
		compactTurnID := st.compactTurnID
		st.mu.Unlock()
		if compacting {
			if compactTurnID != "" && p.Turn.ID == compactTurnID {
				st.finishCompact(p.Turn.Error.Message)
			}
			return
		}
		st.completeOwnedTurn(codexTurnTerminal{ID: p.Turn.ID, Status: p.Turn.Status,
			Message: p.Turn.Error.Message, ErrorCode: codexErrorCode(p.Turn.Error.Info, p.Turn.Error.Message),
			Failure: recovery.ClassifyCodex(p.Turn.Error.Info, p.Turn.Error.Message)})

	case "thread/compacted":
		// Modern daemons report a correlated turn/completed as well. Wait for
		// it so an uncorrelated, delayed compact notification cannot release
		// a later operation. Legacy daemons do not send turn/started.
		st.mu.Lock()
		legacyCompact := isRootThread && st.compactActive && st.compactTurnID == ""
		st.mu.Unlock()
		if legacyCompact {
			st.finishCompact("")
		}

	case "thread/tokenUsage/updated":
		if !isRootThread {
			return
		}
		used, maxCtx := codexUsageValues(p.TokenUsage)
		if used == 0 && maxCtx == 0 {
			used, maxCtx = codexUsageValues(p.Usage)
		}
		st.mu.Lock()
		if used > 0 {
			st.contextUsed = used
		}
		if maxCtx > 0 {
			st.contextMax = maxCtx
		}
		st.mu.Unlock()

	case "error":
		failure := recovery.ClassifyCodex(p.Error.Info, p.Error.Message)
		if !isRootThread {
			// A child retry is not a parent terminal, nor a completed child.
			if p.WillRetry != nil && !*p.WillRetry {
				c.finishCodexAgent(st, p.ThreadID, time.Now().UnixMilli())
				c.emitCodexAgentTree(s)
			}
			return
		}
		if p.WillRetry != nil && !*p.WillRetry {
			msg := p.Error.Message
			if msg == "" {
				msg = "unknown codex error"
			}
			// An uncorrelated error cannot end a newer operation on this thread.
			if p.TurnID == "" {
				return
			}
			if c.finishObservedTurn(s, st, p.TurnID, "failed", failure.ErrorCode(), failure.PublicMessage(msg)) {
				c.recordCodexFailure(s.ID, reqID, p.ThreadID, p.TurnID, failure, recovery.Context{Acceptance: recovery.Accepted, Terminal: true, NativeWillRetry: p.WillRetry})
				return
			}
			st.mu.Lock()
			compacting := st.compactActive
			compactTurnID := st.compactTurnID
			st.mu.Unlock()
			if compacting {
				if p.TurnID == compactTurnID {
					st.finishCompact(msg)
				}
			} else if p.TurnID != "" {
				st.completeOwnedTurn(codexTurnTerminal{ID: p.TurnID, Status: "failed",
					Message: msg, ErrorCode: failure.ErrorCode(), Failure: failure, NativeWillRetry: p.WillRetry})
			}
		} else {
			c.observeCodexRetryHint(s, st, p.ThreadID, p.TurnID, failure, p.WillRetry)
		}
	}
}

func codexHasInlineImage(raw json.RawMessage) bool {
	return codexInlineImageRE.Match(bytes.TrimSpace(raw))
}

func codexGeneratedImagePaths(raw json.RawMessage) []string {
	if !codexHasInlineImage(raw) {
		return nil
	}
	seen := make(map[string]bool)
	var paths []string
	for _, match := range codexGeneratedImagePathRE.FindAllSubmatch(raw, -1) {
		if len(match) < 2 {
			continue
		}
		path := strings.ReplaceAll(string(match[1]), `\/`, "/")
		if seen[path] {
			continue
		}
		info, err := os.Stat(path)
		if err != nil || info.IsDir() {
			continue
		}
		seen[path] = true
		paths = append(paths, path)
	}
	return paths
}

func firstPtr(a, b *string, fallback string) string {
	if a != nil && *a != "" {
		return *a
	}
	if b != nil && *b != "" {
		return *b
	}
	return fallback
}

func (c *Codex) ensureCodexAgent(s *session.Session, id, parentID, agentType, description string) {
	if id == "" {
		return
	}
	st := c.state(s.ID)
	now := time.Now().UnixMilli()
	st.mu.Lock()
	a := st.agents[id]
	if a == nil {
		a = &codexAgent{id: id, startMS: &now}
		st.agents[id] = a
	}
	if parentID != st.threadID {
		a.parentID = parentID
	}
	if agentType != "" {
		a.agentType = agentType
	}
	if description != "" {
		a.description = description
	}
	st.mu.Unlock()
	c.mu.Lock()
	c.threadToSession[id] = s
	c.mu.Unlock()
}

func (c *Codex) updateCodexCollabItem(s *session.Session, st *codexState, tool, sender string, receivers []string, prompt, model *string, states map[string]struct {
	Status  string  `json:"status"`
	Message *string `json:"message"`
}, now int64) {
	desc := firstPtr(prompt, nil, "")
	kind := firstPtr(model, nil, "subagent")
	for _, id := range receivers {
		c.ensureCodexAgent(s, id, sender, kind, desc)
	}
	st.mu.Lock()
	for id, state := range states {
		a := st.agents[id]
		if a == nil {
			a = &codexAgent{id: id, agentType: kind, description: desc, startMS: &now}
			st.agents[id] = a
		}
		if state.Message != nil && *state.Message != "" {
			a.output = *state.Message
		}
		switch state.Status {
		case "completed", "errored", "interrupted", "shutdown", "notFound":
			end := now
			a.endMS = &end
		}
	}
	st.mu.Unlock()
	if tool == "closeAgent" {
		for _, id := range receivers {
			st.mu.Lock()
			if a := st.agents[id]; a != nil {
				end := now
				a.endMS = &end
			}
			st.mu.Unlock()
		}
	}
	c.emitCodexAgentTree(s)
}

func (c *Codex) recordCodexAgentTool(st *codexState, threadID, name string, now int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if a := st.agents[threadID]; a != nil && name != "" {
		ts := now
		a.tools = append(a.tools, protocol.AgentToolCall{Name: name, TS: &ts})
	}
}

func (c *Codex) appendCodexAgentOutput(st *codexState, threadID, delta string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if a := st.agents[threadID]; a != nil {
		a.output += delta
		if len(a.output) > 1000 {
			a.output = a.output[len(a.output)-1000:]
		}
	}
}

func (c *Codex) finishCodexAgent(st *codexState, threadID string, now int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if a := st.agents[threadID]; a != nil {
		end := now
		a.endMS = &end
	}
}

func (c *Codex) emitCodexAgentTree(s *session.Session) {
	total, tree := c.BuildAgentTree(s.ResumeID())
	c.sink.Emit(protocol.NewAgentTree(s.ID, s.ResumeID(), total, tree))
}

// BuildAgentTree satisfies core.agentTreeBuilder for Codex and uses live
// app-server lifecycle data instead of parsing Claude transcripts.
func (c *Codex) BuildAgentTree(resumeID string) (int, []*protocol.AgentNode) {
	c.mu.Lock()
	var st *codexState
	for _, candidate := range c.states {
		candidate.mu.Lock()
		match := candidate.threadID == resumeID
		candidate.mu.Unlock()
		if match {
			st = candidate
			break
		}
	}
	c.mu.Unlock()
	if st == nil {
		return 0, []*protocol.AgentNode{}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	children := map[string][]*codexAgent{}
	for _, a := range st.agents {
		children[a.parentID] = append(children[a.parentID], a)
	}
	var build func(*codexAgent) *protocol.AgentNode
	build = func(a *codexAgent) *protocol.AgentNode {
		n := &protocol.AgentNode{AgentID: a.id, AgentType: firstNonEmpty(a.agentType, "subagent"), Description: a.description, StartTS: a.startMS, EndTS: a.endMS, ToolCalls: append([]protocol.AgentToolCall{}, a.tools...), OutputPreview: a.output, Children: []*protocol.AgentNode{}}
		if a.parentID != "" && a.parentID != st.threadID {
			p := a.parentID
			n.ParentAgentID = &p
		}
		if a.startMS != nil && a.endMS != nil {
			d := *a.endMS - *a.startMS
			n.DurationMS = &d
		}
		for _, child := range children[a.id] {
			n.Children = append(n.Children, build(child))
		}
		return n
	}
	roots := []*protocol.AgentNode{}
	for _, a := range st.agents {
		if a.parentID == "" || a.parentID == st.threadID {
			roots = append(roots, build(a))
		}
	}
	return len(st.agents), roots
}

func (c *Codex) handleServerRequest(id any, method string, raw json.RawMessage) {
	if c.handleExternalAuthRefresh(id, method, raw) {
		return
	}
	if c.handleTaskAPIServerRequest(id, method, raw) {
		return
	}
	if c.handleSessionControlServerRequest(id, method, raw) {
		return
	}
	if c.handlePMServerRequest(id, method, raw) {
		return
	}
	if c.handleDelegationServerRequest(id, method, raw) {
		return
	}
	approval := map[string]bool{
		"item/commandExecution/requestApproval": true,
		"item/fileChange/requestApproval":       true,
		"item/permissions/requestApproval":      true,
		"applyPatchApproval":                    true,
		"execCommandApproval":                   true,
	}
	switch {
	case approval[method]:
		c.emitCodexApproval(method, raw)
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"decision": "accept"}})
	case method == "item/tool/requestUserInput":
		if !c.createUserInputRequest(id, raw) {
			_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"answers": []any{}}})
		}
	case method == "mcpServer/elicitation/request":
		if result, handled := mcpAutomaticResponse(raw); handled {
			_ = c.rpc.write(map[string]any{"id": id, "result": result})
		} else if !c.createMcpElicitationRequest(id, raw) {
			_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"action": "cancel", "content": nil, "_meta": nil}})
		}
	case method == "item/tool/call":
		if !c.createDynamicToolRequest(id, raw) {
			toolName := c.emitUnsupportedCodexTool(raw)
			_ = c.rpc.write(map[string]any{"id": id, "error": map[string]any{"code": -32000, "message": "Codex hosted tool '" + toolName + "' is not supported by this bridge"}})
		}
	case method == "currentTime/read":
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"currentTimeAt": time.Now().Unix()}})
	default:
		_ = c.rpc.write(map[string]any{"id": id, "error": map[string]any{"code": -32601, "message": "unknown method: " + method}})
	}
}

func (c *Codex) emitCodexApproval(method string, raw json.RawMessage) {
	params := codexParams(raw)
	s := c.sessionForCodexParams(params)
	if s == nil {
		return
	}
	itemID := codexRequestItemID(params, "codex_approval_"+randHex(8))
	command := codexAnyString(codexFirstAny(params, "command", "changes", "permission"))
	summary := map[string]any{
		"method":        method,
		"environmentId": codexAnyString(codexFirstAny(params, "environmentId", "environment_id")),
		"cwd":           codexAnyString(codexFirstAny(params, "cwd", "workingDirectory")),
	}
	c.tools.Start(s.ID, c.state(s.ID).reqID, itemID, "codex_approval", command)
	c.tools.Result(s.ID, c.state(s.ID).reqID, itemID, codexJSON(summary))
	c.tools.End(s.ID, c.state(s.ID).reqID, itemID)
}

func (c *Codex) emitUnsupportedCodexTool(raw json.RawMessage) string {
	params := codexParams(raw)
	toolName := codexRequestToolName(params)
	s := c.sessionForCodexParams(params)
	if s == nil {
		return toolName
	}
	itemID := codexRequestItemID(params, "codex_tool_"+randHex(8))
	command := codexAnyString(codexFirstAny(params, "input", "arguments", "args"))
	c.tools.Start(s.ID, c.state(s.ID).reqID, itemID, toolName, command)
	c.tools.Result(s.ID, c.state(s.ID).reqID, itemID, "Unsupported Codex hosted tool: "+toolName)
	c.tools.End(s.ID, c.state(s.ID).reqID, itemID)
	return toolName
}

func (c *Codex) createUserInputRequest(rpcID any, raw json.RawMessage) bool {
	var p struct {
		TurnID           string                     `json:"turnId"`
		ThreadID         string                     `json:"threadId"`
		ItemID           string                     `json:"itemId"`
		CallID           string                     `json:"callId"`
		ToolID           string                     `json:"toolUseId"`
		Kind             string                     `json:"kind"`
		Header           string                     `json:"header"`
		Title            string                     `json:"title"`
		Agent            string                     `json:"requesting_agent"`
		IsBlocking       *bool                      `json:"isBlocking"`
		AutoResolutionMS *int64                     `json:"autoResolutionMs"`
		Questions        []map[string]any           `json:"questions"`
		Thread           map[string]json.RawMessage `json:"thread"`
		Item             map[string]json.RawMessage `json:"item"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return false
	}
	threadID := p.ThreadID
	if threadID == "" && p.Thread != nil {
		var id string
		_ = json.Unmarshal(p.Thread["id"], &id)
		threadID = id
	}
	s := c.sessionForThread(threadID)
	if s == nil {
		return false
	}
	toolID := firstNonEmpty(p.ItemID, p.CallID, p.ToolID)
	if toolID == "" && p.Item != nil {
		var id string
		_ = json.Unmarshal(p.Item["id"], &id)
		toolID = id
	}
	reqID := "ui_" + randHex(12)
	header := firstNonEmpty(p.Header, p.Title, "Question")
	kind := firstNonEmpty(p.Kind, "ask_user_question")
	blocking := true
	if p.IsBlocking != nil {
		blocking = *p.IsBlocking
	}
	if !blocking {
		kind = "request_user_input_async"
	}
	agent := firstNonEmpty(p.Agent, "codex")
	createdAt := time.Now().UnixMilli()
	expiresAt := int64(0)
	if p.AutoResolutionMS != nil && *p.AutoResolutionMS > 0 {
		expiresAt = createdAt + *p.AutoResolutionMS
	}
	payload := backend.UserInputPayload{
		RequestID: reqID, SessionID: s.ID, Source: "codex", Kind: kind,
		Header: header, ToolUseID: toolID, RequestingAgent: agent,
		Questions: normalizeCodexQuestions(p.Questions), CreatedAt: createdAt,
		ExpiresAt: expiresAt, IsBlocking: &blocking, Status: "pending",
	}
	responseKind := ""
	if p.IsBlocking != nil {
		responseKind = "request_user_input_v2"
	}
	c.interMu.Lock()
	c.interactions[reqID] = codexInteraction{payload: payload, rpcID: rpcID, responseKind: responseKind}
	c.interMu.Unlock()
	c.sink.Emit(backend.NewUserInputRequest(payload))
	c.observeOwnedQuestion(s, threadID, p.TurnID, toolID, payload)
	return true
}

func (c *Codex) observeOwnedQuestion(s *session.Session, threadID, turnID, callID string, payload backend.UserInputPayload) {
	st := c.state(s.ID)
	st.mu.Lock()
	request, currentTurn := st.reqID, st.currentTurnID
	owned := st.turnActive && !st.turnStartPending && st.threadID == threadID && turnID != "" && turnID == currentTurn
	st.mu.Unlock()
	if observer, ok := c.taskProvider.(backend.TaskQuestionObserver); ok && owned {
		observer.ObserveTaskQuestion(context.Background(), backend.TaskQuestionEvidence{Session: s, RequestID: request, ThreadID: threadID, TurnID: currentTurn, CallID: callID, Payload: payload})
	}
}

func rpcIDKey(value any) string {
	if raw, ok := value.(json.RawMessage); ok {
		var decoded any
		if json.Unmarshal(raw, &decoded) == nil {
			value = decoded
		}
	}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func (c *Codex) resolveServerRequest(requestID json.RawMessage) {
	if len(requestID) == 0 {
		return
	}
	wanted := rpcIDKey(requestID)
	c.interMu.Lock()
	var resolved *codexInteraction
	for bridgeID, interaction := range c.interactions {
		if interaction.rpcID == nil || rpcIDKey(interaction.rpcID) != wanted {
			continue
		}
		copy := interaction
		resolved = &copy
		delete(c.interactions, bridgeID)
		break
	}
	c.interMu.Unlock()
	if resolved != nil {
		c.sink.Emit(backend.NewInteractionResolved(resolved.payload.RequestID, resolved.payload.SessionID, "expired"))
	}
}

func (c *Codex) createMcpElicitationRequest(rpcID any, raw json.RawMessage) bool {
	var p struct {
		ThreadID        string         `json:"threadId"`
		TurnID          string         `json:"turnId"`
		ServerName      string         `json:"serverName"`
		Mode            string         `json:"mode"`
		Message         string         `json:"message"`
		URL             string         `json:"url"`
		ElicitationID   string         `json:"elicitationId"`
		RequestedSchema map[string]any `json:"requestedSchema"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return false
	}
	s := c.sessionForThread(p.ThreadID)
	if s == nil {
		return false
	}
	params := decodeObject(raw)
	questions := mcpElicitationQuestions(params)
	reqID := "mcp_" + randHex(12)
	payload := backend.UserInputPayload{RequestID: reqID, SessionID: s.ID, Source: "codex", Kind: "mcp_" + p.Mode, Header: firstNonEmpty(p.ServerName, "MCP request"), ToolUseID: p.ElicitationID, RequestingAgent: p.ServerName, Questions: questions, CreatedAt: time.Now().UnixMilli(), Status: "pending"}
	c.interMu.Lock()
	c.interactions[reqID] = codexInteraction{payload: payload, rpcID: rpcID, responseKind: "mcp", mcpParams: append(json.RawMessage(nil), raw...)}
	c.interMu.Unlock()
	c.sink.Emit(backend.NewUserInputRequest(payload))
	c.observeOwnedQuestion(s, p.ThreadID, p.TurnID, p.ElicitationID, payload)
	return true
}

func (c *Codex) createDynamicToolRequest(rpcID any, raw json.RawMessage) bool {
	var p struct {
		ThreadID  string  `json:"threadId"`
		TurnID    string  `json:"turnId"`
		CallID    string  `json:"callId"`
		Tool      string  `json:"tool"`
		Namespace *string `json:"namespace"`
		Arguments any     `json:"arguments"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return false
	}
	s := c.sessionForThread(p.ThreadID)
	if s == nil {
		return false
	}
	name := p.Tool
	if p.Namespace != nil && *p.Namespace != "" {
		name = *p.Namespace + "/" + name
	}
	reqID := "tool_" + randHex(12)
	payload := backend.UserInputPayload{RequestID: reqID, SessionID: s.ID, Source: "codex", Kind: "dynamic_tool", Header: "Tool input: " + name, ToolUseID: p.CallID, RequestingAgent: name, Questions: []backend.UserInputQuestion{{QuestionID: "result", Text: "Provide the tool result (arguments: " + codexJSON(p.Arguments) + ")", Header: name, Type: "text", FreeForm: true}}, CreatedAt: time.Now().UnixMilli(), Status: "pending"}
	c.interMu.Lock()
	c.interactions[reqID] = codexInteraction{payload: payload, rpcID: rpcID, responseKind: "dynamic_tool", reqID: c.state(s.ID).reqID}
	c.interMu.Unlock()
	c.tools.Start(s.ID, c.state(s.ID).reqID, p.CallID, name, codexJSON(p.Arguments))
	c.sink.Emit(backend.NewUserInputRequest(payload))
	c.observeOwnedQuestion(s, p.ThreadID, p.TurnID, p.CallID, payload)
	return true
}

func (c *Codex) emitExtractedAskUserQuestion(s *session.Session, st *codexState) {
	st.mu.Lock()
	if st.askExtracted {
		st.mu.Unlock()
		return
	}
	text := st.accumulatedText
	st.mu.Unlock()

	data := extractCodexAskUserQuestion(text)
	if data == nil {
		return
	}
	questions := normalizeCodexQuestions(codexQuestionMaps(codexFirstAny(data, "questions")))
	reqID := "ui_" + randHex(12)
	toolID := firstNonEmpty(
		codexFirstString(data, "tool_use_id", "toolUseId", "itemId", "id"),
		"ask_user_"+randHex(8),
	)
	header := firstNonEmpty(codexFirstString(data, "header", "title"), "Question")
	payload := backend.UserInputPayload{
		RequestID: reqID, SessionID: s.ID, Source: "codex", Kind: "ask_user_question",
		Header: header, ToolUseID: toolID, RequestingAgent: "AskUserQuestion",
		Questions: questions, CreatedAt: time.Now().UnixMilli(), Status: "pending",
	}
	c.interMu.Lock()
	c.interactions[reqID] = codexInteraction{payload: payload}
	c.interMu.Unlock()

	st.mu.Lock()
	st.askExtracted = true
	st.mu.Unlock()
	c.sink.Emit(backend.NewUserInputRequest(payload))
}

func extractCodexAskUserQuestion(text string) map[string]any {
	for _, match := range codexAskUserRE.FindAllStringSubmatch(text, -1) {
		raw := ""
		if len(match) > 1 {
			raw = match[1]
		}
		if raw == "" && len(match) > 2 {
			raw = match[2]
		}
		if raw == "" {
			continue
		}
		var data map[string]any
		if json.Unmarshal([]byte(raw), &data) != nil {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(codexFirstString(data, "type")))
		if kind == "ask_user_question" || kind == "askuserquestion" {
			return data
		}
	}
	return nil
}

func codexParams(raw json.RawMessage) map[string]any {
	var params map[string]any
	if json.Unmarshal(raw, &params) != nil || params == nil {
		return map[string]any{}
	}
	return params
}

func (c *Codex) sessionForCodexParams(params map[string]any) *session.Session {
	threadID := codexAnyString(codexFirstAny(params, "threadId", "thread_id"))
	if threadID == "" {
		if thread, ok := params["thread"].(map[string]any); ok {
			threadID = codexAnyString(thread["id"])
		}
	}
	return c.sessionForThread(threadID)
}

func (c *Codex) sessionForThread(threadID string) *session.Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.activeThreadOwner[threadID]; s != nil {
		return s
	}
	return c.threadToSession[threadID]
}

func codexRequestItemID(params map[string]any, fallback string) string {
	if item, ok := params["item"].(map[string]any); ok {
		if id := codexAnyString(item["id"]); id != "" {
			return id
		}
	}
	if id := codexAnyString(codexFirstAny(params, "itemId", "callId", "toolCallId", "toolUseId")); id != "" {
		return id
	}
	return fallback
}

func codexRequestToolName(params map[string]any) string {
	if tool, ok := params["tool"].(map[string]any); ok {
		if name := codexAnyString(codexFirstAny(tool, "name", "type")); name != "" {
			return name
		}
	}
	if item, ok := params["item"].(map[string]any); ok {
		if name := codexAnyString(codexFirstAny(item, "name", "type")); name != "" {
			return name
		}
	}
	if name := codexAnyString(codexFirstAny(params, "name", "toolName")); name != "" {
		return name
	}
	return "codex_tool"
}

func codexAnyString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return ""
	case map[string]any, []any:
		return codexJSON(x)
	default:
		return strings.TrimSpace(fmt.Sprint(x))
	}
}

func codexJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(raw)
}

// --- Executor interface ----------------------------------------------------

func (c *Codex) Send(ctx context.Context, s *session.Session, reqID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	asyncThread, asyncErr := c.validateAsyncReply(s, reqID, content)
	if asyncErr != nil {
		return asyncErr
	}
	if err := validateCodexInlineFiles(files); err != nil {
		return err
	}
	sandboxOverride, _ := backend.SandboxOverride(ctx)
	if err := c.validateAppSandbox(s, sandboxOverride); err != nil {
		return err
	}
	if err := c.ensureServer(); err != nil {
		c.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrProcessDied, "codex app-server failed: "+err.Error()))
		return err
	}
	st := c.state(s.ID)
	st.mu.Lock()
	threadAlreadyLoaded := st.threadID != ""
	st.mu.Unlock()
	if !threadAlreadyLoaded {
		if s.ResumeID() != "" {
			c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "resuming_thread", "Loading the existing Codex conversation"))
		} else {
			c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "starting_thread", "Creating a Codex conversation"))
		}
	}

	var threadErr error
	if asyncThread != "" {
		threadErr = c.ensureExactAsyncThread(s, st, asyncThread)
	} else if controllerBoundRequest(reqID) {
		_, threadErr = c.resumeExactControllerThread(s)
	} else {
		threadErr = c.ensureThread(s, st)
	}
	if err := threadErr; err != nil {
		if !errors.Is(err, backend.ErrThreadActiveWriter) {
			c.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrProcessDied, "failed to start codex thread: "+err.Error()))
		}
		return err
	}
	admission, err := c.readStartAdmission(s, st, reqID)
	if err != nil {
		if !errors.Is(err, backend.ErrNativeSubmissionBusy) {
			c.sink.Emit(backend.NativeSubmissionUncertain{SessionID: s.ID, RequestID: reqID, Reason: codexCorrelationReason(err)})
		}
		return err
	}
	// Durable chat admission pins a configuration per message. Apply it only
	// after Mux authorization and after the prior turn is terminal; no daemon
	// restart, fork or mutation of a currently running turn is involved.
	if s.ActiveQueuedID() != "" {
		if err := c.UpdateSessionSettings(ctx, s); err != nil && err.Error() != "pm_role_settings_locked" {
			c.sink.Emit(backend.NewError(s.ID, reqID, "queued_configuration_rejected", err.Error()))
			return err
		}
	}
	c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "submitting_turn", "Sending the turn to Codex"))
	isCompactCommand := strings.TrimSpace(content) == "/compact"
	if isCompactCommand {
		st.mu.Lock()
		st.reqID = reqID
		st.stopping = false
		st.mu.Unlock()
		c.sink.Emit(backend.NewSessionCommandStarted(s.ID, reqID, 0))
		go c.runCompactCommand(s, st, reqID)
		return nil
	}
	st.mu.Lock()
	handoff := st.pendingHandoff
	st.pendingHandoff = ""
	st.mu.Unlock()
	if handoff != "" && asyncThread != "" {
		// A native question reply must remain a standalone typed annotation;
		// retain recovery context for the next ordinary human message.
		st.mu.Lock()
		st.pendingHandoff = handoff
		st.mu.Unlock()
	} else if handoff != "" {
		content = handoff + "\n\n<current_user_request>\n" + content + "\n</current_user_request>"
	}

	st.mu.Lock()
	st.reqID = reqID
	st.startAdmission = admission
	st.stopping = false
	st.turnErr = ""
	st.turnErrorCode = ""
	st.turnFailure = nil
	st.failureNoticeKey = ""
	st.turnActive = true
	st.turnDone = make(chan struct{})
	st.accumulatedText = ""
	st.askExtracted = false
	st.currentTurnID = ""
	st.turnStartPending = true
	st.pendingTerminals = nil
	st.pendingNotifications = nil
	st.pendingNotificationBytes = 0
	st.confirmedRequestID, st.confirmedTurnID = "", ""
	st.lastEventAt = time.Now()
	st.stallWarned = false
	st.inactivityStopRequested = false
	st.inactivityStopReason = ""
	threadID := st.threadID
	done := st.turnDone
	st.mu.Unlock()

	if err := c.claimActiveThread(threadID, s); err != nil {
		st.finish(err.Error())
		return err
	}
	input := c.codexInput(s, reqID, reqID, backend.PresentationContent(ctx, content), images, files, st)
	go c.runTurn(s, st, threadID, input, done, sandboxOverride)
	return nil
}

// Steer injects another user message into the currently active Codex turn.
// The app-server validates expectedTurnId atomically, so a turn that finishes
// between the local snapshot and the RPC is safely rejected instead of being
// attached to the next turn.
func (c *Codex) Steer(ctx context.Context, s *session.Session, clientUserMessageID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) (backend.SteerResult, error) {
	override, _ := backend.SandboxOverride(ctx)
	if err := c.validateAppSandbox(s, override); err != nil {
		return backend.SteerResult{}, err
	}
	if _, err := c.validateAsyncReply(s, clientUserMessageID, content); err != nil {
		return backend.SteerResult{}, fmt.Errorf("%w: %v", backend.ErrSteerRejected, err)
	}
	if err := validateCodexInlineFiles(files); err != nil {
		return backend.SteerResult{}, fmt.Errorf("%w: %v", backend.ErrSteerRejected, err)
	}
	if err := c.ensureServer(); err != nil {
		return backend.SteerResult{}, fmt.Errorf("%w: %v", backend.ErrSteerRejected, err)
	}
	return c.steerActiveTurn(s, clientUserMessageID, backend.PresentationContent(ctx, content), images, files)
}

func (c *Codex) steerActiveTurn(s *session.Session, clientUserMessageID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) (backend.SteerResult, error) {
	st := c.state(s.ID)
	source := s.Snapshot()
	config := session.ConfigurationFrom(source)
	st.mu.Lock()
	active := st.turnActive && !st.turnStartPending && !st.stopping
	threadID := st.threadID
	turnID := st.currentTurnID
	activeRequestID := st.reqID
	observed := !st.turnActive && !st.compactActive && !st.stopping && !st.turnStartPending &&
		st.observedTurnID != "" && st.observedRequestID != "" && st.threadID == source.ResumeID
	if observed {
		active = true
		turnID, activeRequestID = st.observedTurnID, st.observedRequestID
	}
	epoch := c.rpc.transportEpoch.Load()
	st.mu.Unlock()
	if !active || threadID == "" || turnID == "" || activeRequestID == "" || clientUserMessageID == "" {
		return backend.SteerResult{}, backend.ErrNoActiveTurn
	}

	input := c.codexInput(s, clientUserMessageID, activeRequestID, content, images, files, st)
	params := map[string]any{
		"threadId":            threadID,
		"expectedTurnId":      turnID,
		"input":               input,
		"clientUserMessageId": clientUserMessageID,
	}
	raw, err := c.rpcCallAtEpoch("turn/steer", params, 15*time.Second, &epoch)
	if err != nil {
		var rejected *rpcResponseError
		if errors.As(err, &rejected) {
			return backend.SteerResult{}, fmt.Errorf("%w: %v", backend.ErrSteerRejected, err)
		}
		return backend.SteerResult{}, err
	}
	var response struct {
		TurnID string `json:"turnId"`
	}
	decodeErr := json.Unmarshal(raw, &response)
	if decodeErr != nil || response.TurnID != turnID {
		// A malformed/mismatched ACK is uncertain, not a definitive rejection
		// that can be automatically replayed into another turn.
		return backend.SteerResult{}, fmt.Errorf("native async reply acceptance could not be correlated to the expected turn")
	}
	st.mu.Lock()
	current := st.turnActive && !st.turnStartPending && !st.stopping && st.threadID == threadID && st.currentTurnID == turnID && st.reqID == activeRequestID
	if observed {
		current = !st.turnActive && !st.compactActive && !st.stopping && !st.turnStartPending &&
			st.threadID == threadID && st.observedTurnID == turnID && st.observedRequestID == activeRequestID
	}
	current = current && c.rpc.transportEpoch.Load() == epoch
	st.mu.Unlock()
	if observed {
		current = current && s.ResumeID() == threadID && session.ConfigurationFrom(s.Snapshot()) == config
	}
	if !current {
		return backend.SteerResult{}, &codexAcceptanceError{Reason: "native_current_request_changed"}
	}
	st.touch(time.Now())
	if strings.HasPrefix(clientUserMessageID, "ui_async_") {
		c.resolveAsyncReplies(s, threadID, content)
	}
	return backend.SteerResult{TurnID: response.TurnID, RequestID: activeRequestID, StrictACK: true}, nil
}

func (c *Codex) runTurn(s *session.Session, st *codexState, threadID string, input []map[string]any, done chan struct{}, sandboxOverride string) {
	st.mu.Lock()
	requestID := st.reqID
	st.mu.Unlock()
	defer c.cleanupTempImages(st, requestID)
	c.sink.Emit(backend.NewTurnProgress(s.ID, requestID, "waiting_model", "Waiting for Codex to accept the turn"))
	if err := c.startTurnWithStaleRetry(s, st, threadID, input, sandboxOverride); err != nil {
		st.mu.Lock()
		if st.turnActive && st.reqID == requestID {
			st.turnFailure = codexSubmissionFailure(err)
			// Contradictory live/terminal notifications defeat rejection proof.
			if st.currentTurnID != "" || len(st.pendingTerminals) > 0 || st.unconfirmedActivity {
				st.turnFailure.Acceptance = recovery.AcceptanceUnknown
				st.turnFailure.IngressRejected = false
			}
			st.turnErrorCode = st.turnFailure.Failure.ErrorCode()
			st.finishTurnLocked("turn/start failed: " + err.Error())
		}
		st.mu.Unlock()
	}

	stallTicker := time.NewTicker(c.stallCheckEvery)
	defer stallTicker.Stop()
	// Long turns remain live while progress continues. Only inactivity (not
	// total wall-clock runtime) is subject to the watchdog.
	c.waitForCodexTurn(s, st, done, stallTicker.C)

	st.mu.Lock()
	completedThreadID := st.threadID
	st.mu.Unlock()
	c.finalizePendingRecovery(s, st, completedThreadID)

	st.mu.Lock()
	stopping, turnErr := st.stopping, st.turnErr
	st.mu.Unlock()

	// Release before emitting the terminal event. Hub.Emit(done/error/stopped)
	// immediately unlocks the per-session queue, so a deferred release could
	// race the next turn from the same session and delete its newly acquired
	// route.
	switch {
	case turnErr == "stopped":
		c.releaseActiveThreads(s)
		c.sink.Emit(backend.NewStopped(s.ID, requestID))
	case turnErr != "":
		st.mu.Lock()
		code := st.turnErrorCode
		detail := st.turnFailure
		st.mu.Unlock()
		if code == "" {
			code = codexErrorCode(nil, turnErr)
		}
		if detail == nil {
			detail = &codexFailureDetail{Failure: recovery.ClassifyCodex(nil, turnErr), Acceptance: recovery.AcceptanceUnknown}
			st.mu.Lock()
			if st.confirmedRequestID == requestID && st.confirmedTurnID != "" {
				detail.Acceptance = recovery.Accepted
				detail.TurnID = st.confirmedTurnID
				detail.Terminal = true
			}
			st.mu.Unlock()
		}
		c.recordCodexFailure(s.ID, requestID, completedThreadID, detail.TurnID, detail.Failure, recovery.Context{
			Owned: true, OrdinaryChat: !codexPinnedRequest(requestID), Acceptance: detail.Acceptance,
			IngressRejected: detail.IngressRejected, Terminal: detail.Terminal,
			NativeWillRetry: detail.NativeWillRetry,
		})
		turnErr = detail.Failure.PublicMessage(turnErr)
		c.releaseActiveThreads(s)
		if detail.Acceptance == recovery.AcceptanceUnknown {
			reason := detail.Failure.Source
			if !strings.HasPrefix(reason, "native_") {
				reason = "native_acceptance_unknown"
			}
			c.sink.Emit(backend.NativeSubmissionUncertain{SessionID: s.ID, RequestID: requestID, Reason: reason})
		} else {
			c.sink.Emit(backend.NewError(s.ID, requestID, code, turnErr))
		}
	default:
		c.emitExtractedAskUserQuestion(s, st)
		// Goal state is durable thread metadata, separate from turn/completed.
		// Re-read it before emitting done so clients cannot miss a terminal goal
		// transition when the app-server notification was dropped or arrived while
		// they were reconnecting.
		c.reconcileGoalAfterTurn(s, st)
		st.mu.Lock()
		stopping = st.stopping
		st.mu.Unlock()
		if !stopping && c.shouldAutoCompact(st) {
			// Keep the durable session worker occupied until maintenance ends.
			// Emitting Done first lets the next queued Send race compact/start.
			c.runAutoCompact(s, st)
		}
		c.releaseActiveThreads(s)
		// The owned native turn completed naturally. A concurrent stop intent
		// (or stopping optional maintenance afterwards) cannot rewrite that
		// confirmed terminal as 'interrupted'.
		c.sink.Emit(backend.NewDone(s.ID, requestID))
	}
}

type codexLivenessAction uint8

const (
	codexLivenessNone codexLivenessAction = iota
	codexLivenessWarn
	codexLivenessAbort
)

func codexLivenessActionAt(now, lastEventAt time.Time, warned, waitingForInput bool, warnAfter, abortAfter time.Duration) codexLivenessAction {
	if waitingForInput || lastEventAt.IsZero() {
		return codexLivenessNone
	}
	idle := now.Sub(lastEventAt)
	if abortAfter > 0 && idle >= abortAfter {
		return codexLivenessAbort
	}
	if !warned && warnAfter > 0 && idle >= warnAfter {
		return codexLivenessWarn
	}
	return codexLivenessNone
}

func (c *Codex) hasPendingInteraction(sessionID string) bool {
	c.interMu.Lock()
	defer c.interMu.Unlock()
	for _, interaction := range c.interactions {
		blocking := interaction.payload.IsBlocking == nil || *interaction.payload.IsBlocking
		if interaction.payload.SessionID == sessionID && blocking {
			return true
		}
	}
	return false
}

func (c *Codex) interruptCodexTurn(threadID, turnID string) error {
	if threadID == "" || turnID == "" {
		return fmt.Errorf("cannot interrupt without an active thread and turn id")
	}
	if _, err := c.rpcCall("turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, 5*time.Second); err != nil {
		log.Printf("[codex] turn interrupt failed thread=%s turn=%s: %v", threadID, turnID, err)
		return err
	}
	return nil
}

func (c *Codex) runCompactCommand(s *session.Session, st *codexState, reqID string) {
	if err := c.runCompact(st, 120*time.Second, func() {
		c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "composing", "整理上下文耗時較久，仍在等待執行端確認"))
	}); err != nil {
		c.sink.Emit(backend.NewSessionCommandFailed(s.ID, reqID, "compact failed: "+err.Error(), 0))
		c.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrTurn, "compact failed: "+err.Error()))
		return
	}
	c.sink.Emit(backend.NewSessionCommandDone(s.ID, reqID, 0))
	c.sink.Emit(backend.NewDone(s.ID, reqID))
}

func (c *Codex) runAutoCompact(s *session.Session, st *codexState) {
	c.sink.Emit(backend.NewTurnProgress(s.ID, st.reqID, "composing", "正在整理上下文；後續訊息會依序處理"))
	if err := c.runCompact(st, 120*time.Second, func() {
		c.sink.Emit(backend.NewTurnProgress(s.ID, st.reqID, "composing", "整理上下文耗時較久，仍在等待執行端確認"))
	}); err != nil {
		c.sink.Emit(backend.NewSessionWarning(s.ID, "上下文整理未完成："+err.Error()))
		log.Printf("[codex] auto compact failed session=%s: %v", s.ID, err)
		return
	}
}

func (c *Codex) runCompact(st *codexState, timeout time.Duration, onSlow ...func()) error {
	st.mu.Lock()
	if st.compactActive {
		done := st.compactDone
		st.mu.Unlock()
		if done != nil {
			<-done
		}
		st.mu.Lock()
		errStr := st.compactErr
		st.mu.Unlock()
		if errStr != "" {
			return errors.New(errStr)
		}
		return nil
	}
	threadID := st.threadID
	if threadID == "" {
		st.mu.Unlock()
		return fmt.Errorf("no codex thread")
	}
	st.compactActive = true
	st.compactErr = ""
	st.compactDone = make(chan struct{})
	st.compactTurnID = ""
	done := st.compactDone
	st.mu.Unlock()
	maintenanceID, persistErr := c.beginMaintenance(st)
	if persistErr != nil {
		st.finishCompact(persistErr.Error())
		return persistErr
	}

	if _, err := c.rpcCall("thread/compact/start", map[string]any{"threadId": threadID}, 30*time.Second); err != nil {
		st.finishCompact(err.Error())
	}

	select {
	case <-done:
	case <-time.After(timeout):
		// The deadline measures our wait, not the daemon operation. Only an
		// actual terminal event or transport loss may settle this operation.
		log.Printf("[codex] compact still running thread=%s after=%s", threadID, timeout)
		c.updateMaintenance(maintenanceID, "running", "整理上下文耗時較久，仍在等待執行端確認", "")
		for _, notify := range onSlow {
			notify()
		}
		<-done
	}

	st.mu.Lock()
	errStr := st.compactErr
	st.mu.Unlock()
	if errStr != "" {
		c.updateMaintenance(maintenanceID, "unknown", errStr, "")
		return fmt.Errorf("%s", errStr)
	}
	c.updateMaintenance(maintenanceID, "completed", "上下文整理完成", "")
	return nil
}

// Goal operations are independent of generation. Their failures must remain
// warnings: a request-less Error would settle the active turn in both the Hub
// and TerminalSink and falsely fail the client's streaming assistant message.
func (c *Codex) SetGoal(ctx context.Context, s *session.Session, objective, status string, tokenBudget *int) error {
	if err := c.ensureServer(); err != nil {
		c.sink.Emit(backend.NewSessionWarning(s.ID, "codex app-server failed: "+err.Error()))
		return err
	}
	st := c.state(s.ID)
	if err := c.ensureThread(s, st); err != nil {
		if !errors.Is(err, backend.ErrThreadActiveWriter) {
			c.sink.Emit(backend.NewSessionWarning(s.ID, "failed to start codex thread: "+err.Error()))
		}
		return err
	}
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	params := map[string]any{"threadId": threadID}
	if objective != "" {
		params["objective"] = objective
	}
	if status != "" {
		params["status"] = status
	}
	if tokenBudget != nil {
		params["tokenBudget"] = *tokenBudget
	}
	raw, err := c.rpcCall("thread/goal/set", params, 30*time.Second)
	if err != nil {
		c.sink.Emit(backend.NewSessionWarning(s.ID, "goal set failed: "+err.Error()))
		return err
	}
	goal, ok := decodeCodexGoal(raw)
	if !ok {
		err := fmt.Errorf("thread/goal/set returned no goal")
		c.sink.Emit(backend.NewSessionWarning(s.ID, err.Error()))
		return err
	}
	c.sink.Emit(backend.NewGoalUpdate(s.ID, goal))
	return nil
}

func (c *Codex) GetGoal(ctx context.Context, s *session.Session) error {
	if err := c.ensureServer(); err != nil {
		c.sink.Emit(backend.NewSessionWarning(s.ID, "codex app-server failed: "+err.Error()))
		return err
	}
	st := c.state(s.ID)
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	if threadID == "" {
		// Goal metadata is daemon-global and can be read by thread id while a
		// desktop TUI owns the writer. Do not resume merely to perform this
		// read-only query; Codex rejects that second attachment as an active writer.
		threadID = s.ResumeID()
	}
	if threadID == "" {
		if err := c.ensureThread(s, st); err != nil {
			if !errors.Is(err, backend.ErrThreadActiveWriter) {
				c.sink.Emit(backend.NewSessionWarning(s.ID, "failed to start codex thread: "+err.Error()))
			}
			return err
		}
		st.mu.Lock()
		threadID = st.threadID
		st.mu.Unlock()
	}
	raw, err := c.rpcCall("thread/goal/get", map[string]any{"threadId": threadID}, 30*time.Second)
	if err != nil && isStaleThreadError(err) {
		// An unloaded historical thread still needs one normal resume. A thread
		// already loaded by a desktop client takes the fast read-only path above.
		if resumeErr := c.ensureThread(s, st); resumeErr != nil {
			if !errors.Is(resumeErr, backend.ErrThreadActiveWriter) {
				c.sink.Emit(backend.NewSessionWarning(s.ID, "failed to start codex thread: "+resumeErr.Error()))
			}
			return resumeErr
		}
		st.mu.Lock()
		threadID = st.threadID
		st.mu.Unlock()
		raw, err = c.rpcCall("thread/goal/get", map[string]any{"threadId": threadID}, 30*time.Second)
	}
	if err != nil {
		c.sink.Emit(backend.NewSessionWarning(s.ID, "goal get failed: "+err.Error()))
		return err
	}
	goal, ok := decodeCodexGoal(raw)
	if !ok {
		c.sink.Emit(backend.NewGoalCleared(s.ID))
		return nil
	}
	c.sink.Emit(backend.NewGoalUpdate(s.ID, goal))
	return nil
}

func (c *Codex) reconcileGoalAfterTurn(s *session.Session, st *codexState) {
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	if threadID == "" {
		return
	}

	raw, err := c.rpcCall("thread/goal/get", map[string]any{"threadId": threadID}, 3*time.Second)
	if err != nil {
		// A successful turn must remain successful even if this best-effort state
		// reconciliation fails. The frontend also refreshes Goal unconditionally
		// on done, and reconnect receives the durable bridge snapshot.
		log.Printf("[%s] post-turn goal reconcile failed: %v", s.ID, err)
		return
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		log.Printf("[%s] post-turn goal response invalid: %v", s.ID, err)
		return
	}
	rawGoal, exists := envelope["goal"]
	if !exists {
		log.Printf("[%s] post-turn goal response missing goal", s.ID)
		return
	}
	if string(rawGoal) == "null" {
		c.sink.Emit(backend.NewGoalCleared(s.ID))
		return
	}
	var goal backend.Goal
	if err := json.Unmarshal(rawGoal, &goal); err != nil || goal.ThreadID == "" {
		log.Printf("[%s] post-turn goal payload invalid", s.ID)
		return
	}
	c.sink.Emit(backend.NewGoalUpdate(s.ID, goal))
}

func (c *Codex) ClearGoal(ctx context.Context, s *session.Session) error {
	if err := c.ensureServer(); err != nil {
		c.sink.Emit(backend.NewSessionWarning(s.ID, "codex app-server failed: "+err.Error()))
		return err
	}
	st := c.state(s.ID)
	if err := c.ensureThread(s, st); err != nil {
		if !errors.Is(err, backend.ErrThreadActiveWriter) {
			c.sink.Emit(backend.NewSessionWarning(s.ID, "failed to start codex thread: "+err.Error()))
		}
		return err
	}
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	if _, err := c.rpcCall("thread/goal/clear", map[string]any{"threadId": threadID}, 30*time.Second); err != nil {
		c.sink.Emit(backend.NewSessionWarning(s.ID, "goal clear failed: "+err.Error()))
		return err
	}
	c.sink.Emit(backend.NewGoalCleared(s.ID))
	return nil
}

func decodeCodexGoal(raw json.RawMessage) (backend.Goal, bool) {
	var out struct {
		Goal *backend.Goal `json:"goal"`
	}
	if json.Unmarshal(raw, &out) != nil || out.Goal == nil || out.Goal.ThreadID == "" {
		return backend.Goal{}, false
	}
	return *out.Goal, true
}

func (c *Codex) shouldAutoCompact(st *codexState) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.compactActive || st.threadID == "" || st.contextMax <= 0 || st.contextUsed <= 0 {
		return false
	}
	return float64(st.contextUsed)/float64(st.contextMax) >= codexCompactThreshold
}

func (c *Codex) startTurnWithStaleRetry(s *session.Session, st *codexState, threadID string, input []map[string]any, sandboxOverride string) error {
	snap := s.Snapshot()
	st.mu.Lock()
	requestID := st.reqID
	st.mu.Unlock()
	err := c.startTurn(threadID, input, snap, sandboxOverride, requestID)
	if err == nil {
		if strings.HasPrefix(requestID, "ui_async_") && len(input) > 0 {
			text, _ := input[0]["text"].(string)
			c.resolveAsyncReplies(s, threadID, text)
		}
		c.finalizePendingRecovery(s, st, threadID)
		return nil
	}
	pinned, pinErr := c.delegationContextPinned(s)
	if pinErr != nil {
		return pinErr
	}
	if pinned && isStaleThreadError(err) {
		return fmt.Errorf("delegation_original_thread_unavailable_successor_required: %w", err)
	}
	if !isStaleThreadError(err) || codexPinnedRequest(requestID) {
		return err
	}

	log.Printf("[codex] stale thread on turn/start session=%s thread=%s, respawning", s.ID, threadID)
	c.forgetThread(s, st, threadID)
	if err := c.ensureThread(s, st); err != nil {
		return err
	}
	st.mu.Lock()
	newThreadID := st.threadID
	st.mu.Unlock()
	if err := c.moveActiveThreadClaim(threadID, newThreadID, s); err != nil {
		return err
	}
	admission, err := c.readStartAdmission(s, st, requestID)
	if err != nil {
		return err
	}
	st.mu.Lock()
	st.startAdmission = admission
	st.mu.Unlock()
	if err := c.startTurn(newThreadID, input, snap, sandboxOverride, requestID); err != nil {
		return err
	}
	c.finalizePendingRecovery(s, st, newThreadID)
	return nil
}

// finalizePendingRecovery advances the durable session mapping only after
// turn/start succeeds. thread/start alone does not necessarily materialize a
// rollout, so committing in ensureThread can leave a registry pointing at an
// in-memory-only thread after the app-server restarts.
func (c *Codex) finalizePendingRecovery(s *session.Session, st *codexState, threadID string) {
	st.mu.Lock()
	recovery := st.pendingRecovery
	st.mu.Unlock()
	if recovery == nil {
		return
	}
	if !c.waitForCodexRollout(threadID, 2*time.Second) {
		// Keep the transaction pending. runTurn retries finalization after the
		// terminal notification, by which point Codex must have flushed the file.
		log.Printf("[codex] recovery rollout not materialized yet session=%s thread=%s", s.ID, threadID)
		return
	}
	if err := c.commitColdRecovery(recovery, threadID); err != nil {
		// The turn was accepted already. Do not retry it and risk duplicate work;
		// retain the usable Codex thread and surface the secondary metadata error.
		log.Printf("[codex] recovery manifest commit failed session=%s thread=%s: %v", s.ID, threadID, err)
		c.sink.Emit(backend.NewSessionWarning(s.ID, "Codex recovery metadata could not be finalized; the active thread remains usable."))
	}
	st.mu.Lock()
	if st.pendingRecovery == recovery {
		st.pendingRecovery = nil
	}
	st.mu.Unlock()
	s.SetResumeID(threadID)
	c.sink.Emit(backend.NewSessionUUID(s.ID, threadID))
	log.Printf("[codex] recovery committed session=%s thread=%s", s.ID, threadID)
}

func (c *Codex) waitForCodexRollout(threadID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if fileSize(c.findCodexSessionFile(threadID)) > 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (c *Codex) claimActiveThread(threadID string, s *session.Session) error {
	if threadID == "" {
		return errNoThreadID
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if owner := c.activeThreadOwner[threadID]; owner != nil && owner.ID != s.ID {
		return fmt.Errorf("Codex thread %s already has an active turn in session %s", threadID, owner.ID)
	}
	c.activeThreadOwner[threadID] = s
	// Keep the idle/fallback route aligned too. Notifications for an active turn
	// always prefer activeThreadOwner, so another duplicate session cannot steal
	// tool, text, interaction, or terminal events by merely resuming the thread.
	c.threadToSession[threadID] = s
	return nil
}

func (c *Codex) moveActiveThreadClaim(oldThreadID, newThreadID string, s *session.Session) error {
	if newThreadID == "" {
		return errNoThreadID
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if owner := c.activeThreadOwner[newThreadID]; owner != nil && owner.ID != s.ID {
		return fmt.Errorf("Codex thread %s already has an active turn in session %s", newThreadID, owner.ID)
	}
	if owner := c.activeThreadOwner[oldThreadID]; owner == s {
		delete(c.activeThreadOwner, oldThreadID)
	}
	c.activeThreadOwner[newThreadID] = s
	c.threadToSession[newThreadID] = s
	return nil
}

func (c *Codex) releaseActiveThreads(s *session.Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for threadID, owner := range c.activeThreadOwner {
		if owner == s {
			delete(c.activeThreadOwner, threadID)
		}
	}
}

func codexTurnParams(threadID string, input []map[string]any, effort string) map[string]any {
	params := map[string]any{
		"threadId":          threadID,
		"input":             input,
		"approvalPolicy":    "never",
		"approvalsReviewer": "user",
	}
	switch effort {
	case "low", "medium", "high", "xhigh", "max", "ultra":
		params["effort"] = effort
	}
	return params
}

func (c *Codex) codexTurnParamsForSession(threadID string, input []map[string]any, snap session.Snapshot, sandboxOverride string) map[string]any {
	params := codexTurnParams(threadID, input, snap.Effort)
	if snap.Model != "" {
		// Keep the admitted model explicit when resuming an older native thread.
		params["model"] = snap.Model
	}
	params["sandboxPolicy"] = c.sessionSandboxPolicy(snap, sandboxOverride)
	if snap.ServiceTier != "" {
		params["serviceTier"] = snap.ServiceTier
	}
	if snap.Personality != "" {
		params["personality"] = snap.Personality
	}
	if snap.CollaborationMode != "" {
		params["collaborationMode"] = c.collaborationModeValue(snap)
	}
	return params
}

func (c *Codex) startTurn(threadID string, input []map[string]any, snap session.Snapshot, sandboxOverride, requestID string) error {
	params := c.codexTurnParamsForSession(threadID, input, snap, sandboxOverride)
	if err := c.applyPMTurnPolicy(snap.ID, params); err != nil {
		return err
	}
	st := c.state(snap.ID)
	epoch := c.rpc.transportEpoch.Load()
	st.mu.Lock()
	if st.startAdmission != nil && (!st.startAdmission.sourceKnown() || st.startAdmission.ThreadID != threadID || st.startAdmission.RequestID != requestID || st.startAdmission.Epoch != epoch || st.startAdmission.Boundary != st.nativeBoundary || snap.ResumeID != threadID || snap.ConfigRevision != st.startAdmission.ConfigRevision) {
		st.mu.Unlock()
		return &codexAcceptanceError{Reason: "native_source_changed"}
	}
	if !st.turnActive || st.stopping || st.reqID != requestID || st.threadID != threadID {
		st.mu.Unlock()
		return &codexAcceptanceError{Reason: "native_current_request_changed"}
	}
	st.submissionEpoch = epoch
	st.unconfirmedActivity = false
	if st.turnActive && st.reqID == requestID && st.threadID == threadID {
		st.turnStartPending = true
	}
	st.mu.Unlock()
	// Keep the existing health/tool policy and atomically guard the transport.
	raw, err := c.rpcCallAtEpoch("turn/start", params, 30*time.Second, &epoch)
	if err == nil {
		var response struct {
			Turn codexTurnResponse `json:"turn"`
		}
		decodeErr := json.Unmarshal(raw, &response)
		if c.rpc.transportEpoch.Load() != epoch {
			return &codexAcceptanceError{Reason: "native_source_changed"}
		}
		if decodeErr != nil || response.Turn.ID == "" || (response.Turn.Status != "inProgress" && response.Turn.Status != "completed") {
			reason := "native_acceptance_unknown"
			if response.Turn.Status == "failed" || response.Turn.Status == "interrupted" {
				reason = "native_start_failed_before_acceptance"
			}
			return &codexAcceptanceError{Reason: reason, Failure: recovery.ClassifyCodex(response.Turn.Error.Info, "")}
		}
		return c.confirmOwnedTurnSubmission(st, threadID, requestID, response.Turn, func() {
			c.sink.Emit(backend.NativeTaskAccepted{SessionID: snap.ID, RequestID: requestID, ThreadID: threadID, TurnID: response.Turn.ID})
		})
	}
	return err
}

func (c *Codex) collaborationModeValue(snap session.Snapshot) map[string]any {
	modeName := strings.ToLower(strings.TrimSpace(snap.CollaborationMode))
	if modeName == "default" {
		// Codex treats a null collaborationMode as "keep the current mode".
		// Send an explicit Default preset while retaining this session's model and
		// effort, instead of silently leaving a previously selected Plan active.
		model := strings.TrimSpace(snap.Model)
		if model == "" {
			model = c.defaultModel()
		}
		var effort any
		if snap.Effort != "" && snap.Effort != "auto" {
			effort = snap.Effort
		}
		return map[string]any{"mode": "default", "settings": map[string]any{"model": model, "reasoning_effort": effort, "developer_instructions": nil}}
	}
	c.catalogMu.RLock()
	mode := c.collaborationModes[modeName]
	c.catalogMu.RUnlock()
	if mode != nil {
		return mode
	}
	return map[string]any{"mode": modeName, "settings": map[string]any{"model": snap.Model, "reasoning_effort": nil, "developer_instructions": nil}}
}

func (c *Codex) forgetThread(s *session.Session, st *codexState, threadID string) {
	st.mu.Lock()
	if st.threadID == threadID {
		st.threadID = ""
	}
	st.currentTurnID = ""
	st.mu.Unlock()
	c.mu.Lock()
	if c.threadToSession[threadID] == s {
		delete(c.threadToSession, threadID)
	}
	if c.activeThreadOwner[threadID] == s {
		delete(c.activeThreadOwner, threadID)
	}
	c.mu.Unlock()
}

func isStaleThreadError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown session") || strings.Contains(msg, "thread not found") ||
		strings.Contains(msg, "no rollout found")
}

func isActiveWriterError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "already has an active writer")
}

func codexUsageValues(u codexTokenUsage) (int, int) {
	used := u.Last.TotalTokens
	if used == 0 {
		used = u.Last.TotalTokens2
	}
	maxCtx := u.ModelContextWindow
	if maxCtx == 0 {
		maxCtx = u.ModelContextWindow2
	}
	return used, maxCtx
}

// ensureThread starts or resumes the codex thread for this session.
func (c *Codex) ensureThread(s *session.Session, st *codexState) error {
	if c.taskProvider != nil {
		scope, err := c.taskProvider.TaskWorkerScope(s)
		if err != nil {
			return err
		}
		if scope != nil {
			tools, err := c.taskProvider.TaskTools(s)
			if err != nil || len(tools) == 0 {
				return errors.New("unsupported: bounded task tools not loaded")
			}
		}
	}

	st.ensureMu.Lock()
	defer st.ensureMu.Unlock()
	st.mu.Lock()
	have := st.threadID
	st.mu.Unlock()
	snap := s.Snapshot()
	preserveContext, pinErr := c.delegationContextPinned(s)
	if pinErr != nil {
		return pinErr
	}
	if c.taskProvider != nil {
		scope, e := c.taskProvider.TaskWorkerScope(s)
		if e != nil {
			return e
		}
		if scope != nil {
			preserveContext = true
		}
	}
	if preserveContext {
		if fi, e := os.Stat(runtime.ExpandPath(snap.Cwd)); e != nil || !fi.IsDir() {
			return errors.New("delegation_original_workspace_unavailable")
		}
	}
	if have != "" && c.taskProvider != nil {
		scope, err := c.taskProvider.TaskWorkerScope(s)
		if err != nil {
			return err
		}
		if scope != nil {
			if err := c.ValidateExistingTaskScope(s, *scope, s.Snapshot().Cwd); err != nil {
				return err
			}
			params := map[string]any{"threadId": have, "excludeTurns": true}
			if err = c.applyTaskAPIThreadTools(s, params); err != nil {
				return err
			}
			reply, err := c.rpcCall("thread/resume", params, 15*time.Second)
			if err != nil {
				return errors.New("unsupported: bounded task policy rebind failed")
			}
			var resumed struct{ Thread struct{ ID string } }
			if json.Unmarshal(reply, &resumed) != nil || resumed.Thread.ID != have {
				return errors.New("unsupported: bounded original thread changed")
			}

		}
	}
	if have != "" {
		// Rebind managed tools on the SAME thread, including v1 -> v2 upgrades.
		// No global daemon config or native history is reset.
		if c.pmProvider != nil {
			policy, err := c.pmProvider.PMConfiguration(s.ID)
			if err != nil {
				return err
			}
			if policy != nil {
				params := map[string]any{"threadId": have, "excludeTurns": true, "approvalPolicy": "never"}
				if err := c.applyPMThreadPolicy(s, params); err != nil {
					return err
				}
				if _, err := c.rpcCall("thread/resume", params, 15*time.Second); err != nil {
					return fmt.Errorf("collaboration policy rebind: %w", err)
				}
			}
		}
		return nil
	}

	cwd := runtime.ExpandPath(snap.Cwd)
	if fi, err := os.Stat(cwd); err != nil || !fi.IsDir() {
		if preserveContext {
			return errors.New("delegation_original_workspace_unavailable")
		}
		cwd, _ = os.UserHomeDir()
	}
	sandbox := codexSandboxForSession(snap)

	var threadID string
	var threadMetadataRaw json.RawMessage
	var recovery *codexRecovery
	if snap.ResumeID != "" {
		rolloutPath := c.findCodexSessionFile(snap.ResumeID)
		rolloutBytes := fileSize(rolloutPath)
		// The shared daemon can rejoin a running or persisted thread without
		// returning its history (excludeTurns below), so rollout byte size is no
		// longer a reason to fork that canonical thread. Keep checkpoint rollover
		// only for the legacy private stdio transport whose resume response still
		// contains the complete history.
		if c.appServerMode != "daemon" && c.rolloverEnabled && rolloutBytes > c.coldResumeMaxBytes {
			if preserveContext {
				return errors.New("delegation_context_limit_successor_required")
			}
			var err error
			recovery, err = c.prepareColdRecovery(snap, rolloutPath, rolloutBytes, "cold_resume_hard_limit")
			if err != nil {
				return fmt.Errorf("cold recovery required for %s (%d bytes): %w", snap.ResumeID, rolloutBytes, err)
			}
			log.Printf("[codex] cold resume guarded session=%s thread=%s bytes=%d", s.ID, snap.ResumeID, rolloutBytes)
		} else {
			resumeParams := map[string]any{
				"threadId": snap.ResumeID, "cwd": cwd,
				"approvalPolicy": "never", "approvalsReviewer": "user", "sandbox": sandbox,
			}
			if c.appServerMode == "daemon" {
				// A shared daemon already owns the canonical rollout and only needs
				// to subscribe this Bridge connection. Returning every historical
				// turn can produce a hundreds-of-MiB JSON-RPC frame and time out even
				// though the thread itself is healthy. excludeTurns preserves resume
				// and notification subscription semantics while returning metadata.
				resumeParams["excludeTurns"] = true
			}
			c.applyDelegationThreadTools(s, resumeParams)
			c.applySessionControlThreadTools(s, resumeParams)
			if err := c.applyTaskAPIThreadTools(s, resumeParams); err != nil {
				return err
			}
			if err := c.applyPMThreadPolicy(s, resumeParams); err != nil {
				return err
			}
			raw, err := c.rpcCall("thread/resume", resumeParams, 15*time.Second)
			if err == nil {
				threadID = extractThreadID(raw, snap.ResumeID)
				if preserveContext && threadID != snap.ResumeID {
					return errors.New("delegation_resume_thread_changed")
				}
				threadMetadataRaw = append(json.RawMessage(nil), raw...)
			} else if isActiveWriterError(err) {
				return fmt.Errorf("thread/resume refused: %w: %v", backend.ErrThreadActiveWriter, err)
			} else if isStaleThreadError(err) {
				if preserveContext {
					return fmt.Errorf("delegation_original_thread_unavailable_successor_required: %w", err)
				}
				recoverySnap, recoveryPath, recoveryBytes, ok := c.latestAvailableRecoverySource(snap)
				if !ok {
					return fmt.Errorf("thread not found and no local rollout generation is available: %w", err)
				}
				recovery, err = c.prepareColdRecovery(recoverySnap, recoveryPath, recoveryBytes, "thread_not_found")
				if err != nil {
					return fmt.Errorf("thread not found and recovery checkpoint failed: %w", err)
				}
				log.Printf("[codex] stale thread session=%s; starting recovery generation", s.ID)
			} else {
				// A timeout or transport failure is not proof that the old thread is
				// unusable. Starting a blank fallback here forks the logical session.
				return fmt.Errorf("thread/resume failed without safe fallback: %w", err)
			}
		}
	}
	if threadID == "" {
		model := snap.Model
		if model == "" {
			model = c.defaultModel()
		}
		startParams := map[string]any{
			"model": model, "cwd": cwd, "ephemeral": false,
			"approvalPolicy": "never", "approvalsReviewer": "user", "sandbox": sandbox,
			"serviceTier": func() any {
				if snap.ServiceTier == "" {
					return nil
				}
				return snap.ServiceTier
			}(),
			"personality": func() any {
				if snap.Personality == "" {
					return nil
				}
				return snap.Personality
			}(),
		}
		c.applyDelegationThreadTools(s, startParams)
		c.applySessionControlThreadTools(s, startParams)
		if err := c.applyTaskAPIThreadTools(s, startParams); err != nil {
			return err
		}
		if err := c.applyPMThreadPolicy(s, startParams); err != nil {
			return err
		}
		raw, err := c.rpcCall("thread/start", startParams, 15*time.Second)
		if err != nil {
			return err
		}
		threadID = extractThreadID(raw, "")
		threadMetadataRaw = append(json.RawMessage(nil), raw...)
		if threadID == "" {
			return errNoThreadID
		}
	}
	st.mu.Lock()
	st.threadID = threadID
	if recovery != nil {
		st.pendingHandoff = recovery.Handoff
		st.pendingRecovery = recovery
	}
	st.mu.Unlock()
	c.mu.Lock()
	c.threadToSession[threadID] = s
	c.mu.Unlock()
	if !preserveContext {
		c.applyThreadRuntimeMetadata(s, threadMetadataRaw)
	}
	// Retained task/owner profiles are immutable. Thread defaults are not the
	// admitted turn configuration and must not disable its scoped toolset.

	if recovery == nil {
		s.SetResumeID(threadID)
		c.sink.Emit(backend.NewSessionUUID(s.ID, threadID))
		log.Printf("[codex] session=%s thread=%s", s.ID, threadID)
	} else {
		log.Printf("[codex] recovery staged session=%s source=%s thread=%s", s.ID, recovery.OldResumeID, threadID)
	}
	return nil
}

func (c *Codex) UpdateSessionSettings(ctx context.Context, s *session.Session) error {
	if err := c.validateAppSandbox(s, ""); err != nil {
		return err
	}
	if c.pmProvider != nil {
		p, err := c.pmProvider.PMConfiguration(s.ID)
		if err != nil {
			return err
		}
		if p != nil {
			return errors.New("pm_role_settings_locked")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	st := c.state(s.ID)
	st.ensureMu.Lock()
	defer st.ensureMu.Unlock()
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	if threadID == "" {
		// A native Codex Desktop/CLI may already own the durable thread while
		// Bridge has no local live state. Address the persisted thread directly
		// so the caller receives a real runtime ACK/rejection instead of a false
		// success that only changes Bridge's next-turn snapshot.
		threadID = s.ResumeID()
	}
	if threadID == "" {
		// Brand-new session: there is no runtime object to update yet. Persisted
		// settings are authoritative and will be applied by thread/start.
		return nil
	}
	snap := s.Snapshot()
	if snap.ResumeID != "" && threadID != snap.ResumeID {
		return errors.New("session_native_thread_mismatch")
	}
	params := map[string]any{
		"threadId":          threadID,
		"model":             nil,
		"effort":            nil,
		"serviceTier":       nil,
		"personality":       nil,
		"collaborationMode": nil,
	}
	for key, value := range c.sessionPermissionSettings(threadID, snap) {
		params[key] = value
	}
	if snap.Model != "" {
		params["model"] = snap.Model
	}
	if snap.ServiceTier != "" {
		params["serviceTier"] = snap.ServiceTier
	}
	if snap.Personality != "" {
		params["personality"] = snap.Personality
	}
	if snap.Effort != "" && snap.Effort != "auto" {
		params["effort"] = snap.Effort
	}
	if snap.CollaborationMode != "" {
		params["collaborationMode"] = c.collaborationModeValue(snap)
	}
	// Settings can be the first operation after Bridge startup; catalog warmup
	// may not have connected the daemon yet. Initialize before writing the RPC.
	if !c.rpc.hasWriter() {
		if err := c.ensureServer(); err != nil {
			return err
		}
	}
	_, err := c.rpcCall("thread/settings/update", params, 15*time.Second)
	if err == nil || !isStaleThreadError(err) {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// settings/update only addresses loaded threads. A durable thread can still
	// exist on disk after daemon/Bridge restarts. Resume that exact ID, never use
	// ensureThread's missing-history recovery (which may create a new thread).
	resumeParams := map[string]any{"threadId": threadID}
	if c.appServerMode == "daemon" {
		resumeParams["excludeTurns"] = true
	}
	raw, resumeErr := c.rpcCall("thread/resume", resumeParams, 15*time.Second)
	if resumeErr != nil {
		return fmt.Errorf("load existing thread for settings: %w", resumeErr)
	}
	if extractThreadID(raw, "") != threadID {
		return fmt.Errorf("load existing thread for settings returned an unexpected thread id")
	}
	st.mu.Lock()
	st.threadID = threadID
	st.mu.Unlock()
	c.mu.Lock()
	c.threadToSession[threadID] = s
	c.mu.Unlock()
	// Preserve the requested settings snapshot; resume metadata describes the
	// old settings. Only an explicit rejection is retried, never a timeout.
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err = c.rpcCall("thread/settings/update", params, 15*time.Second)
	return err
}

func (c *Codex) Stop(ctx context.Context, s *session.Session) error {
	st := c.state(s.ID)
	st.mu.Lock()
	st.stopping = true
	threadID, turnID, active := st.threadID, st.currentTurnID, st.turnActive
	observedTurnID := st.observedTurnID
	compacting := st.compactActive
	if compacting {
		turnID = st.compactTurnID
	}
	st.mu.Unlock()
	if !active && !compacting && observedTurnID != "" {
		if threadID == "" {
			threadID = s.ResumeID()
		}
		// Wait for the exact native terminal notification; never report an
		// unconfirmed interrupt as completed or release the Bridge queue.
		_, err := c.rpcCall("turn/interrupt", map[string]any{"threadId": threadID, "turnId": observedTurnID}, 5*time.Second)
		return err
	}
	if compacting {
		if threadID == "" || turnID == "" {
			return fmt.Errorf("context maintenance is awaiting its operation ID; stop is not yet confirmed")
		}
		_, err := c.rpcCall("turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, 5*time.Second)
		// Let the correlated terminal notification settle the waiter and queue.
		return err
	}

	if active {
		if threadID == "" || turnID == "" {
			st.mu.Lock()
			if st.turnActive && st.currentTurnID == "" {
				st.stopping = false
			}
			st.mu.Unlock()
			return fmt.Errorf("Codex stop is unconfirmed while the turn identity is pending; retry after acceptance")
		}
		if err := c.interruptCodexTurn(threadID, turnID); err != nil {
			st.mu.Lock()
			if st.turnActive && st.threadID == threadID && st.currentTurnID == turnID {
				st.stopping = false
			}
			st.mu.Unlock()
			return err
		}
		// An RPC ACK means 'requested', not 'interrupted'. Do not finish here:
		// only the exact turn/completed notification may settle the waiter.
	} else if s.IsStreaming() {
		st.mu.Lock()
		st.stopping = false
		st.mu.Unlock()
		return fmt.Errorf("Codex stop is unconfirmed while the backend is preparing the turn")
	} else {
		c.sink.Emit(backend.NewStopped(s.ID, st.reqID))
	}
	return nil
}

func (c *Codex) Clear(ctx context.Context, s *session.Session) error {
	st := c.state(s.ID)
	_ = c.Stop(ctx, s)
	st.mu.Lock()
	threadID := st.threadID
	st.threadID = ""
	st.mu.Unlock()
	c.tools.ResetSession(s.ID)
	if threadID != "" {
		c.mu.Lock()
		fallbackOwner := c.threadToSession[threadID]
		activeOwner := c.activeThreadOwner[threadID]
		ownedElsewhere := (fallbackOwner != nil && fallbackOwner != s) ||
			(activeOwner != nil && activeOwner != s)
		if fallbackOwner == s {
			delete(c.threadToSession, threadID)
		}
		if activeOwner == s {
			delete(c.activeThreadOwner, threadID)
		}
		c.mu.Unlock()
		// A legacy duplicate session may still share this native thread. Detach
		// only the cleared bridge row; archiving would destroy the canonical
		// session's history and can interrupt its active turn.
		if !ownedElsewhere {
			_, _ = c.rpcCall("thread/archive", map[string]any{"threadId": threadID}, 5*time.Second)
		}
	}
	s.ClearResumeIDs()
	c.sink.Emit(backend.NewSessionWarning(s.ID, "Session history cleared."))
	c.sink.Emit(backend.NewGoalCleared(s.ID))
	return nil
}

func (c *Codex) Close(ctx context.Context, s *session.Session) error {
	st := c.state(s.ID)
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	c.mu.Lock()
	delete(c.states, s.ID)
	if threadID != "" {
		if c.threadToSession[threadID] == s {
			delete(c.threadToSession, threadID)
		}
		if c.activeThreadOwner[threadID] == s {
			delete(c.activeThreadOwner, threadID)
		}
	}
	c.mu.Unlock()
	return nil
}

// ReleaseSession detaches only this daemon connection from a thread. It does
// not archive, delete, or clear the rollout, so a desktop client can resume the
// same thread id without competing with the Bridge's subscription.
func (c *Codex) ReleaseSession(ctx context.Context, s *session.Session) error {
	if err := c.ensureServer(); err != nil {
		return err
	}
	st := c.state(s.ID)
	st.ensureMu.Lock()
	defer st.ensureMu.Unlock()
	st.mu.Lock()
	threadID := st.threadID
	st.mu.Unlock()
	if threadID == "" {
		threadID = s.ResumeID()
	}
	if threadID == "" {
		return nil
	}
	if _, err := c.rpcCall("thread/unsubscribe", map[string]any{"threadId": threadID}, 15*time.Second); err != nil {
		if isActiveWriterError(err) {
			// Another client already owns the thread; the desired handoff state is
			// therefore already true from the Bridge's perspective.
			c.forgetThread(s, st, threadID)
			return nil
		}
		return fmt.Errorf("thread/unsubscribe: %w", err)
	}
	c.forgetThread(s, st, threadID)
	return nil
}

// ClaimSession proves that the desktop writer is gone by successfully
// resuming the thread before the control lease is returned to mobile.
func (c *Codex) ClaimSession(ctx context.Context, s *session.Session) error {
	if err := c.ensureServer(); err != nil {
		return err
	}
	return c.ensureThread(s, c.state(s.ID))
}

func (c *Codex) RespondUserInput(id string, answers map[string]any, cancelled bool) bool {
	c.interMu.Lock()
	ci, ok := c.interactions[id]
	if !ok {
		for rid, pending := range c.interactions {
			if pending.payload.ToolUseID == id {
				ci, ok = pending, true
				id = rid
				break
			}
		}
	}
	if ok && (ci.nativeThread != "" || ci.submitting) {
		c.interMu.Unlock()
		return false
	}
	if ok {
		ci.submitting = true
		c.interactions[id] = ci
	}
	c.interMu.Unlock()
	if !ok {
		return false
	}
	// Human wait time is not model inactivity. Start a fresh liveness window
	// when the answer is submitted, even before the next model event arrives.
	c.state(ci.payload.SessionID).touch(time.Now())
	if answers == nil {
		answers = map[string]any{}
	}
	if ci.rpcID != nil {
		var result any
		switch ci.responseKind {
		case "mcp":
			result = mcpElicitationResponse(ci.mcpParams, answers, cancelled)
		case "dynamic_tool":
			text := codexAnyString(answers["result"])
			result = map[string]any{"contentItems": []any{map[string]any{"type": "inputText", "text": text}}, "success": !cancelled}
		case "request_user_input_v2":
			result = map[string]any{"answers": codexV2UserInputAnswers(ci.payload.Questions, answers, cancelled)}
		default:
			result = map[string]any{"answers": answers, "cancelled": cancelled}
		}
		if err := c.rpc.write(map[string]any{"id": ci.rpcID, "result": result}); err != nil {
			c.interMu.Lock()
			if pending, exists := c.interactions[id]; exists {
				pending.submitting = false
				c.interactions[id] = pending
			}
			c.interMu.Unlock()
			return false
		}
		if ci.responseKind == "dynamic_tool" {
			c.tools.ResultEnd(ci.payload.SessionID, ci.reqID, ci.payload.ToolUseID, codexAnyString(answers["result"]))
		}
	}
	c.interMu.Lock()
	delete(c.interactions, id)
	c.interMu.Unlock()
	status := "resolved"
	if cancelled {
		status = "cancelled"
	}
	c.sink.Emit(backend.NewInteractionResolved(ci.payload.RequestID, ci.payload.SessionID, status))
	return true
}

func codexV2UserInputAnswers(questions []backend.UserInputQuestion, answers map[string]any, cancelled bool) map[string]any {
	out := make(map[string]any, len(questions))
	for _, question := range questions {
		values := []string{}
		if !cancelled {
			switch value := answers[question.QuestionID].(type) {
			case string:
				if value != "" {
					values = []string{value}
				}
			case []string:
				values = append(values, value...)
			case []any:
				for _, item := range value {
					if text := codexAnyString(item); text != "" {
						values = append(values, text)
					}
				}
			default:
				if text := codexAnyString(value); text != "" {
					values = []string{text}
				}
			}
		}
		out[question.QuestionID] = map[string]any{"answers": values}
	}
	return out
}

func (c *Codex) PendingInteractions(sessionID string) []backend.UserInputPayload {
	c.interMu.Lock()
	defer c.interMu.Unlock()
	out := []backend.UserInputPayload{}
	for _, pending := range c.interactions {
		if sessionID == "" || pending.payload.SessionID == sessionID {
			out = append(out, pending.payload)
		}
	}
	return out
}

// --- helpers ---------------------------------------------------------------

func validateCodexInlineFiles(files []backend.FileAttachment) error {
	for _, file := range files {
		if file.MediaType == "application/pdf" || file.RemotePath != "" || file.AttachmentID != "" {
			return errors.New("binary attachments must be materialized and validated by Bridge admission before Codex execution")
		}
	}
	return nil
}

func (c *Codex) codexInput(s *session.Session, reqID, imageOwnerID, content string, images []backend.ImageAttachment, files []backend.FileAttachment, st *codexState) []map[string]any {
	userText := content
	for _, f := range files {
		name := f.Name
		if name == "" {
			name = "file"
		}
		userText += "\n\n[File: " + name + "]\n" + f.Content
	}
	input := []map[string]any{{"type": "text", "text": userText, "text_elements": []any{}}}
	for _, img := range images {
		if item := c.prepareCodexImageInput(s, reqID, imageOwnerID, img, st); item != nil {
			input = append(input, item)
		}
	}
	return input
}

func (c *Codex) prepareCodexImageInput(s *session.Session, reqID, imageOwnerID string, img backend.ImageAttachment, st *codexState) map[string]any {
	raw := strings.TrimSpace(img.Data)
	if raw == "" {
		return nil
	}
	if strings.HasPrefix(strings.ToLower(raw), "data:") {
		if _, rest, ok := strings.Cut(raw, ","); ok {
			raw = rest
		}
	}
	blob, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil
	}
	// Stage prompt images under Bridge runtime state, not the Session cwd.
	// Downloads/Desktop/Documents are TCC-protected on macOS and the shared
	// Codex daemon (rather than Everything Go.app) is the process that opens a
	// localImage. Keeping the short-lived handoff outside those folders avoids a
	// fresh protected-folder prompt after Codex binary updates.
	root := filepath.Join(c.dataDir, "codex-input-images")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil
	}
	sessionHash := sha256.Sum256([]byte(s.ID))
	requestHash := sha256.Sum256([]byte(reqID))
	path := filepath.Join(root, fmt.Sprintf("%x_%x_%s%s", sessionHash[:6], requestHash[:6], randHex(8), codexImageExt(img.MediaType)))
	if err := os.WriteFile(path, blob, 0o600); err != nil {
		return nil
	}
	st.mu.Lock()
	if st.tempImages == nil {
		st.tempImages = make(map[string][]string)
	}
	st.tempImages[imageOwnerID] = append(st.tempImages[imageOwnerID], path)
	st.mu.Unlock()
	return map[string]any{"type": "localImage", "path": path}
}

func (c *Codex) cleanupTempImages(st *codexState, imageOwnerID string) {
	st.mu.Lock()
	paths := append([]string(nil), st.tempImages[imageOwnerID]...)
	delete(st.tempImages, imageOwnerID)
	st.mu.Unlock()
	for _, path := range paths {
		_ = os.Remove(path)
	}
}

func normalizeCodexQuestions(raw []map[string]any) []backend.UserInputQuestion {
	if len(raw) == 0 {
		return []backend.UserInputQuestion{{
			QuestionID: "q1",
			Text:       "Question",
			Type:       "question",
			FreeForm:   true,
		}}
	}
	out := make([]backend.UserInputQuestion, 0, len(raw))
	for i, q := range raw {
		options := normalizeCodexOptions(codexFirstAny(q, "options", "choices"))
		qtype := codexFirstString(q, "type", "kind")
		multi := codexBoolField(q, "multiSelect", "multi_select", "multiple")
		freeForm := codexBoolField(q, "freeForm", "free_form", "allowFreeForm", "isOther", "is_other")
		secret := codexBoolField(q, "isSecret", "is_secret", "secret")
		if qtype == "" {
			switch {
			case secret:
				qtype = "secret"
				freeForm = true
			case multi:
				qtype = "multi_choice"
			case len(options) > 0:
				qtype = "choice"
			default:
				qtype = "question"
				freeForm = true
			}
		}
		qid := codexFirstString(q, "question_id", "id")
		if qid == "" {
			qid = "q" + strconv.Itoa(i+1)
		}
		text := codexFirstString(q, "text", "question", "label")
		if text == "" {
			text = "Question"
		}
		out = append(out, backend.UserInputQuestion{
			QuestionID: qid, Text: text, Header: codexFirstString(q, "header", "title"),
			Type: qtype, Options: options, MultiSelect: multi, FreeForm: freeForm, Secret: secret,
		})
	}
	return out
}

func normalizeCodexOptions(raw any) []backend.UserInputOption {
	list, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]backend.UserInputOption, 0, len(list))
	for i, item := range list {
		if m, ok := item.(map[string]any); ok {
			label := codexFirstString(m, "label", "text", "value", "id")
			if label == "" {
				label = strconv.Itoa(i)
			}
			id := codexFirstString(m, "id", "value", "label")
			if id == "" {
				id = strconv.Itoa(i)
			}
			out = append(out, backend.UserInputOption{
				ID: id, Label: label, Description: codexFirstString(m, "description", "detail"),
				Recommended: codexBoolField(m, "recommended", "isRecommended"),
			})
			continue
		}
		label := strings.TrimSpace(fmt.Sprint(item))
		id := label
		if id == "" {
			id = strconv.Itoa(i)
		}
		out = append(out, backend.UserInputOption{ID: id, Label: label})
	}
	return out
}

func codexQuestionMaps(raw any) []map[string]any {
	list, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(list))
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func codexFirstAny(m map[string]any, keys ...string) any {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			return v
		}
	}
	return nil
}

func codexFirstString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func codexBoolField(m map[string]any, keys ...string) bool {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if b, ok := v.(bool); ok {
				return b
			}
		}
	}
	return false
}

func codexImageExt(mediaType string) string {
	mt := strings.ToLower(mediaType)
	switch {
	case strings.Contains(mt, "png"):
		return ".png"
	case strings.Contains(mt, "webp"):
		return ".webp"
	case strings.Contains(mt, "gif"):
		return ".gif"
	default:
		return ".jpg"
	}
}

func codexSandbox(s string) string {
	switch s {
	case "read-only", "workspace-write", "danger-full-access":
		return s
	default:
		return "workspace-write"
	}
}

// Browser Use reads its approval configuration through Codex's privileged
// runtime and needs outbound network access. Current Codex builds only expose
// both reliably through danger-full-access. Match the Python bridge for an
// unset sandbox when browser networking is enabled, while preserving every
// explicit sandbox choice and the network opt-out.
func codexSandboxForSession(snap session.Snapshot) string {
	base := codexSandbox(snap.Sandbox)
	if strings.TrimSpace(snap.Sandbox) != "" || strings.EqualFold(strings.TrimSpace(os.Getenv("BRIDGE_BROWSER_ORIGIN_MODE")), "deny") {
		return base
	}
	if disabled := strings.ToLower(strings.TrimSpace(os.Getenv("BRIDGE_BROWSER_ENABLE_NETWORK"))); disabled == "0" || disabled == "false" || disabled == "no" || disabled == "off" {
		return base
	}
	return "danger-full-access"
}

func codexTurnSandboxPolicy(snap session.Snapshot, override string) map[string]any {
	sandbox := strings.TrimSpace(override)
	if sandbox == "" {
		sandbox = codexSandboxForSession(snap)
	}
	switch sandbox {
	case "read-only":
		return map[string]any{"type": "readOnly", "networkAccess": false}
	case "workspace-write":
		roots := []string{}
		if cwd := runtime.ExpandPath(snap.Cwd); filepath.IsAbs(cwd) {
			roots = append(roots, cwd)
		}
		return map[string]any{"type": "workspaceWrite", "networkAccess": false, "writableRoots": roots}
	default:
		return map[string]any{"type": "dangerFullAccess"}
	}
}

func extractThreadID(raw json.RawMessage, fallback string) string {
	var r struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if json.Unmarshal(raw, &r) == nil && r.Thread.ID != "" {
		return r.Thread.ID
	}
	return fallback
}

func decodeThreadRuntimeMetadata(raw json.RawMessage) (model, effort *string) {
	var response struct {
		Model           *string `json:"model"`
		ReasoningEffort *string `json:"reasoningEffort"`
		Thread          struct {
			Model           *string `json:"model"`
			ReasoningEffort *string `json:"reasoningEffort"`
		} `json:"thread"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return nil, nil
	}
	model, effort = response.Thread.Model, response.Thread.ReasoningEffort
	if model == nil {
		model = response.Model
	}
	if effort == nil {
		effort = response.ReasoningEffort
	}
	return model, effort
}

func (c *Codex) applyThreadRuntimeMetadata(s *session.Session, raw json.RawMessage) {
	model, effort := decodeThreadRuntimeMetadata(raw)
	snap := s.Snapshot()
	var changedModel, changedEffort *string
	if model != nil && strings.TrimSpace(*model) != "" && snap.Model != *model {
		value := *model
		s.ApplyConfig("", value, "")
		changedModel = &value
	}
	if effort != nil && strings.TrimSpace(*effort) != "" && snap.Effort != *effort {
		value := *effort
		s.SetEffort(value)
		changedEffort = &value
	}
	if changedModel != nil || changedEffort != nil {
		c.sink.Emit(backend.NewSessionRuntimeMetadata(s.ID, changedModel, changedEffort))
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func rawToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}
