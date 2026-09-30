// Package goexec is the pure-Go Executor: it spawns the Claude CLI as a
// persistent stream-json subprocess per session and parses its NDJSON stdout
// into normalized wire events. This is config 2 (pure Go).
//
// Fidelity reference: bridge/backends/claude_stream.py (_stdout_reader) and
// bridge/backends/claude_cli.py (spawn command + stdin message format).
package goexec

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/executor"
	"everything-go/internal/protocol"
	"everything-go/internal/runtime"
	"everything-go/internal/session"
)

const (
	// maxLine bounds a single NDJSON stdout line. Claude emits large tool results;
	// 16 MiB mirrors the Python _STREAM_READER_LIMIT order of magnitude.
	maxLine = 16 * 1024 * 1024

	claudeCompactThreshold = 0.80
	claudeIdleTimeout      = 6000 * time.Second
	claudeAskUserMaxWait   = 30 * time.Minute
)

// todoTools are Task/plan tools normalized into todo_update events instead of
// rendered as tool cards. Mirrors claude_stream.py's _TODO_TOOLS.
var todoTools = map[string]bool{
	"TodoWrite": true, "TaskCreate": true, "TaskUpdate": true, "TaskDelete": true,
}

type proc struct {
	pmSession      bool
	readOnlyWorker bool
	cmd            *exec.Cmd
	stdin          *bufWriteCloser
	cancel         context.CancelFunc
	exited         chan struct{}
	reqID          string // request_id of the in-flight turn, stamped onto events
	model          string

	// Tool/todo presentation state, touched only by this proc's readStdout goroutine.
	tools *toolNormalizer

	mu                sync.Mutex
	lastActivity      time.Time
	compactInProgress bool
}

func (p *proc) beginTurn(reqID string, compact bool) {
	p.mu.Lock()
	p.reqID = reqID
	p.compactInProgress = compact
	p.lastActivity = time.Now()
	p.mu.Unlock()
}

func (p *proc) touch() {
	p.mu.Lock()
	p.lastActivity = time.Now()
	p.mu.Unlock()
}

func (p *proc) currentReqID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reqID
}

func (p *proc) finishTurn() (reqID string, wasCompact bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	reqID = p.reqID
	wasCompact = p.compactInProgress
	p.reqID = ""
	p.compactInProgress = false
	return reqID, wasCompact
}

func (p *proc) markCompact(reqID string) {
	p.beginTurn(reqID, true)
}

func (p *proc) idleFor() (string, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reqID == "" {
		return "", 0
	}
	return p.reqID, time.Since(p.lastActivity)
}

func (p *proc) currentModel() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.model
}

type claudeState struct {
	mu           sync.Mutex
	restartCount int
	badResume    bool
}

// Claude implements executor.Executor over the local `claude` CLI.
type Claude struct {
	pmProvider  backend.PMProvider
	sink        executor.Sink
	tools       *toolEmitter
	claudeBin   string
	projectsDir string

	mu     sync.Mutex
	procs  map[string]*proc // sessionID -> running subprocess
	states map[string]*claudeState

	interMu sync.Mutex
	pending map[string]*pendingInteraction // request_id -> paused AskUserQuestion

	mcp *askUserMCP // in-process ask_user MCP server (nil if it failed to start)

	treeScanMu    sync.Mutex                 // guards treeScanCache
	treeScanCache map[string]cachedAgentScan // agent jsonl path -> mtime-keyed parse
}

func (c *Claude) SetPMProvider(provider backend.PMProvider) { c.pmProvider = provider }

func NewClaude(sink executor.Sink, claudeBin string) *Claude {
	if claudeBin == "" {
		claudeBin = "claude"
	}
	projectsDir := ""
	if home, err := os.UserHomeDir(); err == nil {
		projectsDir = filepath.Join(home, ".claude", "projects")
	}
	c := &Claude{
		sink: sink, claudeBin: claudeBin, projectsDir: projectsDir,
		tools:   newToolEmitter(sink),
		procs:   make(map[string]*proc),
		states:  make(map[string]*claudeState),
		pending: make(map[string]*pendingInteraction),

		treeScanCache: make(map[string]cachedAgentScan),
	}
	// Host the ask_user MCP server so AskUserQuestion-style prompts can actually
	// be answered from the app (the built-in tool can't be answered in headless
	// mode). Best-effort: if it won't start, we fall back to the native path.
	if mcp, err := startAskUserMCP(c); err != nil {
		log.Printf("[mcp] ask_user disabled: %v", err)
	} else {
		c.mcp = mcp
	}
	return c
}

func (c *Claude) state(sessionID string) *claudeState {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.states[sessionID]
	if st == nil {
		st = &claudeState{}
		c.states[sessionID] = st
	}
	return st
}

// Send writes a user message to the session's claude process (spawning it on
// first use) and lets the stdout reader stream the response.
func (c *Claude) Send(ctx context.Context, s *session.Session, reqID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	c.mu.Lock()
	p := c.procs[s.ID]
	if p == nil {
		if s.ResumeID() != "" {
			c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "resuming_thread", "Loading the existing Claude conversation"))
		} else {
			c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "starting_thread", "Starting Claude"))
		}
		var err error
		p, err = c.spawn(s)
		if err != nil {
			c.mu.Unlock()
			c.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrProcessDied, "Failed to spawn claude: "+err.Error()))
			return err
		}
		c.procs[s.ID] = p
	}
	isCompact := strings.TrimSpace(content) == "/compact"
	p.beginTurn(reqID, isCompact)
	c.mu.Unlock()
	c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "submitting_turn", "Sending the turn to Claude"))
	if isCompact {
		c.sink.Emit(backend.NewSessionCommandStarted(s.ID, reqID, 0))
	}

	payload := userMessageJSON(content, images, files)
	if _, err := p.stdin.Write(payload); err != nil {
		if isCompact {
			c.sink.Emit(backend.NewSessionCommandFailed(s.ID, reqID, "stdin write failed: "+err.Error(), 0))
		}
		c.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrSend, "stdin write failed: "+err.Error()))
		return err
	}
	if err := p.stdin.Flush(); err != nil {
		if isCompact {
			c.sink.Emit(backend.NewSessionCommandFailed(s.ID, reqID, "stdin flush failed: "+err.Error(), 0))
		}
		return err
	}
	c.sink.Emit(backend.NewTurnProgress(s.ID, reqID, "waiting_model", "Claude accepted the turn"))
	go c.agentTreePoller(s, p)
	return nil
}

// The CLI's spawn arguments are immutable. An idle process is retired before
// acknowledging new settings, and the next turn resumes the SAME conversation
// with the persisted configuration. Never restart an active writer.
func (c *Claude) UpdateSessionSettings(ctx context.Context, s *session.Session) error {
	if c.pmProvider != nil {
		policy, err := c.pmProvider.PMConfiguration(s.ID)
		if err != nil {
			return err
		}
		if policy != nil {
			return errors.New("pm_role_settings_locked")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	p := c.procs[s.ID]
	if p == nil {
		c.mu.Unlock()
		return nil
	}
	if p.currentReqID() != "" {
		c.mu.Unlock()
		return errors.New("runtime_busy")
	}
	delete(c.procs, s.ID)
	c.mu.Unlock()
	p.cancel()
	if p.exited != nil {
		select {
		case <-p.exited:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Second):
			return errors.New("runtime_retire_timeout")
		}
	}
	return nil
}

func (c *Claude) Stop(ctx context.Context, s *session.Session) error {
	c.mu.Lock()
	p := c.procs[s.ID]
	delete(c.procs, s.ID)
	c.mu.Unlock()

	if p != nil {
		p.cancel() // SIGKILL via context; the reader goroutine exits on EOF
	}
	c.cancelInteractionsFor(s.ID)
	c.sink.Emit(backend.NewStopped(s.ID, ""))
	return nil
}

func (c *Claude) Clear(ctx context.Context, s *session.Session) error {
	c.mu.Lock()
	p := c.procs[s.ID]
	delete(c.procs, s.ID)
	c.mu.Unlock()
	if p != nil {
		p.cancel()
	}
	c.cancelInteractionsFor(s.ID)
	s.ClearResumeIDs()
	// The next Send spawns a fresh proc (and todoStore), so the panel resets;
	// tell the app to clear it now. Mirrors claude_cli.clear's _evt_todo_update([]).
	c.sink.Emit(backend.NewTodoUpdate(s.ID, "", nil))
	c.sink.Emit(backend.NewSessionWarning(s.ID, "Session history cleared."))
	return nil
}

// PID reports the live claude subprocess pid for this session, if running.
// Used by get_tasks.
func (c *Claude) PID(s *session.Session) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.procs[s.ID]
	if p == nil || p.cmd.Process == nil {
		return 0, false
	}
	return p.cmd.Process.Pid, true
}

// KillProc terminates this session's claude subprocess (kill_task). The reader
// goroutine exits on EOF; the next Send re-spawns.
func (c *Claude) KillProc(s *session.Session) bool {
	c.mu.Lock()
	p := c.procs[s.ID]
	delete(c.procs, s.ID)
	c.mu.Unlock()
	if p == nil {
		return false
	}
	p.cancel()
	c.cancelInteractionsFor(s.ID)
	return true
}

func (c *Claude) Close(ctx context.Context, s *session.Session) error {
	c.mu.Lock()
	p := c.procs[s.ID]
	delete(c.procs, s.ID)
	c.mu.Unlock()
	if p != nil {
		p.cancel()
	}
	c.cancelInteractionsFor(s.ID)
	return nil
}

// spawn starts a persistent claude subprocess. Caller holds c.mu.
func (c *Claude) spawn(s *session.Session) (*proc, error) {
	snap := s.Snapshot() // consistent view of model/resume/effort/cwd
	ctx, cancel := context.WithCancel(context.Background())

	// Register the in-process ask_user MCP server (per-session URL) and steer
	// Claude to use it instead of the built-in AskUserQuestion, which can't be
	// answered in headless mode. skip-permissions already allows MCP tools.
	mcpURL := ""
	if c.mcp != nil {
		mcpURL = c.mcp.sessionURL(s.ID)
	}
	args := claudeSpawnArgs(snap, mcpURL)
	var pm *backend.PMConfiguration
	readOnlyWorker := false
	if c.pmProvider != nil {
		var err error
		pm, err = c.pmProvider.PMConfiguration(s.ID)
		if err != nil {
			cancel()
			return nil, err
		}
		if pm != nil {
			args = claudePMSpawnArgs(snap, *pm)
		}
		if workers, ok := c.pmProvider.(backend.CollaborationWorkerProvider); ok && pm == nil {
			readOnly, err := workers.IsReadOnlyCollaborationWorker(s.ID)
			if err != nil {
				cancel()
				return nil, err
			}
			if readOnly {
				readOnlyWorker = true
				args = claudeReadOnlyWorkerArgs(snap, mcpURL)
			}
		}
	}

	cmd := exec.CommandContext(ctx, c.claudeBin, args...)
	if snap.Cwd != "" {
		// Expand "~"/"~/..." like Python's os.path.expanduser before chdir;
		// the app sends "~" as the default cwd and exec won't expand it.
		cmd.Dir = runtime.ExpandPath(snap.Cwd)
	}
	if c.mcp != nil {
		// The per-server timeout in --mcp-config is sometimes ignored; the env var
		// is honored. 30 min lets a human take their time answering ask_user.
		cmd.Env = append(os.Environ(), "MCP_TOOL_TIMEOUT=1800000")
	}
	if pm != nil {
		cmd.Dir = pm.RuntimeDir
		cmd.Env = append(os.Environ(), "MCP_TOOL_TIMEOUT=30000", "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1", "ENABLE_CLAUDEAI_MCP_SERVERS=false", "BRIDGE_PM_SESSION_TOKEN="+pm.Token)
	}
	if readOnlyWorker {
		cmd.Env = append(os.Environ(), "MCP_TOOL_TIMEOUT=1800000", "ENABLE_CLAUDEAI_MCP_SERVERS=false")
	}
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}

	log.Printf("[%s] spawned claude pid=%d cwd=%s resume=%s", s.ID, cmd.Process.Pid, snap.Cwd, snap.ResumeID)
	p := &proc{
		pmSession:      pm != nil,
		readOnlyWorker: readOnlyWorker,
		cmd:            cmd, stdin: newBufWriteCloser(stdinPipe), cancel: cancel, exited: make(chan struct{}),
		tools: newToolNormalizer(c.sink, c), model: snap.Model,
	}
	p.touch()

	go c.readStdout(s, p, stdoutPipe)
	go c.readStderr(s.ID, stderrPipe)
	go c.idleWatchdog(s, p)
	go c.watchProc(s, p)
	return p, nil
}

func claudeSpawnArgs(snap session.Snapshot, mcpURL string) []string {
	args := []string{
		"--print",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
		"--verbose",
		// Token-level streaming: emit stream_event deltas so clients render text
		// as it generates instead of one block per assistant message.
		"--include-partial-messages",
	}
	model := snap.Model
	if model == "opusplan" {
		args = append(args, "--model", "opus", "--permission-mode", "plan")
	} else if model == "fable" {
		args = append(args, "--model", "claude-fable-5")
	} else {
		switch snap.Sandbox {
		case "read-only":
			args = append(args,
				"--dangerously-skip-permissions",
				"--allowedTools", "Read,Glob,Grep,WebSearch,WebFetch",
			)
		case "workspace-write":
			args = append(args,
				"--dangerously-skip-permissions",
				"--disallowedTools", "Bash",
			)
		default:
			args = append(args, "--dangerously-skip-permissions")
		}
		if model != "" {
			args = append(args, "--model", model)
		}
	}
	if mcpURL != "" {
		cfg := fmt.Sprintf(`{"mcpServers":{"ask_user":{"type":"http","url":%q,"timeout":1800000}}}`, mcpURL)
		// NB: keep the append-system-prompt ASCII-only. A non-ASCII char in this
		// prompt can make the claude CLI hang when the turn also contains an image.
		args = append(args,
			"--mcp-config", cfg,
			"--append-system-prompt", "When you need to ask the user a question or have them choose between options, call the ask_user MCP tool (mcp__ask_user__ask_question) and wait for the result. Do NOT use the built-in AskUserQuestion tool, which cannot be answered in this environment.",
		)
	}
	if snap.ResumeID != "" {
		args = append(args, "--resume", snap.ResumeID)
	}
	if snap.Effort != "" && snap.Effort != "auto" {
		args = append(args, "--effort", snap.Effort)
	}
	return args
}

// ndLine is the union of stdout line shapes we care about.
type ndLine struct {
	IsAPIErrorMessage bool   `json:"isApiErrorMessage"`
	APIError          string `json:"error"`
	Type              string `json:"type"`
	Subtype           string `json:"subtype"`
	Message           struct {
		Content []json.RawMessage `json:"content"`
	} `json:"message"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`    // tool_result payload
	SessionID string          `json:"session_id"` // result → new resume uuid
	Model     string          `json:"model"`      // system/init
	Result    json.RawMessage `json:"result"`     // result error payload
	Usage     struct {
		InputTokens              int `json:"input_tokens"`
		CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	} `json:"usage"`
	// Non-empty on events produced inside a Task subagent. Those are internal
	// to the subagent and must not pollute the main-chain stream.
	ParentToolUseID string `json:"parent_tool_use_id"`
	// stream_event payload (--include-partial-messages): the wrapped Anthropic
	// SSE event. Only content_block_delta text/thinking deltas are consumed.
	Event struct {
		Type  string `json:"type"`
		Delta struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			Thinking string `json:"thinking"`
		} `json:"delta"`
	} `json:"event"`
	// system/init capability surface forwarded to clients (A4).
	Tools        []string          `json:"tools"`
	SlashCmds    []string          `json:"slash_commands"`
	PermissionMd string            `json:"permissionMode"`
	MCPServers   []json.RawMessage `json:"mcp_servers"`
}

type block struct {
	Type     string          `json:"type"`
	Text     string          `json:"text"`
	Thinking string          `json:"thinking"`
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Input    json.RawMessage `json:"input"`
}

func (c *Claude) readStdout(s *session.Session, p *proc, stdout interface{ Read([]byte) (int, error) }) {
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	// True once this proc has emitted any stream_event: text/thinking then
	// arrive as deltas, so the aggregate assistant blocks must be skipped to
	// avoid double emission. Old CLIs without stream_event keep the aggregate
	// path. readStdout is the proc's only reader, so a local is race-free.
	sawStreamEvent := false
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var evt ndLine
		if err := json.Unmarshal(line, &evt); err != nil {
			continue // non-JSON diagnostic line
		}
		p.touch()
		if evt.ParentToolUseID != "" {
			continue // Task-subagent internal event — keep it out of the main stream
		}
		reqID := p.currentReqID()
		if evt.IsAPIErrorMessage {
			p.finishTurn()
			p.cancel()
			c.sink.Emit(backend.NewError(s.ID, reqID, "pm_provider_"+evt.APIError, "PM model authentication or API request failed; check the provider login before continuing."))
			return
		}
		switch evt.Type {
		case "stream_event":
			sawStreamEvent = true
			if evt.Event.Type != "content_block_delta" {
				continue
			}
			switch evt.Event.Delta.Type {
			case "text_delta":
				if evt.Event.Delta.Text != "" {
					c.sink.Emit(backend.NewTextChunk(s.ID, reqID, evt.Event.Delta.Text))
				}
			case "thinking_delta":
				if evt.Event.Delta.Thinking != "" {
					c.sink.Emit(backend.NewThinkingChunk(s.ID, reqID, evt.Event.Delta.Thinking))
				}
			}
		case "assistant":
			var askWaits []<-chan struct{}
			for _, raw := range evt.Message.Content {
				var b block
				if json.Unmarshal(raw, &b) != nil {
					continue
				}
				switch b.Type {
				case "thinking":
					if b.Thinking != "" && !sawStreamEvent {
						c.sink.Emit(backend.NewThinkingChunk(s.ID, reqID, b.Thinking))
					}
				case "text":
					if b.Text != "" && !sawStreamEvent {
						c.sink.Emit(backend.NewTextChunk(s.ID, reqID, b.Text))
					}
				case "tool_use":
					if p.tools.HandleClaudeToolUse(s.ID, reqID, b.ID, b.Name, b.Input) {
						continue
					}
					command := extractCommand(b.Input)
					if ch := p.tools.HandleClaudeVisibleToolUse(s, b.ID, b.Name, b.Input); ch != nil {
						askWaits = append(askWaits, ch)
					}
					c.tools.Start(s.ID, reqID, b.ID, b.Name, command)
				}
			}
			if len(askWaits) > 0 {
				c.waitForClaudeAskUser(s, askWaits)
			}
		case "tool_result":
			output := flattenToolOutput(evt.Content)
			if p.tools.HandleClaudeToolResult(s.ID, reqID, evt.ToolUseID, output) {
				continue
			}
			c.tools.ResultEnd(s.ID, reqID, evt.ToolUseID, output)
		case "result":
			if evt.Subtype != "" && evt.Subtype != "success" {
				msg := claudeRawToString(evt.Result)
				if msg == "" {
					msg = "Claude result failed"
				}
				_, wasCompact := p.finishTurn()
				if wasCompact {
					c.sink.Emit(backend.NewSessionCommandFailed(s.ID, reqID, msg, 0))
				}
				c.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrTurn, msg))
				continue
			}
			st := c.state(s.ID)
			st.mu.Lock()
			st.restartCount = 0
			st.mu.Unlock()
			contextUsed := evt.Usage.InputTokens + evt.Usage.CacheCreationInputTokens
			contextLimit := claudeContextLimit(p.currentModel())
			if contextLimit > 0 || contextUsed > 0 {
				s.SetContext(contextUsed, contextLimit)
			}
			if evt.SessionID != "" && evt.SessionID != s.ResumeID() {
				s.SetResumeID(evt.SessionID)
				c.sink.Emit(backend.NewSessionUUID(s.ID, evt.SessionID))
			}
			doneReqID, wasCompact := p.finishTurn()
			if doneReqID != "" {
				reqID = doneReqID
			}
			c.sink.Emit(backend.NewDone(s.ID, reqID))
			if wasCompact {
				c.sink.Emit(backend.NewSessionCommandDone(s.ID, reqID, 0))
			}
			if !wasCompact && c.shouldAutoCompact(contextUsed, contextLimit) {
				c.startAutoCompact(s, p)
			}
		case "system":
			if evt.Subtype == "init" && p.readOnlyWorker && !validReadOnlyWorkerTools(evt.Tools) {
				p.cancel()
				c.sink.Emit(backend.NewError(s.ID, p.currentReqID(), "pm_worker_tool_policy_mismatch", "Read-only worker exposed unexpected tools; execution stopped."))
				return
			}
			if evt.Subtype == "init" && c.pmProvider != nil {
				pm, err := c.pmProvider.PMConfiguration(s.ID)
				if err != nil || (pm != nil && !validPMTools(evt.Tools)) {
					p.cancel()
					c.sink.Emit(backend.NewError(s.ID, p.currentReqID(), "pm_tool_policy_mismatch", "PM runtime exposed unexpected tools; execution was stopped."))
					return
				}
			}
			if evt.Model != "" {
				p.mu.Lock()
				p.model = evt.Model
				p.mu.Unlock()
			}
			if evt.Subtype == "init" && evt.SessionID != "" && evt.SessionID != s.ResumeID() {
				first := s.ResumeID() == ""
				s.SetResumeID(evt.SessionID)
				if first {
					c.sink.Emit(backend.NewSessionUUID(s.ID, evt.SessionID))
				}
			}
			if evt.Subtype == "init" && (len(evt.Tools) > 0 || len(evt.SlashCmds) > 0) {
				var servers []backend.MCPServerStatus
				for _, raw := range evt.MCPServers {
					var srv backend.MCPServerStatus
					if json.Unmarshal(raw, &srv) == nil && srv.Name != "" {
						servers = append(servers, srv)
					}
				}
				c.sink.Emit(backend.NewSessionInitInfo(s.ID, evt.Model, evt.PermissionMd, evt.Tools, evt.SlashCmds, servers))
			}
		}
	}
	// stdout closed → process gone
}

func (c *Claude) readStderr(sessionID string, r interface{ Read([]byte) (int, error) }) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		line := sc.Text()
		log.Printf("[%s] claude stderr: %s", sessionID, line)
		if strings.Contains(line, "No conversation found") {
			st := c.state(sessionID)
			st.mu.Lock()
			st.badResume = true
			st.mu.Unlock()
		}
	}
}

func (c *Claude) waitForClaudeAskUser(s *session.Session, waits []<-chan struct{}) {
	deadline := time.After(claudeAskUserMaxWait)
	for _, ch := range waits {
		select {
		case <-ch:
		case <-deadline:
			c.sink.Emit(backend.NewSessionWarning(s.ID, "AskUserQuestion timed out; cancelling pending question."))
			c.cancelInteractionsFor(s.ID)
			return
		}
	}
}

func (c *Claude) watchProc(s *session.Session, p *proc) {
	if p.exited != nil {
		defer close(p.exited)
	}
	_ = p.cmd.Wait()
	rc := 0
	if p.cmd.ProcessState != nil {
		rc = p.cmd.ProcessState.ExitCode()
	}

	c.mu.Lock()
	if c.procs[s.ID] != p {
		c.mu.Unlock()
		return
	}
	delete(c.procs, s.ID)
	c.mu.Unlock()

	reqID := p.reqID
	if reqID != "" {
		c.sink.Emit(backend.NewError(
			s.ID, reqID, backend.ErrProcessDied,
			fmt.Sprintf("Claude process exited (rc=%d); current response was stopped.", rc),
		))
		_, wasCompact := p.finishTurn()
		if wasCompact {
			c.sink.Emit(backend.NewSessionCommandFailed(
				s.ID, reqID,
				fmt.Sprintf("Claude process exited during compact (rc=%d)", rc),
				0,
			))
		}
	}

	st := c.state(s.ID)
	st.mu.Lock()
	badResume := st.badResume
	if badResume {
		st.badResume = false
		st.restartCount = 0
	}
	restartCount := st.restartCount
	st.mu.Unlock()

	if badResume {
		old := s.ResumeID()
		s.SetResumeID("")
		c.sink.Emit(backend.NewSessionWarning(
			s.ID,
			fmt.Sprintf("Resume session %s not found, starting fresh...", old),
		))
		c.restartClaude(s, reqID)
		return
	}
	if rc != 0 && restartCount < 3 {
		st.mu.Lock()
		st.restartCount++
		attempt := st.restartCount
		st.mu.Unlock()
		c.sink.Emit(backend.NewSessionWarning(
			s.ID,
			fmt.Sprintf("Claude process exited (rc=%d), restarting (%d/3)...", rc, attempt),
		))
		c.restartClaude(s, reqID)
		return
	}
	if rc != 0 {
		c.sink.Emit(backend.NewSessionWarning(
			s.ID,
			fmt.Sprintf("Claude process exited (rc=%d) and will not restart.", rc),
		))
	}
}

func (c *Claude) restartClaude(s *session.Session, reqID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.procs[s.ID] != nil {
		return
	}
	p, err := c.spawn(s)
	if err != nil {
		c.sink.Emit(backend.NewError(s.ID, reqID, backend.ErrProcessDied, "Failed to restart claude: "+err.Error()))
		return
	}
	c.procs[s.ID] = p
}

func (c *Claude) shouldAutoCompact(contextUsed, contextLimit int) bool {
	return contextLimit > 0 && contextUsed >= int(float64(contextLimit)*claudeCompactThreshold)
}

func (c *Claude) startAutoCompact(s *session.Session, p *proc) {
	reqID := "compact_" + s.ID
	p.markCompact(reqID)
	c.sink.Emit(backend.NewSessionCommandStarted(s.ID, reqID, 0))
	payload := userMessageJSON("/compact", nil, nil)
	if _, err := p.stdin.Write(payload); err != nil {
		p.finishTurn()
		c.sink.Emit(backend.NewSessionCommandFailed(s.ID, reqID, err.Error(), 0))
		c.sink.Emit(backend.NewSessionWarning(s.ID, "Claude auto-compact failed: "+err.Error()))
		return
	}
	if err := p.stdin.Flush(); err != nil {
		p.finishTurn()
		c.sink.Emit(backend.NewSessionCommandFailed(s.ID, reqID, err.Error(), 0))
		c.sink.Emit(backend.NewSessionWarning(s.ID, "Claude auto-compact failed: "+err.Error()))
		return
	}
	log.Printf("[%s] claude auto-compact triggered: context_used=%d context_max=%d", s.ID, s.Snapshot().ContextUsed, s.Snapshot().ContextMax)
}

func claudeContextLimit(model string) int {
	m := strings.ToLower(model)
	if !strings.Contains(m, "claude") {
		return 0
	}
	if strings.Contains(m, "[1m]") || strings.Contains(m, "-1m") || strings.Contains(m, "1000000") {
		return 1_000_000
	}
	return 200_000
}

func (c *Claude) idleWatchdog(s *session.Session, p *proc) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		current := c.procs[s.ID] == p
		c.mu.Unlock()
		if !current {
			return
		}
		reqID, idle := p.idleFor()
		if reqID == "" {
			continue
		}
		if idle < claudeIdleTimeout {
			continue
		}
		c.sink.Emit(backend.NewSessionWarning(
			s.ID,
			fmt.Sprintf("Tool idle timeout after %.0fs; restarting Claude...", idle.Seconds()),
		))
		p.cancel()
		return
	}
}

func (c *Claude) agentTreePoller(s *session.Session, p *proc) {
	time.Sleep(3 * time.Second)
	lastTotal, lastDone := -1, -1
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		c.mu.Lock()
		current := c.procs[s.ID] == p
		c.mu.Unlock()
		if !current || p.currentReqID() == "" {
			return
		}
		resumeID := s.ResumeID()
		if resumeID != "" {
			total, tree := c.buildAgentTree(resumeID, time.Now().UnixMilli())
			if total > 0 {
				done := countDoneAgents(tree)
				if total != lastTotal || done != lastDone {
					lastTotal, lastDone = total, done
					c.sink.Emit(protocol.NewAgentTree(s.ID, resumeID, total, tree))
				}
			}
		}
		<-ticker.C
	}
}

func countDoneAgents(nodes []*protocol.AgentNode) int {
	total := 0
	for _, n := range nodes {
		if n.EndTS != nil {
			total++
		}
		total += countDoneAgents(n.Children)
	}
	return total
}

func claudeRawToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

// extractCommand mirrors Python: input.command if present, else the raw input
// JSON string.
func extractCommand(input json.RawMessage) string {
	if len(input) == 0 {
		return ""
	}
	var m map[string]any
	if json.Unmarshal(input, &m) == nil {
		if cmd, ok := m["command"].(string); ok {
			return cmd
		}
	}
	return string(input)
}

// flattenToolOutput mirrors Python: if content is a list of blocks, join their
// text with newlines; otherwise stringify.
func flattenToolOutput(content json.RawMessage) string {
	if len(content) == 0 {
		return ""
	}
	var blocks []block
	if json.Unmarshal(content, &blocks) == nil && len(blocks) > 0 {
		out := ""
		for i, b := range blocks {
			if b.Type == "text" {
				if i > 0 {
					out += "\n"
				}
				out += b.Text
			}
		}
		return out
	}
	var str string
	if json.Unmarshal(content, &str) == nil {
		return str
	}
	return string(content)
}

// userMessageJSON builds the stream-json `user` frame. Attachments become
// content blocks in the same order/shape as claude_cli.py: images first
// (base64 image blocks), then files (PDF → document block, else a fenced text
// block), then the text content last.
func userMessageJSON(content string, images []backend.ImageAttachment, files []backend.FileAttachment) []byte {
	blocks := make([]map[string]any, 0, len(images)+len(files)+1)
	for _, img := range images {
		mt := img.MediaType
		if mt == "" {
			mt = "image/jpeg"
		}
		blocks = append(blocks, map[string]any{
			"type":   "image",
			"source": map[string]any{"type": "base64", "media_type": mt, "data": img.Data},
		})
	}
	for _, f := range files {
		mt := f.MediaType
		if mt == "" {
			mt = "text/plain"
		}
		if mt == "application/pdf" {
			blocks = append(blocks, map[string]any{
				"type":   "document",
				"source": map[string]any{"type": "base64", "media_type": "application/pdf", "data": f.Content},
			})
			continue
		}
		name := f.Name
		if name == "" {
			name = "file"
		}
		ext := ""
		if i := strings.LastIndex(name, "."); i >= 0 {
			ext = name[i+1:]
		}
		fenced := f.Content
		if ext != "" {
			fenced = "```" + ext + "\n" + f.Content + "\n```"
		}
		blocks = append(blocks, map[string]any{"type": "text", "text": "[File: " + name + "]\n" + fenced})
	}
	if content != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": content})
	}
	frame := map[string]any{
		"type":    "user",
		"message": map[string]any{"role": "user", "content": blocks},
	}
	b, _ := json.Marshal(frame)
	return append(b, '\n')
}

func drainStderr(sessionID string, r interface{ Read([]byte) (int, error) }) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		log.Printf("[%s] claude stderr: %s", sessionID, sc.Text())
	}
}

// bufWriteCloser wraps the stdin pipe with a buffered writer + flush.
type bufWriteCloser struct {
	w  *bufio.Writer
	c  interface{ Close() error }
	mu sync.Mutex
}

func newBufWriteCloser(wc interface {
	Write([]byte) (int, error)
	Close() error
}) *bufWriteCloser {
	return &bufWriteCloser{w: bufio.NewWriter(wc), c: wc}
}

func (b *bufWriteCloser) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.w.Write(p)
}

func (b *bufWriteCloser) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.w.Flush()
}

func (b *bufWriteCloser) Close() error { return b.c.Close() }
