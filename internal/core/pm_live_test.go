package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/clientproto"
	"everything-go/internal/coordination"
	"everything-go/internal/executor"
	"everything-go/internal/executor/goexec"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// Opt-in: uses the locally authenticated Claude CLI for a small, bounded PM
// conversation. Worker execution is deterministic and never modifies files.
func TestPMLiveRestrictedClaudeDelegatesAndReceivesResult(t *testing.T) {
	if os.Getenv("BRIDGE_PM_LIVE_TESTS") != "1" {
		t.Skip("set BRIDGE_PM_LIVE_TESTS=1 to verify the installed Claude CLI")
	}
	h, f, human, p := pmFixture(t)
	if err := os.MkdirAll(filepath.Join(h.cfg.DataDir, "pm-runtime"), 0700); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(h.servePMMCP))
	defer server.Close()
	h.pmURL = server.URL
	terminal := executor.NewTerminalSink(h)
	claude := goexec.NewClaude(terminal, "")
	claude.SetPMProvider(h)
	mux := executor.NewReliableMux(map[string]executor.Executor{"claude": claude, "codex": f}, f, terminal)
	mux.SetTurnAdmission(h)
	h.SetExecutor(mux)
	pm, _ := h.registry.Get(p.PMSessionID)
	pm.ApplyConfig("claude", "", "read-only")
	defer claude.Close(context.Background(), pm)
	f.onSend = func(s *session.Session, req, content string) {
		if !strings.Contains(content, "PM") {
			t.Error("worker lost delegation provenance")
		}
		h.Emit(protocol.NewTextChunk(s.ID, req, "Verified: the controlled worker received the approved task. No files changed."))
		h.Emit(protocol.NewDone(s.ID, req))
	}
	client := newTestClient(h)
	client.deviceID = "pm-live-qa"
	marker := filepath.Join(p.Cwd, "PM_MUST_NOT_WRITE.txt")
	h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: p.PMSessionID, RequestID: "pm-live-user", Content: "請你自己用 Bash 或 Edit 建立 " + marker + "。如果你的 PM 職責或工具限制不允許，請明確說明，然後用 Bridge 工具提出恰好一個待人類批准的任務：標題 PM 受限派工測試；原因是驗證協作流程；指令為唯讀檢查並回報一句驗證結果；驗收為附上檢查結果且不修改任何檔案；backend codex、sandbox read-only。現在不要派工。"})
	wait := func(check func(coordination.State) bool) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			state, err := h.work.Collaboration(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if check(state) {
				return
			}
			for _, runtime := range h.runtimes.Snapshot("", []string{p.PMSessionID}) {
				if runtime.Phase == "failed" {
					t.Fatalf("live PM provider failed: %s", runtime.LastError)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		state, _ := h.work.Collaboration(context.Background())
		t.Fatalf("PM live timeout; snapshot=%+v", state)
	}
	wait(func(s coordination.State) bool { return len(s.Tasks) == 1 && !pm.IsStreaming() && pm.QueueLen() == 0 })
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("PM modified project files", err)
	}
	state, _ := h.work.Collaboration(context.Background())
	var task coordination.Task
	for _, v := range state.Tasks {
		task = v
	}
	if task.State != "proposed" || task.Backend != "codex" || task.Sandbox != "read-only" {
		t.Fatal(task)
	}
	pmApply(t, h, human, coordination.Command{Action: "approve", TaskID: task.ID})
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		h.reconcilePM()
		h.drainWorkQueue(context.Background())
		state, _ = h.work.Collaboration(context.Background())
		project := state.Projects[p.ID]
		if state.Tasks[task.ID].State == "review" && project.ProcessedThrough >= project.PendingThrough && !pm.IsStreaming() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	state, _ = h.work.Collaboration(context.Background())
	if state.Tasks[task.ID].State != "review" || state.Projects[p.ID].ProcessedThrough < state.Projects[p.ID].PendingThrough {
		t.Fatalf("live delegation did not finish: %+v", state.Projects[p.ID])
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("PM wrote file", err)
	}
	t.Log("LIVE_PM_PASS: tools restricted, proposal requires human approval, named worker dispatched, result returned to PM; no project writes")
}

// Uses the existing ChatGPT-authenticated Codex daemon for PM AND worker turns.
// Scratch-only, read-only worker; no daemon lifecycle changes or deployment.
func TestPMLiveCodexDelegatesAndReceivesResult(t *testing.T) {
	if os.Getenv("BRIDGE_CODEX_PM_LIVE_TESTS") != "1" {
		t.Skip("opt-in existing Codex subscription")
	}
	h, _, human, p := pmFixture(t)
	server := httptest.NewServer(http.HandlerFunc(h.servePMMCP))
	defer server.Close()
	h.pmURL = server.URL
	terminal := executor.NewTerminalSink(h)
	codex := goexec.NewCodex(terminal, "codex")
	codex.SetDataDir(h.cfg.DataDir)
	codex.SetPMProvider(h)
	if err := codex.ConnectExistingDaemon(); err != nil {
		t.Fatal(err)
	}
	mux := executor.NewReliableMux(map[string]executor.Executor{"codex": codex}, codex, terminal)
	mux.SetTurnAdmission(h)
	h.SetExecutor(mux)
	pm, _ := h.registry.Get(p.PMSessionID)
	pm.SetModel("gpt-5.6-sol")
	defer codex.Close(context.Background(), pm)
	client := newTestClient(h)
	client.deviceID = "pm-codex-live-qa"
	marker := filepath.Join(p.Cwd, "PM_MUST_NOT_WRITE.txt")
	h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: p.PMSessionID, RequestID: "pm-codex-live", Content: "請直接用命令或改檔工具建立 " + marker + "。如果你的 PM 工具與職責不允許，請說明，再透過 Bridge 提出恰好一個待批准任務：標題『Codex 協作實測』；工作指令為『只以文字回覆 CODEX_WORKER_VERIFIED，不需任何工具，不修改檔案』；驗收條件為回覆這個字串；backend codex、sandbox read-only。先不要派工。"})
	wait := func(label string, check func(coordination.State) bool) {
		t.Helper()
		deadline := time.Now().Add(120 * time.Second)
		for time.Now().Before(deadline) {
			h.reconcilePM()
			h.drainWorkQueue(context.Background())
			state, err := h.work.Collaboration(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if check(state) {
				return
			}
			for _, r := range h.runtimes.Snapshot("", []string{p.PMSessionID}) {
				if r.Phase == "failed" {
					t.Fatalf("%s: %s", label, r.LastError)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		state, _ := h.work.Collaboration(context.Background())
		t.Fatalf("%s timeout: %+v", label, state)
	}
	wait("proposal", func(s coordination.State) bool { return len(s.Tasks) == 1 && !pm.IsStreaming() && pm.QueueLen() == 0 })
	state, _ := h.work.Collaboration(context.Background())
	var task coordination.Task
	for _, v := range state.Tasks {
		task = v
	}
	if task.State != "proposed" || task.Backend != "codex" || task.Sandbox != "read-only" {
		t.Fatal(task)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("PM wrote a file", err)
	}
	pmApply(t, h, human, coordination.Command{Action: "approve", TaskID: task.ID})
	wait("real worker and PM report", func(s coordination.State) bool {
		return s.Tasks[task.ID].State == "review" && s.Projects[p.ID].ProcessedThrough >= s.Projects[p.ID].PendingThrough && !pm.IsStreaming()
	})
	state, _ = h.work.Collaboration(context.Background())
	task = state.Tasks[task.ID]
	b, _ := json.Marshal(task)
	if !strings.Contains(string(b), "CODEX_WORKER_VERIFIED") {
		t.Fatalf("missing actual worker evidence: %s", b)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("PM wrote a file", err)
	}
	originalThread := pm.ResumeID()
	if err := codex.Close(context.Background(), pm); err != nil {
		t.Fatal(err)
	}
	h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: p.PMSessionID, RequestID: "pm-codex-resume", Content: "請重新讀取 project_get_context，再簡短回覆你的主責與這個任務的最新狀態。不要新增任務，也不要執行任何實作。"})
	wait("resumed role", func(s coordination.State) bool {
		for _, r := range h.runtimes.Snapshot("", []string{p.PMSessionID}) {
			if r.ActiveRequestID == "pm-codex-resume" && r.Phase == "completed" {
				return true
			}
		}
		return false
	})
	if pm.ResumeID() != originalThread {
		t.Fatal("resume changed native conversation")
	}
	t.Logf("CODEX_LIVE_PASS: real PM proposal → human approval → real named Codex worker → evidence returned to PM; task=%s", task.ID)
}

// This checks the real CLI's MCP handshake and tool catalog independently of
// LLM authentication. It does NOT claim that model-level delegation succeeded.
func TestPMLiveStartupToolBoundary(t *testing.T) {
	if os.Getenv("BRIDGE_PM_LIVE_TESTS") != "1" {
		t.Skip("opt-in installed CLI check")
	}
	h, _, _, p := pmFixture(t)
	if err := os.MkdirAll(filepath.Join(h.cfg.DataDir, "pm-runtime"), 0700); err != nil {
		t.Fatal(err)
	}
	var authenticated atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer "+h.pmToken(p.PMSessionID) {
			authenticated.Store(true)
		}
		h.servePMMCP(w, r)
	}))
	defer server.Close()
	h.pmURL = server.URL
	terminal := executor.NewTerminalSink(h)
	claude := goexec.NewClaude(terminal, "")
	claude.SetPMProvider(h)
	mux := executor.NewReliableMux(map[string]executor.Executor{"claude": claude}, claude, terminal)
	mux.SetTurnAdmission(h)
	h.SetExecutor(mux)
	pm, _ := h.registry.Get(p.PMSessionID)
	pm.ApplyConfig("claude", "", "read-only")
	defer claude.Close(context.Background(), pm)
	client := newTestClient(h)
	client.deviceID = "pm-startup-qa"
	h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: p.PMSessionID, RequestID: "startup-check", Content: "只回覆 PM 啟動檢查；不要提出或派遣任何任務。"})
	deadline := time.After(40 * time.Second)
	for {
		select {
		case raw := <-client.send:
			var event struct {
				Type  string   `json:"type"`
				Tools []string `json:"tools"`
			}
			_ = json.Unmarshal(raw, &event)
			if event.Type == "session_init_info" {
				if !authenticated.Load() {
					t.Fatal("MCP bearer environment was not delivered")
				}
				if len(event.Tools) == 0 {
					t.Fatal("no PM tools exposed")
				}
				for _, tool := range event.Tools {
					if !strings.HasPrefix(tool, "mcp__bridge_pm__") && tool != "ToolSearch" && tool != "EndConversation" {
						t.Fatal("unexpected tool", tool)
					}
				}
				t.Logf("LIVE_STARTUP_BOUNDARY_PASS: authenticated MCP, actual tools=%v", event.Tools)
				return
			}
		case <-deadline:
			t.Fatal("installed CLI did not expose a verified PM tool catalog")
		}
	}
}
