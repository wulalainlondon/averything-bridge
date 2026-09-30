package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"everything-go/internal/clientproto"
	"everything-go/internal/coordination"
	"everything-go/internal/executor"
	"everything-go/internal/executor/goexec"
)

func TestCollaborationV2LiveCodexClosedLoop(t *testing.T) {
	if os.Getenv("BRIDGE_COLLAB_LIVE_TESTS") != "1" {
		t.Skip("opt-in existing subscription; isolated test conversations")
	}
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.cfg.RootDir = t.TempDir()
	attachWorkService(t, h, h.cfg.DataDir)
	h.pmEnabled = true
	h.pmSecret = []byte("isolated-live-collaboration-test")
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
	human := coordination.Principal{Human: true, ID: "collaboration-live-human"}
	_, err := h.applyCollaboration(human, coordination.CollaborationCommand{Command: coordination.Command{Action: "create", ProjectID: "live_v2", Name: "Isolated collaboration v2 test", Cwd: h.cfg.RootDir, MutationID: "create"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		state, _ := h.work.Collaboration(context.Background())
		for _, project := range state.Projects {
			if sess, ok := h.registry.Get(project.PMSessionID); ok {
				_ = codex.Close(context.Background(), sess)
			}
		}
		for _, task := range state.Tasks {
			if sess, ok := h.registry.Get(task.SessionID); ok {
				_ = codex.Close(context.Background(), sess)
			}
		}
	}()
	pm, _ := h.registry.Get("pm_live_v2")
	pm.SetModel("gpt-5.6-sol")
	client := newTestClient(h)
	client.deviceID = human.ID
	drainCtx, cancelDrain := context.WithCancel(context.Background())
	defer cancelDrain()
	go func() {
		for {
			select {
			case <-drainCtx.Done():
				return
			case <-client.send:
			}
		}
	}()
	h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: pm.ID, RequestID: "live-v2-proposal", Content: "請用 project_propose_task 提出恰好一個小任務，先不要派工。標題『協作閉環實測』，backend codex、sandbox read-only。指令：『先讀 work_get_context。在 preflight 回報 received/confirmed 後結束回合。execution 時用 work_save_artifact 儲存內容為 COLLABORATION_V2_LIVE_VERIFIED 的純文字成果，再用 work_submit_result 提交；不要修改任何專案檔案。』驗收條件只有 AC01：成果內容包含 COLLABORATION_V2_LIVE_VERIFIED 且有 artifact 與 evidence 引用。請明確設定 criteria 的 id=AC01、mandatory=true。non_goals 包含不修改專案、不發布、不另開代理。"})
	wait := func(label string, check func(coordination.State) bool) coordination.State {
		t.Helper()
		deadline := time.Now().Add(4 * time.Minute)
		lastLog := time.Time{}
		for time.Now().Before(deadline) {
			h.reconcilePM()
			h.drainWorkQueue(context.Background())
			state, err := h.work.Collaboration(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if check(state) {
				return state
			}
			if time.Since(lastLog) > 15*time.Second {
				lastLog = time.Now()
				states := []string{}
				for _, task := range state.Tasks {
					states = append(states, task.ID+":"+task.State+":"+state.Collaboration.Tasks[task.ID].Phase)
				}
				t.Log(label, states)
			}
			for _, run := range h.runtimes.Snapshot("", nil) {
				if run.Phase == "failed" {
					t.Fatalf("%s runtime failure: %s", label, run.LastError)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		state, _ := h.work.Collaboration(context.Background())
		t.Fatalf("%s timeout; project mode=%s note=%s tasks=%d pending=%d", label, state.Projects["live_v2"].Mode, state.Projects["live_v2"].Note, len(state.Tasks), len(state.PendingActionables("live_v2", "")))
		return state
	}
	state := wait("proposal", func(s coordination.State) bool { return len(s.Tasks) == 1 && !pm.IsStreaming() && pm.QueueLen() == 0 })
	var task coordination.Task
	for _, value := range state.Tasks {
		task = value
	}
	meta := state.Collaboration.Tasks[task.ID]
	_, err = h.applyCollaboration(human, coordination.CollaborationCommand{Command: coordination.Command{Action: "approve", ProjectID: "live_v2", TaskID: task.ID, MutationID: "approve"}, ExpectedEntityRevision: meta.Revision, ContractID: meta.ContractID})
	if err != nil {
		t.Fatal(err)
	}
	state = wait("preflight execution and PM disposition", func(s coordination.State) bool {
		worker, ok := h.registry.Get(task.SessionID)
		if !ok || worker.IsStreaming() || worker.QueueLen() > 0 || pm.IsStreaming() {
			return false
		}
		for _, a := range s.PendingActionables("live_v2", "human") {
			if a.Kind == "submission" && s.Tasks[task.ID].State == "review" {
				return true
			}
		}
		return false
	})
	meta = state.Collaboration.Tasks[task.ID]
	sub := state.Collaboration.Submissions[meta.SubmissionID]
	if sub.Status != "submitted" || len(sub.Input.ArtifactIDs) == 0 {
		t.Fatal("missing sealed artifact")
	}
	body, err := h.readCollaborationArtifact(state.Collaboration.Artifacts[sub.Input.ArtifactIDs[0]])
	if err != nil || !strings.Contains(body, "COLLABORATION_V2_LIVE_VERIFIED") {
		t.Fatal("actual artifact mismatch", err)
	}
	_, err = h.applyCollaboration(human, coordination.CollaborationCommand{Command: coordination.Command{Action: "accept", ProjectID: "live_v2", TaskID: task.ID, MutationID: "accept"}, ExpectedEntityRevision: meta.Revision, ContractID: meta.ContractID, EntityID: sub.ID})
	if err != nil {
		t.Fatal(err)
	}
	worker, _ := h.registry.Get(task.SessionID)
	t.Logf("COLLABORATION_V2_LIVE_PASS task=%s worker_thread=%s submission=%s; real PM proposal/dispatch/disposition, real worker preflight/report/artifact/submission, human acceptance", task.ID, worker.ResumeID(), sub.ID)
}
