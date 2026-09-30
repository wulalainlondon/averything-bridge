package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/coordination"
	"everything-go/internal/executor"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// Local UI -> real WS router -> real SQLite -> UI test host. No model account,
// production Bridge or project files are touched. The model adapter is fake.
func TestPMBrowserHarness(t *testing.T) {
	if os.Getenv("BRIDGE_PM_BROWSER_HARNESS") != "1" {
		t.Skip("local browser fixture")
	}
	h, f := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.cfg.RootDir = t.TempDir()
	attachWorkService(t, h, h.cfg.DataDir)
	h.pmEnabled = true
	h.pmSecret = []byte("qa-only-secret")
	h.registry.Create("qa-seed", "開始討論", h.cfg.RootDir, "codex", "", "read-only", "")
	if err := h.pairing.Claim("pm-qa-only", "pm-browser"); err != nil {
		t.Fatal(err)
	}
	mux := executor.NewReliableMux(map[string]executor.Executor{"claude": f, "codex": f}, f, executor.NewTerminalSink(h))
	mux.SetTurnAdmission(h)
	h.SetExecutor(mux)
	f.onSend = func(s *session.Session, req, content string) {
		go func() {
			time.Sleep(30 * time.Millisecond) // return executor admission before tool call
			state, err := h.work.Collaboration(context.Background())
			if err != nil {
				return
			}
			p, task, ok := state.ProjectForSession(s.ID)
			if !ok {
				return
			}
			text := "工作者檢查完成：提供主對話入口、工作卡與接管按鈕；沒有修改任何檔案。"
			if task == nil {
				actor := coordination.Principal{ID: "pm:" + s.ID, SessionID: s.ID}
				proposed := false
				for _, task := range state.Tasks {
					if task.ProjectID != p.ID {
						continue
					}
					proposed = true
					if task.State == "approved" && task.Owner == "pm" {
						_, err = h.applyPM(actor, coordination.Command{Action: "dispatch", ProjectID: p.ID, TaskID: task.ID, ExpectedRevision: p.Revision, MutationID: "dispatch-" + req})
						text = "已派出工作對話；你可以直接開啟查看或接管。"
						break
					}
				}
				if !proposed {
					_, err = h.applyPM(actor, coordination.Command{Action: "propose", ProjectID: p.ID, ExpectedRevision: p.Revision, MutationID: "proposal-" + req, Title: "主對話導覽檢查", Reason: "根據你在主對話的想法，先確認現有導覽結構。", Instruction: "唯讀檢查前端導覽，回報改善建議。", Acceptance: "說明主對話、工作入口與接管位置；不修改檔案。", Backend: "codex", Sandbox: "read-only", OriginRequestID: req})
					text = "已提出一份任務；等待你批准，尚未派工。"
				} else if !strings.Contains(text, "已派出") {
					text = "已讀取最新工作與人類修正。請查看工作卡的成果；最終驗收由你決定。"
				}
			}
			if err != nil {
				h.Emit(backend.NewError(s.ID, req, "pm_fixture", err.Error()))
				return
			}
			h.Emit(protocol.NewTextChunk(s.ID, req, text))
			h.Emit(protocol.NewDone(s.ID, req))
		}()
	}
	server := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	defer server.Close()
	manifest := map[string]string{"url": "ws" + strings.TrimPrefix(server.URL, "http"), "cwd": h.cfg.RootDir, "token": "pm-qa-only"}
	encoded, _ := json.Marshal(manifest)
	if err := os.WriteFile(os.Getenv("BRIDGE_PM_QA_MANIFEST"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("PM_BROWSER_HARNESS_READY")
	deadline := time.Now().Add(20 * time.Minute)
	for time.Now().Before(deadline) {
		h.reconcilePM()
		h.drainWorkQueue(context.Background())
		time.Sleep(100 * time.Millisecond)
	}
}
