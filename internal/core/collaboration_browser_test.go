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

// Real UI -> real WS router -> SQLite -> real domain transitions. Only model
// inference is deterministic, so this harness does not claim a model eval.
func TestCollaborationV2BrowserHarness(t *testing.T) {
	if os.Getenv("BRIDGE_COLLAB_BROWSER_HARNESS") != "1" {
		t.Skip("local browser harness")
	}
	h, f := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.cfg.RootDir = t.TempDir()
	attachWorkService(t, h, h.cfg.DataDir)
	h.pmEnabled = true
	h.pmSecret = []byte("collaboration-browser-test-only")
	h.registry.Create("qa-seed", "討論需求", h.cfg.RootDir, "codex", "", "read-only", "")
	if err := h.pairing.Claim("collaboration-qa-only", "collaboration-browser"); err != nil {
		t.Fatal(err)
	}
	mux := executor.NewReliableMux(map[string]executor.Executor{"claude": f, "codex": f}, f, executor.NewTerminalSink(h))
	mux.SetTurnAdmission(h)
	h.SetExecutor(mux)
	f.onSend = func(sess *session.Session, requestID, content string) {
		go func() {
			time.Sleep(15 * time.Millisecond)
			s, err := h.work.Collaboration(context.Background())
			if err != nil {
				return
			}
			project, task, ok := s.ProjectForSession(sess.ID)
			if !ok {
				return
			}
			text := ""
			if task == nil {
				p := coordination.Principal{ID: "pm:" + sess.ID, SessionID: sess.ID}
				found := false
				for _, worker := range s.Tasks {
					if worker.ProjectID != project.ID {
						continue
					}
					found = true
					if worker.State == "approved" {
						_, err = h.applyCollaboration(p, coordination.CollaborationCommand{Command: coordination.Command{Action: "dispatch", ProjectID: project.ID, TaskID: worker.ID, MutationID: "dispatch-" + requestID}, ExpectedEntityRevision: s.Collaboration.Tasks[worker.ID].Revision})
						text = "已啟動批准的工作，先確認需求再執行。"
						break
					}
				}
				if !found {
					_, err = h.applyCollaboration(p, coordination.CollaborationCommand{Command: coordination.Command{Action: "propose", ProjectID: project.ID, MutationID: "propose-" + requestID, Title: "相片流程檢查", Reason: "依你提出的需求，先釐清行為再交付可檢查報告。", Instruction: "確認相片預覽行為，交付一份有證據的報告。", Acceptance: "包含已確認的選擇與對應證據。", Backend: "codex", Sandbox: "workspace-write"}, NonGoals: []string{"不修改導航，不發布或部署。"}})
					text = "已提出任務；請確認目標、限制與驗收條件後批准。"
				} else if text == "" {
					for _, a := range s.PendingActionables(project.ID, "pm") {
						action := "escalate_to_human"
						if a.Kind == "submission" {
							action = "recommend_acceptance"
						}
						_, err = h.applyCollaboration(p, coordination.CollaborationCommand{Command: coordination.Command{Action: "dispose", ProjectID: project.ID, MutationID: "dispose-" + a.ID + "-" + requestID}, ExpectedEntityRevision: a.Revision, Disposition: &coordination.DispositionInput{ActionableID: a.ID, Action: action, Text: "已讀取回報與來源；請確認卡片中的成果或下一步。"}})
						break
					}
					text = "已檢查待處置事項；最終驗收由你決定。"
				}
			} else if task.Owner == "human" {
				text = "已接續你的修改；此段對話會保留在交接紀錄。"
			} else {
				meta := s.Collaboration.Tasks[task.ID]
				p := coordination.Principal{ID: "worker:" + sess.ID, SessionID: sess.ID, RunID: meta.ActiveRunID, Epoch: task.Epoch}
				if meta.Phase == "preflight" {
					_, err = h.applyCollaboration(p, coordination.CollaborationCommand{Command: coordination.Command{Action: "report", ProjectID: project.ID, TaskID: task.ID, MutationID: "received-" + requestID}, Report: &coordination.ReportInput{Kind: "received", ContractID: meta.ContractID, ManifestID: meta.ManifestID, Text: "已確認目標、限制及驗收條件。", Readiness: "confirmed"}})
					text = "需求確認完成，接著由 Bridge 啟動已批准的執行。"
				} else {
					asked := false
					for _, r := range s.Collaboration.Reports {
						if r.TaskID == task.ID && r.Input.Kind == "question" {
							asked = true
						}
					}
					if !asked {
						_, err = h.applyCollaboration(p, coordination.CollaborationCommand{Command: coordination.Command{Action: "report", ProjectID: project.ID, TaskID: task.ID, MutationID: "question-" + requestID}, Report: &coordination.ReportInput{Kind: "question", ContractID: meta.ContractID, ManifestID: meta.ManifestID, Text: "旋轉裝置後，要保留尚未送出的照片嗎？", Blocking: true, DecisionOwner: "human"}})
						text = "已提出具體問題，等待你的選擇；沒有自行猜測。"
					} else {
						for _, a := range s.PendingActionables(project.ID, "worker") {
							if a.TaskID == task.ID && a.AnswerID != "" && a.Kind == "question" {
								_, err = h.applyCollaboration(p, coordination.CollaborationCommand{Command: coordination.Command{Action: "report", ProjectID: project.ID, TaskID: task.ID, MutationID: "ack-" + requestID}, Report: &coordination.ReportInput{Kind: "answer_ack", ContractID: meta.ContractID, ManifestID: meta.ManifestID, Text: "已採用你的答案。", QuestionID: a.ID, AnswerID: a.AnswerID, Adopted: true}})
							}
						}
						var artifact collaborationArtifactResult
						if err == nil {
							artifact, err = h.saveCollaborationArtifact(p, project.ID, task.ID, collaborationArtifactInput{MutationID: "artifact-" + requestID, Name: "相片流程報告.md", Kind: "text", Text: "# 相片流程報告\n\n依人類決策保留旋轉前的照片；未修改導航。\n\n這是確定性 QA 產物，不是實機相機驗證。", Evidence: "報告包含選擇、限制與驗證範圍。"})
						}
						if err == nil {
							_, err = h.applyCollaboration(p, coordination.CollaborationCommand{Command: coordination.Command{Action: "submit", ProjectID: project.ID, TaskID: task.ID, MutationID: "submit-" + requestID}, Submission: &coordination.SubmissionInput{ContractID: meta.ContractID, ManifestID: meta.ManifestID, Summary: "已交付可追溯報告；沒有把 QA 模型替身當成真機驗證。", ArtifactIDs: []string{artifact.ArtifactID}, Coverage: []coordination.Coverage{{CriterionID: "AC01", Claim: "passed", EvidenceIDs: []string{artifact.EvidenceID}}}, Limitations: []string{"這個案例验证協作閉環，不驗證相機硬體。"}}})
						}
						text = "成果已提交，等待封存與驗收。"
					}
				}
			}
			if err != nil {
				h.Emit(backend.NewError(sess.ID, requestID, "collaboration_fixture", err.Error()))
				return
			}
			h.Emit(protocol.NewTextChunk(sess.ID, requestID, text))
			h.Emit(protocol.NewDone(sess.ID, requestID))
		}()
	}
	server := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	defer server.Close()
	manifest := map[string]string{"url": "ws" + strings.TrimPrefix(server.URL, "http"), "cwd": h.cfg.RootDir, "authority": h.cfg.InstanceID}
	encoded, _ := json.Marshal(manifest)
	if err := os.WriteFile(os.Getenv("BRIDGE_COLLAB_QA_MANIFEST"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("COLLABORATION_BROWSER_HARNESS_READY")
	deadline := time.Now().Add(30 * time.Minute)
	for time.Now().Before(deadline) {
		h.reconcilePM()
		h.drainWorkQueue(context.Background())
		time.Sleep(100 * time.Millisecond)
	}
}
