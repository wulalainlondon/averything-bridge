package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"everything-go/internal/clientproto"
	"everything-go/internal/coordination"
)

type collaborationFixture struct {
	t      *testing.T
	h      *Hub
	taskID string
	n      int
}

func newCollaborationFixture(t *testing.T) *collaborationFixture {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.cfg.RootDir = t.TempDir()
	attachWorkService(t, h, h.cfg.DataDir)
	h.pmEnabled = true
	h.pmSecret = []byte("collaboration-test-only-secret")
	f := &collaborationFixture{t: t, h: h}
	f.apply(coordination.Principal{Human: true, ID: "human"}, coordination.CollaborationCommand{Command: coordination.Command{Action: "create", ProjectID: "p2", Name: "Collaboration", Cwd: h.cfg.RootDir}})
	r := f.apply(coordination.Principal{ID: "pm", SessionID: "pm_p2"}, coordination.CollaborationCommand{Command: coordination.Command{Action: "propose", ProjectID: "p2", Title: "Report", Reason: "Test delivery", Instruction: "Deliver a text artifact", Acceptance: "Return evidence", Backend: "codex", Sandbox: "workspace-write"}})
	f.taskID = r.TaskID
	f.apply(coordination.Principal{Human: true, ID: "human"}, coordination.CollaborationCommand{Command: coordination.Command{Action: "approve"}})
	f.apply(coordination.Principal{ID: "pm", SessionID: "pm_p2"}, coordination.CollaborationCommand{Command: coordination.Command{Action: "dispatch"}})
	h.reconcilePM()
	return f
}
func (f *collaborationFixture) state() coordination.State {
	f.t.Helper()
	s, err := f.h.work.Collaboration(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	return s
}
func (f *collaborationFixture) apply(p coordination.Principal, c coordination.CollaborationCommand) coordination.CollaborationResult {
	f.t.Helper()
	f.n++
	if c.ProjectID == "" {
		c.ProjectID = "p2"
	}
	if c.MutationID == "" {
		c.MutationID = fmt.Sprintf("fixture-%d", f.n)
	}
	if c.TaskID == "" && c.Action != "create" && c.Action != "propose" {
		c.TaskID = f.taskID
	}
	s := f.state()
	if c.TaskID != "" && s.Collaboration != nil {
		m := s.Collaboration.Tasks[c.TaskID]
		c.ExpectedEntityRevision = m.Revision
		if c.ContractID == "" {
			c.ContractID = m.ContractID
		}
	}
	r, err := f.h.applyCollaboration(p, c)
	if err != nil {
		f.t.Fatalf("%s: %v", c.Action, err)
	}
	return r
}
func (f *collaborationFixture) admit() coordination.Principal {
	f.t.Helper()
	state := f.state()
	task := state.Tasks[f.taskID]
	session, ok := f.h.registry.Get(task.SessionID)
	if !ok {
		f.t.Fatal("worker missing")
	}
	_, release, err := f.h.AdmitTurn(session, task.RequestID, "task")
	if err != nil {
		f.t.Fatal(err)
	}
	release()
	return coordination.Principal{ID: "worker:" + task.SessionID, SessionID: task.SessionID, RunID: task.RunID, Epoch: task.Epoch}
}
func (f *collaborationFixture) finish() {
	f.t.Helper()
	s := f.state()
	t := s.Tasks[f.taskID]
	m := s.Collaboration.Tasks[f.taskID]
	if _, err := f.h.work.AdvanceRun(context.Background(), t.SessionID, m.ActiveRequestID, "succeeded", ""); err != nil {
		f.t.Fatal(err)
	}
	f.h.finishPMTurn(t.SessionID, m.ActiveRequestID, "succeeded", "Model ended")
}
func (f *collaborationFixture) execute() coordination.Principal {
	p := f.admit()
	s := f.state()
	m := s.Collaboration.Tasks[f.taskID]
	f.apply(p, coordination.CollaborationCommand{Command: coordination.Command{Action: "report"}, Report: &coordination.ReportInput{Kind: "received", Text: "Understood", ContractID: m.ContractID, ManifestID: m.ManifestID, Readiness: "confirmed"}})
	f.finish()
	f.h.reconcilePM()
	return f.admit()
}

func TestCollaborationV2CoreDeliveryIsAtomicWithWorkAcceptance(t *testing.T) {
	f := newCollaborationFixture(t)
	p := f.execute()
	artifact, err := f.h.saveCollaborationArtifact(p, "p2", f.taskID, collaborationArtifactInput{MutationID: "artifact", Name: "report.txt", Kind: "text", Text: "Verified text artifact", Evidence: "Contains expected text"})
	if err != nil {
		t.Fatal(err)
	}
	s := f.state()
	m := s.Collaboration.Tasks[f.taskID]
	r := f.apply(p, coordination.CollaborationCommand{Command: coordination.Command{Action: "submit"}, Submission: &coordination.SubmissionInput{ContractID: m.ContractID, ManifestID: m.ManifestID, Summary: "Report delivered", ArtifactIDs: []string{artifact.ArtifactID}, Coverage: []coordination.Coverage{{CriterionID: "AC01", Claim: "passed", EvidenceIDs: []string{artifact.EvidenceID}}}, Limitations: []string{}}})
	f.finish()
	f.apply(coordination.Principal{Human: true, ID: "human"}, coordination.CollaborationCommand{Command: coordination.Command{Action: "accept"}, EntityID: r.EntityID})
	s = f.state()
	item, err := f.h.work.GetItem(context.Background(), s.Tasks[f.taskID].WorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	if item.Lifecycle != "done" || s.Tasks[f.taskID].State != "done" || s.Collaboration.Tasks[f.taskID].AcceptedSubmissionID != r.EntityID {
		t.Fatal("outcome owners diverged")
	}
}

func TestCollaborationV2CoreSnapshotOmitsSecretsAndLegacyCannotSeeNewEnums(t *testing.T) {
	f := newCollaborationFixture(t)
	p := f.execute()
	_, err := f.h.saveCollaborationArtifact(p, "p2", f.taskID, collaborationArtifactInput{MutationID: "artifact", Name: "report", Kind: "text", Text: "artifact body"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(f.h.collaborationSnapshot("", nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"\"receipts\"", "\"locator\"", "\"source_path\"", "collaboration-test-only-secret", f.h.cfg.DataDir} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("snapshot leaked %q", secret)
		}
	}
	legacy := f.h.pmSnapshot("", nil, nil)
	if len(legacy.Projects) != 0 || len(legacy.Tasks) != 0 {
		t.Fatal("new state leaked into legacy snapshot")
	}
}

func TestCollaborationV2CoreOldWorkAPIAndCommandsCannotBypass(t *testing.T) {
	f := newCollaborationFixture(t)
	f.execute()
	s := f.state()
	task := s.Tasks[f.taskID]
	for _, operation := range []string{"context", "events"} {
		req := httptest.NewRequest(http.MethodGet, "/api/work/v1/items/"+task.WorkItemID+"/"+operation, nil)
		req.RemoteAddr = "127.0.0.1:1234"
		w := httptest.NewRecorder()
		f.h.ServeWorkAPI(w, req)
		if w.Code != http.StatusForbidden {
			t.Fatal(operation, w.Code, w.Body.String())
		}
	}
	c := &Client{hub: f.h, deviceID: "human", send: make(chan []byte, 8), quit: make(chan struct{}), ctx: context.Background()}
	f.h.handleWorkCommand(c, clientproto.Command{Kind: "work_item_move", WorkItemID: task.WorkItemID, Lifecycle: "done", MutationID: "legacy-bypass"})
	if f.state().Tasks[f.taskID].State == "done" {
		t.Fatal("legacy accepted task")
	}
}

func TestCollaborationV2CoreWorkerMCPIsRunScoped(t *testing.T) {
	f := newCollaborationFixture(t)
	p := f.admit()
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"work_get_context","arguments":{}}}`
	call := func(epoch uint64, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/collaboration/%s/%s/%d", p.SessionID, p.RunID, epoch), strings.NewReader(body))
		r.RemoteAddr = "127.0.0.1:1234"
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		f.h.serveCollaborationMCP(w, r)
		return w
	}
	if got := call(p.Epoch, "wrong"); got.Code != http.StatusUnauthorized {
		t.Fatal(got.Code)
	}
	token := f.h.collaborationWorkerToken(p.SessionID, p.RunID, p.Epoch)
	if got := call(p.Epoch, token); got.Code != http.StatusOK || !strings.Contains(got.Body.String(), "preflight") {
		t.Fatal(got.Code, got.Body.String())
	}
	f.apply(coordination.Principal{Human: true, ID: "human"}, coordination.CollaborationCommand{Command: coordination.Command{Action: "take_over"}})
	if got := call(p.Epoch, token); got.Code != http.StatusForbidden {
		t.Fatal("old run token survived takeover", got.Code)
	}
}

func TestCollaborationV2CoreArtifactPathAndDrift(t *testing.T) {
	f := newCollaborationFixture(t)
	p := f.execute()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(f.h.cfg.RootDir, "escape")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../secret.txt", secret, "escape", ".env"} {
		_, err := f.h.saveCollaborationArtifact(p, "p2", f.taskID, collaborationArtifactInput{MutationID: "bad-" + fmt.Sprint(len(path)), Name: "bad", Kind: "file", Path: path})
		if err == nil {
			t.Fatal("accepted unsafe path", path)
		}
	}
	file := filepath.Join(f.h.cfg.RootDir, "report.txt")
	if err := os.WriteFile(file, []byte("v1"), 0600); err != nil {
		t.Fatal(err)
	}
	a, err := f.h.saveCollaborationArtifact(p, "p2", f.taskID, collaborationArtifactInput{MutationID: "good", Name: "report", Kind: "file", Path: "report.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("v2"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = f.h.verifyCollaborationArtifact(f.state().Collaboration.Artifacts[a.ArtifactID]); err == nil {
		t.Fatal("did not detect source drift")
	}
}

func TestCollaborationV2CorePMNoDispositionDoesNotConsumeActionable(t *testing.T) {
	f := newCollaborationFixture(t)
	p := f.execute()
	s := f.state()
	m := s.Collaboration.Tasks[f.taskID]
	r := f.apply(p, coordination.CollaborationCommand{Command: coordination.Command{Action: "report"}, Report: &coordination.ReportInput{Kind: "question", Text: "Need decision", ContractID: m.ContractID, ManifestID: m.ManifestID}})
	for i := 0; i < 2; i++ {
		req := fmt.Sprintf("pmwake-fixture-%d", i)
		_, err := f.h.work.UpdateCollaboration(context.Background(), func(s *coordination.State) error {
			project := s.Projects["p2"]
			project.WakeRequestID = req
			project.WakeThrough = project.PendingThrough
			project.WakeDispositionCount = s.ProjectDispositionCount("p2")
			s.Projects["p2"] = project
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		f.h.finishPMTurn("pm_p2", req, "succeeded", "Read but did not dispose")
	}
	s = f.state()
	if s.Projects["p2"].Mode != "paused" || s.Collaboration.Actionables[r.ActionableID].State != "open" {
		t.Fatal("actionable disappeared or PM spun forever")
	}
}
