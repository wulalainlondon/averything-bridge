package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"everything-go/internal/clientproto"
	"everything-go/internal/coordination"
	"everything-go/internal/executor"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/workitems"
)

func pmFixture(t *testing.T) (*Hub, *fakeExec, coordination.Principal, coordination.Project) {
	t.Helper()
	h, f := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	attachWorkService(t, h, h.cfg.DataDir)
	h.pmEnabled = true
	h.pmSecret = []byte("test-secret")
	h.pmURL = "http://127.0.0.1:1"
	mux := executor.NewReliableMux(map[string]executor.Executor{"claude": f, "codex": f}, f, executor.NewTerminalSink(h))
	mux.SetTurnAdmission(h)
	h.SetExecutor(mux)
	human := coordination.Principal{Human: true, ID: "phone"}
	r, err := h.applyPM(human, coordination.Command{Action: "create", ProjectID: "p1", Name: "PM test", Cwd: t.TempDir(), MutationID: "create"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.work.Collaboration(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return h, f, human, s.Projects[r.ProjectID]
}
func pmApply(t *testing.T, h *Hub, p coordination.Principal, c coordination.Command) coordination.Result {
	t.Helper()
	state, err := h.work.Collaboration(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c.ProjectID = "p1"
	c.ExpectedRevision = state.Projects["p1"].Revision
	c.MutationID = randomID()
	r, err := h.applyPM(p, c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func pmProposal(t *testing.T, h *Hub, p coordination.Project) coordination.Result {
	return pmApply(t, h, coordination.Principal{ID: "pm", SessionID: p.PMSessionID}, coordination.Command{Action: "propose", Title: "Inspect", Reason: "Trace the feature", Instruction: "Inspect only", Acceptance: "Cite evidence", Backend: "codex", Sandbox: "read-only"})
}
func TestPMWorkflowProvisionOnceHumanHandoffAndAcceptance(t *testing.T) {
	h, f, human, p := pmFixture(t)
	r := pmProposal(t, h, p)
	pmApply(t, h, human, coordination.Command{Action: "approve", TaskID: r.TaskID})
	pmApply(t, h, coordination.Principal{ID: "pm", SessionID: p.PMSessionID}, coordination.Command{Action: "dispatch", TaskID: r.TaskID})
	// Keep PM wakes queued during this deterministic policy test.
	pmSession, _ := h.registry.Get(p.PMSessionID)
	started := make(chan struct{})
	pmSession.Submit(func() { close(started) })
	<-started
	defer pmSession.EndTurn()
	h.reconcilePM()
	h.reconcilePM()
	state, _ := h.work.Collaboration(context.Background())
	task := state.Tasks[r.TaskID]
	if !task.Provisioned || task.State != "queued" {
		t.Fatal(task)
	}
	snapshot, _ := h.work.Snapshot(context.Background())
	if len(snapshot.Runs) != 1 || len(snapshot.SessionLinks) != 1 {
		t.Fatalf("duplicate provisioning %+v", snapshot)
	}
	f.onSend = func(s *session.Session, req, content string) {
		h.Emit(protocol.NewTextChunk(s.ID, req, "Evidence: inspected component"))
		h.Emit(protocol.NewDone(s.ID, req))
	}
	h.drainWorkQueue(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state, _ = h.work.Collaboration(context.Background())
		if state.Tasks[r.TaskID].State == "review" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if state.Tasks[r.TaskID].State != "review" {
		t.Fatal("no worker report", state.Tasks[r.TaskID])
	}
	worker, _ := h.registry.Get(task.SessionID)
	for worker.IsStreaming() {
		time.Sleep(time.Millisecond)
	}
	pmApply(t, h, human, coordination.Command{Action: "take_over", TaskID: r.TaskID})
	_, release, err := h.AdmitTurn(worker, "human-correction", "Revise the explanation")
	if err != nil {
		t.Fatal(err)
	}
	release()
	h.finishPMTurn(task.SessionID, "human-correction", "succeeded", "Updated evidence")
	pmApply(t, h, human, coordination.Command{Action: "return_to_pm", TaskID: r.TaskID, Text: "Use the revised direction"})
	_, _, err = h.AdmitTurn(worker, task.RequestID, "stale")
	if err == nil {
		t.Fatal("stale PM turn revived")
	}
	pmApply(t, h, human, coordination.Command{Action: "accept", TaskID: r.TaskID})
	h.reconcilePM()
	item, err := h.work.GetItem(context.Background(), task.WorkItemID)
	if err != nil || item.Lifecycle != workitems.LifecycleDone {
		t.Fatal("human acceptance not projected", item, err)
	}
}
func TestPMMCPRejectsImpersonationAndOtherProjects(t *testing.T) {
	h, _, _, p := pmFixture(t)
	call := func(token, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "http://localhost/mcp/"+p.PMSessionID, strings.NewReader(body))
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.servePMMCP(w, req)
		return w
	}
	if w := call("wrong", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`); w.Code != 401 {
		t.Fatal(w.Code)
	}
	valid := h.pmToken(p.PMSessionID)
	w := call(valid, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	if w.Code != 200 || strings.Contains(w.Body.String(), "approve_plan") {
		t.Fatal(w.Body.String())
	}
	w = call(valid, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"project_propose_task","arguments":{"action":"approve","mutation_id":"x"}}}`)
	if !strings.Contains(w.Body.String(), `"isError":true`) {
		t.Fatal("model selected human action", w.Body.String())
	}
	w = call(valid, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"project_delegate_task","arguments":{"project_id":"another","mutation_id":"x"}}}`)
	if !strings.Contains(w.Body.String(), `"isError":true`) {
		t.Fatal("model selected another project", w.Body.String())
	}
	w = call(valid, `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"project_get_context","arguments":{}}}`)
	if strings.Contains(w.Body.String(), "test-secret") || strings.Contains(w.Body.String(), valid) || w.Code != 200 {
		t.Fatal("credential exposed")
	}
}
func TestPMGovernanceCannotBeBypassedViaOrdinarySessionCommands(t *testing.T) {
	h, _, _, p := pmFixture(t)
	client := newTestClient(h)
	client.deviceID = "phone"
	for _, kind := range []string{"new_session", "switch_session_config", "fork_session", "handoff_to_desktop", "clear_session", "close_session"} {
		if !h.rejectPMCommand(client, clientproto.Command{Kind: kind, SessionID: p.PMSessionID}) {
			t.Fatal("allowed", kind)
		}
	}
	pm, _ := h.registry.Get(p.PMSessionID)
	pm.ApplyConfig("codex", "", "danger-full-access")
	if _, _, err := h.AdmitTurn(pm, "human", "do work"); err == nil {
		t.Fatal("PM backend/sandbox downgrade accepted")
	}
}

func TestPMRejectedMessageCannotReplayAfterTakeover(t *testing.T) {
	h, f, _, p := pmFixture(t)
	r := pmProposal(t, h, p)
	state, _ := h.work.Collaboration(context.Background())
	task := state.Tasks[r.TaskID]
	h.registry.Create(task.SessionID, "worker", p.Cwd, "codex", "", "read-only", "")
	c := newTestClient(h)
	cmd := clientproto.Command{Kind: "message", SessionID: task.SessionID, RequestID: "before-takeover", Content: "must never execute"}
	if !h.rejectPMCommand(c, cmd) {
		t.Fatal("unowned message accepted")
	}
	_, err := h.work.UpdateCollaboration(context.Background(), func(s *coordination.State) error {
		v := s.Tasks[task.ID]
		v.Owner = "human"
		v.Epoch++
		s.Tasks[task.ID] = v
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if h.rejectPMCommand(c, cmd) {
		t.Fatal("human role itself should now allow new messages")
	}
	sent := make(chan string, 2)
	f.onSend = func(s *session.Session, req, content string) { h.Emit(protocol.NewDone(s.ID, req)); sent <- req }
	h.enqueueChatMessage(c, cmd)
	if _, found, err := h.messageQueue.Get(task.SessionID, cmd.RequestID); err != nil || found {
		t.Fatal("rejected retry became a queue entry", err)
	}
	if _, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: task.SessionID, RequestID: cmd.RequestID, Payload: []byte("any")}); !errors.Is(err, messagequeue.ErrRejected) {
		t.Fatal("negative receipt missing", err)
	}
	cmd.RequestID = "after-takeover"
	cmd.Content = "explicit newly authorized message"
	h.enqueueChatMessage(c, cmd)
	select {
	case req := <-sent:
		if req != cmd.RequestID {
			t.Fatal("replayed denied request", req)
		}
	case <-time.After(time.Second):
		t.Fatal("new human message blocked")
	}
}
func TestPMSnapshotExcludesCredentialsAndReceipts(t *testing.T) {
	h, _, _, _ := pmFixture(t)
	raw, err := json.Marshal(h.pmSnapshot("", nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "receipts") || strings.Contains(string(raw), "test-secret") {
		t.Fatal(string(raw))
	}
	if _, err = h.PMConfiguration("pm_unregistered"); err == nil {
		t.Fatal("orphan PM role allowed")
	}
}

func TestPMQueuedWorkSurvivesPauseWithoutExecution(t *testing.T) {
	h, _, human, p := pmFixture(t)
	r := pmProposal(t, h, p)
	pmApply(t, h, human, coordination.Command{Action: "approve", TaskID: r.TaskID})
	pmApply(t, h, coordination.Principal{ID: "pm", SessionID: p.PMSessionID}, coordination.Command{Action: "dispatch", TaskID: r.TaskID})
	pm, _ := h.registry.Get(p.PMSessionID)
	started := make(chan struct{})
	pm.Submit(func() { close(started) })
	<-started
	defer pm.EndTurn()
	h.reconcilePM()
	pmApply(t, h, human, coordination.Command{Action: "pause"})
	h.drainWorkQueue(context.Background())
	snap, err := h.work.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Runs) != 1 || snap.Runs[0].FinishedAt != nil || snap.Runs[0].QueueReason != "pm_paused" {
		t.Fatalf("pause destroyed queued work: %+v", snap.Runs)
	}
}

func TestPMCrashAfterAdmissionQuarantinesInsteadOfReplaying(t *testing.T) {
	h, _, human, p := pmFixture(t)
	r := pmProposal(t, h, p)
	pmApply(t, h, human, coordination.Command{Action: "approve", TaskID: r.TaskID})
	pmApply(t, h, coordination.Principal{ID: "pm", SessionID: p.PMSessionID}, coordination.Command{Action: "dispatch", TaskID: r.TaskID})
	pm, _ := h.registry.Get(p.PMSessionID)
	started := make(chan struct{})
	pm.Submit(func() { close(started) })
	<-started
	defer pm.EndTurn()
	h.reconcilePM()
	state, _ := h.work.Collaboration(context.Background())
	task := state.Tasks[r.TaskID]
	worker, _ := h.registry.Get(task.SessionID)
	_, release, err := h.AdmitTurn(worker, task.RequestID, "approved work")
	if err != nil {
		t.Fatal(err)
	}
	release()
	if err = h.recoverPMCollaboration(); err != nil {
		t.Fatal(err)
	}
	state, _ = h.work.Collaboration(context.Background())
	if state.Tasks[task.ID].State != "uncertain" || state.WorkerAllowed(task.SessionID, task.RequestID) {
		t.Fatal("uncertain work would replay", state.Tasks[task.ID])
	}
}
