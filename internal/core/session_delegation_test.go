package core

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func TestValidatedDelegationArtifactsKeepsOnlyExistingWorkspaceFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.md")
	if err := os.WriteFile(path, []byte("result"), 0600); err != nil {
		t.Fatal(err)
	}
	got := validatedDelegationArtifacts("[report](report.md) [missing](missing.md) [outside](/etc/hosts) [web](https://example.com)", dir)
	canonical, _ := filepath.EvalSymlinks(path)
	if len(got) != 1 || got[0] != canonical {
		t.Fatalf("artifacts=%v", got)
	}
}

func TestDelegationDoesNotMistakeBridgeRestartMarkerForChildFailure(t *testing.T) {
	dir := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", DataDir: dir},
		governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	ctx := context.Background()
	h.registry.Create("parent", "Parent", dir, backend.Codex, "", "", "")
	h.registry.Create("child", "Child", dir, backend.Codex, "", "", "")
	r := delegation.Record{ID: "d1", ParentSessionID: "parent", ParentRequestID: "dgreturn_d1", ChildSessionID: "child", ChildRequestID: "dgtask_d1",
		ChildName: "Child", Cwd: dir, Instruction: "Check"}
	if err := h.delegations.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := h.delegations.MarkRunning(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	h.runtimes.Update("child", "running", r.ChildRequestID, 0, "", "")
	h.runtimes.RecoverAllStale()
	if err := h.reconcileDelegations(ctx); err != nil {
		t.Fatal(err)
	}
	got, found, err := h.delegations.ByChildRequest(ctx, "child", r.ChildRequestID)
	if err != nil || !found || got.State != "running" || got.TerminalStatus != "" {
		t.Fatalf("restart marker falsely completed child: %+v found=%v err=%v", got, found, err)
	}
}

func TestDelegationRefusesWorkspaceEscapeAndSandboxEscalation(t *testing.T) {
	dir := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", DataDir: dir},
		governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	parent := h.registry.Create("parent", "Parent", dir, backend.Codex, "", "read-only", "")
	started := make(chan struct{})
	parent.SubmitNamed("parent-turn", func() { close(started) })
	<-started
	defer parent.EndTurn()
	for _, spec := range []backend.DelegationSpec{
		{Name: "escape", Cwd: filepath.Dir(dir), Instruction: "Check"},
		{Name: "escalate", Cwd: dir, Instruction: "Check", Sandbox: "danger-full-access"},
		{Name: "empty", Cwd: dir, Instruction: ""},
	} {
		if receipt, err := h.DelegateSession(parent, "parent-turn", "call-invalid", spec); err == nil {
			t.Fatalf("unauthorized delegation accepted: %+v", receipt)
		}
	}
	if pending, err := h.delegations.Pending(context.Background()); err != nil || len(pending) != 0 {
		t.Fatalf("invalid delegation persisted: %+v err=%v", pending, err)
	}
}

type delegationFakeExec struct{ *fakeExec }

func (*delegationFakeExec) FinalAnswerForSession(_ *session.Session, _ string) (string, bool, error) {
	return "Independent final result. [Report](/workspace/report.md)", true, nil
}

func TestDelegationReturnsExactFinalOnceAfterParentIsIdle(t *testing.T) {
	dir := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", InstanceName: "test", DataDir: dir},
		governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	fe := &delegationFakeExec{fakeExec: &fakeExec{sink: h}}
	h.SetExecutor(fe)
	parent := h.registry.Create("parent", "Parent", dir, backend.Codex, "", "workspace-write", "")
	started := make(chan struct{})
	if !parent.SubmitNamed("parent-turn", func() { close(started) }) {
		t.Fatal("parent did not start")
	}
	<-started
	childTurn := make(chan string, 1)
	parentTurn := make(chan string, 1)
	fe.onSend = func(s *session.Session, requestID, content string) {
		switch s.ID {
		case "parent":
			parentTurn <- content
		case "":
			t.Error("missing child id")
		default:
			childTurn <- requestID
		}
		h.Emit(protocol.NewDone(s.ID, requestID))
	}
	receipt, err := h.DelegateSession(parent, "parent-turn", "call-check", backend.DelegationSpec{Name: "Independent check", Cwd: dir, Instruction: "Check one fact"})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := h.DelegateSession(parent, "parent-turn", "call-check", backend.DelegationSpec{Name: "Independent check", Cwd: dir, Instruction: "Check one fact"})
	if err != nil || retry != receipt {
		t.Fatalf("same tool call made a duplicate child: %+v err=%v", retry, err)
	}
	if _, err := h.DelegateSession(parent, "parent-turn", "call-check", backend.DelegationSpec{Name: "Independent check", Cwd: dir, Instruction: "Different work"}); err == nil {
		t.Fatal("changed intent reused the same tool call")
	}
	if err := h.reconcileDelegations(context.Background()); err != nil {
		t.Fatal(err)
	}
	annotated := h.annotateDelegationHistory("parent", []map[string]any{{"role": "user", "request_id": "dgreturn_" + receipt.ID, "source": "codex"}})
	if annotated[0]["source"] != "codex" || annotated[0]["origin"] != "bridge_delegation_result" || annotated[0]["source_session_id"] != receipt.ChildSessionID {
		t.Fatalf("parent provenance not verified: %+v", annotated[0])
	}
	select {
	case got := <-childTurn:
		if got != receipt.ChildRequestID {
			t.Fatalf("child request %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("child did not start")
	}
	if err := h.reconcileDelegations(context.Background()); err != nil {
		t.Fatal(err)
	}
	page, err := h.ReadDelegationResult(parent, receipt.ID, 0, 11)
	if err != nil || page.Text != "Independent" || !page.HasMore || page.NextOffset != 11 {
		t.Fatalf("bounded result page: %+v err=%v", page, err)
	}
	other := h.registry.Create("other-parent", "Other", dir, backend.Codex, "", "", "")
	if _, err := h.ReadDelegationResult(other, receipt.ID, 0, 100); err == nil {
		t.Fatal("another session read delegated result")
	}
	select {
	case <-parentTurn:
		t.Fatal("parent was interrupted before becoming idle")
	default:
	}
	parent.EndTurn()
	deadline := time.Now().Add(2 * time.Second)
	for parent.IsStreaming() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := h.reconcileDelegations(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case content := <-parentTurn:
		if !strings.Contains(content, "Independent final result") || !strings.Contains(content, receipt.ChildSessionID) {
			t.Fatalf("wrong return content: %q", content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parent was not resumed")
	}
	if err := h.reconcileDelegations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.reconcileDelegations(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-parentTurn:
		t.Fatal("duplicate parent wake")
	default:
	}
}

func TestDelegationResultSurvivesBridgeRestartBeforeParentDelivery(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "sessions.json")
	firstRegistry := session.NewRegistry()
	firstRegistry.AttachStore(session.NewStore(storePath))
	first := NewHub(firstRegistry, Config{InstanceID: "i1", InstanceName: "test", DataDir: dir},
		governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	firstExec := &delegationFakeExec{fakeExec: &fakeExec{sink: first}}
	first.SetExecutor(firstExec)
	parent := first.registry.Create("parent", "Parent", dir, backend.Codex, "", "workspace-write", "")
	if err := first.registry.PersistDurably(); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	parent.SubmitNamed("parent-turn", func() { close(started) })
	<-started
	childDone := make(chan struct{})
	firstExec.onSend = func(s *session.Session, requestID, _ string) {
		first.Emit(protocol.NewDone(s.ID, requestID))
		if s.ID != "parent" {
			close(childDone)
		}
	}
	receipt, err := first.DelegateSession(parent, "parent-turn", "call-check", backend.DelegationSpec{Name: "Check", Cwd: dir, Instruction: "Verify"})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.reconcileDelegations(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-childDone:
	case <-time.After(2 * time.Second):
		t.Fatal("child did not complete")
	}
	if err := first.reconcileDelegations(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, found, err := first.delegations.ByChildRequest(context.Background(), receipt.ChildSessionID, receipt.ChildRequestID)
	if err != nil || !found || stored.State != "terminal" || stored.DeliveryState != "pending" {
		t.Fatalf("result not sealed before restart: %+v found=%v err=%v", stored, found, err)
	}
	parent.EndTurn()
	child, _ := first.registry.Get(receipt.ChildSessionID)
	deadline := time.Now().Add(2 * time.Second)
	for (child.ActiveQueuedID() != "" || parent.ActiveQueuedID() != "") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if child.ActiveQueuedID() != "" || parent.ActiveQueuedID() != "" {
		t.Fatal("first Hub still owns a turn")
	}
	_ = first.messageQueue.Close()
	_ = first.delegations.Close()

	secondRegistry := session.NewRegistry()
	secondRegistry.AttachStore(session.NewStore(storePath))
	second := NewHub(secondRegistry, Config{InstanceID: "i1", InstanceName: "test", DataDir: dir},
		governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	defer second.messageQueue.Close()
	defer second.delegations.Close()
	secondExec := &delegationFakeExec{fakeExec: &fakeExec{sink: second}}
	second.SetExecutor(secondExec)
	returned := make(chan string, 1)
	secondExec.onSend = func(s *session.Session, requestID, content string) {
		if s.ID == "parent" {
			returned <- content
		}
		second.Emit(protocol.NewDone(s.ID, requestID))
	}
	if err := second.reconcileDelegations(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case content := <-returned:
		if !strings.Contains(content, "Independent final result") {
			t.Fatalf("lost result after restart: %q", content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parent not resumed after restart")
	}
	deadline = time.Now().Add(2 * time.Second)
	secondParent, _ := second.registry.Get("parent")
	for secondParent.ActiveQueuedID() != "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}
