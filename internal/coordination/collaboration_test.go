package coordination

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

type collabFixture struct {
	s      State
	t      *testing.T
	taskID string
	n      int
	at     int64
}

func newCollabFixture(t *testing.T) *collabFixture {
	f := &collabFixture{s: NewState(), t: t, at: 1000}
	f.human(CollaborationCommand{Command: Command{Action: "create", ProjectID: "p", Cwd: "/fixture", Name: "Test"}})
	r := f.pm(CollaborationCommand{Command: Command{Action: "propose", ProjectID: "p", Title: "Deliver", Reason: "User request", Instruction: "Produce a report", Acceptance: "Cite evidence", Backend: "codex", Sandbox: "workspace-write"}, NonGoals: []string{"Do not deploy"}, Criteria: []Criterion{{ID: "AC1", Text: "Include evidence", Mandatory: true}}})
	f.taskID = r.TaskID
	return f
}
func (f *collabFixture) command(p Principal, c CollaborationCommand) (CollaborationResult, error) {
	f.n++
	f.at++
	if c.ProjectID == "" {
		c.ProjectID = "p"
	}
	if c.MutationID == "" {
		c.MutationID = fmt.Sprintf("m%d", f.n)
	}
	if c.TaskID == "" && c.Action != "create" && c.Action != "propose" {
		c.TaskID = f.taskID
	}
	if c.TaskID != "" {
		c.ExpectedEntityRevision = f.s.Collaboration.Tasks[c.TaskID].Revision
	}
	return f.s.ApplyCollaboration(p, c, f.at)
}
func (f *collabFixture) must(p Principal, c CollaborationCommand) CollaborationResult {
	f.t.Helper()
	r, err := f.command(p, c)
	if err != nil {
		f.t.Fatalf("%s: %v", c.Action, err)
	}
	return r
}
func (f *collabFixture) human(c CollaborationCommand) CollaborationResult {
	return f.must(Principal{Human: true, ID: "human"}, c)
}
func (f *collabFixture) pm(c CollaborationCommand) CollaborationResult {
	return f.must(Principal{ID: "pm", SessionID: "pm_p"}, c)
}
func (f *collabFixture) approve(mode string, max int) {
	m := f.s.Collaboration.Tasks[f.taskID]
	f.human(CollaborationCommand{Command: Command{Action: "approve"}, ContractID: m.ContractID, Mode: mode, MaxRevisions: max})
	f.pm(CollaborationCommand{Command: Command{Action: "dispatch"}})
}
func (f *collabFixture) admit() Principal {
	f.t.Helper()
	task := f.s.Tasks[f.taskID]
	if !f.s.WorkerAllowed(task.SessionID, task.RequestID) {
		f.t.Fatal("worker not eligible")
	}
	if err := f.s.AdmitCollaborationRun(task.SessionID, task.RequestID, "task", f.at); err != nil {
		f.t.Fatal(err)
	}
	task.State = "running"
	task.Provisioned = true
	f.s.Tasks[task.ID] = task
	return Principal{ID: "worker", SessionID: task.SessionID, RunID: task.RunID, Epoch: task.Epoch}
}
func (f *collabFixture) report(p Principal, kind, text string) CollaborationResult {
	m := f.s.Collaboration.Tasks[f.taskID]
	r := ReportInput{Kind: kind, Text: text, ContractID: m.ContractID, ManifestID: m.ManifestID}
	if kind == "received" {
		r.Readiness = "confirmed"
	}
	return f.must(p, CollaborationCommand{Command: Command{Action: "report"}, Report: &r})
}
func (f *collabFixture) finish(status string) {
	task := f.s.Tasks[f.taskID]
	m := f.s.Collaboration.Tasks[f.taskID]
	f.s.FinishCollaborationRun(task.SessionID, m.ActiveRequestID, status, "Finished model turn", f.at)
}
func (f *collabFixture) execute() Principal {
	f.approve("strict_task", 0)
	p := f.admit()
	f.report(p, "received", "Understand outcome and limits")
	f.finish("succeeded")
	return f.admit()
}
func (f *collabFixture) deliver(p Principal, claim string) CollaborationResult {
	f.s.Collaboration.Artifacts["art"] = Artifact{ID: "art", ProjectID: "p", TaskID: f.taskID, Kind: "text", Digest: "digest", Content: "report"}
	f.s.Collaboration.Evidence["ev"] = Evidence{ID: "ev", ProjectID: "p", TaskID: f.taskID, ArtifactID: "art", Text: "Evidence", Trust: "agent_claim", Observer: p.SessionID}
	m := f.s.Collaboration.Tasks[f.taskID]
	return f.must(p, CollaborationCommand{Command: Command{Action: "submit"}, Submission: &SubmissionInput{ContractID: m.ContractID, ManifestID: m.ManifestID, Summary: "Delivered", ArtifactIDs: []string{"art"}, Coverage: []Coverage{{CriterionID: "AC1", Claim: claim, EvidenceIDs: []string{"ev"}}}, Limitations: []string{}}})
}

func TestCollaborationV2FullDeliveryAndHumanAcceptance(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	sub := f.deliver(p, "passed")
	if f.s.Collaboration.Submissions[sub.EntityID].Status != "sealing_pending" {
		t.Fatal("unsealed output exposed")
	}
	m := f.s.Collaboration.Tasks[f.taskID]
	_, err := f.command(Principal{Human: true, ID: "human"}, CollaborationCommand{Command: Command{Action: "accept"}, ContractID: m.ContractID, EntityID: sub.EntityID})
	if err == nil {
		t.Fatal("accepted during execution")
	}
	f.finish("succeeded")
	m = f.s.Collaboration.Tasks[f.taskID]
	f.human(CollaborationCommand{Command: Command{Action: "accept"}, ContractID: m.ContractID, EntityID: sub.EntityID})
	if f.s.Tasks[f.taskID].State != "done" || f.s.Collaboration.Tasks[f.taskID].AcceptedSubmissionID != sub.EntityID {
		t.Fatal("acceptance binding missing")
	}
}

func TestCollaborationV2TransportReceiptIsNotReadiness(t *testing.T) {
	f := newCollabFixture(t)
	f.approve("strict_task", 0)
	f.admit()
	f.finish("succeeded")
	m := f.s.Collaboration.Tasks[f.taskID]
	if m.Phase != "preflight" || m.Readiness == "confirmed" || f.s.Tasks[f.taskID].State != "waiting" {
		t.Fatal("implicitly confirmed worker")
	}
	if len(f.s.PendingActionables("p", "pm")) != 1 {
		t.Fatal("missing preflight attention")
	}
}

func TestCollaborationV2ReportsSurviveRecentEventWindow(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	m := f.s.Collaboration.Tasks[f.taskID]
	r := f.must(p, CollaborationCommand{Command: Command{Action: "report"}, Report: &ReportInput{Kind: "question", Text: "Need a decision", Blocking: true, ContractID: m.ContractID, ManifestID: m.ManifestID}})
	for i := 0; i < 35; i++ {
		f.report(p, "progress", fmt.Sprintf("Milestone %d", i))
	}
	if len(f.s.PendingActionables("p", "pm")) != 1 || f.s.Collaboration.Actionables[r.ActionableID].State != "open" {
		t.Fatal("question fell out of history window")
	}
	// Reading an actionable does not mutate it.
	before, _ := json.Marshal(f.s)
	f.s.PendingActionables("p", "")
	after, _ := json.Marshal(f.s)
	if string(before) != string(after) {
		t.Fatal("read mutated state")
	}
}

func TestCollaborationV2AnswerNeedsWorkerAcknowledgement(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	m := f.s.Collaboration.Tasks[f.taskID]
	r := f.must(p, CollaborationCommand{Command: Command{Action: "report"}, Report: &ReportInput{Kind: "question", Text: "Which option?", Blocking: true, DecisionOwner: "human", ContractID: m.ContractID, ManifestID: m.ManifestID}})
	a := f.s.Collaboration.Actionables[r.ActionableID]
	answer, err := f.s.ApplyCollaboration(Principal{Human: true, ID: "human"}, CollaborationCommand{Command: Command{Action: "human_answer", ProjectID: "p", MutationID: "answer"}, ExpectedEntityRevision: a.Revision, Disposition: &DispositionInput{ActionableID: a.ID, Action: "answer_from_context", Text: "Use A"}}, f.at)
	if err != nil {
		t.Fatal(err)
	}
	a = f.s.Collaboration.Actionables[a.ID]
	if a.State != "waiting" || a.AnswerID != answer.EntityID {
		t.Fatal("answer treated as resolution")
	}
	f.must(p, CollaborationCommand{Command: Command{Action: "report"}, Report: &ReportInput{Kind: "answer_ack", Text: "Adopted A", QuestionID: a.ID, AnswerID: a.AnswerID, Adopted: true, ContractID: m.ContractID, ManifestID: m.ManifestID}})
	if f.s.Collaboration.Actionables[a.ID].State != "resolved" {
		t.Fatal("ack failed")
	}
}

func TestCollaborationV2IdempotencyAndRejectedMutationRollback(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	m := f.s.Collaboration.Tasks[f.taskID]
	c := CollaborationCommand{Command: Command{Action: "report", ProjectID: "p", TaskID: f.taskID, MutationID: "stable"}, Report: &ReportInput{Kind: "progress", Text: "Once", ContractID: m.ContractID, ManifestID: m.ManifestID}}
	a, err := f.s.ApplyCollaboration(p, c, f.at)
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.s.ApplyCollaboration(p, c, f.at+1)
	if err != nil || a != b {
		t.Fatal("duplicate mutation", err)
	}
	before, _ := json.Marshal(f.s)
	c.Report.Text = "Different"
	_, err = f.s.ApplyCollaboration(p, c, f.at+2)
	after, _ := json.Marshal(f.s)
	if err == nil || string(before) != string(after) {
		t.Fatal("failed mutation changed state")
	}
}

func TestCollaborationV2HumanControlFencesLateRuntimeAndReports(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	task := f.s.Tasks[f.taskID]
	oldReq := task.RequestID
	f.human(CollaborationCommand{Command: Command{Action: "take_over"}})
	m := f.s.Collaboration.Tasks[f.taskID]
	_, err := f.command(p, CollaborationCommand{Command: Command{Action: "report"}, Report: &ReportInput{Kind: "progress", Text: "late", ContractID: m.ContractID, ManifestID: m.ManifestID}})
	if err == nil {
		t.Fatal("old worker retained authority")
	}
	before := f.s.Tasks[f.taskID]
	f.s.FinishCollaborationRun(task.SessionID, oldReq, "succeeded", "Old output", f.at)
	after := f.s.Tasks[f.taskID]
	if before.State != after.State || after.Result == "Old output" {
		t.Fatal("late result overwrote human state")
	}
}

func TestCollaborationV2MissingSubmissionIsNotReview(t *testing.T) {
	f := newCollabFixture(t)
	f.execute()
	f.finish("succeeded")
	if f.s.Tasks[f.taskID].State != "waiting" {
		t.Fatal("turn success presented as delivery")
	}
	items := f.s.PendingActionables("p", "pm")
	if len(items) != 1 || items[0].Kind != "missing_submission" {
		t.Fatal(items)
	}
}

func TestCollaborationV2RequiredContextCannotBeTruncated(t *testing.T) {
	f := newCollabFixture(t)
	f.approve("strict_task", 0)
	m := f.s.Collaboration.Tasks[f.taskID]
	manifest := f.s.Collaboration.Manifests[m.ManifestID]
	if !strings.Contains(manifest.Required, "DO NOT: Do not deploy") || !strings.Contains(manifest.Required, "AC1") {
		t.Fatal("required context missing")
	}
	_, err := f.s.BuildManifest(f.taskID, f.at, 20)
	if err == nil {
		t.Fatal("silently truncated required context")
	}
}

func TestCollaborationV2DecisionInvalidatesOnlyScopedTask(t *testing.T) {
	f := newCollabFixture(t)
	f.approve("strict_task", 0)
	old := f.s.Collaboration.Tasks[f.taskID].ManifestID
	f.human(CollaborationCommand{Command: Command{Action: "decision", Text: "Do not change navigation"}})
	if f.s.ManifestCurrent(old) || !f.s.Collaboration.Tasks[f.taskID].NeedsContext {
		t.Fatal("old decision remained valid")
	}
}

func TestCollaborationV2CrossScopeAndFakeHumanRejected(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	m := f.s.Collaboration.Tasks[f.taskID]
	_, err := f.command(p, CollaborationCommand{Command: Command{Action: "approve"}, ContractID: m.ContractID})
	if err == nil {
		t.Fatal("worker approved")
	}
	p.RunID = "another-run"
	_, err = f.command(p, CollaborationCommand{Command: Command{Action: "report"}, Report: &ReportInput{Kind: "progress", Text: "fake", ContractID: m.ContractID, ManifestID: m.ManifestID}})
	if err == nil {
		t.Fatal("wrong run reported")
	}
	_, err = f.s.Apply(Principal{Human: true, ID: "human"}, Command{Action: "accept", ProjectID: "p", TaskID: f.taskID, MutationID: "old"}, f.at)
	if err == nil || err.Error() != "protocol_upgrade_required" {
		t.Fatal("legacy mutation accepted", err)
	}
}

func TestCollaborationV2AssistanceBudgetAndNoRecursion(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	for i := 0; i < 3; i++ {
		r := f.must(p, CollaborationCommand{Command: Command{Action: "request_assistance", Text: fmt.Sprintf("Question %d", i)}, Mode: "consult"})
		a := f.s.Collaboration.Assistance[r.EntityID]
		_, err := f.s.ApplyCollaboration(Principal{Human: true, ID: "human"}, CollaborationCommand{Command: Command{Action: "approve_assistance", ProjectID: "p", MutationID: fmt.Sprintf("approve-help-%d", i)}, EntityID: a.ID, ExpectedEntityRevision: a.Revision}, f.at)
		if i < 2 && err != nil {
			t.Fatal(err)
		}
		if i == 2 && (err == nil || err.Error() != "assistance_limit") {
			t.Fatal("assistance reservation limit", err)
		}
	}
}

func TestCollaborationV2RuleRequiresHumanEvaluation(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	f.report(p, "checkpoint", "What failed")
	var rid string
	for id := range f.s.Collaboration.Reports {
		rid = id
	}
	r := f.pm(CollaborationCommand{Command: Command{Action: "propose_rule", Text: "Always attach evidence"}, Sources: []SourceRef{{Kind: "report", ID: rid}}, Mode: "evidence"})
	_, err := f.s.ApplyCollaboration(Principal{ID: "pm", SessionID: "pm_p"}, CollaborationCommand{Command: Command{Action: "approve_rule", ProjectID: "p", MutationID: "fake"}, EntityID: r.EntityID, ExpectedEntityRevision: 1, Evaluation: "Looks good"}, f.at)
	if err == nil {
		t.Fatal("AI published a rule")
	}
	_, err = f.s.ApplyCollaboration(Principal{Human: true, ID: "human"}, CollaborationCommand{Command: Command{Action: "approve_rule", ProjectID: "p", MutationID: "no-eval"}, EntityID: r.EntityID, ExpectedEntityRevision: 1}, f.at)
	if err == nil {
		t.Fatal("rule approved without evaluation")
	}
}
