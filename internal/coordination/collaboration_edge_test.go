package coordination

import (
	"fmt"
	"testing"
)

func TestCollaborationV2ReopenPreservesSessionAndAcceptanceHistory(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	result := f.deliver(p, "passed")
	f.finish("succeeded")
	m := f.s.Collaboration.Tasks[f.taskID]
	f.human(CollaborationCommand{Command: Command{Action: "accept"}, ContractID: m.ContractID, EntityID: result.EntityID})
	before := f.s.Tasks[f.taskID]
	f.human(CollaborationCommand{Command: Command{Action: "reopen", Text: "Add the missing example"}})
	after := f.s.Tasks[f.taskID]
	meta := f.s.Collaboration.Tasks[f.taskID]
	if before.SessionID != after.SessionID || after.State != "approved" || meta.AcceptedSubmissionID != result.EntityID || meta.ContractID == m.ContractID {
		t.Fatal("reopen lost identity or acceptance history")
	}
}

func TestCollaborationV2IncompleteHandbackCannotBeApprovedAway(t *testing.T) {
	f := newCollabFixture(t)
	f.execute()
	f.human(CollaborationCommand{Command: Command{Action: "take_over"}})
	f.human(CollaborationCommand{Command: Command{Action: "return_to_pm", Text: "Some context is missing"}, Complete: false})
	task := f.s.Tasks[f.taskID]
	if task.State != "waiting" {
		t.Fatal("handback without submission was presented as review")
	}
	f.pm(CollaborationCommand{Command: Command{Action: "propose", TaskID: task.ID, Title: task.Title, Reason: task.Reason, Instruction: task.Instruction, Acceptance: task.Acceptance, Backend: task.Backend, Sandbox: task.Sandbox}})
	m := f.s.Collaboration.Tasks[task.ID]
	_, err := f.command(Principal{Human: true, ID: "human"}, CollaborationCommand{Command: Command{Action: "approve"}, ContractID: m.ContractID})
	if err == nil || err.Error() != "context_incomplete" {
		t.Fatal("incomplete handback bypassed", err)
	}
}

func TestCollaborationV2ConfirmedHandbackCanBeExplicitlyReworked(t *testing.T) {
	f := newCollabFixture(t)
	f.execute()
	f.human(CollaborationCommand{Command: Command{Action: "take_over"}})
	f.human(CollaborationCommand{Command: Command{Action: "return_to_pm", Text: "Keep the manual correction; add evidence"}, Complete: true})
	m := f.s.Collaboration.Tasks[f.taskID]
	_, err := f.command(Principal{Human: true, ID: "human"}, CollaborationCommand{Command: Command{Action: "continue"}, ContractID: m.ContractID})
	if err == nil {
		t.Fatal("stale handback resumed without an explicit revision")
	}
	f.human(CollaborationCommand{Command: Command{Action: "rework", Text: "Use the confirmed handback and deliver evidence"}})
	after := f.s.Collaboration.Tasks[f.taskID]
	if after.NeedsContext || after.ContractID == m.ContractID || !f.s.ManifestCurrent(after.ManifestID) {
		t.Fatal("confirmed human rework could not establish a new execution basis")
	}
}

func TestCollaborationV2BoundedRetryUsesSameContractAndStopsAtLimit(t *testing.T) {
	f := newCollabFixture(t)
	f.approve("bounded_plan", 2)
	p := f.admit()
	f.report(p, "received", "Ready")
	f.finish("succeeded")
	f.admit()
	f.finish("succeeded")
	contract := f.s.Collaboration.Tasks[f.taskID].ContractID
	for i := 0; i < 2; i++ {
		f.pm(CollaborationCommand{Command: Command{Action: "retry"}})
		if f.s.Collaboration.Tasks[f.taskID].ContractID != contract {
			t.Fatal("retry changed contract")
		}
		f.admit()
		f.finish("succeeded")
	}
	_, err := f.command(Principal{ID: "pm", SessionID: "pm_p"}, CollaborationCommand{Command: Command{Action: "retry"}})
	if err == nil || err.Error() != "budget_exhausted" {
		t.Fatal("retry exceeded grant", err)
	}
}

func TestCollaborationV2ExpiredGrantFencesCurrentRun(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	m := f.s.Collaboration.Tasks[f.taskID]
	g := f.s.Collaboration.Grants[m.GrantID]
	if !f.s.SweepCollaboration(g.ExpiresAt + 1) {
		t.Fatal("expiry ignored")
	}
	if f.s.Tasks[f.taskID].Epoch == p.Epoch || f.s.Tasks[f.taskID].State != "uncertain" {
		t.Fatal("expired run kept authority")
	}
	items := f.s.PendingActionables("p", "human")
	if len(items) == 0 || items[0].Kind != "grant_expired" {
		t.Fatal("expiry not visible")
	}
}

func TestCollaborationV2DeferralBecomesPendingAgain(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	r := f.report(p, "question", "Need a fact")
	a := f.s.Collaboration.Actionables[r.ActionableID]
	_, err := f.s.ApplyCollaboration(Principal{ID: "pm", SessionID: "pm_p"}, CollaborationCommand{Command: Command{Action: "dispose", ProjectID: "p", MutationID: "defer"}, ExpectedEntityRevision: a.Revision, Disposition: &DispositionInput{ActionableID: a.ID, Action: "defer", Text: "Wait for dependency"}}, f.at)
	if err != nil {
		t.Fatal(err)
	}
	a = f.s.Collaboration.Actionables[a.ID]
	if a.DueAt == 0 {
		t.Fatal("deferred forever")
	}
	f.s.SweepCollaboration(a.DueAt + 1)
	if f.s.Collaboration.Actionables[a.ID].State != "open" {
		t.Fatal("deferred action disappeared")
	}
}

func TestCollaborationV2CancelledRunCannotPublishLateSubmission(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	task := f.s.Tasks[f.taskID]
	f.human(CollaborationCommand{Command: Command{Action: "cancel"}})
	f.s.FinishCollaborationRun(task.SessionID, task.RequestID, "succeeded", "late", f.at)
	if f.s.Tasks[f.taskID].State != "cancelled" {
		t.Fatal("cancelled task resurrected")
	}
	m := f.s.Collaboration.Tasks[f.taskID]
	_, err := f.command(p, CollaborationCommand{Command: Command{Action: "report"}, Report: &ReportInput{Kind: "progress", Text: "late", ContractID: m.ContractID, ManifestID: m.ManifestID}})
	if err == nil {
		t.Fatal("cancelled producer retained scope")
	}
}

func TestCollaborationV2ToolSchemasDeclareReportAndCoverageEnums(t *testing.T) {
	schema := StructSchema(SubmissionInput{})
	coverage := schema["properties"].(map[string]any)["coverage"].(map[string]any)["items"].(map[string]any)
	claim := coverage["properties"].(map[string]any)["claim"].(map[string]any)
	if got := fmt.Sprint(claim["enum"]); got != "[passed failed unknown not_run]" {
		t.Fatal("model must not guess claim enum", got)
	}
}

func TestCollaborationV2ConsultantCannotRecursivelyDelegate(t *testing.T) {
	f := newCollabFixture(t)
	p := f.execute()
	r := f.must(p, CollaborationCommand{Command: Command{Action: "request_assistance", Text: "Check one fact"}, Mode: "consult"})
	_, err := f.s.ApplyCollaboration(Principal{Human: true, ID: "human"}, CollaborationCommand{Command: Command{Action: "approve_assistance", ProjectID: "p", MutationID: "approve-helper"}, EntityID: r.EntityID, ExpectedEntityRevision: 1}, f.at)
	if err != nil {
		t.Fatal(err)
	}
	a := f.s.Collaboration.Assistance[r.EntityID]
	f.taskID = a.TargetTaskID
	helper := f.admit()
	_, err = f.command(helper, CollaborationCommand{Command: Command{Action: "request_assistance", Text: "Delegate again"}, Mode: "consult"})
	if err == nil || err.Error() != "recursive_assistance_forbidden" {
		t.Fatal("unbounded agent tree", err)
	}
}
