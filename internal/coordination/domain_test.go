package coordination

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func fixture(t *testing.T) (*State, Principal, Principal) {
	t.Helper()
	s := NewState()
	human := Principal{Human: true, ID: "phone"}
	_, err := s.Apply(human, Command{Action: "create", ProjectID: "p1", Cwd: "/project", Name: "Project", MutationID: "create"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	return &s, human, Principal{ID: "pm", SessionID: s.Projects["p1"].PMSessionID}
}
func apply(t *testing.T, s *State, p Principal, c Command) Result {
	t.Helper()
	c.ProjectID = "p1"
	c.ExpectedRevision = s.Projects["p1"].Revision
	c.MutationID = fmt.Sprintf("m%d", s.Revision+1)
	r, err := s.Apply(p, c, 10)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func propose(t *testing.T, s *State, pm Principal) Result {
	return apply(t, s, pm, Command{Action: "propose", Title: "Inspect UI", Reason: "User needs a design", Instruction: "Inspect the UI and report", Acceptance: "Cite components", Backend: "codex", Sandbox: "read-only"})
}
func TestDiscussionAndApprovalBoundary(t *testing.T) {
	s, human, pm := fixture(t)
	r := propose(t, s, pm)
	if s.Projects["p1"].Mode != "discussion" || s.Tasks[r.TaskID].State != "proposed" {
		t.Fatal("proposal dispatched work")
	}
	for _, action := range []string{"approve", "accept", "take_over", "return_to_pm", "resume", "pause", "rework"} {
		_, err := s.Apply(pm, Command{Action: action, ProjectID: "p1", TaskID: r.TaskID, ExpectedRevision: s.Projects["p1"].Revision, MutationID: action}, 2)
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("%s: %v", action, err)
		}
	}
	_, err := s.Apply(pm, Command{Action: "dispatch", ProjectID: "p1", TaskID: r.TaskID, ExpectedRevision: s.Projects["p1"].Revision, MutationID: "early"}, 3)
	if err == nil {
		t.Fatal("unapproved task dispatched")
	}
	apply(t, s, human, Command{Action: "approve", TaskID: r.TaskID})
	apply(t, s, pm, Command{Action: "dispatch", TaskID: r.TaskID})
	task := s.Tasks[r.TaskID]
	if !s.WorkerAllowed(task.SessionID, task.RequestID) {
		t.Fatal("approved dispatch denied")
	}
}
func TestTakeoverInvalidatesAcceptedCommandsAndHandbackDoesNotRevive(t *testing.T) {
	s, human, pm := fixture(t)
	r := propose(t, s, pm)
	apply(t, s, human, Command{Action: "approve", TaskID: r.TaskID})
	apply(t, s, pm, Command{Action: "dispatch", TaskID: r.TaskID})
	task := s.Tasks[r.TaskID]
	task.Provisioned = true
	s.Tasks[task.ID] = task
	apply(t, s, human, Command{Action: "take_over", TaskID: task.ID})
	if s.WorkerAllowed(task.SessionID, task.RequestID) {
		t.Fatal("old PM instruction survived takeover")
	}
	if !s.WorkerAllowed(task.SessionID, "human-turn") {
		t.Fatal("human cannot continue")
	}
	apply(t, s, human, Command{Action: "return_to_pm", TaskID: task.ID, Text: "Use the revised layout"})
	if s.WorkerAllowed(task.SessionID, task.RequestID) || s.WorkerAllowed(task.SessionID, "old-human-turn") {
		t.Fatal("stale queued instruction revived")
	}
	if s.Tasks[task.ID].Decision != "Use the revised layout" {
		t.Fatal("lost human decision")
	}
}
func TestIdentityRevisionAndIdempotency(t *testing.T) {
	s, _, pm := fixture(t)
	r := propose(t, s, pm)
	if err := s.Authorize(Principal{ID: "other", SessionID: "other"}, "p1"); !errors.Is(err, ErrForbidden) {
		t.Fatal(err)
	}
	if err := s.Authorize(Principal{ID: "worker", SessionID: s.Tasks[r.TaskID].SessionID}, "p1"); !errors.Is(err, ErrForbidden) {
		t.Fatal(err)
	}
	before := s.Revision
	_, err := s.Apply(pm, Command{Action: "propose", ProjectID: "p1", MutationID: "stale", ExpectedRevision: 0}, 1)
	if !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if s.Revision != before {
		t.Fatal("rejected action mutated revision")
	}
	c := Command{Action: "propose", ProjectID: "p1", ExpectedRevision: s.Projects["p1"].Revision, MutationID: "retry", Title: "Second", Reason: "why", Instruction: "read", Acceptance: "report", Backend: "claude"}
	first, err := s.Apply(pm, c, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Apply(pm, c, 2)
	if err != nil || first != second || len(s.Tasks) != 2 {
		t.Fatalf("retry duplicated: %+v %+v %v", first, second, err)
	}
	c.Instruction = "Different work with a reused key"
	if _, err = s.Apply(pm, c, 3); err == nil {
		t.Fatal("changed intent reused original receipt")
	}
}
func TestRolesSurviveSerializationAndApplyToEveryProject(t *testing.T) {
	s, human, _ := fixture(t)
	_, err := s.Apply(human, Command{Action: "create", ProjectID: "p2", Cwd: "/another", Name: "Another", MutationID: "c2"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(s)
	var restored State
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	for _, p := range restored.Projects {
		if p.ProfileID != ProfileID || p.ProfileVersion != 1 || p.Mode != "discussion" {
			t.Fatal(p)
		}
	}
	if err = restored.Authorize(Principal{ID: "pm", SessionID: "pm_p1"}, "p2"); !errors.Is(err, ErrForbidden) {
		t.Fatal("cross-project access", err)
	}
}
func TestNoPrivilegeEscalationAndTaskLimit(t *testing.T) {
	s, _, pm := fixture(t)
	project := s.Projects["p1"]
	project.MaxTasks = 1
	s.Projects["p1"] = project
	c := Command{Action: "propose", ProjectID: "p1", ExpectedRevision: project.Revision, MutationID: "bad", Title: "x", Reason: "x", Instruction: "x", Acceptance: "x", Backend: "codex", Sandbox: "danger-full-access"}
	if _, err := s.Apply(pm, c, 2); err == nil {
		t.Fatal("full access accepted")
	}
	propose(t, s, pm)
	c.ExpectedRevision = s.Projects["p1"].Revision
	c.Sandbox = "read-only"
	c.MutationID = "over-limit"
	if _, err := s.Apply(pm, c, 2); err == nil {
		t.Fatal("unbounded tasks")
	}
}

func TestRevisionUsesSameWorkerAndNeedsRenewedApproval(t *testing.T) {
	s, human, pm := fixture(t)
	r := propose(t, s, pm)
	apply(t, s, human, Command{Action: "approve", TaskID: r.TaskID})
	apply(t, s, pm, Command{Action: "dispatch", TaskID: r.TaskID})
	task := s.Tasks[r.TaskID]
	task.Provisioned = true
	task.State = "review"
	s.Tasks[task.ID] = task
	r = apply(t, s, pm, Command{Action: "propose", TaskID: task.ID, Title: task.Title, Reason: "Need better evidence", Instruction: "Check the updated version", Acceptance: "Cite updated evidence", Backend: "codex", Sandbox: "read-only"})
	updated := s.Tasks[r.TaskID]
	if updated.SessionID != task.SessionID || updated.State != "proposed" || updated.RequestID == task.RequestID || len(s.Tasks) != 1 {
		t.Fatal(updated)
	}
	if s.WorkerAllowed(updated.SessionID, updated.RequestID) {
		t.Fatal("revision bypassed renewed approval")
	}
}
