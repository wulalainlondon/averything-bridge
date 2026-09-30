package coordination

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

func collabError(code string) error { return errors.New(code) }

func digestJSON(value any) string {
	b, _ := json.Marshal(value)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func collaborationID(prefix string, p Principal, mutation string) string {
	return prefix + "_" + digestJSON([]string{p.ID, p.SessionID, p.RunID, mutation})[:24]
}

// ApplyCollaboration is transactional even when used without a database. An
// invalid command cannot leave half a grant, answer, contract or receipt behind.
func (s *State) ApplyCollaboration(p Principal, c CollaborationCommand, at int64) (CollaborationResult, error) {
	if p.ID == "" || strings.TrimSpace(c.MutationID) == "" || len(c.MutationID) > 100 {
		return CollaborationResult{}, collabError("identity_and_mutation_required")
	}
	raw, err := json.Marshal(c)
	if err != nil || len(raw) > 128*1024 || len(c.Text) > 32000 || len(c.Cwd) > 4096 {
		return CollaborationResult{}, collabError("collaboration_request_too_large")
	}
	b, err := json.Marshal(s)
	if err != nil {
		return CollaborationResult{}, err
	}
	var next State
	if err = json.Unmarshal(b, &next); err != nil {
		return CollaborationResult{}, err
	}
	next.Normalize()
	next.NormalizeCollaboration()
	key := p.ID + ":" + p.SessionID + ":" + p.RunID + ":" + c.MutationID
	intent := c
	intent.ExpectedRevision = 0
	intent.ExpectedEntityRevision = 0
	intent.ExpectedTaskRevision = 0
	intent.MutationID = ""
	hash := digestJSON(intent)
	type receipt struct {
		CollaborationResult
		IntentHash string `json:"intent_hash"`
	}
	if previous, ok := next.Collaboration.Receipts[key]; ok {
		var r receipt
		if err := json.Unmarshal(previous, &r); err != nil {
			return CollaborationResult{}, err
		}
		if r.IntentHash != hash {
			return CollaborationResult{}, collabError("mutation_intent_conflict")
		}
		return r.CollaborationResult, nil
	}
	if len(next.Collaboration.Receipts) >= 50000 {
		return CollaborationResult{}, collabError("receipt_limit_requires_maintenance")
	}
	out, err := next.applyCollaboration(p, c, at)
	if err != nil {
		return CollaborationResult{}, err
	}
	next.InvalidateDependentManifests(at)
	out.Revision = next.Revision
	encoded, err := json.Marshal(receipt{out, hash})
	if err != nil {
		return CollaborationResult{}, err
	}
	next.Collaboration.Receipts[key] = encoded
	// The v2 receipt owns the whole operation, including its internal legacy
	// transitions. Do not persist a second set of v2 receipts in the v1 blob.
	next.Receipts = s.Receipts
	*s = next
	return out, nil
}

func (s *State) applyCollaboration(p Principal, c CollaborationCommand, at int64) (CollaborationResult, error) {
	out := CollaborationResult{Result: Result{ProjectID: c.ProjectID, TaskID: c.TaskID}}
	if c.Action == "upgrade" {
		return s.upgradeCollaboration(p, c, at)
	}
	if c.Action == "create" {
		if !p.Human {
			return out, ErrForbidden
		}
		result, err := s.applyLegacy(p, c.Command, at)
		if err != nil {
			return out, err
		}
		project := s.Projects[result.ProjectID]
		project.EngineVersion = 2
		s.Projects[project.ID] = project
		out.Result = result
		return out, nil
	}
	project, ok := s.Projects[c.ProjectID]
	if !ok || project.EngineVersion != 2 {
		return out, collabError("collaboration_project_not_found")
	}
	pm := !p.Human && p.SessionID == project.PMSessionID
	worker := !p.Human && !pm
	if worker {
		task, ok := s.Tasks[c.TaskID]
		meta := s.Collaboration.Tasks[c.TaskID]
		if !ok || task.ProjectID != project.ID || task.SessionID != p.SessionID || p.RunID == "" || p.RunID != meta.ActiveRunID || p.Epoch != task.Epoch || meta.ActiveEpoch != p.Epoch {
			return out, collabError("scope_forbidden")
		}
		if task.State == "done" || task.State == "cancelled" {
			return out, collabError("task_not_active")
		}
	} else if err := s.Authorize(p, project.ID); err != nil {
		return out, err
	}
	if c.TaskID != "" && s.Tasks[c.TaskID].ProjectID != project.ID {
		return out, collabError("scope_forbidden")
	}
	c.ExpectedRevision = project.Revision // unrelated progress must not starve an entity mutation
	switch c.Action {
	case "propose", "approve", "dispatch", "pause", "resume", "take_over", "return_to_pm", "rework", "reopen", "accept", "continue":
		if worker {
			return out, ErrForbidden
		}
		return s.applyTaskCommand(p, c, at)
	case "report":
		if !worker || c.Report == nil {
			return out, ErrForbidden
		}
		return s.applyReport(p, c, at)
	case "dispose", "human_answer":
		if worker || c.Disposition == nil || (c.Action == "human_answer" && !p.Human) {
			return out, ErrForbidden
		}
		return s.applyDisposition(p, c, at)
	case "human_answer_continue":
		if !p.Human || c.Disposition == nil {
			return out, ErrForbidden
		}
		a := s.Collaboration.Actionables[c.Disposition.ActionableID]
		m := s.Collaboration.Tasks[a.TaskID]
		if a.ProjectID != project.ID || m.Revision != c.ExpectedTaskRevision || m.ContractID != c.ContractID {
			return out, collabError("entity_revision_conflict")
		}
		answered, err := s.applyDisposition(p, c, at)
		if err != nil {
			return out, err
		}
		continued := c
		continued.Action = "continue"
		continued.TaskID = a.TaskID
		continued.ExpectedEntityRevision = m.Revision
		continued.MutationID = "continue-" + collaborationID("answer", p, c.MutationID)
		_, err = s.applyTaskCommand(p, continued, at)
		return answered, err
	case "submit":
		if !worker || c.Submission == nil {
			return out, ErrForbidden
		}
		return s.applySubmission(p, c, at)
	case "decision", "prepare_handback", "cancel", "request_assistance", "approve_assistance", "adopt_assistance", "propose_rule", "approve_rule", "revoke_rule", "retry":
		return s.applyCooperation(p, c, at)
	default:
		return out, collabError("collaboration_unknown_action")
	}
}

func (s *State) applyTaskCommand(p Principal, c CollaborationCommand, at int64) (CollaborationResult, error) {
	c.ExpectedRevision = s.Projects[c.ProjectID].Revision
	out := CollaborationResult{Result: Result{ProjectID: c.ProjectID, TaskID: c.TaskID}}
	v := s.Collaboration
	task, hasTask := s.Tasks[c.TaskID]
	meta := v.Tasks[c.TaskID]
	if c.Action == "reopen" {
		if !p.Human || !hasTask || task.State != "done" || strings.TrimSpace(c.Text) == "" {
			return out, collabError("reopen_requires_human_feedback")
		}
		c.Action = "rework"
		task.State = "review"
		s.Tasks[task.ID] = task
	}
	if c.Action != "propose" && c.Action != "pause" && c.Action != "resume" && !hasTask {
		return out, collabError("task_not_found")
	}
	if hasTask && c.ExpectedEntityRevision != meta.Revision {
		return out, collabError("entity_revision_conflict")
	}
	if c.Action == "approve" || c.Action == "accept" || c.Action == "continue" {
		if c.ContractID == "" || c.ContractID != meta.ContractID {
			return out, collabError("contract_stale")
		}
	}
	if c.Action == "accept" {
		if !p.Human {
			return out, ErrForbidden
		}
		sub, exists := v.Submissions[c.EntityID]
		if !exists || sub.ID != meta.SubmissionID || sub.TaskID != task.ID || sub.Status != "submitted" || sub.Input.ContractID != meta.ContractID || !s.ManifestCurrent(sub.Input.ManifestID) {
			return out, collabError("submission_stale_or_unsealed")
		}
		if s.UnansweredBlocking(task.ID) {
			return out, collabError("unresolved_blocking_question")
		}
		if strings.TrimSpace(c.Waiver) == "" && !s.SubmissionCovered(sub) {
			return out, collabError("acceptance_evidence_or_waiver_required")
		}
	}
	if c.Action == "dispatch" {
		if err := s.validGrant(task.ID, at); err != nil {
			return out, err
		}
		if meta.NeedsContext {
			return out, collabError("context_incomplete")
		}
	}
	if c.Action == "approve" || c.Action == "rework" || c.Action == "continue" {
		if c.Mode != "" && c.Mode != "strict_task" && c.Mode != "bounded_plan" {
			return out, collabError("invalid_approval_mode")
		}
		if c.MaxRevisions < 0 || c.MaxRevisions > 2 || (c.Mode != "bounded_plan" && c.MaxRevisions != 0) {
			return out, collabError("invalid_revision_budget")
		}
		if c.Deadline != 0 && (c.Deadline <= at || c.Deadline > at+7*24*60*60*1000) {
			return out, collabError("invalid_grant_deadline")
		}
		incompleteHandback := meta.HandoffID != "" && !v.Handoffs[meta.HandoffID].Complete
		if incompleteHandback || (meta.NeedsContext && c.Action != "rework") {
			return out, collabError("context_incomplete")
		}
	}
	if c.Action == "propose" {
		if c.Backend != "codex" {
			return out, collabError("backend_capability_missing")
		}
		if hasTask && task.State != "proposed" && task.State != "approved" && task.State != "review" && task.State != "failed" && task.State != "uncertain" && task.State != "waiting" {
			return out, collabError("task_not_quiescent")
		}
		if hasTask {
			if task.Owner != "pm" {
				return out, collabError("pm_human_controls_task")
			}
			task.State = "review" // legacy transition is reused only after v2 eligibility checks
			s.Tasks[task.ID] = task
		}
		if err := s.validateSources(c.ProjectID, c.TaskID, c.Inputs); err != nil {
			return out, err
		}
	}
	if c.Action == "rework" || c.Action == "continue" {
		if !p.Human {
			return out, ErrForbidden
		}
		if task.Owner != "pm" || task.State == "running" || task.State == "queued" || task.State == "provisioning" || task.State == "done" || task.State == "cancelled" {
			return out, collabError("task_not_quiescent")
		}
		if c.Action == "continue" {
			if s.UnansweredBlocking(task.ID) {
				return out, collabError("unresolved_blocking_question")
			}
			c.Text = task.Instruction
			c.Action = "rework"
		} else {
			c.Text = task.Instruction + "\n\nHuman revision within the original task:\n" + c.Text
		}
		task.State = "review"
		s.Tasks[task.ID] = task
	}
	if c.Action == "return_to_pm" && strings.TrimSpace(c.Text) == "" {
		return out, collabError("handback_summary_required")
	}
	legacy := c.Command
	legacy.MutationID = "v2-" + c.MutationID
	result, err := s.applyLegacy(p, legacy, at)
	if err != nil {
		return out, err
	}
	out.Result = result
	if c.Action == "pause" || c.Action == "resume" {
		if c.Action == "resume" {
			for id, m := range v.Tasks {
				if m.ProjectID == c.ProjectID {
					m.NoProgressRounds = 0
					v.Tasks[id] = m
				}
			}
		}
		return out, nil
	}
	task = s.Tasks[result.TaskID]
	meta = v.Tasks[task.ID]
	if meta.ID == "" {
		meta = TaskCoordination{ID: task.ID, ProjectID: task.ProjectID, Readiness: "pending_receipt", Phase: "preflight"}
	}
	meta.Revision++
	switch c.Action {
	case "propose", "rework":
		prior := v.Contracts[meta.ContractID]
		criteria := c.Criteria
		if len(criteria) == 0 && c.Action == "rework" {
			criteria = prior.Criteria
		}
		if len(criteria) == 0 {
			criteria = []Criterion{{ID: "AC01", Text: task.Acceptance, Mandatory: true}}
		}
		if err := validateCriteria(criteria); err != nil {
			return out, err
		}
		inputs, nonGoals := c.Inputs, c.NonGoals
		if c.Action == "rework" {
			inputs, nonGoals = prior.Inputs, prior.NonGoals
		}
		contract := TaskContract{ID: fmt.Sprintf("contract_%s_%d", task.ID, prior.Revision+1), ProjectID: task.ProjectID, TaskID: task.ID, Revision: prior.Revision + 1, Outcome: task.Instruction, NonGoals: nonNil(nonGoals), Criteria: criteria, Inputs: nonNil(inputs), Sources: nonNil(c.Sources), Sandbox: task.Sandbox, Backend: task.Backend, ProfileID: "general_delivery_v1", CreatedAt: at}
		contract.Hash = digestJSON(contract)
		v.Contracts[contract.ID] = contract
		s.revokeGrant(meta.GrantID, at)
		meta.ContractID, meta.GrantID, meta.ManifestID, meta.SubmissionID = contract.ID, "", "", ""
		meta.Readiness, meta.Phase = "pending_receipt", "preflight"
		meta.NeedsContext = meta.HandoffID != "" && !v.Handoffs[meta.HandoffID].Complete
		if c.Action == "rework" {
			task.State = "approved"
		}
	case "take_over":
		s.revokeGrant(meta.GrantID, at)
		meta.Readiness = "stale"
		meta.HandoffID = fmt.Sprintf("handoff_%s_%d", task.ID, task.Epoch)
		v.Handoffs[meta.HandoffID] = Handoff{ID: meta.HandoffID, ProjectID: task.ProjectID, TaskID: task.ID, Epoch: task.Epoch, FromEvent: s.Revision, ThroughEvent: s.Revision, Status: "draft", CreatedAt: at}
	case "return_to_pm":
		handoff := v.Handoffs[meta.HandoffID]
		handoff.Summary, handoff.ConfirmedBy, handoff.Complete, handoff.Status, handoff.ThroughEvent = c.Text, p.ID, c.Complete, "confirmed", s.Revision
		v.Handoffs[handoff.ID] = handoff
		d := Decision{ID: collaborationID("decision", p, c.MutationID), ProjectID: task.ProjectID, TaskID: task.ID, Text: c.Text, Sources: nonNil(c.Sources), ConfirmedBy: p.ID, CreatedAt: at}
		v.Decisions[d.ID] = d
		meta.Readiness, meta.NeedsContext = "stale", !c.Complete
		meta.GrantID = ""
		// A handback is context, not a sealed submission or acceptance request.
		task.State = "waiting"
		s.newActionable(task, "handback", c.Text, "pm", "", "", false, at)
	case "accept":
		meta.AcceptedSubmissionID, meta.AcceptedContractID = c.EntityID, meta.ContractID
		for id, a := range v.Actionables {
			if a.TaskID == task.ID && !actionableTerminal(a.State) {
				a.State = "resolved"
				a.Revision++
				a.UpdatedAt = at
				v.Actionables[id] = a
			}
		}
	}
	v.Tasks[task.ID] = meta
	s.Tasks[task.ID] = task
	if c.Action == "approve" || c.Action == "rework" {
		mode := c.Mode
		if mode == "" {
			mode = "strict_task"
		}
		expires := c.Deadline
		if expires == 0 {
			expires = at + 24*60*60*1000
		}
		contract := v.Contracts[meta.ContractID]
		grant := Grant{ID: collaborationID("grant", p, c.MutationID), ProjectID: task.ProjectID, TaskID: task.ID, ContractID: contract.ID, ContractHash: contract.Hash, ApprovedBy: p.ID, Mode: mode, MaxRevisions: c.MaxRevisions, ExpiresAt: expires, CreatedAt: at}
		v.Grants[grant.ID] = grant
		meta.GrantID, meta.Phase, meta.Readiness = grant.ID, "preflight", "pending_receipt"
		for id, a := range v.Actionables {
			if a.TaskID == task.ID && !actionableTerminal(a.State) {
				oldReport := v.Reports[a.ReportID]
				if a.Kind == "grant_expired" || a.Kind == "input_version_changed" || a.Kind == "decision_changed" || a.Kind == "handback" || a.Kind == "missing_submission" || a.Kind == "preflight_incomplete" || a.Kind == "submission" || (oldReport.ID != "" && oldReport.Input.ContractID != meta.ContractID && a.AnswerID == "") {
					a.State = "superseded"
					a.WaitingReason = "new_contract_approved"
					a.Revision++
					a.UpdatedAt = at
					v.Actionables[id] = a
				}
			}
		}
		v.Tasks[task.ID] = meta
		manifest, err := s.BuildManifest(task.ID, at, 24000)
		if err != nil {
			return out, err
		}
		meta.ManifestID = manifest.ID
		task.RunID = fmt.Sprintf("wr_%s_e%d_preflight", task.ID, task.Epoch)
		task.RequestID = fmt.Sprintf("pmrun_%s_e%d_preflight", task.ID, task.Epoch)
		task.Provisioned = false
		v.Tasks[task.ID], s.Tasks[task.ID] = meta, task
	}
	out.EntityID, out.EntityRevision = task.ID, meta.Revision
	return out, nil
}

func validateCriteria(criteria []Criterion) error {
	if len(criteria) == 0 || len(criteria) > 40 {
		return collabError("invalid_criteria")
	}
	ids := map[string]bool{}
	for _, criterion := range criteria {
		if criterion.ID == "" || len(criterion.ID) > 80 || strings.TrimSpace(criterion.Text) == "" || len(criterion.Text) > 4000 || ids[criterion.ID] {
			return collabError("invalid_criteria")
		}
		ids[criterion.ID] = true
	}
	return nil
}

func nonNil[T any](items []T) []T {
	if items == nil {
		return []T{}
	}
	return items
}
func actionableTerminal(state string) bool {
	return state == "resolved" || state == "superseded" || state == "cancelled"
}

func (s *State) revokeGrant(id string, at int64) {
	if grant, ok := s.Collaboration.Grants[id]; ok {
		grant.RevokedAt = at
		s.Collaboration.Grants[id] = grant
	}
}

func (s *State) validGrant(taskID string, at int64) error {
	meta := s.Collaboration.Tasks[taskID]
	g, ok := s.Collaboration.Grants[meta.GrantID]
	if !ok || g.ApprovedBy == "" || g.ContractID != meta.ContractID || g.ContractHash != s.Collaboration.Contracts[meta.ContractID].Hash {
		return collabError("approval_required")
	}
	if g.RevokedAt != 0 || g.ExpiresAt <= at {
		return collabError("grant_expired")
	}
	return nil
}

func (s *State) activeDecisionIDs(projectID, taskID string) []string {
	superseded := map[string]bool{}
	for _, d := range s.Collaboration.Decisions {
		if d.ProjectID == projectID {
			superseded[d.Supersedes] = true
		}
	}
	ids := []string{}
	for _, d := range s.Collaboration.Decisions {
		if d.ProjectID == projectID && (d.TaskID == "" || d.TaskID == taskID) && !superseded[d.ID] {
			ids = append(ids, d.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

func (s *State) BuildManifest(taskID string, at int64, maxCharacters int) (ContextManifest, error) {
	s.NormalizeCollaboration()
	task, ok := s.Tasks[taskID]
	if !ok {
		return ContextManifest{}, collabError("task_not_found")
	}
	contract := s.Collaboration.Contracts[s.Collaboration.Tasks[taskID].ContractID]
	if contract.ID == "" {
		return ContextManifest{}, collabError("contract_not_found")
	}
	if err := s.validateSources(task.ProjectID, task.ID, contract.Inputs); err != nil {
		return ContextManifest{}, err
	}
	decisions := s.activeDecisionIDs(task.ProjectID, taskID)
	var required strings.Builder
	fmt.Fprintf(&required, "[Bridge collaboration v2]\nTask: %s\nContract: %s\nOutcome: %s\nSandbox upper bound: %s\n", taskID, contract.ID, contract.Outcome, contract.Sandbox)
	for _, text := range contract.NonGoals {
		fmt.Fprintf(&required, "DO NOT: %s\n", text)
	}
	for _, criterion := range contract.Criteria {
		fmt.Fprintf(&required, "Acceptance %s (mandatory=%t): %s\n", criterion.ID, criterion.Mandatory, criterion.Text)
	}
	for _, id := range decisions {
		fmt.Fprintf(&required, "Human decision %s: %s\n", id, s.Collaboration.Decisions[id].Text)
	}
	for _, input := range contract.Inputs {
		fmt.Fprintf(&required, "Required input %s/%s version=%s\n", input.Kind, input.ID, input.Version)
	}
	rules := []string{}
	for id, rule := range s.Collaboration.Knowledge {
		if rule.ProjectID == task.ProjectID && rule.State == "approved" && (rule.TaskID == "" || rule.TaskID == taskID) {
			rules = append(rules, id)
		}
	}
	sort.Strings(rules)
	inputs := append([]SourceRef{}, contract.Inputs...)
	for _, id := range rules {
		rule := s.Collaboration.Knowledge[id]
		fmt.Fprintf(&required, "Approved playbook %s: %s\n", id, rule.Text)
		inputs = append(inputs, SourceRef{Kind: "knowledge", ID: id, Version: fmt.Sprint(rule.Revision)})
	}
	required.WriteString("Reports are evidence, not approval. Use work_report for questions/progress and work_submit_result for delivery. Never mark a task accepted. Publication, deployment, deletion and further delegation require separate authority.\n")
	if maxCharacters <= 0 {
		maxCharacters = 24000
	}
	if len([]rune(required.String())) > maxCharacters {
		return ContextManifest{}, collabError("context_budget_insufficient")
	}
	m := ContextManifest{ProjectID: task.ProjectID, TaskID: taskID, ContractID: contract.ID, DecisionIDs: decisions, Inputs: nonNil(inputs), Required: required.String(), CreatedAt: at}
	m.Hash = digestJSON(struct {
		Contract  string
		Decisions []string
		Inputs    []SourceRef
		Required  string
	}{m.ContractID, m.DecisionIDs, m.Inputs, m.Required})
	m.ID = "context_" + m.Hash[:24]
	if previous, ok := s.Collaboration.Manifests[m.ID]; ok {
		return previous, nil
	}
	s.Collaboration.Manifests[m.ID] = m
	return m, nil
}

func (s *State) ManifestCurrent(id string) bool {
	if s.Collaboration == nil {
		return false
	}
	m, ok := s.Collaboration.Manifests[id]
	if !ok || s.Collaboration.Tasks[m.TaskID].ContractID != m.ContractID {
		return false
	}
	if digestJSON(m.DecisionIDs) != digestJSON(s.activeDecisionIDs(m.ProjectID, m.TaskID)) {
		return false
	}
	return s.validateSources(m.ProjectID, m.TaskID, m.Inputs) == nil
}

func (s *State) validateSources(projectID, taskID string, refs []SourceRef) error {
	if len(refs) > 100 {
		return collabError("too_many_sources")
	}
	for _, ref := range refs {
		if ref.ID == "" || len(ref.ID) > 200 {
			return collabError("invalid_source")
		}
		var sourceProject, version string
		switch ref.Kind {
		case "contract":
			contract := s.Collaboration.Contracts[ref.ID]
			sourceProject, version = contract.ProjectID, fmt.Sprint(contract.Revision)
		case "artifact":
			a := s.Collaboration.Artifacts[ref.ID]
			sourceProject, version = a.ProjectID, a.Digest
		case "evidence":
			e := s.Collaboration.Evidence[ref.ID]
			sourceProject = e.ProjectID
		case "decision":
			d := s.Collaboration.Decisions[ref.ID]
			sourceProject = d.ProjectID
		case "report":
			r := s.Collaboration.Reports[ref.ID]
			sourceProject = r.ProjectID
		case "submission":
			sub := s.Collaboration.Submissions[ref.ID]
			sourceProject, version = sub.ProjectID, fmt.Sprint(sub.Version)
			if sub.Status != "submitted" || s.Collaboration.Tasks[sub.TaskID].ContractID != sub.Input.ContractID {
				return collabError("source_unavailable")
			}
		case "knowledge":
			rule := s.Collaboration.Knowledge[ref.ID]
			sourceProject, version = rule.ProjectID, fmt.Sprint(rule.Revision)
			if rule.State != "approved" {
				return collabError("source_stale")
			}
		default:
			return collabError("source_kind_forbidden")
		}
		if sourceProject == "" || sourceProject != projectID {
			return collabError("scope_forbidden")
		}
		if ref.Version != "" && version != "" && ref.Version != version {
			return collabError("source_stale")
		}
	}
	return nil
}

func (s *State) newActionable(task Task, kind, text, assignee, reportID, submissionID string, blocking bool, at int64) Actionable {
	id := "action_" + digestJSON([]any{task.ID, kind, reportID, submissionID, s.Revision})[:24]
	a := Actionable{ID: id, ProjectID: task.ProjectID, TaskID: task.ID, Kind: kind, Text: text, Assignee: assignee, State: "open", ReportID: reportID, SubmissionID: submissionID, Blocking: blocking, Revision: 1, CreatedAt: at, UpdatedAt: at}
	s.Collaboration.Actionables[id] = a
	return a
}

func (s *State) UnansweredBlocking(taskID string) bool {
	for _, a := range s.Collaboration.Actionables {
		if a.TaskID == taskID && a.Blocking && !actionableTerminal(a.State) && (a.AnswerID == "" || a.Kind == "assistance_approval") {
			return true
		}
	}
	return false
}

func (s *State) applyReport(p Principal, c CollaborationCommand, at int64) (CollaborationResult, error) {
	task := s.Tasks[c.TaskID]
	meta := s.Collaboration.Tasks[c.TaskID]
	r := *c.Report
	out := CollaborationResult{Result: Result{ProjectID: c.ProjectID, TaskID: task.ID, SessionID: task.SessionID}}
	if r.ContractID != meta.ContractID || r.ManifestID != meta.ManifestID || !s.ManifestCurrent(r.ManifestID) {
		return out, collabError("contract_stale")
	}
	if strings.TrimSpace(r.Text) == "" || len(r.Text) > 16000 {
		return out, collabError("report_invalid")
	}
	if err := s.validateSources(task.ProjectID, task.ID, r.Sources); err != nil {
		return out, err
	}
	for _, ref := range r.Sources {
		if !s.WorkerSourceAllowed(task.ID, ref) {
			return out, collabError("scope_forbidden")
		}
	}
	switch r.Kind {
	case "received":
		if meta.Phase != "preflight" || (r.Readiness != "confirmed" && r.Readiness != "needs_clarification") {
			return out, collabError("invalid_receipt")
		}
		meta.Readiness = r.Readiness
		meta.Revision++
		s.Collaboration.Tasks[task.ID] = meta
	case "progress", "checkpoint", "question", "blocked", "change_requested":
	case "answer_ack":
		a, ok := s.Collaboration.Actionables[r.QuestionID]
		if !ok || a.TaskID != task.ID || a.AnswerID == "" || a.AnswerID != r.AnswerID || actionableTerminal(a.State) {
			return out, collabError("answer_stale")
		}
		if r.Adopted {
			a.State = "resolved"
			a.WaitingReason = ""
		} else {
			a.State = "open"
			a.Assignee = "pm"
			a.WaitingReason = ""
			a.AnswerID = ""
			a.Answer = ""
		}
		a.Revision++
		a.UpdatedAt = at
		s.Collaboration.Actionables[a.ID] = a
	default:
		return out, collabError("report_kind_invalid")
	}
	id := collaborationID("report", p, c.MutationID)
	report := WorkerReport{ID: id, ProjectID: task.ProjectID, TaskID: task.ID, SessionID: p.SessionID, RunID: p.RunID, Epoch: p.Epoch, Actor: "agent:" + p.SessionID, Input: r, CreatedAt: at}
	s.Collaboration.Reports[id] = report
	wake := r.Kind == "question" || r.Kind == "blocked" || r.Kind == "change_requested" || r.Kind == "answer_ack" || (r.Kind == "received" && r.Readiness == "needs_clarification")
	if wake && r.Kind != "answer_ack" {
		count := 0
		for _, a := range s.Collaboration.Actionables {
			if a.TaskID == task.ID && !actionableTerminal(a.State) {
				count++
			}
		}
		if count >= 8 {
			return out, collabError("pending_question_limit_resolve_existing_first")
		}
	}
	s.AddEvent(p, task.ProjectID, task.ID, "worker_report_"+r.Kind, r.Text, meta.ActiveRequestID, at, wake)
	if wake && r.Kind != "answer_ack" {
		assignee := "pm"
		if r.DecisionOwner == "human" {
			assignee = "human"
		}
		a := s.newActionable(task, r.Kind, r.Text, assignee, id, "", r.Blocking || r.Kind == "blocked" || r.Kind == "change_requested" || r.Readiness == "needs_clarification", at)
		out.ActionableID = a.ID
	}
	out.EntityID = id
	out.EntityRevision = 1
	return out, nil
}

func (s *State) applyDisposition(p Principal, c CollaborationCommand, at int64) (CollaborationResult, error) {
	d := *c.Disposition
	a, ok := s.Collaboration.Actionables[d.ActionableID]
	out := CollaborationResult{Result: Result{ProjectID: c.ProjectID, TaskID: a.TaskID}}
	if !ok || a.ProjectID != c.ProjectID {
		return out, collabError("scope_forbidden")
	}
	if a.Revision != c.ExpectedEntityRevision {
		return out, collabError("entity_revision_conflict")
	}
	if actionableTerminal(a.State) {
		return out, collabError("actionable_closed")
	}
	if strings.TrimSpace(d.Text) == "" || len(d.Text) > 16000 {
		return out, collabError("disposition_reason_required")
	}
	if !p.Human && a.Assignee != "pm" {
		return out, collabError("actionable_not_assigned")
	}
	if err := s.validateSources(c.ProjectID, a.TaskID, d.Sources); err != nil {
		return out, err
	}
	id := collaborationID("disposition", p, c.MutationID)
	switch d.Action {
	case "answer_from_context":
		if a.Kind != "question" && a.Kind != "received" && a.Kind != "blocked" {
			return out, collabError("not_a_question")
		}
		if !p.Human && len(d.Sources) == 0 {
			return out, collabError("answer_source_required")
		}
		a.AnswerID, a.Answer = id, d.Text
		a.State, a.Assignee, a.WaitingReason = "waiting", "worker", "answer_ack"
	case "escalate_to_human":
		a.State, a.Assignee, a.WaitingReason = "waiting", "human", "decision"
	case "recommend_acceptance":
		if a.SubmissionID == "" || s.Collaboration.Submissions[a.SubmissionID].Status != "submitted" {
			return out, collabError("submission_unsealed")
		}
		a.State, a.Assignee, a.WaitingReason = "waiting", "human", "acceptance"
	case "ask_for_evidence", "propose_rework":
		a.State, a.Assignee, a.WaitingReason = "waiting", "human", "approve_rework"
	case "defer":
		a.State, a.WaitingReason = "waiting", "deferred"
		a.DueAt = at + 5*60*1000
	default:
		return out, collabError("disposition_action_invalid")
	}
	a.Revision++
	a.UpdatedAt = at
	s.Collaboration.Actionables[a.ID] = a
	actor := "agent:" + p.SessionID
	if p.Human {
		actor = "human:" + p.ID
	}
	s.Collaboration.Dispositions[id] = Disposition{ID: id, ProjectID: a.ProjectID, TaskID: a.TaskID, ActionableID: a.ID, Action: d.Action, Text: d.Text, Actor: actor, Sources: nonNil(d.Sources), CreatedAt: at}
	s.AddEvent(p, a.ProjectID, a.TaskID, "report_disposed", d.Text, "", at, p.Human)
	out.EntityID, out.EntityRevision, out.ActionableID = id, a.Revision, a.ID
	return out, nil
}

func (s *State) applySubmission(p Principal, c CollaborationCommand, at int64) (CollaborationResult, error) {
	v := s.Collaboration
	meta := v.Tasks[c.TaskID]
	task := s.Tasks[c.TaskID]
	input := *c.Submission
	out := CollaborationResult{Result: Result{ProjectID: c.ProjectID, TaskID: task.ID}}
	if meta.Phase != "execution" || meta.Readiness != "confirmed" {
		return out, collabError("task_not_ready")
	}
	if input.ContractID != meta.ContractID || input.ManifestID != meta.ManifestID || !s.ManifestCurrent(input.ManifestID) {
		return out, collabError("contract_stale")
	}
	if strings.TrimSpace(input.Summary) == "" || len(input.Summary) > 16000 || len(input.ArtifactIDs) == 0 || len(input.ArtifactIDs) > 40 {
		return out, collabError("submission_incomplete")
	}
	for _, id := range input.ArtifactIDs {
		a, ok := v.Artifacts[id]
		if !ok || a.TaskID != task.ID {
			return out, collabError("scope_forbidden")
		}
	}
	contract := v.Contracts[meta.ContractID]
	coverage := map[string]bool{}
	for _, entry := range input.Coverage {
		if coverage[entry.CriterionID] {
			return out, collabError("coverage_duplicate")
		}
		coverage[entry.CriterionID] = true
		found := false
		for _, criterion := range contract.Criteria {
			if criterion.ID == entry.CriterionID {
				found = true
			}
		}
		if !found {
			return out, collabError("criterion_unknown")
		}
		if entry.Claim != "passed" && entry.Claim != "failed" && entry.Claim != "unknown" && entry.Claim != "not_run" {
			return out, collabError("coverage_invalid")
		}
		for _, id := range entry.EvidenceIDs {
			e, ok := v.Evidence[id]
			if !ok || e.TaskID != task.ID {
				return out, collabError("scope_forbidden")
			}
		}
	}
	for _, criterion := range contract.Criteria {
		if !coverage[criterion.ID] {
			return out, collabError("coverage_incomplete")
		}
	}
	version := uint64(1)
	for _, old := range v.Submissions {
		if old.TaskID == task.ID && old.Version >= version {
			version = old.Version + 1
		}
	}
	id := collaborationID("submission", p, c.MutationID)
	sub := Submission{ID: id, ProjectID: task.ProjectID, TaskID: task.ID, RunID: p.RunID, Epoch: p.Epoch, Version: version, Input: input, Status: "sealing_pending", CreatedAt: at}
	if old, ok := v.Submissions[meta.SubmissionID]; ok {
		old.Status = "superseded"
		v.Submissions[old.ID] = old
	}
	v.Submissions[id] = sub
	meta.SubmissionID = id
	meta.Revision++
	v.Tasks[task.ID] = meta
	s.AddEvent(p, task.ProjectID, task.ID, "submission_received", input.Summary, meta.ActiveRequestID, at, false)
	out.EntityID, out.EntityRevision = id, version
	return out, nil
}

func (s *State) SubmissionCovered(sub Submission) bool {
	contract := s.Collaboration.Contracts[sub.Input.ContractID]
	for _, criterion := range contract.Criteria {
		if !criterion.Mandatory {
			continue
		}
		valid := false
		for _, coverage := range sub.Input.Coverage {
			if coverage.CriterionID == criterion.ID && coverage.Claim == "passed" && len(coverage.EvidenceIDs) > 0 {
				valid = true
			}
		}
		if !valid {
			return false
		}
	}
	return true
}
