package coordination

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func (s *State) applyCooperation(p Principal, c CollaborationCommand, at int64) (CollaborationResult, error) {
	v := s.Collaboration
	project := s.Projects[c.ProjectID]
	task := s.Tasks[c.TaskID]
	meta := v.Tasks[c.TaskID]
	out := CollaborationResult{Result: Result{ProjectID: c.ProjectID, TaskID: c.TaskID}}
	pm := p.SessionID == project.PMSessionID && !p.Human
	id := collaborationID(c.Action, p, c.MutationID)
	switch c.Action {
	case "decision":
		if !p.Human {
			return out, ErrForbidden
		}
		if strings.TrimSpace(c.Text) == "" {
			return out, collabError("decision_text_required")
		}
		if c.Supersedes != "" {
			previous, ok := v.Decisions[c.Supersedes]
			if !ok || previous.ProjectID != project.ID || previous.TaskID != c.TaskID {
				return out, collabError("decision_scope_conflict")
			}
		}
		if err := s.validateSources(project.ID, c.TaskID, c.Sources); err != nil {
			return out, err
		}
		v.Decisions[id] = Decision{ID: id, ProjectID: project.ID, TaskID: c.TaskID, Text: c.Text, Supersedes: c.Supersedes, Sources: nonNil(c.Sources), ConfirmedBy: p.ID, CreatedAt: at}
		for taskID, m := range v.Tasks {
			if m.ProjectID != project.ID || (c.TaskID != "" && taskID != c.TaskID) {
				continue
			}
			m.Readiness = "stale"
			m.NeedsContext = true
			m.Revision++
			v.Tasks[taskID] = m
			s.revokeGrant(m.GrantID, at)
			if sub, ok := v.Submissions[m.SubmissionID]; ok && s.Tasks[taskID].State != "done" {
				sub.Status = "superseded"
				v.Submissions[sub.ID] = sub
			}
			s.newActionable(s.Tasks[taskID], "decision_changed", c.Text, "pm", "", "", false, at)
		}
	case "prepare_handback":
		if !p.Human && !pm {
			return out, ErrForbidden
		}
		if task.Owner != "human" || meta.HandoffID == "" {
			return out, collabError("human_control_required")
		}
		h := v.Handoffs[meta.HandoffID]
		h.ThroughEvent = s.Revision
		var b strings.Builder
		for _, event := range s.Events {
			if event.TaskID == task.ID && event.ID > h.FromEvent {
				fmt.Fprintf(&b, "[%s] %s\n", event.Kind, event.Text)
			}
		}
		h.Summary = b.String()
		if strings.TrimSpace(c.Text) != "" {
			h.Summary = c.Text
		}
		if len([]rune(h.Summary)) > 16000 {
			h.Summary = string([]rune(h.Summary)[:16000])
		}
		h.Complete = false // a generated draft never claims the human confirmed it
		v.Handoffs[h.ID] = h
		id = h.ID
	case "cancel":
		if !p.Human {
			return out, ErrForbidden
		}
		if meta.Revision != c.ExpectedEntityRevision {
			return out, collabError("entity_revision_conflict")
		}
		if task.ID == "" || task.State == "done" {
			return out, collabError("task_not_active")
		}
		task.State = "cancelled"
		task.Epoch++
		meta.Revision++
		s.revokeGrant(meta.GrantID, at)
		s.Tasks[task.ID] = task
		v.Tasks[task.ID] = meta
		for aid, a := range v.Actionables {
			if a.TaskID == task.ID && !actionableTerminal(a.State) {
				a.State = "cancelled"
				a.Revision++
				a.UpdatedAt = at
				v.Actionables[aid] = a
			}
		}
		for aid, a := range v.Assistance {
			if a.RequesterTaskID == task.ID && a.State != "adopted" {
				a.State = "cancelled"
				a.Revision++
				v.Assistance[aid] = a
				if target, ok := s.Tasks[a.TargetTaskID]; ok && target.Owner == "pm" && target.State != "done" {
					target.State = "cancelled"
					target.Epoch++
					s.Tasks[target.ID] = target
					s.revokeGrant(v.Tasks[target.ID].GrantID, at)
				}
			}
		}
	case "request_assistance":
		if task.ID == "" || meta.AssistanceID != "" {
			return out, collabError("recursive_assistance_forbidden")
		}
		if c.Mode != "consult" && c.Mode != "review" {
			return out, collabError("assistance_mode_invalid")
		}
		if strings.TrimSpace(c.Text) == "" || len(c.Text) > 8000 {
			return out, collabError("assistance_question_required")
		}
		if err := s.validateSources(project.ID, task.ID, c.Inputs); err != nil {
			return out, err
		}
		if !p.Human && !pm {
			for _, ref := range c.Inputs {
				if !s.WorkerSourceAllowed(task.ID, ref) {
					return out, collabError("scope_forbidden")
				}
			}
		}
		for _, a := range v.Assistance {
			if a.RequesterTaskID == task.ID && a.RequesterEpoch == task.Epoch && a.Text == c.Text && a.Mode == c.Mode && digestJSON(a.Inputs) == digestJSON(nonNil(c.Inputs)) && a.State != "cancelled" && a.State != "expired" {
				out.EntityID = a.ID
				out.EntityRevision = a.Revision
				return out, nil
			}
		}
		deadline := c.Deadline
		if deadline == 0 {
			deadline = at + 30*60*1000
		}
		if deadline <= at || deadline > at+24*60*60*1000 {
			return out, collabError("invalid_assistance_deadline")
		}
		v.Assistance[id] = Assistance{ID: id, ProjectID: project.ID, RequesterTaskID: task.ID, RequesterEpoch: task.Epoch, Mode: c.Mode, Text: c.Text, Inputs: nonNil(c.Inputs), Blocking: c.Blocking, State: "proposed", Revision: 1, Deadline: deadline, CreatedAt: at}
		a := s.newActionable(task, "assistance_approval", c.Text, "human", "", "", c.Blocking, at)
		a.AnswerID = id
		v.Actionables[a.ID] = a
		out.ActionableID = a.ID
	case "approve_assistance":
		if !p.Human {
			return out, ErrForbidden
		}
		a, ok := v.Assistance[c.EntityID]
		if !ok || a.ProjectID != project.ID {
			return out, collabError("scope_forbidden")
		}
		if a.Revision != c.ExpectedEntityRevision {
			return out, collabError("entity_revision_conflict")
		}
		requester := s.Tasks[a.RequesterTaskID]
		if a.State != "proposed" || a.Deadline <= at || requester.Epoch != a.RequesterEpoch || requester.Owner != "pm" || requester.State == "cancelled" {
			return out, collabError("assistance_stale")
		}
		count := 0
		for _, other := range v.Assistance {
			if other.RequesterTaskID == a.RequesterTaskID && other.State == "running" {
				count++
			}
		}
		if count >= 2 {
			return out, collabError("assistance_limit")
		}
		proposal := CollaborationCommand{Command: Command{Action: "propose", ProjectID: project.ID, MutationID: id + "-task", Title: a.Mode + " · " + requester.Title, Reason: a.Text, Instruction: a.Text, Acceptance: "Answer the exact request, cite the supplied sources, and state uncertainty. Do not modify source artifacts or create further agents.", Backend: "codex", Sandbox: "read-only"}, Inputs: a.Inputs, NonGoals: []string{"Do not modify the requester artifacts.", "Do not delegate or create further sessions."}}
		created, err := s.applyTaskCommand(p, proposal, at)
		if err != nil {
			return out, err
		}
		targetMeta := v.Tasks[created.TaskID]
		targetMeta.AssistanceID = a.ID
		v.Tasks[created.TaskID] = targetMeta
		approved, err := s.applyTaskCommand(p, CollaborationCommand{Command: Command{Action: "approve", ProjectID: project.ID, TaskID: created.TaskID, MutationID: id + "-approve", ExpectedRevision: s.Projects[project.ID].Revision}, ExpectedEntityRevision: targetMeta.Revision, ContractID: targetMeta.ContractID}, at)
		if err != nil {
			return out, err
		}
		_, err = s.applyTaskCommand(p, CollaborationCommand{Command: Command{Action: "dispatch", ProjectID: project.ID, TaskID: created.TaskID, MutationID: id + "-dispatch", ExpectedRevision: s.Projects[project.ID].Revision}, ExpectedEntityRevision: approved.EntityRevision}, at)
		if err != nil {
			return out, err
		}
		a.TargetTaskID = created.TaskID
		a.State = "running"
		a.Revision++
		v.Assistance[a.ID] = a
		for aid, action := range v.Actionables {
			if action.TaskID == requester.ID && action.Kind == "assistance_approval" && action.AnswerID == a.ID {
				action.State = "waiting"
				action.Assignee = "worker"
				action.WaitingReason = "assistance"
				action.Revision++
				v.Actionables[aid] = action
			}
		}
		out.TaskID, out.SessionID = created.TaskID, created.SessionID
		id = a.ID
		out.EntityRevision = a.Revision
	case "adopt_assistance":
		a, ok := v.Assistance[c.EntityID]
		if !ok || a.ProjectID != project.ID || a.RequesterTaskID != task.ID {
			return out, collabError("scope_forbidden")
		}
		if a.Revision != c.ExpectedEntityRevision {
			return out, collabError("entity_revision_conflict")
		}
		if a.State != "ready" || a.RequesterEpoch != task.Epoch || a.Deadline <= at {
			return out, collabError("assistance_stale")
		}
		a.State = "adopted"
		a.Revision++
		v.Assistance[a.ID] = a
		for aid, action := range v.Actionables {
			if action.TaskID == task.ID && action.Kind == "assistance_approval" && action.AnswerID == a.ID {
				action.State = "resolved"
				action.Revision++
				v.Actionables[aid] = action
			}
		}
		id = a.ID
		out.EntityRevision = a.Revision
	case "retry":
		if !pm {
			return out, ErrForbidden
		}
		if task.Owner != "pm" || (task.State != "review" && task.State != "waiting" && task.State != "failed") {
			return out, collabError("task_not_quiescent")
		}
		if err := s.validGrant(task.ID, at); err != nil {
			return out, err
		}
		g := v.Grants[meta.GrantID]
		if g.Mode != "bounded_plan" || g.UsedRevisions >= g.MaxRevisions {
			return out, collabError("budget_exhausted")
		}
		if s.UnansweredBlocking(task.ID) || !s.ManifestCurrent(meta.ManifestID) {
			return out, collabError("task_not_ready")
		}
		// Retry only the SAME immutable contract. A PM cannot use a retry to
		// insert arbitrary instructions or enlarge an approved outcome.
		if c.Text != "" || c.Instruction != "" {
			return out, collabError("retry_cannot_change_contract")
		}
		g.UsedRevisions++
		v.Grants[g.ID] = g
		task.Epoch++
		task.RunID = fmt.Sprintf("wr_%s_e%d_execute", task.ID, task.Epoch)
		task.RequestID = fmt.Sprintf("pmrun_%s_e%d_execute", task.ID, task.Epoch)
		task.DispatchEpoch = task.Epoch
		task.State = "provisioning"
		task.Provisioned = false
		meta.Phase = "execution"
		meta.Revision++
		meta.SubmissionID = ""
		s.Tasks[task.ID] = task
		v.Tasks[task.ID] = meta
		out.EntityRevision = meta.Revision
	case "propose_rule":
		if strings.TrimSpace(c.Text) == "" || len(c.Text) > 8000 || len(c.Sources) == 0 {
			return out, collabError("rule_sources_required")
		}
		if err := s.validateSources(project.ID, task.ID, c.Sources); err != nil {
			return out, err
		}
		v.Knowledge[id] = KnowledgeRule{ID: id, ProjectID: project.ID, TaskID: c.TaskID, Text: c.Text, FailureClass: c.Mode, Sources: c.Sources, State: "proposed", Supersedes: c.Supersedes, Revision: 1, CreatedAt: at}
	case "approve_rule", "revoke_rule":
		if !p.Human {
			return out, ErrForbidden
		}
		rule, ok := v.Knowledge[c.EntityID]
		if !ok || rule.ProjectID != project.ID {
			return out, collabError("scope_forbidden")
		}
		if rule.Revision != c.ExpectedEntityRevision {
			return out, collabError("entity_revision_conflict")
		}
		if c.Action == "approve_rule" {
			if rule.State != "proposed" || strings.TrimSpace(c.Evaluation) == "" {
				return out, collabError("rule_evaluation_required")
			}
			if rule.Supersedes != "" {
				old, ok := v.Knowledge[rule.Supersedes]
				if !ok || old.ProjectID != project.ID || old.TaskID != rule.TaskID {
					return out, collabError("rule_scope_conflict")
				}
				old.State = "superseded"
				old.Revision++
				v.Knowledge[old.ID] = old
			}
			rule.State = "approved"
			rule.Evaluation = c.Evaluation
			rule.ApprovedBy = p.ID
		} else {
			rule.State = "revoked"
		}
		rule.Revision++
		v.Knowledge[rule.ID] = rule
		id = rule.ID
		out.EntityRevision = rule.Revision
	default:
		return out, collabError("cooperation_action_invalid")
	}
	s.AddEvent(p, project.ID, c.TaskID, c.Action, c.Text, "", at, true)
	out.EntityID = id
	if out.EntityRevision == 0 {
		out.EntityRevision = 1
	}
	return out, nil
}

// AdmitCollaborationRun records the actual admitted request, including human
// turns. It is called under the common Bridge admission gate, never by a model.
func (s *State) AdmitCollaborationRun(sessionID, requestID, content string, at int64) error {
	s.NormalizeCollaboration()
	project, task, ok := s.ProjectForSession(sessionID)
	if !ok || task == nil || project.EngineVersion != 2 {
		return nil
	}
	meta := s.Collaboration.Tasks[task.ID]
	if task.State == "cancelled" || task.State == "done" {
		return collabError("task_not_active")
	}
	if task.Owner == "pm" {
		if err := s.validGrant(task.ID, at); err != nil {
			return err
		}
		if !s.ManifestCurrent(meta.ManifestID) || meta.NeedsContext {
			return collabError("contract_stale")
		}
		if meta.Phase == "execution" && (meta.Readiness != "confirmed" || s.UnansweredBlocking(task.ID)) {
			return collabError("task_not_ready")
		}
		meta.ActiveRunID = task.RunID
	} else {
		meta.ActiveRunID = "humanrun_" + digestJSON([]string{sessionID, requestID})[:24]
		meta.Phase = "execution"
		s.AddEvent(Principal{ID: "system"}, project.ID, task.ID, "human_instruction", content, requestID, at, false)
	}
	meta.ActiveRequestID = requestID
	meta.ActiveEpoch = task.Epoch
	s.Collaboration.Tasks[task.ID] = meta
	return nil
}

func (s *State) FinishCollaborationRun(sessionID, requestID, status, text string, at int64) bool {
	project, task, ok := s.ProjectForSession(sessionID)
	if !ok || task == nil || project.EngineVersion != 2 {
		return false
	}
	s.NormalizeCollaboration()
	v := s.Collaboration
	meta := v.Tasks[task.ID]
	key := "terminal:" + sessionID + ":" + requestID
	if _, ok := v.Receipts[key]; ok {
		return true
	}
	v.Receipts[key] = json.RawMessage(`{}`)
	if meta.ActiveRequestID != requestID || meta.ActiveEpoch != task.Epoch || task.State == "cancelled" || task.State == "done" {
		s.AddEvent(Principal{ID: "system"}, project.ID, task.ID, "historical_run_finished", "Earlier run finished; current task was not changed.", requestID, at, false)
		return true
	}
	task.Result = text // complete text is persisted; UI projections may paginate
	if meta.Phase == "preflight" && task.Owner == "pm" {
		if status == "succeeded" && meta.Readiness == "confirmed" && s.ManifestCurrent(meta.ManifestID) && s.validGrant(task.ID, at) == nil && !s.UnansweredBlocking(task.ID) {
			meta.Phase = "execution"
			task.State = "provisioning"
			task.Provisioned = false
			task.RunID = fmt.Sprintf("wr_%s_e%d_execute", task.ID, task.Epoch)
			task.RequestID = fmt.Sprintf("pmrun_%s_e%d_execute", task.ID, task.Epoch)
			task.DispatchEpoch = task.Epoch
		} else {
			task.State = "waiting"
			if meta.Readiness != "needs_clarification" {
				s.newActionable(*task, "preflight_incomplete", "Worker has not confirmed this task. Review its response before continuing.", "pm", "", "", false, at)
			}
		}
	} else if status == "succeeded" {
		sub, exists := v.Submissions[meta.SubmissionID]
		if exists && sub.RunID == meta.ActiveRunID && sub.Epoch == task.Epoch && sub.Status == "sealing_pending" && s.ManifestCurrent(sub.Input.ManifestID) {
			sub.Status = "submitted"
			v.Submissions[sub.ID] = sub
			task.State = "review"
			s.newActionable(*task, "submission", sub.Input.Summary, "pm", "", sub.ID, false, at)
			if assistance, ok := v.Assistance[meta.AssistanceID]; ok && assistance.State == "running" {
				requester := s.Tasks[assistance.RequesterTaskID]
				if requester.Epoch == assistance.RequesterEpoch && requester.Owner == "pm" && assistance.Deadline > at {
					assistance.State = "ready"
					assistance.ResultSubmissionID = sub.ID
				} else {
					assistance.State = "expired"
				}
				assistance.Revision++
				v.Assistance[assistance.ID] = assistance
			}
		} else {
			task.State = "waiting"
			if task.Owner == "pm" && !s.UnansweredBlocking(task.ID) {
				s.newActionable(*task, "missing_submission", "The model turn ended without a sealed delivery. Request a structured submission; do not treat this as accepted.", "pm", "", "", false, at)
			}
		}
	} else {
		task.State = "failed"
		s.newActionable(*task, "execution_failed", "Execution ended: "+status+". Check the original conversation and side effects before retrying.", "pm", "", "", false, at)
	}
	meta.Revision++
	meta.ActiveRequestID = ""
	meta.ActiveRunID = ""
	s.Tasks[task.ID] = *task
	v.Tasks[task.ID] = meta
	s.AddEvent(Principal{ID: "system", SessionID: sessionID}, project.ID, task.ID, "worker_"+status, text, requestID, at, true)
	return true
}

func (s *State) PendingActionables(projectID, assignee string) []Actionable {
	items := []Actionable{}
	if s.Collaboration == nil {
		return items
	}
	for _, a := range s.Collaboration.Actionables {
		if a.ProjectID == projectID && !actionableTerminal(a.State) && (assignee == "" || a.Assignee == assignee) {
			items = append(items, a)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Blocking != items[j].Blocking {
			return items[i].Blocking
		}
		if items[i].CreatedAt != items[j].CreatedAt {
			return items[i].CreatedAt < items[j].CreatedAt
		}
		return items[i].ID < items[j].ID
	})
	return items
}

func (s *State) ProjectDispositionCount(projectID string) int {
	n := 0
	if s.Collaboration != nil {
		for _, d := range s.Collaboration.Dispositions {
			if d.ProjectID == projectID {
				n++
			}
		}
	}
	return n
}

func (s *State) EffectiveSandbox(sessionID string) string {
	project, task, ok := s.ProjectForSession(sessionID)
	if !ok || task == nil || project.EngineVersion != 2 || s.Collaboration == nil {
		return ""
	}
	if task.Owner == "pm" && s.Collaboration.Tasks[task.ID].Phase == "preflight" {
		return "read-only"
	}
	return task.Sandbox
}

func (s *State) WorkerSourceAllowed(taskID string, ref SourceRef) bool {
	task, ok := s.Tasks[taskID]
	if !ok || s.validateSources(task.ProjectID, taskID, []SourceRef{ref}) != nil {
		return false
	}
	v := s.Collaboration
	for _, input := range v.Contracts[v.Tasks[taskID].ContractID].Inputs {
		if input.Kind == ref.Kind && input.ID == ref.ID {
			return true
		}
	}
	switch ref.Kind {
	case "contract":
		return v.Contracts[ref.ID].TaskID == taskID
	case "artifact":
		return v.Artifacts[ref.ID].TaskID == taskID
	case "evidence":
		return v.Evidence[ref.ID].TaskID == taskID
	case "report":
		return v.Reports[ref.ID].TaskID == taskID
	case "decision":
		d := v.Decisions[ref.ID]
		return d.TaskID == "" || d.TaskID == taskID
	case "knowledge":
		r := v.Knowledge[ref.ID]
		return r.TaskID == "" || r.TaskID == taskID
	case "submission":
		if v.Submissions[ref.ID].TaskID == taskID {
			return true
		}
		for _, a := range v.Assistance {
			if a.RequesterTaskID == taskID && a.ResultSubmissionID == ref.ID && (a.State == "ready" || a.State == "adopted") {
				return true
			}
		}
	}
	return false
}

func (s *State) upgradeCollaboration(p Principal, c CollaborationCommand, at int64) (CollaborationResult, error) {
	out := CollaborationResult{Result: Result{ProjectID: c.ProjectID}}
	project, ok := s.Projects[c.ProjectID]
	if !p.Human || !ok {
		return out, ErrForbidden
	}
	if project.EngineVersion == 2 {
		return out, collabError("already_upgraded")
	}
	if err := s.Authorize(p, project.ID); err != nil {
		return out, err
	}
	for _, task := range s.Tasks {
		if task.ProjectID == project.ID && task.State != "done" && (task.Owner == "human" || task.Backend != "codex") {
			return out, collabError("upgrade_requires_supported_workers_and_handback")
		}
		if task.ProjectID == project.ID && (task.State == "running" || task.State == "queued" || task.State == "provisioning") {
			return out, collabError("upgrade_requires_quiescence")
		}
	}
	project.EngineVersion = 2
	project.Mode = "paused"
	project.WakeRequestID = ""
	project.WakeState = ""
	project.Note = "協作已升級；原對話保留，未完成任務需重新確認版本與批准。"
	s.Projects[project.ID] = project
	for id, task := range s.Tasks {
		if task.ProjectID != project.ID {
			continue
		}
		contract := TaskContract{ID: "contract_" + task.ID + "_1", ProjectID: project.ID, TaskID: task.ID, Revision: 1, Outcome: task.Instruction, NonGoals: []string{"Publication, deployment and deletion require separate approval."}, Criteria: []Criterion{{ID: "AC01", Text: task.Acceptance, Mandatory: true}}, Inputs: []SourceRef{}, Sources: []SourceRef{}, Sandbox: task.Sandbox, Backend: task.Backend, ProfileID: "general_delivery_v1", CreatedAt: at}
		contract.Hash = digestJSON(contract)
		s.Collaboration.Contracts[contract.ID] = contract
		m := TaskCoordination{ID: id, ProjectID: project.ID, Revision: 1, ContractID: contract.ID, Readiness: "pending_receipt", Phase: "preflight"}
		if task.State != "done" {
			task.State = "proposed"
			task.Epoch++
			task.Owner = "pm"
			task.ApprovedBy = ""
		}
		s.Collaboration.Tasks[id] = m
		s.Tasks[id] = task
	}
	s.AddEvent(p, project.ID, "", "collaboration_upgraded", project.Note, "", at, false)
	out.SessionID = project.PMSessionID
	return out, nil
}
