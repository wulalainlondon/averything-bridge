package coordination

import "fmt"

// InvalidateDependentManifests follows explicit input references. Unrelated
// tasks are left alone, and accepted outcomes remain historical acceptances.
func (s *State) InvalidateDependentManifests(at int64) bool {
	if s.Collaboration == nil {
		return false
	}
	changed := false
	for id, m := range s.Collaboration.Tasks {
		t := s.Tasks[id]
		if m.ManifestID == "" || m.NeedsContext || t.State == "done" || t.State == "cancelled" || s.ManifestCurrent(m.ManifestID) {
			continue
		}
		m.NeedsContext = true
		m.Readiness = "stale"
		m.Revision++
		s.Collaboration.Tasks[id] = m
		s.revokeGrant(m.GrantID, at)
		s.AddEvent(Principal{ID: "system"}, t.ProjectID, t.ID, "input_version_changed", "A consumed input or playbook changed. Review the new basis before continuing.", "", at, true)
		s.newActionable(t, "input_version_changed", "相依輸入或工作方法已變更，請核對新版來源後重新提案。", "pm", "", "", false, at)
		changed = true
	}
	return changed
}

func (s *State) SweepCollaboration(at int64) bool {
	if s.Collaboration == nil {
		return false
	}
	changed := s.InvalidateDependentManifests(at)
	v := s.Collaboration
	for id, a := range v.Actionables {
		if a.State == "waiting" && a.WaitingReason == "deferred" && a.DueAt > 0 && a.DueAt <= at {
			a.State = "open"
			a.WaitingReason = ""
			a.DueAt = 0
			a.Revision++
			a.UpdatedAt = at
			v.Actionables[id] = a
			s.AddEvent(Principal{ID: "system"}, a.ProjectID, a.TaskID, "disposition_due", "Deferred action is due for review.", "", at, true)
			changed = true
		}
	}
	for id, a := range v.Assistance {
		requester := s.Tasks[a.RequesterTaskID]
		if a.State == "adopted" || a.State == "expired" || a.State == "cancelled" {
			continue
		}
		if a.Deadline > at && requester.Epoch == a.RequesterEpoch && requester.State != "cancelled" {
			continue
		}
		a.State = "expired"
		a.Revision++
		v.Assistance[id] = a
		if target, ok := s.Tasks[a.TargetTaskID]; ok && target.Owner == "pm" && target.State != "done" && target.State != "cancelled" {
			target.State = "cancelled"
			target.Epoch++
			s.Tasks[target.ID] = target
			s.revokeGrant(v.Tasks[target.ID].GrantID, at)
		}
		for qid, q := range v.Actionables {
			if q.Kind == "assistance_approval" && q.AnswerID == a.ID && !actionableTerminal(q.State) {
				q.State = "open"
				q.Assignee = "human"
				q.WaitingReason = "assistance_expired"
				q.Text = "協助已逾時或原任務已變更；請選擇新路線，不會採用遲到結果。"
				q.Revision++
				q.UpdatedAt = at
				v.Actionables[qid] = q
			}
		}
		s.AddEvent(Principal{ID: "system"}, a.ProjectID, a.RequesterTaskID, "assistance_expired", "Assistance expired; no stale continuation was dispatched.", "", at, true)
		changed = true
	}
	for id, m := range v.Tasks {
		t := s.Tasks[id]
		g, ok := v.Grants[m.GrantID]
		if !ok || g.RevokedAt != 0 || g.ExpiresAt > at || t.Owner != "pm" || t.State == "done" || t.State == "cancelled" {
			continue
		}
		s.revokeGrant(g.ID, at)
		t.Epoch++
		t.State = "uncertain"
		s.Tasks[id] = t
		m.Readiness = "stale"
		m.Revision++
		v.Tasks[id] = m
		s.AddEvent(Principal{ID: "system"}, t.ProjectID, t.ID, "grant_expired", "Execution grant expired; review results before granting a continuation.", "", at, true)
		s.newActionable(t, "grant_expired", fmt.Sprintf("任務授權已到期（%d）；請確認已發生的結果，再決定是否續行。", g.ExpiresAt), "human", "", "", false, at)
		changed = true
	}
	return changed
}

func (s *State) RecoverCollaborationTask(taskID string, at int64) {
	s.NormalizeCollaboration()
	t := s.Tasks[taskID]
	m := s.Collaboration.Tasks[taskID]
	t.State = "uncertain"
	t.Epoch++
	t.Result = "Bridge 在執行期間重啟；請查看原對話與實際成果，系統不會自動重跑。"
	m.Readiness = "stale"
	m.Revision++
	s.revokeGrant(m.GrantID, at)
	s.Tasks[taskID] = t
	s.Collaboration.Tasks[taskID] = m
	s.AddEvent(Principal{ID: "system"}, t.ProjectID, t.ID, "execution_uncertain", t.Result, t.RequestID, at, true)
	s.newActionable(t, "execution_uncertain", t.Result, "human", "", "", false, at)
}
