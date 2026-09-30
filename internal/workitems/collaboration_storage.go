package workitems

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"everything-go/internal/coordination"
)

type collaborationRecord struct {
	Kind, ID, ProjectID, TaskID, WorkItemID, Status string
	Revision                                        uint64
	Payload                                         string
}

func recordKey(kind, id string) string { return kind + "\x00" + id }

func readCollaboration(ctx context.Context, tx *sql.Tx) (coordination.State, map[string]collaborationRecord, error) {
	s := coordination.NewState()
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT payload FROM work_pm_state WHERE id=1").Scan(&raw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return s, nil, err
	}
	if raw != "" {
		if err = json.Unmarshal([]byte(raw), &s); err != nil {
			return s, nil, err
		}
	}
	s.Normalize()
	rows, err := tx.QueryContext(ctx, "SELECT kind,id,project_id,task_id,work_item_id,revision,status,payload FROM work_collaboration_records ORDER BY kind,id")
	if err != nil {
		return s, nil, err
	}
	defer rows.Close()
	records := map[string]collaborationRecord{}
	fields := map[string]map[string]json.RawMessage{}
	for rows.Next() {
		var r collaborationRecord
		if err = rows.Scan(&r.Kind, &r.ID, &r.ProjectID, &r.TaskID, &r.WorkItemID, &r.Revision, &r.Status, &r.Payload); err != nil {
			return s, nil, err
		}
		records[recordKey(r.Kind, r.ID)] = r
		switch r.Kind {
		case "project":
			var p coordination.Project
			if err = json.Unmarshal([]byte(r.Payload), &p); err == nil {
				s.Projects[p.ID] = p
			}
		case "task":
			var t coordination.Task
			if err = json.Unmarshal([]byte(r.Payload), &t); err == nil {
				s.Tasks[t.ID] = t
			}
		case "event":
			var e coordination.Event
			if err = json.Unmarshal([]byte(r.Payload), &e); err == nil {
				s.Events = append(s.Events, e)
			}
		default:
			if !strings.HasPrefix(r.Kind, "v2.") {
				return s, nil, fmt.Errorf("unknown collaboration record kind %s", r.Kind)
			}
			name := strings.TrimPrefix(r.Kind, "v2.")
			if fields[name] == nil {
				fields[name] = map[string]json.RawMessage{}
			}
			fields[name][r.ID] = json.RawMessage(r.Payload)
		}
		if err != nil {
			return s, nil, err
		}
	}
	if err = rows.Err(); err != nil {
		return s, nil, err
	}
	if len(fields) > 0 {
		encoded, err := json.Marshal(fields)
		if err != nil {
			return s, nil, err
		}
		var v coordination.CollaborationState
		if err = json.Unmarshal(encoded, &v); err != nil {
			return s, nil, err
		}
		s.Collaboration = &v
	}
	s.NormalizeCollaboration()
	sort.Slice(s.Events, func(i, j int) bool { return s.Events[i].ID < s.Events[j].ID })
	return s, records, nil
}

// Each v2 entity is stored in its own indexed row. The legacy JSON contains
// only v1 projects/tasks/events; no v2 task has a second writable copy there.
func collaborationRecords(s coordination.State) (coordination.State, map[string]collaborationRecord, error) {
	legacy := coordination.NewState()
	legacy.Revision = s.Revision
	legacy.Receipts = s.Receipts
	records := map[string]collaborationRecord{}
	add := func(kind, id string, value any) error {
		payload, err := json.Marshal(value)
		if err != nil {
			return err
		}
		var meta struct {
			ProjectID  string `json:"project_id"`
			TaskID     string `json:"task_id"`
			WorkItemID string `json:"work_item_id"`
			Revision   uint64 `json:"revision"`
			State      string `json:"state"`
			Status     string `json:"status"`
		}
		if err = json.Unmarshal(payload, &meta); err != nil {
			return err
		}
		if kind == "project" {
			meta.ProjectID = id
		}
		if kind == "task" || kind == "v2.task_states" {
			meta.TaskID = id
		}
		status := meta.Status
		if status == "" {
			status = meta.State
		}
		r := collaborationRecord{Kind: kind, ID: id, ProjectID: meta.ProjectID, TaskID: meta.TaskID, WorkItemID: meta.WorkItemID, Revision: meta.Revision, Status: status, Payload: string(payload)}
		records[recordKey(kind, id)] = r
		return nil
	}
	for id, p := range s.Projects {
		if p.EngineVersion == 2 {
			if err := add("project", id, p); err != nil {
				return legacy, nil, err
			}
		} else {
			legacy.Projects[id] = p
		}
	}
	for id, t := range s.Tasks {
		if s.Projects[t.ProjectID].EngineVersion == 2 {
			if err := add("task", id, t); err != nil {
				return legacy, nil, err
			}
		} else {
			legacy.Tasks[id] = t
		}
	}
	for _, e := range s.Events {
		if s.Projects[e.ProjectID].EngineVersion == 2 {
			if err := add("event", strconv.FormatUint(e.ID, 10), e); err != nil {
				return legacy, nil, err
			}
		} else {
			legacy.Events = append(legacy.Events, e)
		}
	}
	if s.Collaboration != nil {
		b, err := json.Marshal(s.Collaboration)
		if err != nil {
			return legacy, nil, err
		}
		var fields map[string]map[string]json.RawMessage
		if err = json.Unmarshal(b, &fields); err != nil {
			return legacy, nil, err
		}
		for kind, items := range fields {
			for id, raw := range items {
				if err := add("v2."+kind, id, raw); err != nil {
					return legacy, nil, err
				}
			}
		}
	}
	return legacy, records, nil
}

func (s *Store) writeCollaborationRecords(ctx context.Context, tx *sql.Tx, previous, records map[string]collaborationRecord, state coordination.State) error {
	touched := map[string]bool{}
	for key := range previous {
		if _, ok := records[key]; !ok {
			return errors.New("collaboration_history_removal_forbidden")
		}
	}
	for key, r := range records {
		if old, ok := previous[key]; ok && old.Payload == r.Payload {
			continue
		}
		if r.TaskID != "" && (r.Kind == "task" || r.Kind == "v2.task_states" || r.Kind == "v2.actionables" || r.Kind == "v2.dispositions" || r.Kind == "v2.submissions") {
			touched[r.TaskID] = true
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO work_collaboration_records(kind,id,project_id,task_id,work_item_id,revision,status,payload)
		VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET project_id=excluded.project_id,task_id=excluded.task_id,work_item_id=excluded.work_item_id,revision=excluded.revision,status=excluded.status,payload=excluded.payload`, r.Kind, r.ID, r.ProjectID, r.TaskID, r.WorkItemID, r.Revision, r.Status, r.Payload); err != nil {
			return err
		}
	}
	for _, task := range state.Tasks {
		if state.Projects[task.ProjectID].EngineVersion != 2 {
			continue
		}
		item, err := getItemTx(ctx, tx, task.WorkItemID)
		if errors.Is(err, ErrNotFound) || errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		lifecycle := item.Lifecycle
		switch task.State {
		case "proposed", "approved":
			lifecycle = LifecycleReady
		case "done":
			lifecycle = LifecycleDone
		case "cancelled":
			lifecycle = LifecycleCancelled
		case "review":
			lifecycle = LifecycleReview
		case "running", "waiting", "failed", "uncertain", "provisioning", "queued":
			lifecycle = LifecycleActive
		}
		hasLabel := false
		for _, label := range item.Labels {
			if label == coordination.CollaborationCapability {
				hasLabel = true
			}
		}
		if lifecycle == item.Lifecycle && !touched[task.ID] && hasLabel {
			continue
		}
		if !hasLabel {
			item.Labels = append(item.Labels, coordination.CollaborationCapability)
		}
		actor := Actor{Type: ActorSystem, DeviceID: "collaboration-v2"}
		if lifecycle == LifecycleDone {
			meta := state.Collaboration.Tasks[task.ID]
			if task.AcceptedBy == "" || meta.AcceptedSubmissionID == "" {
				return errors.New("collaboration_acceptance_binding_required")
			}
			actor = Actor{Type: ActorUser, DeviceID: task.AcceptedBy}
			now := s.now().UnixMilli()
			item.AcceptedAt = &now
		} else {
			item.AcceptedAt = nil
		}
		item.Lifecycle = lifecycle
		item.Version++
		item.UpdatedAt = s.now().UnixMilli()
		if _, err = s.writeItemMutationTx(ctx, tx, item, "collaboration_lifecycle", actor, ChangePayload{}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) IsCollaborationItem(ctx context.Context, itemID string) (bool, error) {
	var exists bool
	err := s.store.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM work_collaboration_records WHERE kind='task' AND work_item_id=?)", itemID).Scan(&exists)
	return exists, err
}

func (s *Service) WorkRevision(ctx context.Context) (uint64, error) {
	var revision uint64
	err := s.store.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(revision),0) FROM work_changes").Scan(&revision)
	return revision, err
}

func collaborationItemTx(ctx context.Context, tx *sql.Tx, itemID string) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM work_collaboration_records WHERE kind='task' AND work_item_id=?)", itemID).Scan(&exists)
	return exists, err
}
