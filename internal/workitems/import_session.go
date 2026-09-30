package workitems

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
)

// ImportSessionDraftInput captures a verified Bridge Session as an inbox
// WorkItem. It never infers completion or advances the human-owned lifecycle.
type ImportSessionDraftInput struct {
	ProjectID          string
	ProjectName        string
	WorkspacePath      string
	WorkItemID         string
	SessionID          string
	ThreadIDSnapshot   string
	Title              string
	Description        string
	Outcome            string
	NextStep           string
	AcceptanceCriteria string
	Priority           Priority
	SortKey            int64
	Actor              Actor
}

type ImportSessionDraftResult struct {
	Project       Project
	Item          WorkItem
	Link          SessionLink
	FirstRevision uint64
	AlreadyLinked bool
}

// ImportSessionDraft creates (when needed) the project, inbox item and primary
// Session link in one transaction. A retry with a new mutation ID returns the
// existing linked item rather than producing a duplicate draft.
func (s *Store) ImportSessionDraft(ctx context.Context, in ImportSessionDraftInput) (ImportSessionDraftResult, error) {
	title, err := normalizeTitle(in.Title)
	if err != nil {
		return ImportSessionDraftResult{}, err
	}
	if strings.TrimSpace(in.SessionID) == "" {
		return ImportSessionDraftResult{}, errors.New("session id is required")
	}
	if in.Priority == "" {
		in.Priority = PriorityNone
	}
	if !validPriority(in.Priority) {
		return ImportSessionDraftResult{}, errors.New("invalid work item priority")
	}
	if in.ProjectID == "" {
		in.ProjectID = newID("wp")
	}
	if in.WorkItemID == "" {
		in.WorkItemID = newID("wi")
	}
	if in.SortKey == 0 {
		in.SortKey = 1_000_000
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ImportSessionDraftResult{}, err
	}
	defer tx.Rollback()

	if existing, ok, err := s.importedSessionTx(ctx, tx, in.SessionID); err != nil {
		return ImportSessionDraftResult{}, err
	} else if ok {
		existing.AlreadyLinked = true
		return existing, nil
	}

	now := s.now().UnixMilli()
	project, createdProject, err := s.ensureImportProjectTx(ctx, tx, in, now)
	if err != nil {
		return ImportSessionDraftResult{}, err
	}
	firstRevision := uint64(0)
	if createdProject {
		firstRevision, err = recordChange(ctx, tx, "project", project.ID, "created", project, now)
		if err != nil {
			return ImportSessionDraftResult{}, err
		}
	}

	item := WorkItem{
		ID: in.WorkItemID, ProjectID: project.ID, Title: title,
		Description: strings.TrimSpace(in.Description), Outcome: strings.TrimSpace(in.Outcome),
		NextStep: strings.TrimSpace(in.NextStep), AcceptanceCriteria: strings.TrimSpace(in.AcceptanceCriteria),
		Lifecycle: LifecycleInbox, Priority: in.Priority, SortKey: in.SortKey,
		Labels: []string{}, AutomationMode: "manual",
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_items
		(id,project_id,title,description,outcome,next_step,acceptance_criteria,lifecycle,priority,sort_key,
		 labels,automation_mode,version,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, item.ID, item.ProjectID, item.Title, item.Description,
		item.Outcome, item.NextStep, item.AcceptanceCriteria, item.Lifecycle, item.Priority,
		item.SortKey, labelsJSON(item.Labels), item.AutomationMode, item.Version, now, now); err != nil {
		return ImportSessionDraftResult{}, err
	}
	link := SessionLink{
		ID: newID("wl"), WorkItemID: item.ID, SessionID: strings.TrimSpace(in.SessionID),
		ThreadIDSnapshot: strings.TrimSpace(in.ThreadIDSnapshot), Role: "primary", LinkedAt: now,
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_session_links
		(id,work_item_id,session_id,thread_id_snapshot,role,linked_at) VALUES(?,?,?,?,?,?)`,
		link.ID, link.WorkItemID, link.SessionID, link.ThreadIDSnapshot, link.Role, link.LinkedAt); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ImportSessionDraftResult{}, ErrSessionLinked
		}
		return ImportSessionDraftResult{}, err
	}
	revision, err := recordChange(ctx, tx, "work_item", item.ID, "session_imported", ChangePayload{Item: &item, Link: &link}, now)
	if err != nil {
		return ImportSessionDraftResult{}, err
	}
	if firstRevision == 0 {
		firstRevision = revision
	}
	item.ActivityRevision = revision
	if _, err := tx.ExecContext(ctx, "UPDATE work_items SET activity_revision=? WHERE id=?", revision, item.ID); err != nil {
		return ImportSessionDraftResult{}, err
	}
	activity, err := insertActivity(ctx, tx, revision, item.ID, "session_imported", in.Actor, item, now)
	if err != nil {
		return ImportSessionDraftResult{}, err
	}
	if err := updateChangePayload(ctx, tx, revision, ChangePayload{Item: &item, Link: &link, Activity: &activity}); err != nil {
		return ImportSessionDraftResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ImportSessionDraftResult{}, err
	}
	return ImportSessionDraftResult{Project: project, Item: item, Link: link, FirstRevision: firstRevision}, nil
}

func (s *Store) ensureImportProjectTx(ctx context.Context, tx *sql.Tx, in ImportSessionDraftInput, now int64) (Project, bool, error) {
	var project Project
	err := tx.QueryRowContext(ctx, `SELECT id,authority_instance_id,name,workspace_path,context,version,created_at,updated_at,archived_at
		FROM work_projects WHERE id=?`, in.ProjectID).Scan(&project.ID, &project.AuthorityInstanceID,
		&project.Name, &project.WorkspacePath, &project.Context, &project.Version, &project.CreatedAt, &project.UpdatedAt, &project.ArchivedAt)
	if err == nil {
		if project.AuthorityInstanceID != s.instanceID || project.ArchivedAt != nil {
			return Project{}, false, errors.New("project is unavailable for this authority")
		}
		return project, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Project{}, false, err
	}
	name := strings.TrimSpace(in.ProjectName)
	if name == "" {
		name = filepath.Base(strings.TrimRight(strings.TrimSpace(in.WorkspacePath), "/"))
	}
	if name == "" || name == "." {
		return Project{}, false, errors.New("project name is required")
	}
	project = Project{ID: in.ProjectID, AuthorityInstanceID: s.instanceID, Name: name,
		WorkspacePath: strings.TrimSpace(in.WorkspacePath), Version: 1, CreatedAt: now, UpdatedAt: now}
	_, err = tx.ExecContext(ctx, `INSERT INTO work_projects
		(id,authority_instance_id,name,workspace_path,context,version,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?)`, project.ID, project.AuthorityInstanceID, project.Name,
		project.WorkspacePath, project.Context, project.Version, now, now)
	return project, true, err
}

func (s *Store) importedSessionTx(ctx context.Context, tx *sql.Tx, sessionID string) (ImportSessionDraftResult, bool, error) {
	var link SessionLink
	err := tx.QueryRowContext(ctx, `SELECT id,work_item_id,session_id,thread_id_snapshot,role,linked_at,unlinked_at
		FROM work_item_session_links WHERE session_id=? AND unlinked_at IS NULL LIMIT 1`, sessionID).Scan(
		&link.ID, &link.WorkItemID, &link.SessionID, &link.ThreadIDSnapshot, &link.Role, &link.LinkedAt, &link.UnlinkedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ImportSessionDraftResult{}, false, nil
	}
	if err != nil {
		return ImportSessionDraftResult{}, false, err
	}
	item, err := getItemTx(ctx, tx, link.WorkItemID)
	if err != nil {
		return ImportSessionDraftResult{}, false, err
	}
	var project Project
	err = tx.QueryRowContext(ctx, `SELECT id,authority_instance_id,name,workspace_path,context,version,created_at,updated_at,archived_at
		FROM work_projects WHERE id=?`, item.ProjectID).Scan(&project.ID, &project.AuthorityInstanceID,
		&project.Name, &project.WorkspacePath, &project.Context, &project.Version, &project.CreatedAt, &project.UpdatedAt, &project.ArchivedAt)
	return ImportSessionDraftResult{Project: project, Item: item, Link: link}, err == nil, err
}
