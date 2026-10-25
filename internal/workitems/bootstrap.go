package workitems

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	bootstrapStatusReview  = "review"
	bootstrapStatusApplied = "applied"
	bootstrapContextStart  = "<!-- bridge-project-bootstrap:start -->"
	bootstrapContextEnd    = "<!-- bridge-project-bootstrap:end -->"
)

type SaveBootstrapInput struct {
	ProjectID          string
	ProjectVersion     uint64
	ProjectName        string
	WorkspacePath      string
	Fingerprint        string
	Objective          string
	CurrentState       string
	NextStep           string
	AcceptanceCriteria string
	Constraints        []string
	Decisions          []string
	OpenQuestions      []string
	Suggestions        []BootstrapSuggestion
	Sources            []BootstrapSource
	SessionIDs         []string
}

type SaveBootstrapResult struct {
	Draft         ProjectBootstrapDraft
	FirstRevision uint64
	Changed       bool
}

type ApproveBootstrapInput struct {
	BootstrapID           string
	ExpectedVersion       uint64
	ProjectName           string
	Objective             string
	CurrentState          string
	NextStep              string
	AcceptanceCriteria    string
	SelectedSuggestionIDs []string
	Actor                 Actor
}

type ApproveBootstrapResult struct {
	Draft          ProjectBootstrapDraft
	Project        Project
	Items          []WorkItem
	Links          []SessionLink
	FirstRevision  uint64
	LastRevision   uint64
	AlreadyApplied bool
}

func (s *Store) SaveBootstrapDraft(ctx context.Context, in SaveBootstrapInput) (SaveBootstrapResult, error) {
	in.ProjectName = strings.TrimSpace(in.ProjectName)
	in.WorkspacePath = strings.TrimSpace(in.WorkspacePath)
	in.Fingerprint = strings.TrimSpace(in.Fingerprint)
	if in.ProjectName == "" || in.WorkspacePath == "" || in.Fingerprint == "" {
		return SaveBootstrapResult{}, errors.New("project name, workspace path and fingerprint are required")
	}
	if len(in.Suggestions) > 5 || len(in.Sources) > 16 || len(in.SessionIDs) > 5 {
		return SaveBootstrapResult{}, errors.New("bootstrap evidence exceeds bounded limits")
	}
	for i := range in.Suggestions {
		in.Suggestions[i].EvidenceRefs = cleanStrings(in.Suggestions[i].EvidenceRefs, 8)
	}
	in.Constraints = cleanStrings(in.Constraints, 20)
	in.Decisions = cleanStrings(in.Decisions, 20)
	in.OpenQuestions = cleanStrings(in.OpenQuestions, 20)
	in.SessionIDs = cleanStrings(in.SessionIDs, 5)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SaveBootstrapResult{}, err
	}
	defer tx.Rollback()

	existing, found, err := getBootstrapByWorkspaceTx(ctx, tx, s.instanceID, in.WorkspacePath)
	if err != nil {
		return SaveBootstrapResult{}, err
	}
	if found && existing.Fingerprint == in.Fingerprint && existing.ProjectID == strings.TrimSpace(in.ProjectID) && existing.ProjectVersion == in.ProjectVersion && existing.Status != "failed" {
		return SaveBootstrapResult{Draft: existing, Changed: false}, nil
	}
	now := s.now().UnixMilli()
	draft := ProjectBootstrapDraft{
		ID: newID("pb"), AuthorityInstanceID: s.instanceID, ProjectID: strings.TrimSpace(in.ProjectID),
		ProjectVersion: in.ProjectVersion, ProjectName: in.ProjectName, WorkspacePath: in.WorkspacePath,
		Status: bootstrapStatusReview, Fingerprint: in.Fingerprint,
		Objective: strings.TrimSpace(in.Objective), CurrentState: strings.TrimSpace(in.CurrentState),
		NextStep: strings.TrimSpace(in.NextStep), AcceptanceCriteria: strings.TrimSpace(in.AcceptanceCriteria),
		Constraints: in.Constraints, Decisions: in.Decisions, OpenQuestions: in.OpenQuestions,
		Suggestions: in.Suggestions, Sources: in.Sources, SessionIDs: in.SessionIDs,
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if found {
		draft.ID = existing.ID
		draft.Version = existing.Version + 1
		draft.CreatedAt = existing.CreatedAt
	}
	if err := writeBootstrapTx(ctx, tx, draft, found); err != nil {
		return SaveBootstrapResult{}, err
	}
	revision, err := recordChange(ctx, tx, "bootstrap", draft.ID, "bootstrap_ready", draft, now)
	if err != nil {
		return SaveBootstrapResult{}, err
	}
	if err := pruneBootstrapDraftsTx(ctx, tx, s.instanceID, now); err != nil {
		return SaveBootstrapResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SaveBootstrapResult{}, err
	}
	return SaveBootstrapResult{Draft: draft, FirstRevision: revision, Changed: true}, nil
}

// Keep at most one bounded draft per recent workspace. Formal Project context
// remains durable after an old applied draft is pruned.
func pruneBootstrapDraftsTx(ctx context.Context, tx *sql.Tx, authority string, now int64) error {
	rows, err := tx.QueryContext(ctx, bootstrapSelect+` WHERE authority_instance_id=? ORDER BY updated_at DESC,id LIMIT 16 OFFSET 128`, authority)
	if err != nil {
		return err
	}
	var drafts []ProjectBootstrapDraft
	for rows.Next() {
		draft, err := scanBootstrap(rows)
		if err != nil {
			rows.Close()
			return err
		}
		drafts = append(drafts, draft)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, draft := range drafts {
		if _, err := tx.ExecContext(ctx, `DELETE FROM work_project_bootstraps WHERE id=?`, draft.ID); err != nil {
			return err
		}
		if _, err := recordChange(ctx, tx, "bootstrap", draft.ID, "bootstrap_removed", draft, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ApproveBootstrap(ctx context.Context, in ApproveBootstrapInput) (ApproveBootstrapResult, error) {
	if strings.TrimSpace(in.BootstrapID) == "" {
		return ApproveBootstrapResult{}, errors.New("bootstrap id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ApproveBootstrapResult{}, err
	}
	defer tx.Rollback()
	draft, err := getBootstrapTx(ctx, tx, in.BootstrapID)
	if err != nil {
		return ApproveBootstrapResult{}, err
	}
	if draft.AuthorityInstanceID != s.instanceID {
		return ApproveBootstrapResult{}, errors.New("bootstrap belongs to another authority")
	}
	if draft.Status == bootstrapStatusApplied {
		project, err := getProjectQuery(ctx, tx, draft.ProjectID)
		return ApproveBootstrapResult{Draft: draft, Project: project, AlreadyApplied: true}, err
	}
	if draft.Version != in.ExpectedVersion {
		return ApproveBootstrapResult{}, fmt.Errorf("bootstrap version conflict: expected %d current %d", in.ExpectedVersion, draft.Version)
	}

	selected := make(map[string]bool, len(in.SelectedSuggestionIDs))
	for _, id := range cleanStrings(in.SelectedSuggestionIDs, 5) {
		selected[id] = true
	}
	known := make(map[string]bool, len(draft.Suggestions))
	for _, suggestion := range draft.Suggestions {
		known[suggestion.ID] = true
	}
	for id := range selected {
		if !known[id] {
			return ApproveBootstrapResult{}, errors.New("selected bootstrap suggestion is stale or unknown")
		}
	}

	if strings.TrimSpace(in.ProjectName) != "" {
		draft.ProjectName = strings.TrimSpace(in.ProjectName)
	}
	draft.Objective = strings.TrimSpace(in.Objective)
	draft.CurrentState = strings.TrimSpace(in.CurrentState)
	draft.NextStep = strings.TrimSpace(in.NextStep)
	draft.AcceptanceCriteria = strings.TrimSpace(in.AcceptanceCriteria)
	now := s.now().UnixMilli()
	contextSection := renderBootstrapContext(draft)

	var project Project
	projectCreated := false
	if draft.ProjectID != "" {
		project, err = getProjectQuery(ctx, tx, draft.ProjectID)
		if err != nil {
			return ApproveBootstrapResult{}, err
		}
		if project.AuthorityInstanceID != s.instanceID || project.ArchivedAt != nil {
			return ApproveBootstrapResult{}, errors.New("project is unavailable for this authority")
		}
		if draft.ProjectVersion > 0 && project.Version != draft.ProjectVersion {
			return ApproveBootstrapResult{}, fmt.Errorf("project changed after bootstrap draft: expected %d current %d", draft.ProjectVersion, project.Version)
		}
		project.Name = draft.ProjectName
		project.WorkspacePath = draft.WorkspacePath
		project.Context = mergeBootstrapContext(project.Context, contextSection)
		project.Version++
		project.UpdatedAt = now
		if _, err := tx.ExecContext(ctx, `UPDATE work_projects SET name=?,workspace_path=?,context=?,version=?,updated_at=? WHERE id=?`,
			project.Name, project.WorkspacePath, project.Context, project.Version, now, project.ID); err != nil {
			return ApproveBootstrapResult{}, err
		}
	} else {
		projectCreated = true
		project = Project{ID: newID("wp"), AuthorityInstanceID: s.instanceID, Name: draft.ProjectName,
			WorkspacePath: draft.WorkspacePath, Context: contextSection, Version: 1, CreatedAt: now, UpdatedAt: now}
		if _, err := tx.ExecContext(ctx, `INSERT INTO work_projects
			(id,authority_instance_id,name,workspace_path,context,version,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`,
			project.ID, project.AuthorityInstanceID, project.Name, project.WorkspacePath, project.Context,
			project.Version, now, now); err != nil {
			return ApproveBootstrapResult{}, err
		}
	}
	firstRevision, err := recordChange(ctx, tx, "project", project.ID, map[bool]string{true: "created", false: "bootstrap_applied"}[projectCreated], project, now)
	if err != nil {
		return ApproveBootstrapResult{}, err
	}
	lastRevision := firstRevision
	result := ApproveBootstrapResult{Project: project, FirstRevision: firstRevision, Items: []WorkItem{}, Links: []SessionLink{}}

	for index, suggestion := range draft.Suggestions {
		if !selected[suggestion.ID] {
			continue
		}
		if suggestion.SessionID != "" {
			var linked int
			err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM work_item_session_links WHERE session_id=? AND unlinked_at IS NULL`, suggestion.SessionID).Scan(&linked)
			if err != nil {
				return ApproveBootstrapResult{}, err
			}
			if linked > 0 {
				continue
			}
		}
		title, err := normalizeTitle(suggestion.Title)
		if err != nil {
			return ApproveBootstrapResult{}, err
		}
		var collision int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM work_items WHERE id=?`, suggestion.WorkItemID).Scan(&collision); err != nil {
			return ApproveBootstrapResult{}, err
		}
		if collision > 0 {
			return ApproveBootstrapResult{}, errors.New("bootstrap work item identity already exists")
		}
		item := WorkItem{ID: suggestion.WorkItemID, ProjectID: project.ID, Title: title,
			Description: strings.TrimSpace(suggestion.Description), Outcome: strings.TrimSpace(suggestion.Outcome),
			NextStep: strings.TrimSpace(suggestion.NextStep), AcceptanceCriteria: strings.TrimSpace(suggestion.AcceptanceCriteria),
			Lifecycle: LifecycleInbox, Priority: PriorityNone, SortKey: now + int64(index), Version: 1,
			Labels: []string{"bootstrap"}, AutomationMode: "manual", CreatedAt: now, UpdatedAt: now}
		if _, err := tx.ExecContext(ctx, `INSERT INTO work_items
			(id,project_id,title,description,outcome,next_step,acceptance_criteria,lifecycle,priority,sort_key,
			labels,automation_mode,version,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			item.ID, item.ProjectID, item.Title, item.Description, item.Outcome, item.NextStep,
			item.AcceptanceCriteria, item.Lifecycle, item.Priority, item.SortKey, labelsJSON(item.Labels),
			item.AutomationMode, item.Version, now, now); err != nil {
			return ApproveBootstrapResult{}, err
		}
		var link *SessionLink
		if suggestion.SessionID != "" {
			created := SessionLink{ID: newID("wl"), WorkItemID: item.ID, SessionID: suggestion.SessionID,
				Role: "primary", LinkedAt: now}
			if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_session_links
				(id,work_item_id,session_id,thread_id_snapshot,role,linked_at) VALUES(?,?,?,?,?,?)`,
				created.ID, created.WorkItemID, created.SessionID, created.ThreadIDSnapshot, created.Role, created.LinkedAt); err != nil {
				return ApproveBootstrapResult{}, err
			}
			link = &created
			result.Links = append(result.Links, created)
		}
		revision, err := recordChange(ctx, tx, "work_item", item.ID, "bootstrap_item_created", ChangePayload{Item: &item, Link: link}, now)
		if err != nil {
			return ApproveBootstrapResult{}, err
		}
		item.ActivityRevision = revision
		if _, err := tx.ExecContext(ctx, "UPDATE work_items SET activity_revision=? WHERE id=?", revision, item.ID); err != nil {
			return ApproveBootstrapResult{}, err
		}
		activity, err := insertActivity(ctx, tx, revision, item.ID, "bootstrap_item_created", in.Actor, item, now)
		if err != nil {
			return ApproveBootstrapResult{}, err
		}
		if err := updateChangePayload(ctx, tx, revision, ChangePayload{Item: &item, Link: link, Activity: &activity}); err != nil {
			return ApproveBootstrapResult{}, err
		}
		lastRevision = revision
		result.Items = append(result.Items, item)
	}

	draft.ProjectID = project.ID
	draft.ProjectVersion = project.Version
	draft.Status = bootstrapStatusApplied
	draft.Version++
	draft.UpdatedAt = now
	draft.AppliedAt = &now
	if err := writeBootstrapTx(ctx, tx, draft, true); err != nil {
		return ApproveBootstrapResult{}, err
	}
	lastRevision, err = recordChange(ctx, tx, "bootstrap", draft.ID, "bootstrap_applied", draft, now)
	if err != nil {
		return ApproveBootstrapResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ApproveBootstrapResult{}, err
	}
	result.Draft = draft
	result.LastRevision = lastRevision
	return result, nil
}

func (s *Store) GetBootstrap(ctx context.Context, id string) (ProjectBootstrapDraft, error) {
	return getBootstrapQuery(ctx, s.db, id)
}

func (s *Store) GetProject(ctx context.Context, id string) (Project, error) {
	return getProjectQuery(ctx, s.db, id)
}

func listBootstraps(ctx context.Context, db *sql.DB, authority string) ([]ProjectBootstrapDraft, error) {
	rows, err := db.QueryContext(ctx, bootstrapSelect+` WHERE authority_instance_id=? ORDER BY updated_at DESC,id`, authority)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var drafts []ProjectBootstrapDraft
	for rows.Next() {
		draft, err := scanBootstrap(rows)
		if err != nil {
			return nil, err
		}
		drafts = append(drafts, draft)
	}
	return drafts, rows.Err()
}

const bootstrapSelect = `SELECT id,authority_instance_id,project_id,project_version,project_name,workspace_path,status,
	fingerprint,objective,current_state,next_step,acceptance_criteria,constraints_json,decisions_json,
	open_questions_json,suggestions_json,sources_json,session_ids_json,version,created_at,updated_at,applied_at
	FROM work_project_bootstraps`

func scanBootstrap(row rowScanner) (ProjectBootstrapDraft, error) {
	var draft ProjectBootstrapDraft
	var constraints, decisions, questions, suggestions, sources, sessions string
	err := row.Scan(&draft.ID, &draft.AuthorityInstanceID, &draft.ProjectID, &draft.ProjectVersion,
		&draft.ProjectName, &draft.WorkspacePath, &draft.Status, &draft.Fingerprint, &draft.Objective,
		&draft.CurrentState, &draft.NextStep, &draft.AcceptanceCriteria, &constraints, &decisions,
		&questions, &suggestions, &sources, &sessions, &draft.Version, &draft.CreatedAt,
		&draft.UpdatedAt, &draft.AppliedAt)
	if err != nil {
		return draft, err
	}
	_ = json.Unmarshal([]byte(constraints), &draft.Constraints)
	_ = json.Unmarshal([]byte(decisions), &draft.Decisions)
	_ = json.Unmarshal([]byte(questions), &draft.OpenQuestions)
	_ = json.Unmarshal([]byte(suggestions), &draft.Suggestions)
	_ = json.Unmarshal([]byte(sources), &draft.Sources)
	_ = json.Unmarshal([]byte(sessions), &draft.SessionIDs)
	if draft.Constraints == nil {
		draft.Constraints = []string{}
	}
	if draft.Decisions == nil {
		draft.Decisions = []string{}
	}
	if draft.OpenQuestions == nil {
		draft.OpenQuestions = []string{}
	}
	if draft.Suggestions == nil {
		draft.Suggestions = []BootstrapSuggestion{}
	}
	if draft.Sources == nil {
		draft.Sources = []BootstrapSource{}
	}
	if draft.SessionIDs == nil {
		draft.SessionIDs = []string{}
	}
	return draft, nil
}

func getBootstrapTx(ctx context.Context, tx *sql.Tx, id string) (ProjectBootstrapDraft, error) {
	draft, err := scanBootstrap(tx.QueryRowContext(ctx, bootstrapSelect+" WHERE id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return ProjectBootstrapDraft{}, ErrNotFound
	}
	return draft, err
}

func getBootstrapQuery(ctx context.Context, q queryRower, id string) (ProjectBootstrapDraft, error) {
	draft, err := scanBootstrap(q.QueryRowContext(ctx, bootstrapSelect+" WHERE id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return ProjectBootstrapDraft{}, ErrNotFound
	}
	return draft, err
}

func getBootstrapByWorkspaceTx(ctx context.Context, tx *sql.Tx, authority, workspace string) (ProjectBootstrapDraft, bool, error) {
	draft, err := scanBootstrap(tx.QueryRowContext(ctx, bootstrapSelect+" WHERE authority_instance_id=? AND workspace_path=?", authority, workspace))
	if errors.Is(err, sql.ErrNoRows) {
		return ProjectBootstrapDraft{}, false, nil
	}
	return draft, err == nil, err
}

func writeBootstrapTx(ctx context.Context, tx *sql.Tx, draft ProjectBootstrapDraft, update bool) error {
	values := []any{draft.ProjectID, draft.ProjectVersion, draft.ProjectName, draft.WorkspacePath, draft.Status,
		draft.Fingerprint, draft.Objective, draft.CurrentState, draft.NextStep, draft.AcceptanceCriteria,
		jsonText(draft.Constraints), jsonText(draft.Decisions), jsonText(draft.OpenQuestions),
		jsonText(draft.Suggestions), jsonText(draft.Sources), jsonText(draft.SessionIDs), draft.Version,
		draft.UpdatedAt, draft.AppliedAt}
	if update {
		values = append(values, draft.ID)
		_, err := tx.ExecContext(ctx, `UPDATE work_project_bootstraps SET project_id=?,project_version=?,project_name=?,
			workspace_path=?,status=?,fingerprint=?,objective=?,current_state=?,next_step=?,acceptance_criteria=?,
			constraints_json=?,decisions_json=?,open_questions_json=?,suggestions_json=?,sources_json=?,session_ids_json=?,
			version=?,updated_at=?,applied_at=? WHERE id=?`, values...)
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO work_project_bootstraps
		(id,authority_instance_id,project_id,project_version,project_name,workspace_path,status,fingerprint,
		 objective,current_state,next_step,acceptance_criteria,constraints_json,decisions_json,open_questions_json,
		 suggestions_json,sources_json,session_ids_json,version,created_at,updated_at,applied_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, draft.ID, draft.AuthorityInstanceID,
		draft.ProjectID, draft.ProjectVersion, draft.ProjectName, draft.WorkspacePath, draft.Status, draft.Fingerprint,
		draft.Objective, draft.CurrentState, draft.NextStep, draft.AcceptanceCriteria, jsonText(draft.Constraints),
		jsonText(draft.Decisions), jsonText(draft.OpenQuestions), jsonText(draft.Suggestions), jsonText(draft.Sources),
		jsonText(draft.SessionIDs), draft.Version, draft.CreatedAt, draft.UpdatedAt, draft.AppliedAt)
	return err
}

func getProjectQuery(ctx context.Context, q queryRower, id string) (Project, error) {
	var project Project
	err := q.QueryRowContext(ctx, `SELECT id,authority_instance_id,name,workspace_path,context,version,created_at,updated_at,archived_at
		FROM work_projects WHERE id=?`, id).Scan(&project.ID, &project.AuthorityInstanceID, &project.Name,
		&project.WorkspacePath, &project.Context, &project.Version, &project.CreatedAt, &project.UpdatedAt, &project.ArchivedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Project{}, ErrNotFound
	}
	return project, err
}

func renderBootstrapContext(draft ProjectBootstrapDraft) string {
	var b strings.Builder
	b.WriteString(bootstrapContextStart)
	b.WriteString("\n## Project objective\n")
	b.WriteString(draft.Objective)
	b.WriteString("\n\n## Verified current state\n")
	b.WriteString(draft.CurrentState)
	b.WriteString("\n\n## Current next step\n")
	b.WriteString(draft.NextStep)
	b.WriteString("\n\n## Acceptance criteria\n")
	b.WriteString(draft.AcceptanceCriteria)
	if len(draft.Constraints) > 0 {
		b.WriteString("\n\n## Constraints\n- ")
		b.WriteString(strings.Join(draft.Constraints, "\n- "))
	}
	if len(draft.Decisions) > 0 {
		b.WriteString("\n\n## Evidence-backed project signals\n- ")
		b.WriteString(strings.Join(draft.Decisions, "\n- "))
	}
	b.WriteString("\n")
	b.WriteString(bootstrapContextEnd)
	return strings.TrimSpace(b.String())
}

func mergeBootstrapContext(existing, generated string) string {
	existing = strings.TrimSpace(existing)
	start := strings.Index(existing, bootstrapContextStart)
	end := strings.Index(existing, bootstrapContextEnd)
	if start >= 0 && end >= start {
		end += len(bootstrapContextEnd)
		return strings.TrimSpace(existing[:start] + generated + existing[end:])
	}
	if existing == "" {
		return generated
	}
	return existing + "\n\n" + generated
}

func cleanStrings(values []string, max int) []string {
	seen := map[string]bool{}
	out := make([]string, 0, min(len(values), max))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
		if len(out) >= max {
			break
		}
	}
	return out
}

func jsonText(value any) string {
	body, _ := json.Marshal(value)
	return string(body)
}
