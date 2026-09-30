package workitems

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 7

const schema = `
CREATE TABLE IF NOT EXISTS work_pm_state (
  id INTEGER PRIMARY KEY CHECK(id=1),
  payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS work_collaboration_records (
  kind TEXT NOT NULL,
  id TEXT NOT NULL,
  project_id TEXT NOT NULL DEFAULT '',
  task_id TEXT NOT NULL DEFAULT '',
  work_item_id TEXT NOT NULL DEFAULT '',
  revision INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT '',
  payload TEXT NOT NULL,
  PRIMARY KEY(kind,id)
);
CREATE INDEX IF NOT EXISTS work_collaboration_pending ON work_collaboration_records(project_id,kind,status);
CREATE INDEX IF NOT EXISTS work_collaboration_task ON work_collaboration_records(task_id,kind,revision);
CREATE INDEX IF NOT EXISTS work_collaboration_item ON work_collaboration_records(work_item_id,kind);
CREATE TABLE IF NOT EXISTS work_schema (
  version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS work_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS work_projects (
  id TEXT PRIMARY KEY,
  authority_instance_id TEXT NOT NULL,
  name TEXT NOT NULL,
  workspace_path TEXT NOT NULL DEFAULT '',
  context TEXT NOT NULL DEFAULT '',
  version INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  archived_at INTEGER
);

CREATE TABLE IF NOT EXISTS work_items (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES work_projects(id),
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  outcome TEXT NOT NULL DEFAULT '',
  next_step TEXT NOT NULL DEFAULT '',
  acceptance_criteria TEXT NOT NULL DEFAULT '',
  lifecycle TEXT NOT NULL CHECK (lifecycle IN ('inbox','ready','active','review','done','cancelled')),
  priority TEXT NOT NULL CHECK (priority IN ('none','low','medium','high','urgent')),
  sort_key INTEGER NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  activity_revision INTEGER NOT NULL DEFAULT 0,
  blocked_reason_code TEXT NOT NULL DEFAULT '',
  blocked_note TEXT NOT NULL DEFAULT '',
  assignee TEXT NOT NULL DEFAULT '',
  due_at INTEGER,
  labels TEXT NOT NULL DEFAULT '[]',
  automation_mode TEXT NOT NULL DEFAULT 'manual' CHECK (automation_mode IN ('manual','auto')),
  workflow_id TEXT NOT NULL DEFAULT '',
  workflow_node_id TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  accepted_at INTEGER,
  cancelled_at INTEGER,
  archived_at INTEGER
);

CREATE INDEX IF NOT EXISTS work_items_project_column
  ON work_items(project_id, lifecycle, sort_key, id);
CREATE INDEX IF NOT EXISTS work_items_search_projection
  ON work_items(project_id, title COLLATE NOCASE, updated_at DESC);

CREATE TABLE IF NOT EXISTS work_item_session_links (
  id TEXT PRIMARY KEY,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  session_id TEXT NOT NULL,
  thread_id_snapshot TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL CHECK (role IN ('primary','support','research','verification')),
  linked_at INTEGER NOT NULL,
  unlinked_at INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS work_link_one_active_item_per_session
  ON work_item_session_links(session_id) WHERE unlinked_at IS NULL;
CREATE INDEX IF NOT EXISTS work_links_item ON work_item_session_links(work_item_id, linked_at, id);

CREATE TABLE IF NOT EXISTS work_item_runs (
  id TEXT PRIMARY KEY,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  session_link_id TEXT REFERENCES work_item_session_links(id),
  request_id TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL CHECK (kind IN ('implementation','research','verification')),
  status TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  finished_at INTEGER,
  terminal_reason TEXT NOT NULL DEFAULT ''
  ,session_id TEXT NOT NULL DEFAULT ''
  ,instruction TEXT NOT NULL DEFAULT ''
  ,available_at INTEGER NOT NULL DEFAULT 0
  ,attempt INTEGER NOT NULL DEFAULT 0
  ,max_attempts INTEGER NOT NULL DEFAULT 3
  ,claimed_at INTEGER
  ,queue_reason TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS work_workflows (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES work_projects(id),
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  version INTEGER NOT NULL DEFAULT 1,
  definition TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  archived_at INTEGER
);
CREATE INDEX IF NOT EXISTS work_workflows_project ON work_workflows(project_id, updated_at, id);

CREATE TABLE IF NOT EXISTS work_item_comments (
  id TEXT PRIMARY KEY,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  author_type TEXT NOT NULL,
  author_device_id TEXT NOT NULL DEFAULT '',
  body TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  edited_at INTEGER,
  deleted_at INTEGER
);

CREATE TABLE IF NOT EXISTS work_item_dependencies (
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  depends_on_id TEXT NOT NULL REFERENCES work_items(id),
  created_at INTEGER NOT NULL,
  PRIMARY KEY(work_item_id, depends_on_id),
  CHECK(work_item_id <> depends_on_id)
);

CREATE TABLE IF NOT EXISTS work_item_attachments (
  id TEXT PRIMARY KEY,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  comment_id TEXT REFERENCES work_item_comments(id),
  attachment_id TEXT NOT NULL,
  display_name TEXT NOT NULL DEFAULT '',
  sort_key INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  removed_at INTEGER
);

CREATE TABLE IF NOT EXISTS work_item_activity (
  revision INTEGER PRIMARY KEY,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  kind TEXT NOT NULL,
  actor TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS work_changes (
  revision INTEGER PRIMARY KEY AUTOINCREMENT,
  entity TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS work_device_cursors (
  device_id TEXT NOT NULL,
  authority_instance_id TEXT NOT NULL,
  delivered_revision INTEGER NOT NULL DEFAULT 0,
  acked_revision INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(device_id, authority_instance_id)
);

CREATE TABLE IF NOT EXISTS work_item_reads (
  device_id TEXT NOT NULL,
  work_item_id TEXT NOT NULL REFERENCES work_items(id),
  read_activity_revision INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(device_id, work_item_id)
);

CREATE TABLE IF NOT EXISTS work_mutation_dedup (
  device_id TEXT NOT NULL,
  mutation_id TEXT NOT NULL,
  response TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY(device_id, mutation_id)
);

CREATE TABLE IF NOT EXISTS work_project_bootstraps (
  id TEXT PRIMARY KEY,
  authority_instance_id TEXT NOT NULL,
  project_id TEXT NOT NULL DEFAULT '',
  project_version INTEGER NOT NULL DEFAULT 0,
  project_name TEXT NOT NULL,
  workspace_path TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('review','applied','stale','failed')),
  fingerprint TEXT NOT NULL,
  objective TEXT NOT NULL DEFAULT '',
  current_state TEXT NOT NULL DEFAULT '',
  next_step TEXT NOT NULL DEFAULT '',
  acceptance_criteria TEXT NOT NULL DEFAULT '',
  constraints_json TEXT NOT NULL DEFAULT '[]',
  decisions_json TEXT NOT NULL DEFAULT '[]',
  open_questions_json TEXT NOT NULL DEFAULT '[]',
  suggestions_json TEXT NOT NULL DEFAULT '[]',
  sources_json TEXT NOT NULL DEFAULT '[]',
  session_ids_json TEXT NOT NULL DEFAULT '[]',
  version INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  applied_at INTEGER,
  UNIQUE(authority_instance_id, workspace_path)
);
CREATE INDEX IF NOT EXISTS work_bootstraps_updated
  ON work_project_bootstraps(authority_instance_id, updated_at DESC, id);
`

type Store struct {
	db         *sql.DB
	dbPath     string
	instanceID string
	now        func() time.Time
}

func Open(dataDir, instanceID string) (*Store, error) {
	if strings.TrimSpace(instanceID) == "" {
		return nil, errors.New("authority instance id is required")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, "everything_go_work_items.db")
	dsn := "file:" + path +
		"?_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db, dbPath: path, instanceID: instanceID, now: time.Now}
	if err := store.backupLegacySchema(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) backupLegacySchema(ctx context.Context) error {
	var version int
	err := s.db.QueryRowContext(ctx, "SELECT version FROM work_schema LIMIT 1").Scan(&version)
	if errors.Is(err, sql.ErrNoRows) || (err != nil && strings.Contains(err.Error(), "no such table")) {
		return nil
	}
	if err != nil || version < 1 || version >= schemaVersion {
		return err
	}
	backupPath := s.dbPath + fmt.Sprintf(".pre-v%d.bak", version+1)
	if _, err := os.Stat(backupPath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(FULL)"); err != nil {
		return err
	}
	data, err := os.ReadFile(s.dbPath)
	if err != nil {
		return err
	}
	tmp := backupPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, backupPath)
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM work_schema").Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if _, err := tx.ExecContext(ctx, "INSERT INTO work_schema(version) VALUES (?)", schemaVersion); err != nil {
			return err
		}
	} else {
		var version int
		if err := tx.QueryRowContext(ctx, "SELECT version FROM work_schema LIMIT 1").Scan(&version); err != nil {
			return err
		}
		switch version {
		case 1:
			if _, err := tx.ExecContext(ctx, "ALTER TABLE work_item_attachments ADD COLUMN removed_at INTEGER"); err != nil {
				return err
			}
			fallthrough
		case 2:
			for _, column := range []struct {
				name       string
				definition string
			}{
				{"outcome", "TEXT NOT NULL DEFAULT ''"},
				{"next_step", "TEXT NOT NULL DEFAULT ''"},
				{"acceptance_criteria", "TEXT NOT NULL DEFAULT ''"},
			} {
				if err := addColumnIfMissing(ctx, tx, "work_items", column.name, column.definition); err != nil {
					return err
				}
			}
			fallthrough
		case 3:
			for _, column := range []struct {
				table      string
				name       string
				definition string
			}{
				{"work_projects", "context", "TEXT NOT NULL DEFAULT ''"},
				{"work_items", "assignee", "TEXT NOT NULL DEFAULT ''"},
				{"work_items", "due_at", "INTEGER"},
				{"work_items", "labels", "TEXT NOT NULL DEFAULT '[]'"},
				{"work_items", "automation_mode", "TEXT NOT NULL DEFAULT 'manual'"},
				{"work_items", "workflow_id", "TEXT NOT NULL DEFAULT ''"},
				{"work_items", "workflow_node_id", "TEXT NOT NULL DEFAULT ''"},
				{"work_item_runs", "session_id", "TEXT NOT NULL DEFAULT ''"},
				{"work_item_runs", "instruction", "TEXT NOT NULL DEFAULT ''"},
				{"work_item_runs", "available_at", "INTEGER NOT NULL DEFAULT 0"},
				{"work_item_runs", "attempt", "INTEGER NOT NULL DEFAULT 0"},
				{"work_item_runs", "max_attempts", "INTEGER NOT NULL DEFAULT 3"},
				{"work_item_runs", "claimed_at", "INTEGER"},
				{"work_item_runs", "queue_reason", "TEXT NOT NULL DEFAULT ''"},
			} {
				if err := addColumnIfMissing(ctx, tx, column.table, column.name, column.definition); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(ctx, "UPDATE work_schema SET version=?", schemaVersion); err != nil {
				return err
			}
		case 4, 5, 6:
			if _, err := tx.ExecContext(ctx, "UPDATE work_schema SET version=?", schemaVersion); err != nil {
				return err
			}
		case schemaVersion:
		default:
			return fmt.Errorf("unsupported work item schema version %d", version)
		}
	}
	if _, err := tx.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS work_attachment_active_identity
		ON work_item_attachments(work_item_id, attachment_id) WHERE removed_at IS NULL`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS work_runs_queue
		ON work_item_runs(status, available_at, started_at, id)`); err != nil {
		return err
	}
	return tx.Commit()
}

func addColumnIfMissing(ctx context.Context, tx *sql.Tx, table, column, definition string) error {
	rows, err := tx.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid int
		var name, kind string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		if name == column {
			found = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+column+" "+definition)
	return err
}

type CreateProjectInput struct {
	ID            string
	Name          string
	WorkspacePath string
	Context       string
}

func (s *Store) CreateProject(ctx context.Context, in CreateProjectInput) (Project, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Project{}, errors.New("project name is required")
	}
	id := in.ID
	if id == "" {
		id = newID("wp")
	}
	now := s.now().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Project{}, err
	}
	defer tx.Rollback()
	project := Project{ID: id, AuthorityInstanceID: s.instanceID, Name: name,
		WorkspacePath: strings.TrimSpace(in.WorkspacePath), Context: strings.TrimSpace(in.Context), Version: 1, CreatedAt: now, UpdatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_projects
		(id,authority_instance_id,name,workspace_path,context,version,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?)`, project.ID, project.AuthorityInstanceID, project.Name,
		project.WorkspacePath, project.Context, project.Version, now, now); err != nil {
		return Project{}, err
	}
	if _, err := recordChange(ctx, tx, "project", id, "created", project, now); err != nil {
		return Project{}, err
	}
	if err := tx.Commit(); err != nil {
		return Project{}, err
	}
	return project, nil
}

type CreateItemInput struct {
	ID                 string
	ProjectID          string
	Title              string
	Description        string
	Outcome            string
	NextStep           string
	AcceptanceCriteria string
	Priority           Priority
	SortKey            int64
	Assignee           string
	DueAt              *int64
	Labels             []string
	AutomationMode     string
	WorkflowID         string
	WorkflowNodeID     string
	Actor              Actor
}

func (s *Store) CreateItem(ctx context.Context, in CreateItemInput) (WorkItem, error) {
	title, err := normalizeTitle(in.Title)
	if err != nil {
		return WorkItem{}, err
	}
	if in.Priority == "" {
		in.Priority = PriorityNone
	}
	if !validPriority(in.Priority) {
		return WorkItem{}, fmt.Errorf("invalid work item priority %q", in.Priority)
	}
	if in.ID == "" {
		in.ID = newID("wi")
	}
	if in.SortKey == 0 {
		in.SortKey = 1_000_000
	}
	if in.AutomationMode == "" {
		in.AutomationMode = "manual"
	}
	if in.AutomationMode != "manual" && in.AutomationMode != "auto" {
		return WorkItem{}, errors.New("invalid automation mode")
	}
	now := s.now().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	if err := s.requireProject(ctx, tx, in.ProjectID); err != nil {
		return WorkItem{}, err
	}
	if err := s.requireWorkflow(ctx, tx, in.WorkflowID, in.ProjectID, in.WorkflowNodeID); err != nil {
		return WorkItem{}, err
	}
	item := WorkItem{ID: in.ID, ProjectID: in.ProjectID, Title: title,
		Description: strings.TrimSpace(in.Description), Outcome: strings.TrimSpace(in.Outcome),
		NextStep: strings.TrimSpace(in.NextStep), AcceptanceCriteria: strings.TrimSpace(in.AcceptanceCriteria),
		Lifecycle: LifecycleInbox, Priority: in.Priority,
		SortKey: in.SortKey, Assignee: strings.TrimSpace(in.Assignee), DueAt: in.DueAt,
		Labels: normalizeLabels(in.Labels), AutomationMode: in.AutomationMode,
		WorkflowID: strings.TrimSpace(in.WorkflowID), WorkflowNodeID: strings.TrimSpace(in.WorkflowNodeID),
		Version: 1, CreatedAt: now, UpdatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_items
		(id,project_id,title,description,outcome,next_step,acceptance_criteria,lifecycle,priority,sort_key,
		 assignee,due_at,labels,automation_mode,workflow_id,workflow_node_id,version,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, item.ID, item.ProjectID, item.Title, item.Description,
		item.Outcome, item.NextStep, item.AcceptanceCriteria,
		item.Lifecycle, item.Priority, item.SortKey, item.Assignee, item.DueAt, labelsJSON(item.Labels),
		item.AutomationMode, item.WorkflowID, item.WorkflowNodeID, item.Version, now, now); err != nil {
		return WorkItem{}, err
	}
	revision, err := recordChange(ctx, tx, "work_item", item.ID, "created", ChangePayload{Item: &item}, now)
	if err != nil {
		return WorkItem{}, err
	}
	item.ActivityRevision = revision
	if _, err := tx.ExecContext(ctx, "UPDATE work_items SET activity_revision=? WHERE id=?", revision, item.ID); err != nil {
		return WorkItem{}, err
	}
	activity, err := insertActivity(ctx, tx, revision, item.ID, "created", in.Actor, item, now)
	if err != nil {
		return WorkItem{}, err
	}
	if err := updateChangePayload(ctx, tx, revision, ChangePayload{Item: &item, Activity: &activity}); err != nil {
		return WorkItem{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkItem{}, err
	}
	return item, nil
}

type UpdateItemInput struct {
	ID                 string
	ExpectedVersion    uint64
	Title              *string
	Description        *string
	Outcome            *string
	NextStep           *string
	AcceptanceCriteria *string
	Priority           *Priority
	BlockedReasonCode  *string
	BlockedNote        *string
	Assignee           *string
	DueAt              **int64
	Labels             *[]string
	AutomationMode     *string
	WorkflowID         *string
	WorkflowNodeID     *string
	Actor              Actor
}

func (s *Store) UpdateItem(ctx context.Context, in UpdateItemInput) (WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	current, err := getItemTx(ctx, tx, in.ID)
	if err != nil {
		return WorkItem{}, err
	}
	if current.Version != in.ExpectedVersion {
		return WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: current}
	}
	if in.Title != nil {
		current.Title, err = normalizeTitle(*in.Title)
		if err != nil {
			return WorkItem{}, err
		}
	}
	if in.Description != nil {
		current.Description = strings.TrimSpace(*in.Description)
	}
	if in.Outcome != nil {
		current.Outcome = strings.TrimSpace(*in.Outcome)
	}
	if in.NextStep != nil {
		current.NextStep = strings.TrimSpace(*in.NextStep)
	}
	if in.AcceptanceCriteria != nil {
		current.AcceptanceCriteria = strings.TrimSpace(*in.AcceptanceCriteria)
	}
	if in.Priority != nil {
		if !validPriority(*in.Priority) {
			return WorkItem{}, fmt.Errorf("invalid work item priority %q", *in.Priority)
		}
		current.Priority = *in.Priority
	}
	if in.BlockedReasonCode != nil {
		current.BlockedReasonCode = strings.TrimSpace(*in.BlockedReasonCode)
	}
	if in.BlockedNote != nil {
		current.BlockedNote = strings.TrimSpace(*in.BlockedNote)
	}
	if in.Assignee != nil {
		current.Assignee = strings.TrimSpace(*in.Assignee)
	}
	if in.DueAt != nil {
		current.DueAt = *in.DueAt
	}
	if in.Labels != nil {
		current.Labels = normalizeLabels(*in.Labels)
	}
	if in.AutomationMode != nil {
		if *in.AutomationMode != "manual" && *in.AutomationMode != "auto" {
			return WorkItem{}, errors.New("invalid automation mode")
		}
		current.AutomationMode = *in.AutomationMode
	}
	if in.WorkflowID != nil {
		current.WorkflowID = strings.TrimSpace(*in.WorkflowID)
	}
	if in.WorkflowNodeID != nil {
		current.WorkflowNodeID = strings.TrimSpace(*in.WorkflowNodeID)
	}
	if err := s.requireWorkflow(ctx, tx, current.WorkflowID, current.ProjectID, current.WorkflowNodeID); err != nil {
		return WorkItem{}, err
	}
	current.Version++
	current.UpdatedAt = s.now().UnixMilli()
	return s.writeItemMutation(ctx, tx, current, "updated", in.Actor, ChangePayload{})
}

type MoveItemInput struct {
	ID              string
	ExpectedVersion uint64
	Lifecycle       Lifecycle
	SortKey         int64
	Actor           Actor
}

func (s *Store) MoveItem(ctx context.Context, in MoveItemInput) (WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	current, err := getItemTx(ctx, tx, in.ID)
	if err != nil {
		return WorkItem{}, err
	}
	if current.Version != in.ExpectedVersion {
		return WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: current}
	}
	if current.Lifecycle == LifecycleReview && in.Lifecycle == LifecycleDone {
		if in.Actor.Type != ActorUser && in.Actor.Type != ActorDesktop && in.Actor.Type != ActorMobile {
			return WorkItem{}, ErrHumanRequired
		}
		return WorkItem{}, ErrReviewDecisionRequired
	}
	if current.Lifecycle == LifecycleDone && in.Lifecycle == LifecycleActive {
		return WorkItem{}, ErrReviewDecisionRequired
	}
	if err := validateTransition(current.Lifecycle, in.Lifecycle, in.Actor); err != nil {
		return WorkItem{}, err
	}
	now := s.now().UnixMilli()
	current.Lifecycle = in.Lifecycle
	if in.SortKey != 0 {
		current.SortKey = in.SortKey
	}
	current.Version++
	current.UpdatedAt = now
	if in.Lifecycle == LifecycleDone {
		current.AcceptedAt = &now
	} else {
		current.AcceptedAt = nil
	}
	if in.Lifecycle == LifecycleCancelled {
		current.CancelledAt = &now
	} else {
		current.CancelledAt = nil
	}
	return s.writeItemMutation(ctx, tx, current, "moved", in.Actor, ChangePayload{})
}

func (s *Store) writeItemMutation(ctx context.Context, tx *sql.Tx, item WorkItem, kind string, actor Actor, payload ChangePayload) (WorkItem, error) {
	updated, err := s.writeItemMutationTx(ctx, tx, item, kind, actor, payload)
	if err != nil {
		return WorkItem{}, err
	}
	if err = tx.Commit(); err != nil {
		return WorkItem{}, err
	}
	return updated, nil
}

// writeItemMutationTx participates in an existing transaction. The caller owns
// commit, allowing collaboration records and the outcome to change atomically.
func (s *Store) writeItemMutationTx(ctx context.Context, tx *sql.Tx, item WorkItem, kind string, actor Actor, payload ChangePayload) (WorkItem, error) {
	result, err := tx.ExecContext(ctx, `UPDATE work_items SET
		title=?,description=?,outcome=?,next_step=?,acceptance_criteria=?,lifecycle=?,priority=?,sort_key=?,version=?,
		blocked_reason_code=?,blocked_note=?,assignee=?,due_at=?,labels=?,automation_mode=?,workflow_id=?,workflow_node_id=?,
		updated_at=?,accepted_at=?,cancelled_at=?,archived_at=?
		WHERE id=? AND version=?`, item.Title, item.Description, item.Outcome, item.NextStep,
		item.AcceptanceCriteria, item.Lifecycle, item.Priority,
		item.SortKey, item.Version, item.BlockedReasonCode, item.BlockedNote, item.Assignee, item.DueAt,
		labelsJSON(item.Labels), item.AutomationMode, item.WorkflowID, item.WorkflowNodeID, item.UpdatedAt,
		item.AcceptedAt, item.CancelledAt, item.ArchivedAt, item.ID, item.Version-1)
	if err != nil {
		return WorkItem{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		latest, getErr := getItemTx(ctx, tx, item.ID)
		if getErr != nil {
			return WorkItem{}, getErr
		}
		return WorkItem{}, &ConflictError{Expected: item.Version - 1, Current: latest}
	}
	payload.Item = &item
	revision, err := recordChange(ctx, tx, "work_item", item.ID, kind, payload, item.UpdatedAt)
	if err != nil {
		return WorkItem{}, err
	}
	item.ActivityRevision = revision
	if _, err := tx.ExecContext(ctx, "UPDATE work_items SET activity_revision=? WHERE id=?", revision, item.ID); err != nil {
		return WorkItem{}, err
	}
	activityPayload := any(item)
	if payload.ReviewDecision != nil {
		activityPayload = struct {
			Item           WorkItem             `json:"item"`
			ReviewDecision ReviewDecisionRecord `json:"review_decision"`
		}{Item: item, ReviewDecision: *payload.ReviewDecision}
	}
	activity, err := insertActivity(ctx, tx, revision, item.ID, kind, actor, activityPayload, item.UpdatedAt)
	if err != nil {
		return WorkItem{}, err
	}
	payload.Item = &item
	payload.Activity = &activity
	if err := updateChangePayload(ctx, tx, revision, payload); err != nil {
		return WorkItem{}, err
	}
	return item, nil
}

type LinkSessionInput struct {
	ID               string
	WorkItemID       string
	SessionID        string
	ThreadIDSnapshot string
	Role             string
	ExpectedVersion  uint64
	Actor            Actor
}

func (s *Store) LinkSession(ctx context.Context, in LinkSessionInput) (SessionLink, WorkItem, error) {
	if strings.TrimSpace(in.SessionID) == "" {
		return SessionLink{}, WorkItem{}, errors.New("session id is required")
	}
	if in.Role == "" {
		in.Role = "primary"
	}
	if in.Role != "primary" && in.Role != "support" && in.Role != "research" && in.Role != "verification" {
		return SessionLink{}, WorkItem{}, fmt.Errorf("invalid session role %q", in.Role)
	}
	if in.ID == "" {
		in.ID = newID("wl")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return SessionLink{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	now := s.now().UnixMilli()
	link := SessionLink{ID: in.ID, WorkItemID: in.WorkItemID, SessionID: in.SessionID,
		ThreadIDSnapshot: in.ThreadIDSnapshot, Role: in.Role, LinkedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_session_links
		(id,work_item_id,session_id,thread_id_snapshot,role,linked_at) VALUES(?,?,?,?,?,?)`,
		link.ID, link.WorkItemID, link.SessionID, link.ThreadIDSnapshot, link.Role, link.LinkedAt); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return SessionLink{}, WorkItem{}, ErrSessionLinked
		}
		return SessionLink{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	updated, err := s.writeItemMutation(ctx, tx, item, "session_linked", in.Actor, ChangePayload{Link: &link})
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	return link, updated, nil
}

type UnlinkSessionInput struct {
	LinkID          string
	ExpectedVersion uint64
	Actor           Actor
}

func (s *Store) UnlinkSession(ctx context.Context, in UnlinkSessionInput) (SessionLink, WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	defer tx.Rollback()
	var link SessionLink
	err = tx.QueryRowContext(ctx, `SELECT id,work_item_id,session_id,thread_id_snapshot,role,linked_at,unlinked_at
		FROM work_item_session_links WHERE id=?`, in.LinkID).Scan(&link.ID, &link.WorkItemID,
		&link.SessionID, &link.ThreadIDSnapshot, &link.Role, &link.LinkedAt, &link.UnlinkedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionLink{}, WorkItem{}, ErrNotFound
	}
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	item, err := getItemTx(ctx, tx, link.WorkItemID)
	if err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return SessionLink{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	if link.UnlinkedAt != nil {
		return link, item, nil
	}
	now := s.now().UnixMilli()
	link.UnlinkedAt = &now
	if _, err := tx.ExecContext(ctx, `UPDATE work_item_session_links SET unlinked_at=?
		WHERE id=? AND unlinked_at IS NULL`, now, link.ID); err != nil {
		return SessionLink{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	updated, err := s.writeItemMutation(ctx, tx, item, "session_unlinked", in.Actor, ChangePayload{Link: &link})
	return link, updated, err
}

type AddDependencyInput struct {
	WorkItemID      string
	DependsOnID     string
	ExpectedVersion uint64
	Actor           Actor
}

func (s *Store) AddDependency(ctx context.Context, in AddDependencyInput) (WorkItem, error) {
	if in.WorkItemID == in.DependsOnID {
		return WorkItem{}, ErrDependencyCycle
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	dep, err := getItemTx(ctx, tx, in.DependsOnID)
	if err != nil {
		return WorkItem{}, err
	}
	if item.ProjectID != dep.ProjectID {
		return WorkItem{}, ErrCrossProject
	}
	var cycle int
	err = tx.QueryRowContext(ctx, `WITH RECURSIVE chain(id) AS (
		SELECT depends_on_id FROM work_item_dependencies WHERE work_item_id=?
		UNION
		SELECT d.depends_on_id FROM work_item_dependencies d JOIN chain c ON d.work_item_id=c.id
	) SELECT EXISTS(SELECT 1 FROM chain WHERE id=?)`, in.DependsOnID, in.WorkItemID).Scan(&cycle)
	if err != nil {
		return WorkItem{}, err
	}
	if cycle != 0 {
		return WorkItem{}, ErrDependencyCycle
	}
	now := s.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_dependencies(work_item_id,depends_on_id,created_at)
		VALUES(?,?,?)`, in.WorkItemID, in.DependsOnID, now); err != nil {
		return WorkItem{}, err
	}
	dependency := Dependency{WorkItemID: in.WorkItemID, DependsOn: in.DependsOnID, CreatedAt: now}
	item.Version++
	item.UpdatedAt = now
	return s.writeItemMutation(ctx, tx, item, "dependency_added", in.Actor, ChangePayload{Dependency: &dependency})
}

func (s *Store) RemoveDependency(ctx context.Context, in AddDependencyInput) (WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM work_item_dependencies
		WHERE work_item_id=? AND depends_on_id=?`, in.WorkItemID, in.DependsOnID)
	if err != nil {
		return WorkItem{}, err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return WorkItem{}, ErrNotFound
	}
	now := s.now().UnixMilli()
	dep := Dependency{WorkItemID: in.WorkItemID, DependsOn: in.DependsOnID, CreatedAt: now}
	item.Version++
	item.UpdatedAt = now
	return s.writeItemMutation(ctx, tx, item, "dependency_removed", in.Actor, ChangePayload{Dependency: &dep})
}

type ArchiveItemInput struct {
	ID              string
	ExpectedVersion uint64
	Restore         bool
	Actor           Actor
}

func (s *Store) ArchiveItem(ctx context.Context, in ArchiveItemInput) (WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.ID)
	if err != nil {
		return WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	now := s.now().UnixMilli()
	kind := "archived"
	if in.Restore {
		item.ArchivedAt = nil
		kind = "restored"
	} else {
		item.ArchivedAt = &now
	}
	item.Version++
	item.UpdatedAt = now
	return s.writeItemMutation(ctx, tx, item, kind, in.Actor, ChangePayload{})
}

type AddCommentInput struct {
	ID              string
	WorkItemID      string
	ExpectedVersion uint64
	Body            string
	Actor           Actor
}

func (s *Store) AddComment(ctx context.Context, in AddCommentInput) (Comment, WorkItem, error) {
	body := strings.TrimSpace(in.Body)
	if body == "" {
		return Comment{}, WorkItem{}, errors.New("comment body is required")
	}
	if in.ID == "" {
		in.ID = newID("wc")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Comment{}, WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return Comment{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return Comment{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	now := s.now().UnixMilli()
	author := in.Actor.Type
	if author == "" {
		author = ActorUser
	}
	comment := Comment{ID: in.ID, WorkItemID: in.WorkItemID, AuthorType: author,
		AuthorDeviceID: in.Actor.DeviceID, Body: body, CreatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_comments
		(id,work_item_id,author_type,author_device_id,body,created_at) VALUES(?,?,?,?,?,?)`,
		comment.ID, comment.WorkItemID, comment.AuthorType, comment.AuthorDeviceID, comment.Body, now); err != nil {
		return Comment{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	updated, err := s.writeItemMutation(ctx, tx, item, "comment_added", in.Actor, ChangePayload{Comment: &comment})
	return comment, updated, err
}

type EditCommentInput struct {
	CommentID       string
	ExpectedVersion uint64
	Body            string
	Actor           Actor
}

func (s *Store) EditComment(ctx context.Context, in EditCommentInput) (Comment, WorkItem, error) {
	body := strings.TrimSpace(in.Body)
	if body == "" {
		return Comment{}, WorkItem{}, errors.New("comment body is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Comment{}, WorkItem{}, err
	}
	defer tx.Rollback()
	var comment Comment
	err = tx.QueryRowContext(ctx, `SELECT id,work_item_id,author_type,author_device_id,body,
		created_at,edited_at,deleted_at FROM work_item_comments WHERE id=?`, in.CommentID).Scan(
		&comment.ID, &comment.WorkItemID, &comment.AuthorType, &comment.AuthorDeviceID, &comment.Body,
		&comment.CreatedAt, &comment.EditedAt, &comment.DeletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Comment{}, WorkItem{}, ErrNotFound
	}
	if err != nil {
		return Comment{}, WorkItem{}, err
	}
	item, err := getItemTx(ctx, tx, comment.WorkItemID)
	if err != nil {
		return Comment{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return Comment{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	now := s.now().UnixMilli()
	comment.Body = body
	comment.EditedAt = &now
	if _, err := tx.ExecContext(ctx, `UPDATE work_item_comments SET body=?,edited_at=? WHERE id=?`,
		body, now, comment.ID); err != nil {
		return Comment{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	updated, err := s.writeItemMutation(ctx, tx, item, "comment_edited", in.Actor, ChangePayload{Comment: &comment})
	return comment, updated, err
}

type AddAttachmentInput struct {
	ID              string
	WorkItemID      string
	AttachmentID    string
	DisplayName     string
	SortKey         int64
	ExpectedVersion uint64
	Actor           Actor
}

func (s *Store) AddAttachment(ctx context.Context, in AddAttachmentInput) (AttachmentRef, WorkItem, error) {
	if strings.TrimSpace(in.AttachmentID) == "" {
		return AttachmentRef{}, WorkItem{}, errors.New("attachment id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AttachmentRef{}, WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return AttachmentRef{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return AttachmentRef{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	now := s.now().UnixMilli()
	ref := AttachmentRef{ID: in.ID, WorkItemID: item.ID, AttachmentID: strings.TrimSpace(in.AttachmentID),
		DisplayName: strings.TrimSpace(in.DisplayName), SortKey: in.SortKey, CreatedAt: now}
	if ref.ID == "" {
		ref.ID = newID("wa")
	}
	if ref.SortKey == 0 {
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sort_key),0)+1024 FROM work_item_attachments
			WHERE work_item_id=? AND removed_at IS NULL`, item.ID).Scan(&ref.SortKey); err != nil {
			return AttachmentRef{}, WorkItem{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_attachments
		(id,work_item_id,attachment_id,display_name,sort_key,created_at,removed_at)
		VALUES(?,?,?,?,?,?,NULL)`, ref.ID, ref.WorkItemID, ref.AttachmentID, ref.DisplayName, ref.SortKey, ref.CreatedAt); err != nil {
		return AttachmentRef{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	item, err = s.writeItemMutation(ctx, tx, item, "attachment_added", in.Actor, ChangePayload{Item: &item, Attachment: &ref})
	if err != nil {
		return AttachmentRef{}, WorkItem{}, err
	}
	return ref, item, nil
}

type RemoveAttachmentInput struct {
	AttachmentRefID string
	ExpectedVersion uint64
	Actor           Actor
}

func (s *Store) RemoveAttachment(ctx context.Context, in RemoveAttachmentInput) (AttachmentRef, WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AttachmentRef{}, WorkItem{}, err
	}
	defer tx.Rollback()
	var ref AttachmentRef
	err = tx.QueryRowContext(ctx, `SELECT id,work_item_id,attachment_id,display_name,sort_key,created_at,removed_at
		FROM work_item_attachments WHERE id=? AND removed_at IS NULL`, in.AttachmentRefID).Scan(
		&ref.ID, &ref.WorkItemID, &ref.AttachmentID, &ref.DisplayName, &ref.SortKey, &ref.CreatedAt, &ref.RemovedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return AttachmentRef{}, WorkItem{}, ErrNotFound
	}
	if err != nil {
		return AttachmentRef{}, WorkItem{}, err
	}
	item, err := getItemTx(ctx, tx, ref.WorkItemID)
	if err != nil {
		return AttachmentRef{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return AttachmentRef{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	now := s.now().UnixMilli()
	ref.RemovedAt = &now
	if _, err := tx.ExecContext(ctx, `UPDATE work_item_attachments SET removed_at=? WHERE id=? AND removed_at IS NULL`, now, ref.ID); err != nil {
		return AttachmentRef{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	item, err = s.writeItemMutation(ctx, tx, item, "attachment_removed", in.Actor, ChangePayload{Item: &item, Attachment: &ref})
	if err != nil {
		return AttachmentRef{}, WorkItem{}, err
	}
	return ref, item, nil
}

func (s *Store) GetItem(ctx context.Context, id string) (WorkItem, error) {
	return getItemQuery(ctx, s.db, id)
}

func (s *Store) Snapshot(ctx context.Context) (Snapshot, error) {
	var snap Snapshot
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(revision),0) FROM work_changes").Scan(&snap.Revision); err != nil {
		return snap, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,authority_instance_id,name,workspace_path,context,version,
		created_at,updated_at,archived_at FROM work_projects ORDER BY created_at,id`)
	if err != nil {
		return snap, err
	}
	for rows.Next() {
		var p Project
		if err := rows.Scan(&p.ID, &p.AuthorityInstanceID, &p.Name, &p.WorkspacePath, &p.Context, &p.Version,
			&p.CreatedAt, &p.UpdatedAt, &p.ArchivedAt); err != nil {
			rows.Close()
			return snap, err
		}
		snap.Projects = append(snap.Projects, p)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, itemSelect+" ORDER BY project_id,lifecycle,sort_key,id")
	if err != nil {
		return snap, err
	}
	for rows.Next() {
		item, err := scanItem(rows)
		if err != nil {
			rows.Close()
			return snap, err
		}
		snap.Items = append(snap.Items, item)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT id,work_item_id,session_id,thread_id_snapshot,role,linked_at,unlinked_at
		FROM work_item_session_links WHERE unlinked_at IS NULL ORDER BY linked_at,id`)
	if err != nil {
		return snap, err
	}
	for rows.Next() {
		var link SessionLink
		if err := rows.Scan(&link.ID, &link.WorkItemID, &link.SessionID, &link.ThreadIDSnapshot,
			&link.Role, &link.LinkedAt, &link.UnlinkedAt); err != nil {
			rows.Close()
			return snap, err
		}
		snap.SessionLinks = append(snap.SessionLinks, link)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT work_item_id,depends_on_id,created_at
		FROM work_item_dependencies ORDER BY work_item_id,depends_on_id`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var dep Dependency
		if err := rows.Scan(&dep.WorkItemID, &dep.DependsOn, &dep.CreatedAt); err != nil {
			return snap, err
		}
		snap.Dependencies = append(snap.Dependencies, dep)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT id,work_item_id,author_type,author_device_id,body,
		created_at,edited_at,deleted_at FROM work_item_comments WHERE deleted_at IS NULL ORDER BY created_at,id`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var comment Comment
		if err := rows.Scan(&comment.ID, &comment.WorkItemID, &comment.AuthorType, &comment.AuthorDeviceID,
			&comment.Body, &comment.CreatedAt, &comment.EditedAt, &comment.DeletedAt); err != nil {
			return snap, err
		}
		snap.Comments = append(snap.Comments, comment)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT id,work_item_id,session_link_id,request_id,kind,status,
		started_at,finished_at,terminal_reason,session_id,instruction,available_at,attempt,max_attempts,claimed_at,queue_reason
		FROM work_item_runs ORDER BY started_at,id`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var run Run
		if err := rows.Scan(&run.ID, &run.WorkItemID, &run.SessionLinkID, &run.RequestID,
			&run.Kind, &run.Status, &run.StartedAt, &run.FinishedAt, &run.TerminalReason,
			&run.SessionID, &run.Instruction, &run.AvailableAt, &run.Attempt, &run.MaxAttempts,
			&run.ClaimedAt, &run.QueueReason); err != nil {
			return snap, err
		}
		snap.Runs = append(snap.Runs, run)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT id,work_item_id,attachment_id,display_name,sort_key,created_at,removed_at
		FROM work_item_attachments WHERE removed_at IS NULL ORDER BY work_item_id,sort_key,id`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var ref AttachmentRef
		if err := rows.Scan(&ref.ID, &ref.WorkItemID, &ref.AttachmentID, &ref.DisplayName,
			&ref.SortKey, &ref.CreatedAt, &ref.RemovedAt); err != nil {
			return snap, err
		}
		snap.Attachments = append(snap.Attachments, ref)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT revision,work_item_id,kind,actor,payload,created_at
		FROM work_item_activity ORDER BY revision`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var activity Activity
		if err := rows.Scan(&activity.Revision, &activity.WorkItemID, &activity.Kind, &activity.Actor,
			&activity.Payload, &activity.CreatedAt); err != nil {
			return snap, err
		}
		snap.Activities = append(snap.Activities, activity)
	}
	if err := rows.Close(); err != nil {
		return snap, err
	}
	snap.Workflows, err = s.listWorkflows(ctx)
	if err != nil {
		return snap, err
	}
	snap.Bootstraps, err = listBootstraps(ctx, s.db, s.instanceID)
	return snap, err
}

func (s *Store) requireProject(ctx context.Context, tx *sql.Tx, id string) error {
	var authority string
	if err := tx.QueryRowContext(ctx, "SELECT authority_instance_id FROM work_projects WHERE id=?", id).Scan(&authority); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if authority != s.instanceID {
		return errors.New("project belongs to another authority")
	}
	return nil
}

const itemSelect = `SELECT id,project_id,title,description,outcome,next_step,acceptance_criteria,lifecycle,priority,sort_key,version,
	activity_revision,blocked_reason_code,blocked_note,assignee,due_at,labels,automation_mode,workflow_id,workflow_node_id,
	created_at,updated_at,accepted_at,cancelled_at,archived_at
	FROM work_items`

type rowScanner interface{ Scan(...any) error }

func scanItem(row rowScanner) (WorkItem, error) {
	var item WorkItem
	var labels string
	err := row.Scan(&item.ID, &item.ProjectID, &item.Title, &item.Description, &item.Outcome,
		&item.NextStep, &item.AcceptanceCriteria, &item.Lifecycle,
		&item.Priority, &item.SortKey, &item.Version, &item.ActivityRevision,
		&item.BlockedReasonCode, &item.BlockedNote, &item.Assignee, &item.DueAt, &labels,
		&item.AutomationMode, &item.WorkflowID, &item.WorkflowNodeID, &item.CreatedAt, &item.UpdatedAt,
		&item.AcceptedAt, &item.CancelledAt, &item.ArchivedAt)
	if err == nil {
		_ = json.Unmarshal([]byte(labels), &item.Labels)
		if item.Labels == nil {
			item.Labels = []string{}
		}
		if item.AutomationMode == "" {
			item.AutomationMode = "manual"
		}
	}
	return item, err
}

func normalizeLabels(labels []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(labels))
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" || seen[label] {
			continue
		}
		seen[label] = true
		out = append(out, label)
	}
	sort.Strings(out)
	return out
}

func labelsJSON(labels []string) string {
	body, _ := json.Marshal(normalizeLabels(labels))
	return string(body)
}

func getItemTx(ctx context.Context, tx *sql.Tx, id string) (WorkItem, error) {
	return getItemQuery(ctx, tx, id)
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getItemQuery(ctx context.Context, q queryRower, id string) (WorkItem, error) {
	item, err := scanItem(q.QueryRowContext(ctx, itemSelect+" WHERE id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return WorkItem{}, ErrNotFound
	}
	return item, err
}

func recordChange(ctx context.Context, tx *sql.Tx, entity, entityID, kind string, payload any, now int64) (uint64, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO work_changes(entity,entity_id,kind,payload,created_at)
		VALUES(?,?,?,?,?)`, entity, entityID, kind, string(body), now)
	if err != nil {
		return 0, err
	}
	revision, err := result.LastInsertId()
	return uint64(revision), err
}

func updateChangePayload(ctx context.Context, tx *sql.Tx, revision uint64, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE work_changes SET payload=? WHERE revision=?", string(body), revision)
	return err
}

func insertActivity(ctx context.Context, tx *sql.Tx, revision uint64, itemID, kind string, actor Actor, payload any, now int64) (Activity, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Activity{}, err
	}
	actorType := actor.Type
	if actorType == "" {
		actorType = ActorSystem
	}
	activity := Activity{Revision: revision, WorkItemID: itemID, Kind: kind, Actor: actorType,
		Payload: string(body), CreatedAt: now}
	_, err = tx.ExecContext(ctx, `INSERT INTO work_item_activity
		(revision,work_item_id,kind,actor,payload,created_at) VALUES(?,?,?,?,?,?)`,
		revision, itemID, kind, actorType, string(body), now)
	return activity, err
}

func newID(prefix string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b)
}
