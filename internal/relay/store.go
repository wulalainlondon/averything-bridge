// Package relay owns crash-safe, idempotent work transfer between Bridge
// authorities. It deliberately stores immutable envelopes; source connector
// credentials and transport URLs never cross this boundary.
package relay

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Attachment struct {
	ID          string `json:"attachment_id"`
	Ordinal     int    `json:"ordinal"`
	MIMEType    string `json:"mime_type"`
	DisplayName string `json:"display_name"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
}

type Job struct {
	SchemaVersion    int               `json:"schema_version"`
	ID               string            `json:"relay_job_id"`
	OriginInstanceID string            `json:"origin_instance_id"`
	TargetInstanceID string            `json:"target_instance_id"`
	EventID          string            `json:"event_id"`
	EventIDs         []string          `json:"event_ids,omitempty"`
	EventKey         string            `json:"event_key"`
	Source           string            `json:"source"`
	Kind             string            `json:"kind"`
	Title            string            `json:"title"`
	Body             string            `json:"body,omitempty"`
	Data             json.RawMessage   `json:"data"`
	TargetWorkItemID string            `json:"target_work_item_id"`
	TargetSessionID  string            `json:"target_session_id"`
	Instruction      string            `json:"instruction"`
	ReviewOnly       bool              `json:"review_only"`
	Attachments      []Attachment      `json:"attachments,omitempty"`
	CreatedAt        int64             `json:"created_at"`
	ExpiresAt        int64             `json:"expires_at"`
	TraceID          string            `json:"trace_id"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

type Record struct {
	Job       Job    `json:"job"`
	Direction string `json:"direction"`
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
	Attempt   int    `json:"attempt"`
	RunID     string `json:"run_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	Result    string `json:"result,omitempty"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type Store struct {
	db  *sql.DB
	now func() time.Time
}

const ddl = `
CREATE TABLE IF NOT EXISTS relay_jobs (
  id TEXT NOT NULL, direction TEXT NOT NULL CHECK(direction IN ('outbound','inbound')),
  origin_instance_id TEXT NOT NULL, target_instance_id TEXT NOT NULL,
  envelope_json TEXT NOT NULL, status TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '',
  attempt INTEGER NOT NULL DEFAULT 0, available_at INTEGER NOT NULL,
  lease_owner TEXT NOT NULL DEFAULT '', lease_until INTEGER NOT NULL DEFAULT 0,
  run_id TEXT NOT NULL DEFAULT '', request_id TEXT NOT NULL DEFAULT '', result TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  PRIMARY KEY(id,direction)
);
CREATE INDEX IF NOT EXISTS relay_jobs_queue ON relay_jobs(direction,status,available_at,created_at,id);
`

func Open(dataDir string) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dataDir, "everything_go_relay.db")+
		"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(ddl); err != nil {
		db.Close()
		return nil, err
	}
	now := time.Now().UnixMilli()
	_, _ = db.Exec(`UPDATE relay_jobs SET status=CASE direction WHEN 'outbound' THEN 'pending' ELSE 'accepted' END,
    reason='bridge_restarted',available_at=?,lease_owner='',lease_until=0,updated_at=?
    WHERE status IN ('sending','dispatching')`, now, now)
	return &Store{db: db, now: time.Now}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) EnqueueOutbound(ctx context.Context, job Job) (bool, error) {
	return s.insert(ctx, job, "outbound", "pending")
}

func (s *Store) AcceptInbound(ctx context.Context, job Job) (bool, error) {
	return s.insert(ctx, job, "inbound", "accepted")
}

func (s *Store) insert(ctx context.Context, job Job, direction, status string) (bool, error) {
	if err := ValidateJob(job); err != nil {
		return false, err
	}
	raw, err := json.Marshal(job)
	if err != nil {
		return false, err
	}
	now := s.now().UnixMilli()
	result, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO relay_jobs
    (id,direction,origin_instance_id,target_instance_id,envelope_json,status,available_at,created_at,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?)`, job.ID, direction, job.OriginInstanceID, job.TargetInstanceID, raw, status, now, now, now)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func ValidateJob(job Job) error {
	if job.SchemaVersion != 1 || job.ID == "" || job.OriginInstanceID == "" || job.TargetInstanceID == "" ||
		job.EventID == "" || job.TargetWorkItemID == "" || job.TargetSessionID == "" || job.Instruction == "" {
		return errors.New("invalid relay job identity or target")
	}
	if !job.ReviewOnly || len(job.Instruction) > 32000 || len(job.Attachments) > 10 || len(job.EventIDs) > 100 {
		return errors.New("relay job must be review-only and within limits")
	}
	for _, attachment := range job.Attachments {
		if attachment.ID == "" || attachment.Ordinal < 0 || attachment.SizeBytes <= 0 || attachment.SizeBytes > 20*1024*1024 ||
			len(attachment.SHA256) != 64 || len(attachment.DisplayName) > 240 || !allowedRelayMIME(attachment.MIMEType) {
			return errors.New("invalid relay attachment")
		}
	}
	return nil
}

func allowedRelayMIME(value string) bool {
	switch value {
	case "image/jpeg", "image/png", "image/gif", "image/webp", "video/mp4", "video/quicktime", "audio/mpeg", "audio/mp4", "application/pdf":
		return true
	default:
		return false
	}
}

func (s *Store) Claim(ctx context.Context, direction, owner string, now int64, lease time.Duration) (Record, bool, error) {
	predicate, active := "status IN ('pending','polling')", "sending"
	if direction == "inbound" {
		predicate, active = "status='accepted'", "dispatching"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, false, err
	}
	defer tx.Rollback()
	var record Record
	var raw string
	query := `SELECT id,direction,envelope_json,status,reason,attempt,run_id,request_id,result,created_at,updated_at
    FROM relay_jobs WHERE direction=? AND ` + predicate + ` AND available_at<=? AND lease_until<=?
    ORDER BY created_at,id LIMIT 1`
	err = tx.QueryRowContext(ctx, query, direction, now, now).Scan(&record.Job.ID, &record.Direction, &raw,
		&record.Status, &record.Reason, &record.Attempt, &record.RunID, &record.RequestID, &record.Result,
		&record.CreatedAt, &record.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	if err := json.Unmarshal([]byte(raw), &record.Job); err != nil {
		return Record{}, false, err
	}
	previousStatus := record.Status
	record.Attempt++
	result, err := tx.ExecContext(ctx, `UPDATE relay_jobs SET status=?,attempt=?,lease_owner=?,lease_until=?,updated_at=?
    WHERE id=? AND direction=? AND status=?`, active, record.Attempt, owner, now+lease.Milliseconds(), now,
		record.Job.ID, direction, previousStatus)
	if err != nil {
		return Record{}, false, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return Record{}, false, nil
	}
	record.Status = active
	return record, true, tx.Commit()
}

func (s *Store) Update(ctx context.Context, id, direction, status, reason, runID, requestID, result string, availableAt int64) error {
	if len(reason) > 2000 {
		reason = reason[:2000]
	}
	if len(result) > 64*1024 {
		result = result[:64*1024]
	}
	if availableAt == 0 {
		availableAt = s.now().UnixMilli()
	}
	_, err := s.db.ExecContext(ctx, `UPDATE relay_jobs SET status=?,reason=?,run_id=?,request_id=?,result=?,
    available_at=?,lease_owner='',lease_until=0,updated_at=? WHERE id=? AND direction=?`, status, reason,
		runID, requestID, result, availableAt, s.now().UnixMilli(), id, direction)
	return err
}

func (s *Store) Get(ctx context.Context, id, direction string) (Record, error) {
	var record Record
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT direction,envelope_json,status,reason,attempt,run_id,request_id,result,created_at,updated_at
    FROM relay_jobs WHERE id=? AND direction=?`, id, direction).Scan(&record.Direction, &raw, &record.Status,
		&record.Reason, &record.Attempt, &record.RunID, &record.RequestID, &record.Result, &record.CreatedAt, &record.UpdatedAt)
	if err != nil {
		return Record{}, err
	}
	if err := json.Unmarshal([]byte(raw), &record.Job); err != nil {
		return Record{}, err
	}
	return record, nil
}

func (s *Store) CanPeerReadAttachment(ctx context.Context, targetInstanceID, attachmentID string) bool {
	rows, err := s.db.QueryContext(ctx, `SELECT envelope_json FROM relay_jobs
    WHERE direction='outbound' AND target_instance_id=? AND status NOT IN ('expired')
    ORDER BY created_at DESC LIMIT 1000`, targetInstanceID)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		var job Job
		if rows.Scan(&raw) != nil || json.Unmarshal([]byte(raw), &job) != nil {
			continue
		}
		for _, attachment := range job.Attachments {
			if attachment.ID == attachmentID {
				return true
			}
		}
	}
	return false
}

func (s *Store) CompleteInboundByRequest(ctx context.Context, requestID, status, reason, result string) (string, bool, error) {
	if len(reason) > 2000 {
		reason = reason[:2000]
	}
	if len(result) > 64*1024 {
		result = result[:64*1024]
	}
	mapped := status
	if status == "succeeded" {
		mapped = "review_ready"
	} else if status == "interrupted" {
		mapped = "failed"
	}
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM relay_jobs WHERE direction='inbound' AND request_id=?
    AND status IN ('queued','running','waiting_user','dispatching') LIMIT 1`, requestID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE relay_jobs SET status=?,reason=?,result=?,lease_owner='',lease_until=0,updated_at=?
    WHERE id=? AND direction='inbound'`, mapped, reason, result, s.now().UnixMilli(), id)
	return id, err == nil, err
}
