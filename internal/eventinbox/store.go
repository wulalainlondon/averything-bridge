// Package eventinbox owns durable, source-neutral events delivered to Bridge
// clients. Source-specific webhook payloads must be normalized before entering
// this package so persistence and the app protocol never depend on GitHub,
// monitoring, payments, LINE or any future provider.
package eventinbox

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const (
	maxEvents       = 10000
	defaultListSize = 500
	defaultTTL      = 30 * 24 * time.Hour
	maxDataBytes    = 16 * 1024
)

var ErrNotFound = errors.New("external event not found")

type Input struct {
	Source      string
	EventKey    string
	Kind        string
	Severity    string
	Title       string
	Body        string
	URL         string
	Data        json.RawMessage
	OccurredAt  int64
	ExpiresAt   int64
	Attachments []AttachmentInput
}

// AttachmentInput is a provider attachment reference captured while the
// webhook signature is still available. SourceURL is fetch-only state: it is
// cleared as soon as the bytes have been materialized locally and is never
// exposed to clients as canonical media data.
type AttachmentInput struct {
	ExternalID  string
	SourceURL   string
	MIMEType    string
	DisplayName string
	Ordinal     int
}

type Attachment struct {
	ID          string `json:"attachment_id"`
	EventID     string `json:"event_id"`
	ExternalID  string `json:"external_id,omitempty"`
	Ordinal     int    `json:"ordinal"`
	Status      string `json:"status"`
	MIMEType    string `json:"mime_type,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	LocalPath   string `json:"-"`
	URL         string `json:"url,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

type Event struct {
	ID                  string          `json:"event_id"`
	Sequence            uint64          `json:"sequence"`
	AuthorityInstanceID string          `json:"authority_instance_id"`
	Source              string          `json:"source"`
	EventKey            string          `json:"event_key"`
	Kind                string          `json:"kind"`
	Severity            string          `json:"severity"`
	Title               string          `json:"title"`
	Body                string          `json:"body,omitempty"`
	URL                 string          `json:"url,omitempty"`
	Data                json.RawMessage `json:"data"`
	OccurredAt          int64           `json:"occurred_at"`
	ReceivedAt          int64           `json:"received_at"`
	ExpiresAt           int64           `json:"expires_at"`
	Attachments         []Attachment    `json:"attachments,omitempty"`
}

type View struct {
	Event
	Read      bool `json:"read"`
	Dismissed bool `json:"dismissed"`
}

type Snapshot struct {
	Revision uint64 `json:"revision"`
	Items    []View `json:"items"`
}

type Store struct {
	db         *sql.DB
	instanceID string
	now        func() time.Time
}

const ddl = `
CREATE TABLE IF NOT EXISTS external_events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT NOT NULL UNIQUE,
  authority_instance_id TEXT NOT NULL,
  source TEXT NOT NULL,
  event_key TEXT NOT NULL,
  kind TEXT NOT NULL,
  severity TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL DEFAULT '',
  data_json TEXT NOT NULL DEFAULT '{}',
  occurred_at INTEGER NOT NULL,
  received_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  UNIQUE(source, event_key)
);
CREATE INDEX IF NOT EXISTS idx_external_events_recent
  ON external_events(expires_at, sequence DESC);
CREATE TABLE IF NOT EXISTS external_event_device_state (
  event_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  read INTEGER NOT NULL DEFAULT 0 CHECK(read IN (0,1)),
  dismissed INTEGER NOT NULL DEFAULT 0 CHECK(dismissed IN (0,1)),
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(event_id, device_id),
  FOREIGN KEY(event_id) REFERENCES external_events(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS external_event_attachments (
  id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL REFERENCES external_events(id) ON DELETE CASCADE,
  external_id TEXT NOT NULL DEFAULT '', ordinal INTEGER NOT NULL,
  status TEXT NOT NULL, source_url TEXT NOT NULL DEFAULT '', local_path TEXT NOT NULL DEFAULT '',
  mime_type TEXT NOT NULL DEFAULT '', display_name TEXT NOT NULL DEFAULT '',
  size_bytes INTEGER NOT NULL DEFAULT 0, sha256 TEXT NOT NULL DEFAULT '', error_code TEXT NOT NULL DEFAULT '',
  attempt INTEGER NOT NULL DEFAULT 0, available_at INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT NOT NULL DEFAULT '', lease_until INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  UNIQUE(event_id, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_external_event_attachments_queue
  ON external_event_attachments(status,available_at,ordinal,id);
`

func Open(dataDir, instanceID string) (*Store, error) {
	path := filepath.Join(dataDir, "everything_go_events.db")
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
	if _, err := db.Exec(ddl); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, instanceID: instanceID, now: time.Now}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Insert(ctx context.Context, in Input) (Event, bool, error) {
	return s.InsertWithAttachments(ctx, in, nil)
}

func (s *Store) InsertWithAttachments(ctx context.Context, in Input, attachments []AttachmentInput) (Event, bool, error) {
	in.Source = strings.TrimSpace(in.Source)
	in.EventKey = strings.TrimSpace(in.EventKey)
	in.Kind = strings.TrimSpace(in.Kind)
	in.Severity = strings.ToLower(strings.TrimSpace(in.Severity))
	in.Title = strings.TrimSpace(in.Title)
	in.URL = strings.TrimSpace(in.URL)
	if in.Severity == "" {
		in.Severity = "info"
	}
	if in.Source == "" || len(in.Source) > 64 || in.EventKey == "" || len(in.EventKey) > 128 || in.Kind == "" || len(in.Kind) > 64 {
		return Event{}, false, errors.New("source, event key, and kind are required and exceed their size limit")
	}
	if in.Title == "" || len(in.Title) > 240 || len(in.Body) > 32*1024 || len(in.URL) > 2048 {
		return Event{}, false, errors.New("event title, body, or URL exceeds its size limit")
	}
	if in.URL != "" {
		parsed, err := url.Parse(in.URL)
		if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
			return Event{}, false, errors.New("event URL must be an absolute HTTP or HTTPS URL")
		}
	}
	switch in.Severity {
	case "info", "success", "warning", "error", "critical":
	default:
		return Event{}, false, errors.New("invalid event severity")
	}
	now := s.now().UnixMilli()
	if in.OccurredAt == 0 {
		in.OccurredAt = now
	}
	if in.ExpiresAt == 0 {
		in.ExpiresAt = now + defaultTTL.Milliseconds()
	}
	if in.ExpiresAt <= now {
		return Event{}, false, errors.New("event is already expired")
	}
	if len(in.Data) == 0 {
		in.Data = json.RawMessage(`{}`)
	}
	var metadata map[string]json.RawMessage
	if len(in.Data) > maxDataBytes || json.Unmarshal(in.Data, &metadata) != nil || metadata == nil {
		return Event{}, false, errors.New("event data must be a JSON object up to 16 KB")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, false, err
	}
	defer tx.Rollback()
	if err := gcTx(tx, now); err != nil {
		return Event{}, false, err
	}
	id := "evt_" + randomHex(16)
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO external_events
    (id,authority_instance_id,source,event_key,kind,severity,title,body,url,data_json,occurred_at,received_at,expires_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, s.instanceID, in.Source, in.EventKey, in.Kind, in.Severity, in.Title,
		in.Body, in.URL, string(in.Data), in.OccurredAt, now, in.ExpiresAt)
	if err != nil {
		return Event{}, false, err
	}
	rows, _ := result.RowsAffected()
	deduped := rows == 0
	event, err := getBySourceKey(ctx, tx, in.Source, in.EventKey)
	if err != nil {
		return Event{}, false, err
	}
	if !deduped {
		for index, attachment := range attachments {
			if attachment.Ordinal < 0 {
				attachment.Ordinal = index
			}
			if err := validateAttachmentInput(attachment); err != nil {
				return Event{}, false, err
			}
			attachmentID := stableAttachmentID(event.ID, attachment.ExternalID, attachment.Ordinal)
			_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO external_event_attachments
        (id,event_id,external_id,ordinal,status,source_url,mime_type,display_name,available_at,created_at,updated_at)
        VALUES(?,?,?,?, 'pending',?,?,?,?,?,?)`, attachmentID, event.ID, strings.TrimSpace(attachment.ExternalID),
				attachment.Ordinal, strings.TrimSpace(attachment.SourceURL), strings.TrimSpace(attachment.MIMEType),
				strings.TrimSpace(attachment.DisplayName), now, now, now)
			if err != nil {
				return Event{}, false, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return Event{}, false, err
	}
	event.Attachments, _ = s.Attachments(ctx, event.ID)
	return event, deduped, nil
}

func validateAttachmentInput(in AttachmentInput) error {
	if in.Ordinal < 0 || in.Ordinal > 100 || len(in.ExternalID) > 256 || len(in.DisplayName) > 240 || len(in.MIMEType) > 128 {
		return errors.New("invalid event attachment metadata")
	}
	parsed, err := url.Parse(strings.TrimSpace(in.SourceURL))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || len(in.SourceURL) > 4096 {
		return errors.New("event attachment source must be an absolute HTTPS URL")
	}
	return nil
}

func stableAttachmentID(eventID, externalID string, ordinal int) string {
	sum := sha256.Sum256([]byte(eventID + "\x00" + externalID + "\x00" + fmt.Sprint(ordinal)))
	return "eatt_" + hex.EncodeToString(sum[:16])
}

func (s *Store) Snapshot(ctx context.Context, deviceID string, limit int) (Snapshot, error) {
	if limit <= 0 || limit > defaultListSize {
		limit = defaultListSize
	}
	now := s.now().UnixMilli()
	rows, err := s.db.QueryContext(ctx, `SELECT e.id,e.sequence,e.authority_instance_id,e.source,e.event_key,e.kind,e.severity,
    e.title,e.body,e.url,e.data_json,e.occurred_at,e.received_at,e.expires_at,
    COALESCE(d.read,0),COALESCE(d.dismissed,0)
    FROM external_events e
    LEFT JOIN external_event_device_state d ON d.event_id=e.id AND d.device_id=?
    WHERE e.expires_at>? AND COALESCE(d.dismissed,0)=0
    ORDER BY e.sequence DESC LIMIT ?`, deviceID, now, limit)
	if err != nil {
		return Snapshot{}, err
	}
	defer rows.Close()
	items := make([]View, 0)
	var revision uint64
	for rows.Next() {
		var view View
		var data string
		var read, dismissed int
		if err := rows.Scan(&view.ID, &view.Sequence, &view.AuthorityInstanceID, &view.Source, &view.EventKey,
			&view.Kind, &view.Severity, &view.Title, &view.Body, &view.URL, &data,
			&view.OccurredAt, &view.ReceivedAt, &view.ExpiresAt, &read, &dismissed); err != nil {
			return Snapshot{}, err
		}
		view.Data = json.RawMessage(data)
		view.Read, view.Dismissed = read != 0, dismissed != 0
		if view.Sequence > revision {
			revision = view.Sequence
		}
		items = append(items, view)
	}
	if err := rows.Close(); err != nil {
		return Snapshot{}, err
	}
	for index := range items {
		items[index].Attachments, _ = s.Attachments(ctx, items[index].ID)
	}
	return Snapshot{Revision: revision, Items: items}, nil
}

func (s *Store) Mark(ctx context.Context, eventID, deviceID string, read, dismissed *bool) (View, error) {
	if strings.TrimSpace(eventID) == "" || strings.TrimSpace(deviceID) == "" {
		return View{}, errors.New("event_id and device_id are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return View{}, err
	}
	defer tx.Rollback()
	event, err := getByID(ctx, tx, eventID)
	if err != nil {
		return View{}, err
	}
	currentRead, currentDismissed := false, false
	var readInt, dismissedInt int
	err = tx.QueryRowContext(ctx, `SELECT read,dismissed FROM external_event_device_state WHERE event_id=? AND device_id=?`, eventID, deviceID).Scan(&readInt, &dismissedInt)
	if err != nil && err != sql.ErrNoRows {
		return View{}, err
	}
	currentRead, currentDismissed = readInt != 0, dismissedInt != 0
	if read != nil {
		currentRead = *read
	}
	if dismissed != nil {
		currentDismissed = *dismissed
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO external_event_device_state(event_id,device_id,read,dismissed,updated_at)
    VALUES(?,?,?,?,?) ON CONFLICT(event_id,device_id) DO UPDATE SET
    read=excluded.read,dismissed=excluded.dismissed,updated_at=excluded.updated_at`,
		eventID, deviceID, boolInt(currentRead), boolInt(currentDismissed), s.now().UnixMilli())
	if err != nil {
		return View{}, err
	}
	if err := tx.Commit(); err != nil {
		return View{}, err
	}
	return View{Event: event, Read: currentRead, Dismissed: currentDismissed}, nil
}

func (s *Store) Get(ctx context.Context, eventID string) (Event, error) {
	event, err := getByID(ctx, s.db, eventID)
	if err != nil {
		return Event{}, err
	}
	event.Attachments, _ = s.Attachments(ctx, event.ID)
	return event, nil
}

// Attachments returns canonical, URL-free attachment state in provider order.
func (s *Store) Attachments(ctx context.Context, eventID string) ([]Attachment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,event_id,external_id,ordinal,status,mime_type,display_name,
    local_path,size_bytes,sha256,error_code,created_at,updated_at
    FROM external_event_attachments WHERE event_id=? ORDER BY ordinal,id`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attachment
	for rows.Next() {
		var item Attachment
		if err := rows.Scan(&item.ID, &item.EventID, &item.ExternalID, &item.Ordinal, &item.Status,
			&item.MIMEType, &item.DisplayName, &item.LocalPath, &item.SizeBytes, &item.SHA256,
			&item.ErrorCode, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) GetAttachment(ctx context.Context, attachmentID string) (Attachment, error) {
	var item Attachment
	err := s.db.QueryRowContext(ctx, `SELECT id,event_id,external_id,ordinal,status,mime_type,display_name,
    local_path,size_bytes,sha256,error_code,created_at,updated_at
    FROM external_event_attachments WHERE id=?`, attachmentID).Scan(&item.ID, &item.EventID, &item.ExternalID,
		&item.Ordinal, &item.Status, &item.MIMEType, &item.DisplayName, &item.LocalPath, &item.SizeBytes,
		&item.SHA256, &item.ErrorCode, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Attachment{}, ErrNotFound
	}
	return item, err
}

type AttachmentFetch struct {
	Attachment
	SourceURL string
	Attempt   int
}

// ClaimAttachmentFetch leases one pending attachment. A restart makes expired
// leases retryable; callers must complete or fail the lease explicitly.
func (s *Store) ClaimAttachmentFetch(ctx context.Context, owner string, now int64, lease time.Duration) (AttachmentFetch, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AttachmentFetch{}, false, err
	}
	defer tx.Rollback()
	var item AttachmentFetch
	err = tx.QueryRowContext(ctx, `SELECT id,event_id,external_id,ordinal,status,mime_type,display_name,local_path,
    size_bytes,sha256,error_code,created_at,updated_at,source_url,attempt
    FROM external_event_attachments
    WHERE status IN ('pending','retry') AND available_at<=? AND lease_until<=?
    ORDER BY available_at,ordinal,id LIMIT 1`, now, now).Scan(&item.ID, &item.EventID, &item.ExternalID,
		&item.Ordinal, &item.Status, &item.MIMEType, &item.DisplayName, &item.LocalPath, &item.SizeBytes,
		&item.SHA256, &item.ErrorCode, &item.CreatedAt, &item.UpdatedAt, &item.SourceURL, &item.Attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return AttachmentFetch{}, false, nil
	}
	if err != nil {
		return AttachmentFetch{}, false, err
	}
	item.Attempt++
	result, err := tx.ExecContext(ctx, `UPDATE external_event_attachments
    SET status='fetching',attempt=?,lease_owner=?,lease_until=?,updated_at=?
    WHERE id=? AND status IN ('pending','retry') AND lease_until<=?`, item.Attempt, owner, now+lease.Milliseconds(), now, item.ID, now)
	if err != nil {
		return AttachmentFetch{}, false, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return AttachmentFetch{}, false, nil
	}
	item.Status = "fetching"
	if err := tx.Commit(); err != nil {
		return AttachmentFetch{}, false, err
	}
	return item, true, nil
}

func (s *Store) CompleteAttachmentFetch(ctx context.Context, id, owner, path, mimeType, digest string, size int64) error {
	now := s.now().UnixMilli()
	result, err := s.db.ExecContext(ctx, `UPDATE external_event_attachments
    SET status='available',source_url='',local_path=?,mime_type=?,size_bytes=?,sha256=?,error_code='',
        lease_owner='',lease_until=0,updated_at=?
    WHERE id=? AND status='fetching' AND lease_owner=?`, path, mimeType, size, digest, now, id, owner)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) FailAttachmentFetch(ctx context.Context, id, owner, code string, retryAt int64, terminal bool) error {
	status := "retry"
	clearSource := 0
	if terminal {
		status = "missing"
		clearSource = 1
	}
	code = strings.TrimSpace(code)
	if len(code) > 80 {
		code = code[:80]
	}
	_, err := s.db.ExecContext(ctx, `UPDATE external_event_attachments
    SET status=?,error_code=?,source_url=CASE WHEN ?=1 THEN '' ELSE source_url END,
        available_at=?,lease_owner='',lease_until=0,updated_at=?
    WHERE id=? AND status='fetching' AND lease_owner=?`, status, clearSource, retryAt, s.now().UnixMilli(), id, owner)
	return err
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getBySourceKey(ctx context.Context, q queryRower, source, eventKey string) (Event, error) {
	return scanEvent(q.QueryRowContext(ctx, `SELECT id,sequence,authority_instance_id,source,event_key,kind,severity,
    title,body,url,data_json,occurred_at,received_at,expires_at FROM external_events WHERE source=? AND event_key=?`, source, eventKey))
}

func getByID(ctx context.Context, q queryRower, id string) (Event, error) {
	return scanEvent(q.QueryRowContext(ctx, `SELECT id,sequence,authority_instance_id,source,event_key,kind,severity,
    title,body,url,data_json,occurred_at,received_at,expires_at FROM external_events WHERE id=?`, id))
}

func scanEvent(row *sql.Row) (Event, error) {
	var event Event
	var data string
	if err := row.Scan(&event.ID, &event.Sequence, &event.AuthorityInstanceID, &event.Source, &event.EventKey,
		&event.Kind, &event.Severity, &event.Title, &event.Body, &event.URL, &data,
		&event.OccurredAt, &event.ReceivedAt, &event.ExpiresAt); err != nil {
		if err == sql.ErrNoRows {
			return Event{}, ErrNotFound
		}
		return Event{}, err
	}
	event.Data = json.RawMessage(data)
	return event, nil
}

func gcTx(tx *sql.Tx, now int64) error {
	if _, err := tx.Exec("DELETE FROM external_events WHERE expires_at<=?", now); err != nil {
		return err
	}
	_, err := tx.Exec(`DELETE FROM external_events WHERE sequence IN (
    SELECT sequence FROM external_events ORDER BY sequence DESC LIMIT -1 OFFSET ?
  )`, maxEvents)
	return err
}

func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())[:n]
	}
	return hex.EncodeToString(b)[:n]
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
