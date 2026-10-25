package notificationreply

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var ErrReplyConflict = errors.New("reply id already exists with different content")
var ErrStoreUnavailable = errors.New("reply store recovery unavailable")

const (
	StatusPending     = "pending"
	StatusDispatching = "dispatching"
	StatusUncertain   = "uncertain"
	StatusSent        = "sent"
	StatusFailed      = "failed"
	maxRecords        = 2000
	retention         = 7 * 24 * time.Hour
)

type Record struct {
	ID           string `json:"id"`
	SessionID    string `json:"session_id"`
	Content      string `json:"content"`
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
	RecoveryFrom string `json:"recovery_from,omitempty"`
}

type snapshot struct {
	Records map[string]Record `json:"records"`
}

// Store is the durable handoff between the HTTP receipt and the Session actor.
// Unfinished Dispatching may already have reached the provider. On restart
// keep its original identity/history as Uncertain; only genuine Pending resumes.
type Store struct {
	mu      sync.Mutex
	path    string
	records map[string]Record
	now     func() time.Time
	loadErr error
}

func NewStore(dataDir string) *Store {
	s := &Store{records: make(map[string]Record), now: time.Now}
	if dataDir != "" {
		s.path = filepath.Join(dataDir, "notification_replies.json")
	}
	s.load()
	return s
}

func (s *Store) Enqueue(id, sessionID, content string) (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return Record{}, false, ErrStoreUnavailable
	}
	if current, ok := s.records[id]; ok {
		if current.SessionID != sessionID || current.Content != content {
			return current, false, ErrReplyConflict
		}
		return current, false, nil
	}
	now := s.now().UnixMilli()
	record := Record{ID: id, SessionID: sessionID, Content: content, Status: StatusPending, CreatedAt: now, UpdatedAt: now}
	s.records[id] = record
	s.pruneLocked()
	if err := s.saveLocked(); err != nil {
		return record, false, err
	}
	return record, true, nil
}

func (s *Store) Claim(id string) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if s.loadErr != nil || !ok || record.Status != StatusPending {
		return record, false
	}
	before := record
	record.Status = StatusDispatching
	record.UpdatedAt = s.now().UnixMilli()
	s.records[id] = record
	if err := s.saveLocked(); err != nil {
		s.records[id] = before
		return before, false
	}
	return record, true
}

func (s *Store) MarkSent(id string)           { s.update(id, StatusSent, "") }
func (s *Store) MarkFailed(id, reason string) { s.update(id, StatusFailed, reason) }

func (s *Store) update(id, status, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if s.loadErr != nil || !ok {
		return
	}
	if record.Status == StatusUncertain {
		return
	} // requires original receipt reconciliation, never an unqualified late update
	record.Status, record.Error, record.UpdatedAt = status, reason, s.now().UnixMilli()
	s.records[id] = record
	s.saveLocked()
}

func (s *Store) Pending() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, 0)
	for _, record := range s.records {
		if record.Status == StatusPending {
			out = append(out, record)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	return out
}

func (s *Store) Get(id string) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	return record, ok
}

func (s *Store) load() {
	if s.path == "" {
		return
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if !os.IsNotExist(err) {
			s.loadErr = ErrStoreUnavailable
		}
		return
	}
	var value snapshot
	if json.Unmarshal(data, &value) != nil || value.Records == nil {
		s.loadErr = ErrStoreUnavailable
		return
	}
	changed := false
	for id, record := range value.Records {
		if id == "" || record.ID != id || record.SessionID == "" {
			s.loadErr = ErrStoreUnavailable
			return
		}
		if record.Status == StatusDispatching {
			record.Status = StatusUncertain
			record.RecoveryFrom = StatusDispatching
			changed = true
			value.Records[id] = record
		}
	}
	s.records = value.Records
	s.pruneLocked()
	if changed {
		_ = s.saveLocked()
	}
}

func (s *Store) pruneLocked() {
	cutoff := s.now().Add(-retention).UnixMilli()
	for id, record := range s.records {
		if record.UpdatedAt < cutoff && (record.Status == StatusSent || record.Status == StatusFailed) {
			delete(s.records, id)
		}
	}
	if len(s.records) <= maxRecords {
		return
	}
	ordered := make([]Record, 0, len(s.records))
	for _, record := range s.records {
		if record.Status == StatusSent || record.Status == StatusFailed {
			ordered = append(ordered, record)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].UpdatedAt < ordered[j].UpdatedAt })
	remove := len(s.records) - maxRecords
	if remove > len(ordered) {
		remove = len(ordered)
	}
	for _, record := range ordered[:remove] {
		delete(s.records, record.ID)
	}
}

func (s *Store) saveLocked() error {
	if s.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0700); err != nil {
		return err
	}
	data, err := json.Marshal(snapshot{Records: s.records})
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(tmp, s.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
