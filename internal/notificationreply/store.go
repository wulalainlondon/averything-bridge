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

const (
	StatusPending     = "pending"
	StatusDispatching = "dispatching"
	StatusSent        = "sent"
	StatusFailed      = "failed"
	maxRecords        = 2000
	retention         = 7 * 24 * time.Hour
)

type Record struct {
	ID        string `json:"id"`
	SessionID string `json:"session_id"`
	Content   string `json:"content"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type snapshot struct {
	Records map[string]Record `json:"records"`
}

// Store is the durable handoff between the HTTP receipt and the Session actor.
// Dispatching entries are returned to pending after a Bridge restart, so a
// reply accepted while another turn was running is not silently lost.
type Store struct {
	mu      sync.Mutex
	path    string
	records map[string]Record
	now     func() time.Time
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
	s.saveLocked()
	return record, true, nil
}

func (s *Store) Claim(id string) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if !ok || record.Status != StatusPending {
		return record, false
	}
	record.Status = StatusDispatching
	record.UpdatedAt = s.now().UnixMilli()
	s.records[id] = record
	s.saveLocked()
	return record, true
}

func (s *Store) MarkSent(id string)           { s.update(id, StatusSent, "") }
func (s *Store) MarkFailed(id, reason string) { s.update(id, StatusFailed, reason) }

func (s *Store) update(id, status, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if !ok {
		return
	}
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
		return
	}
	var value snapshot
	if json.Unmarshal(data, &value) != nil || value.Records == nil {
		return
	}
	for id, record := range value.Records {
		if record.Status == StatusDispatching {
			record.Status = StatusPending
			record.Error = ""
			value.Records[id] = record
		}
	}
	s.records = value.Records
	s.pruneLocked()
}

func (s *Store) pruneLocked() {
	cutoff := s.now().Add(-retention).UnixMilli()
	for id, record := range s.records {
		if record.UpdatedAt < cutoff {
			delete(s.records, id)
		}
	}
	if len(s.records) <= maxRecords {
		return
	}
	ordered := make([]Record, 0, len(s.records))
	for _, record := range s.records {
		ordered = append(ordered, record)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].UpdatedAt < ordered[j].UpdatedAt })
	for _, record := range ordered[:len(ordered)-maxRecords] {
		delete(s.records, record.ID)
	}
}

func (s *Store) saveLocked() {
	if s.path == "" || os.MkdirAll(filepath.Dir(s.path), 0o700) != nil {
		return
	}
	data, err := json.Marshal(snapshot{Records: s.records})
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, data, 0o600) == nil {
		_ = os.Rename(tmp, s.path)
	}
}
