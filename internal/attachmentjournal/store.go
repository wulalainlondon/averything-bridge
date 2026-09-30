// Package attachmentjournal persists bridge-discovered media and documents.
// Records contain stable local facts only; delivery URLs are intentionally
// materialized by core at send time because tunnel addresses are ephemeral.
package attachmentjournal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"everything-go/internal/protocol"
)

const (
	maxRecords = 10000
	ttl        = 30 * 24 * time.Hour
)

// Record is the canonical, URL-free representation of one attachment mention.
// RequestID is part of the identity, so the same path referenced by different
// turns remains two independently ordered attachments.
type Record struct {
	AttachmentID  string          `json:"attachment_id"`
	SessionID     string          `json:"session_id"`
	RequestID     string          `json:"request_id,omitempty"`
	Kind          string          `json:"kind"` // media | document
	Path          string          `json:"path"`
	MIMEType      string          `json:"mime_type"`
	DisplayName   string          `json:"display_name,omitempty"`
	MediaType     string          `json:"media_type,omitempty"`
	DocumentType  string          `json:"document_type,omitempty"`
	CreatedAt     int64           `json:"created_at"`
	Sequence      uint64          `json:"sequence"`
	AckedByDevice map[string]bool `json:"acked_by_device,omitempty"`
	PinnedByWork  map[string]bool `json:"pinned_by_work,omitempty"`
}

type snapshot struct {
	NextSequence uint64    `json:"next_sequence"`
	Records      []*Record `json:"records"`
}

// Store is a bounded persistent attachment registry. Acknowledgements are
// retained per device; one device can never consume another device's cursor.
type Store struct {
	mu           sync.Mutex
	path         string
	nextSequence uint64
	records      []*Record
	byID         map[string]*Record
	now          func() time.Time
}

func New(dataDir string) *Store {
	s := &Store{byID: make(map[string]*Record), now: time.Now}
	if dataDir != "" {
		s.path = filepath.Join(dataDir, "attachment_journal.json")
	}
	s.load()
	return s
}

// Add canonicalizes a media/document event. added=false means the exact same
// session/request/path attachment was already registered (idempotent rescan).
func (s *Store) Add(event any) (Record, bool) {
	r, ok := recordFromEvent(event)
	if !ok {
		return Record{}, false
	}
	r.AttachmentID = stableID(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.byID[r.AttachmentID]; existing != nil {
		return cloneRecord(existing), false
	}
	s.gcLocked()
	s.nextSequence++
	r.Sequence = s.nextSequence
	r.CreatedAt = s.now().UnixMilli()
	r.AckedByDevice = make(map[string]bool)
	r.PinnedByWork = make(map[string]bool)
	stored := r
	s.records = append(s.records, &stored)
	s.byID[r.AttachmentID] = &stored
	s.enforceCapLocked()
	s.saveLocked()
	return cloneRecord(&stored), true
}

// AddCanonical registers bytes materialized by a trusted Bridge subsystem
// (for example cross-Bridge relay). Transport URLs never enter this boundary.
func (s *Store) AddCanonical(record Record) (Record, bool) {
	record.SessionID = strings.TrimSpace(record.SessionID)
	record.RequestID = strings.TrimSpace(record.RequestID)
	record.Path = filepath.Clean(strings.TrimSpace(record.Path))
	if record.SessionID == "" || record.Path == "." || record.Path == "" || !filepath.IsAbs(record.Path) || (record.Kind != "media" && record.Kind != "document") {
		return Record{}, false
	}
	record.AttachmentID = stableID(record)
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.byID[record.AttachmentID]; existing != nil {
		return cloneRecord(existing), false
	}
	s.gcLocked()
	s.nextSequence++
	record.Sequence = s.nextSequence
	record.CreatedAt = s.now().UnixMilli()
	record.AckedByDevice = make(map[string]bool)
	record.PinnedByWork = make(map[string]bool)
	stored := record
	s.records = append(s.records, &stored)
	s.byID[stored.AttachmentID] = &stored
	s.enforceCapLocked()
	s.saveLocked()
	return cloneRecord(&stored), true
}

// Get returns canonical attachment facts without materializing a transport
// URL. Callers generate URLs at delivery time from Path.
func (s *Store) Get(attachmentID string) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.byID[attachmentID]
	if record == nil {
		return Record{}, false
	}
	return cloneRecord(record), true
}

// Pin keeps a canonical record alive while a WorkItem references it. The
// WorkItem ID is the idempotency key, so restart/reconciliation can safely pin
// again without inflating a reference count.
func (s *Store) Pin(attachmentID, workItemID string) (found, added bool) {
	if attachmentID == "" || workItemID == "" {
		return false, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.byID[attachmentID]
	if record == nil {
		return false, false
	}
	if record.PinnedByWork == nil {
		record.PinnedByWork = make(map[string]bool)
	}
	if record.PinnedByWork[workItemID] {
		return true, false
	}
	record.PinnedByWork[workItemID] = true
	s.saveLocked()
	return true, true
}

func (s *Store) Unpin(attachmentID, workItemID string) {
	if attachmentID == "" || workItemID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.byID[attachmentID]
	if record == nil || !record.PinnedByWork[workItemID] {
		return
	}
	delete(record.PinnedByWork, workItemID)
	s.saveLocked()
}

// Pending returns a device's unacknowledged records for one session in
// canonical order. Replays are session-scoped so restoring one conversation
// can never ACK attachments belonging to another conversation that is not yet
// present in the client UI.
func (s *Store) Pending(deviceID, sessionID string, limit int) []Record {
	if deviceID == "" || sessionID == "" || limit <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gcLocked() {
		s.saveLocked()
	}
	out := make([]Record, 0, limit)
	for _, r := range s.records {
		if r.SessionID != sessionID {
			continue
		}
		if r.AckedByDevice[deviceID] {
			continue
		}
		out = append(out, cloneRecord(r))
		if len(out) == limit {
			break
		}
	}
	return out
}

// Ack commits only the named device's delivery state.
func (s *Store) Ack(deviceID string, attachmentIDs []string) int {
	if deviceID == "" || len(attachmentIDs) == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	committed := 0
	for _, id := range attachmentIDs {
		r := s.byID[id]
		if r == nil || r.AckedByDevice[deviceID] {
			continue
		}
		if r.AckedByDevice == nil {
			r.AckedByDevice = make(map[string]bool)
		}
		r.AckedByDevice[deviceID] = true
		committed++
	}
	if committed > 0 {
		s.saveLocked()
	}
	return committed
}

func recordFromEvent(event any) (Record, bool) {
	switch e := event.(type) {
	case protocol.Media:
		return Record{SessionID: e.SessionID, RequestID: e.RequestID, Kind: "media", Path: e.Path,
			MIMEType: mimeForMedia(e.MediaType, e.Path), DisplayName: filepath.Base(e.Path), MediaType: e.MediaType}, e.Path != ""
	case protocol.Document:
		name := e.Title
		if name == "" {
			name = filepath.Base(e.Path)
		}
		return Record{SessionID: e.SessionID, RequestID: e.RequestID, Kind: "document", Path: e.Path,
			MIMEType: mimeForDocument(e.DocType), DisplayName: name, DocumentType: e.DocType}, e.Path != ""
	default:
		return Record{}, false
	}
}

func stableID(r Record) string {
	sum := sha256.Sum256([]byte(r.SessionID + "\x00" + r.RequestID + "\x00" + r.Kind + "\x00" + r.Path))
	return "att_" + hex.EncodeToString(sum[:12])
}

func mimeForMedia(kind, path string) string {
	if kind == "video" {
		switch filepath.Ext(path) {
		case ".mov":
			return "video/quicktime"
		default:
			return "video/mp4"
		}
	}
	switch filepath.Ext(path) {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	default:
		return "image/png"
	}
}

func mimeForDocument(kind string) string {
	if kind == "pdf" {
		return "application/pdf"
	}
	return "text/html"
}

func cloneRecord(r *Record) Record {
	out := *r
	out.AckedByDevice = make(map[string]bool, len(r.AckedByDevice))
	for id, acked := range r.AckedByDevice {
		out.AckedByDevice[id] = acked
	}
	out.PinnedByWork = make(map[string]bool, len(r.PinnedByWork))
	for id, pinned := range r.PinnedByWork {
		out.PinnedByWork[id] = pinned
	}
	return out
}

func (s *Store) load() {
	if s.path == "" {
		return
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var snap snapshot
	if json.Unmarshal(data, &snap) != nil {
		return
	}
	s.nextSequence = snap.NextSequence
	for _, r := range snap.Records {
		if r == nil || r.AttachmentID == "" || r.Path == "" {
			continue
		}
		if r.AckedByDevice == nil {
			r.AckedByDevice = make(map[string]bool)
		}
		if r.PinnedByWork == nil {
			r.PinnedByWork = make(map[string]bool)
		}
		s.records = append(s.records, r)
		s.byID[r.AttachmentID] = r
		if r.Sequence > s.nextSequence {
			s.nextSequence = r.Sequence
		}
	}
	s.gcLocked()
	s.enforceCapLocked()
}

// caller holds s.mu (or load is still single-threaded).
func (s *Store) gcLocked() bool {
	cutoff := s.now().Add(-ttl).UnixMilli()
	kept := s.records[:0]
	changed := false
	for _, r := range s.records {
		if r.CreatedAt < cutoff && len(r.PinnedByWork) == 0 {
			delete(s.byID, r.AttachmentID)
			changed = true
			continue
		}
		kept = append(kept, r)
	}
	s.records = kept
	return changed
}

func (s *Store) enforceCapLocked() {
	if len(s.records) <= maxRecords {
		return
	}
	sort.SliceStable(s.records, func(i, j int) bool { return s.records[i].Sequence < s.records[j].Sequence })
	remainingDrop := len(s.records) - maxRecords
	kept := make([]*Record, 0, len(s.records)-remainingDrop)
	for _, record := range s.records {
		if remainingDrop > 0 && len(record.PinnedByWork) == 0 {
			delete(s.byID, record.AttachmentID)
			remainingDrop--
			continue
		}
		kept = append(kept, record)
	}
	// If every excess record is pinned, preserving referential integrity takes
	// precedence over the soft capacity target. Unpinning or TTL cleanup will
	// make the next pass bounded again.
	s.records = kept
}

func (s *Store) saveLocked() {
	if s.path == "" {
		return
	}
	if os.MkdirAll(filepath.Dir(s.path), 0o700) != nil {
		return
	}
	data, err := json.Marshal(snapshot{NextSequence: s.nextSequence, Records: s.records})
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, data, 0o600) == nil {
		_ = os.Rename(tmp, s.path)
	}
}
