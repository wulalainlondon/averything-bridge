package runtimejournal

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Shared reads are a small owner-scoped projection, NOT a delivery cursor.
// The Hub admits only authenticated, paired owner devices to this API.
type sharedReadRecord struct {
	Epoch    string `json:"read_epoch"`
	Revision uint64 `json:"read_revision"`
	Version  uint64 `json:"read_version"`
}

type persistedRuntimeRecord struct {
	revision uint64
	epoch    string
	durable  bool
}

var (
	ErrSharedReadUnknownSession  = errors.New("shared read session is not initialized")
	ErrSharedReadStaleEpoch      = errors.New("stale shared read generation")
	ErrSharedReadInvalidRevision = errors.New("invalid shared read revision")
	ErrSharedReadStaleBoundary   = errors.New("shared read history boundary no longer matches")
)

func (s *Store) SharedReadsAvailable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sharedReadLoadErr == nil
}

type SharedReadState struct {
	SessionID             string `json:"session_id"`
	ReadEpoch             string `json:"read_epoch"`
	ReadRevision          uint64 `json:"read_revision"`
	ReadVersion           uint64 `json:"read_version"`
	RuntimeRevision       uint64 `json:"runtime_revision"`
	Unread                int    `json:"unread"`
	LastCompletedRevision uint64 `json:"last_completed_revision"`
}

func newReadEpoch() string { return rand.Text() }

func (s *Store) loadSharedReads() {
	if s.sharedReadPath == "" {
		return
	}
	data, err := os.ReadFile(s.sharedReadPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			s.sharedReadLoadErr = fmt.Errorf("load shared reads: %w", err)
		}
		return
	}
	var records map[string]sharedReadRecord
	if json.Unmarshal(data, &records) != nil {
		s.sharedReadLoadErr = errors.New("invalid shared read ledger; refusing to overwrite it")
		return
	}
	for id, read := range records {
		r := s.records[id]
		if r == nil || r.ReadEpoch == "" || read.Epoch != r.ReadEpoch {
			continue
		}
		if read.Revision > r.Revision {
			// A restored older lifecycle file is not the same readable generation.
			// Rotate before advertising it; cached/offline old reads cannot consume
			// newly created results whose lifecycle revisions have been reused.
			r.ReadEpoch = newReadEpoch()
			continue
		}
		s.sharedReads[id] = read
	}
}

// SharedSnapshot changes only unread. Receipt/history flags remain per device.
// First enrollment imports only known paired devices' valid historical reads.
func (s *Store) SharedSnapshot(deviceID string, ids, pairedDeviceIDs []string) ([]View, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureSharedReadsLocked(ids, pairedDeviceIDs); err != nil {
		return nil, err
	}
	views := make([]View, 0, len(ids))
	for _, id := range ids {
		r := s.records[id]
		if r == nil {
			continue
		}
		view := viewLocked(r, deviceID)
		read := s.sharedReads[id]
		view.ReadEpoch, view.ReadRevision, view.ReadVersion = read.Epoch, read.Revision, read.Version
		view.Unread = sharedReadState(r, read).Unread
		views = append(views, view)
	}
	return views, nil
}

// MarkSharedRead rejects unknown/future/stale-generation boundaries, rather
// than promoting them to latest as the legacy receipt command does.
func (s *Store) MarkSharedRead(id, epoch string, revision uint64, token string) (SharedReadState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[id]
	read, initialized := s.sharedReads[id]
	if r == nil || !initialized {
		return SharedReadState{}, false, ErrSharedReadUnknownSession
	}
	if epoch == "" || epoch != r.ReadEpoch || epoch != read.Epoch {
		return SharedReadState{}, false, ErrSharedReadStaleEpoch
	}
	if revision == 0 || revision > r.Revision {
		return SharedReadState{}, false, ErrSharedReadInvalidRevision
	}
	if revision <= read.Revision {
		return sharedReadState(r, read), false, nil
	}
	if token == "" || token != readBoundaryTokenLocked(r, revision) {
		return SharedReadState{}, false, ErrSharedReadStaleBoundary
	}
	if err := s.persistReadBoundaryLocked(r, revision); err != nil {
		return SharedReadState{}, false, err
	}
	read.Revision, read.Version = revision, read.Version+1
	candidate := s.copySharedReadsLocked()
	candidate[id] = read
	if err := s.writeSharedReadsLocked(candidate); err != nil {
		return SharedReadState{}, false, err
	}
	s.sharedReads = candidate
	return sharedReadState(r, read), true, nil
}

// This is a content-generation proof, not an authentication credential. It
// binds a loaded history boundary to the exact persisted terminal identity.
// Offline reads from a restored/reused revision cannot clear different results.
func (s *Store) ReadBoundaryToken(id, epoch string, revision uint64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[id]
	if r == nil {
		return "", ErrSharedReadUnknownSession
	}
	if epoch == "" || epoch != r.ReadEpoch {
		return "", ErrSharedReadStaleEpoch
	}
	if revision == 0 || revision > r.Revision {
		return "", ErrSharedReadInvalidRevision
	}
	return readBoundaryTokenLocked(r, revision), nil
}

func readBoundaryTokenLocked(r *Record, revision uint64) string {
	var terminal Terminal
	for _, candidate := range r.Terminals {
		if candidate.Revision <= revision && candidate.Revision >= terminal.Revision {
			terminal = candidate
		}
	}
	data, _ := json.Marshal([]any{"session-read-v1", r.ReadEpoch, revision, terminal.Revision, terminal.RequestID, terminal.Status, terminal.At})
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// A loaded tail must contain the target native reply, not merely some old
// history. Codex supplies exact persisted turn/request identities. Claude's
// precise native timestamp is only a conservative freshness check, never a
// reconstructed request association. Missing/ambiguous evidence stays unread.
func (s *Store) HistoryMatchesReadBoundary(id, epoch string, revision uint64, messages []map[string]any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[id]
	if r == nil || epoch != r.ReadEpoch || revision == 0 || revision > r.Revision || len(messages) == 0 {
		return false
	}
	var target Terminal
	for _, terminal := range r.Terminals {
		if terminal.Revision <= revision && terminal.Revision >= target.Revision {
			target = terminal
		}
	}
	if target.Revision == 0 {
		return true
	}
	if target.StartedAt == 0 && target.RequestID == r.ActiveRequestID {
		target.StartedAt = r.ActiveStartedAt
	}
	for _, message := range messages {
		if message["role"] != "assistant" {
			continue
		}
		content, _ := message["content"].(string)
		if message["history_read_result_verified"] != true || strings.TrimSpace(content) == "" {
			continue
		}
		if origin, _ := message["origin"].(string); origin != "" {
			continue
		}
		if requestID, _ := message["request_id"].(string); requestID != "" {
			if requestID == target.RequestID {
				return true
			}
			continue
		}
		if message["source"] != "claude" || message["history_timestamp_verified"] != true || target.StartedAt <= 0 {
			continue
		}
		var timestamp int64
		switch value := message["timestamp"].(type) {
		case int64:
			timestamp = value
		case float64:
			timestamp = int64(value)
		case int:
			timestamp = int64(value)
		}
		if timestamp > target.StartedAt && timestamp <= target.At {
			return true
		}
	}
	return false
}

func sharedReadState(r *Record, read sharedReadRecord) SharedReadState {
	state := SharedReadState{SessionID: r.SessionID, ReadEpoch: read.Epoch, ReadRevision: read.Revision,
		ReadVersion: read.Version, RuntimeRevision: r.Revision}
	for _, terminal := range r.Terminals {
		if terminal.Status == "completed" && terminal.Revision > state.LastCompletedRevision {
			state.LastCompletedRevision = terminal.Revision
		}
		if terminal.Status == "completed" && terminal.Revision > read.Revision {
			state.Unread++
		}
	}
	return state
}

func (s *Store) ensureSharedReadsLocked(ids, pairedDeviceIDs []string) error {
	if s.sharedReadLoadErr != nil {
		return s.sharedReadLoadErr
	}
	needsInitialization := false
	for _, id := range ids {
		if r := s.records[id]; r != nil {
			if read, ok := s.sharedReads[id]; !ok || read.Epoch != r.ReadEpoch {
				needsInitialization = true
				break
			}
		}
	}
	if !needsInitialization {
		return nil
	}
	candidate := s.copySharedReadsLocked()
	changed := false
	for _, id := range ids {
		r := s.records[id]
		if r == nil {
			continue
		}
		if read, ok := candidate[id]; ok && read.Epoch == r.ReadEpoch {
			continue
		}
		if r.ReadEpoch == "" {
			r.ReadEpoch = newReadEpoch()
		}
		read := sharedReadRecord{Epoch: r.ReadEpoch}
		for _, deviceID := range pairedDeviceIDs {
			if revision := r.ReadByDevice[deviceID]; revision <= r.Revision && revision > read.Revision {
				read.Revision = revision
			}
		}
		if read.Revision > 0 {
			read.Version = 1
		}
		if err := s.persistReadBoundaryLocked(r, read.Revision); err != nil {
			return err
		}
		candidate[id], changed = read, true
	}
	if !changed {
		return nil
	}
	if err := s.writeSharedReadsLocked(candidate); err != nil {
		return err
	}
	s.sharedReads = candidate
	return nil
}

func (s *Store) copySharedReadsLocked() map[string]sharedReadRecord {
	copy := make(map[string]sharedReadRecord, len(s.sharedReads))
	for id, read := range s.sharedReads {
		// Never resurrect removed sessions or entries from a reused session ID.
		if r := s.records[id]; r != nil && read.Epoch == r.ReadEpoch {
			copy[id] = read
		}
	}
	return copy
}

func (s *Store) persistReadBoundaryLocked(r *Record, revision uint64) error {
	if s.path == "" {
		return nil
	} // in-memory fixtures
	persisted := s.persistedRecords[r.SessionID]
	if persisted.epoch == r.ReadEpoch && persisted.revision >= revision {
		if !persisted.durable {
			if err := syncSnapshotFile(s.path); err != nil {
				return err
			}
			for id, record := range s.persistedRecords {
				record.durable = true
				s.persistedRecords[id] = record
			}
		}
		return nil
	}
	// Flush only if a read observes progress that has not reached the canonical
	// lifecycle file yet. Normal completed-result reads use the compact ledger.
	return s.writeRuntimeSnapshotLocked(true)
}

func (s *Store) writeSharedReadsLocked(records map[string]sharedReadRecord) error {
	if s.sharedReadPath == "" {
		return nil
	}
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	if err := writeSnapshotAtomic(s.sharedReadPath, data, true); err != nil {
		return fmt.Errorf("persist shared reads: %w", err)
	}
	return nil
}

func (s *Store) writeRuntimeSnapshotLocked(durable bool) error {
	if s.path == "" {
		return nil
	}
	data, err := json.Marshal(snapshot{Records: s.records})
	if err != nil {
		return err
	}
	if err := writeSnapshotAtomic(s.path, data, durable); err != nil {
		return err
	}
	s.writes++
	s.persistedRecords = make(map[string]persistedRuntimeRecord, len(s.records))
	for id, r := range s.records {
		s.persistedRecords[id] = persistedRuntimeRecord{revision: r.Revision, epoch: r.ReadEpoch, durable: durable}
	}
	// Existing delivery/read cursors have been folded into the canonical file.
	if info, err := os.Stat(s.cursorPath); err == nil && info.Size() > 0 {
		_ = os.WriteFile(s.cursorPath, nil, 0o600)
	}
	return nil
}

func writeSnapshotAtomic(target string, data []byte, durable bool) error {
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".runtime-snapshot-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if durable {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		return err
	}
	if durable {
		d, err := os.Open(dir)
		if err != nil {
			return err
		}
		defer d.Close()
		if err := d.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func syncSnapshotFile(target string) error {
	f, err := os.OpenFile(target, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	d, err := os.Open(filepath.Dir(target))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
