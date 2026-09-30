// Package session holds the bridge's session state. It is intentionally a plain
// data layer with no knowledge of transport or AI runtimes — both the Go
// connection core and any Executor read/mutate Session through here.
//
// State discipline (the hardening contract):
//   - ID and CreatedAt are immutable after Create and may be read without the
//     lock; every other field is private and reached only through methods that
//     take the per-session mutex. No external package mutates a field directly.
//   - Each session has an explicit lifecycle state (see turn.go: Idle →
//     Streaming → Stopping → Closed) and a single-flight turn queue, so two
//     turns for the same session can never run concurrently.
package session

import (
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

// Session carries generic, backend-agnostic fields. Runtime-specific state
// (e.g. the live subprocess) belongs to the Executor, keyed by Session.ID.
//
// All mutable fields are private; callers go through the methods below so the
// mutex is always held and reads see a consistent view.
type Session struct {
	mu             sync.Mutex
	configUpdating bool
	configRevision uint64
	futureConfig   *Configuration

	// ID and CreatedAt are set once at construction and never change, so they
	// are exported and safe to read concurrently without the lock.
	ID        string
	CreatedAt float64 // unix seconds (matches Python time.time())

	name                string
	metadataRevision    uint64
	nameUpdatedAt       int64
	nameUpdatedBy       string
	lastNameMutationID  string
	cwd                 string
	backend             string
	model               string
	sandbox             string
	effort              string
	serviceTier         string
	collaborationMode   string
	personality         string
	resumeID            string   // AI-runtime conversation handle (Claude UUID / Codex thread id)
	historicalResumeIDs []string // archived physical threads belonging to this logical session
	pinned              bool
	hidden              bool

	lastActivity     float64
	previewText      string
	previewRole      string
	previewUpdatedAt int64
	previewRevision  uint64
	contextUsed      int
	contextMax       int

	state State

	// Queue entries have stable IDs so a waiting message can be reserved for
	// steering or removed atomically before the worker begins it.
	mailbox        []*queuedTurn
	queueChanged   chan struct{}
	queueHolds     int
	activeQueuedID string
	quit           chan struct{}
	workerUp       bool
	turnDone       chan struct{}
}

// Snapshot is an immutable, lock-free copy of a session's fields for callers
// that need several at once (summaries, task listings, spawn argument building).
type Snapshot struct {
	ConfigRevision      uint64
	ID                  string
	Name                string
	MetadataRevision    uint64
	NameUpdatedAt       int64
	NameUpdatedBy       string
	LastNameMutationID  string
	Cwd                 string
	Backend             string
	Model               string
	Sandbox             string
	Effort              string
	ServiceTier         string
	CollaborationMode   string
	Personality         string
	ResumeID            string
	HistoricalResumeIDs []string
	CreatedAt           float64
	LastActivity        float64
	PreviewText         string
	PreviewRole         string
	PreviewUpdatedAt    int64
	PreviewRevision     uint64
	ContextUsed         int
	ContextMax          int
	Pinned              bool
	Hidden              bool
	Streaming           bool
	State               State
}

func nowSeconds() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// Snapshot returns a consistent copy of all fields under the lock.
func (s *Session) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Session) snapshotLocked() Snapshot {
	return Snapshot{
		ConfigRevision: s.configRevision,
		ID:             s.ID, Name: s.name, Cwd: s.cwd, Backend: s.backend,
		MetadataRevision: s.metadataRevision, NameUpdatedAt: s.nameUpdatedAt,
		NameUpdatedBy: s.nameUpdatedBy, LastNameMutationID: s.lastNameMutationID,
		Model: s.model, Sandbox: s.sandbox, Effort: s.effort, ResumeID: s.resumeID,
		HistoricalResumeIDs: append([]string(nil), s.historicalResumeIDs...),
		ServiceTier:         s.serviceTier, CollaborationMode: s.collaborationMode, Personality: s.personality,
		CreatedAt: s.CreatedAt, LastActivity: s.lastActivity,
		PreviewText: s.previewText, PreviewRole: s.previewRole,
		PreviewUpdatedAt: s.previewUpdatedAt, PreviewRevision: s.previewRevision,
		ContextUsed: s.contextUsed, ContextMax: s.contextMax,
		Pinned: s.pinned, Hidden: s.hidden,
		Streaming: s.state == Streaming || s.state == Stopping,
		State:     s.state,
	}
}

// --- single-field getters (hot paths) --------------------------------------

func (s *Session) Name() string     { s.mu.Lock(); defer s.mu.Unlock(); return s.name }
func (s *Session) Cwd() string      { s.mu.Lock(); defer s.mu.Unlock(); return s.cwd }
func (s *Session) Backend() string  { s.mu.Lock(); defer s.mu.Unlock(); return s.backend }
func (s *Session) ResumeID() string { s.mu.Lock(); defer s.mu.Unlock(); return s.resumeID }

// IsStreaming reports whether a turn is in flight (Streaming or Stopping).
func (s *Session) IsStreaming() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == Streaming || s.state == Stopping
}

// --- mutators ---------------------------------------------------------------

// SetName renames the session (rename_session).
func (s *Session) SetName(name string) {
	s.mu.Lock()
	s.name = name
	s.mu.Unlock()
}

// SetEffort stores the reasoning effort applied on the next claude spawn.
func (s *Session) SetEffort(effort string) {
	s.mu.Lock()
	s.effort = effort
	s.mu.Unlock()
}

// SetModel stores an exact model override. Empty means follow the runtime
// default, so it cannot use ApplyConfig's omit-empty semantics.
func (s *Session) SetModel(model string) {
	s.mu.Lock()
	s.model = model
	s.mu.Unlock()
}

// ApplyCodexSettings stores app-server thread settings. Empty values are
// meaningful for service tier/personality/mode (they clear an override), so
// callers pass pointers to distinguish omission from clearing.
func (s *Session) ApplyCodexSettings(serviceTier, collaborationMode, personality *string) {
	s.mu.Lock()
	if serviceTier != nil {
		s.serviceTier = *serviceTier
	}
	if collaborationMode != nil {
		s.collaborationMode = *collaborationMode
	}
	if personality != nil {
		s.personality = *personality
	}
	s.mu.Unlock()
}

// RestoreConfig restores an exact previously captured configuration. Unlike
// ApplyConfig, empty strings are meaningful here because they clear overrides.
// It is used to roll back a switch that the active runtime rejected.
func (s *Session) RestoreConfig(snap Snapshot) {
	s.mu.Lock()
	s.backend = snap.Backend
	s.model = snap.Model
	s.sandbox = snap.Sandbox
	s.effort = snap.Effort
	s.serviceTier = snap.ServiceTier
	s.collaborationMode = snap.CollaborationMode
	s.personality = snap.Personality
	s.mu.Unlock()
}

// SetMeta applies optional frontend-visible session metadata.
func (s *Session) SetMeta(pinned, hidden *bool) {
	s.mu.Lock()
	if pinned != nil {
		s.pinned = *pinned
	}
	if hidden != nil {
		s.hidden = *hidden
	}
	s.mu.Unlock()
}

// ApplyConfig overwrites backend/model/sandbox, ignoring empty values
// (switch_session_config semantics: only the supplied fields change).
func (s *Session) ApplyConfig(backend, model, sandbox string) {
	s.mu.Lock()
	if backend != "" {
		s.backend = backend
	}
	if model != "" {
		s.model = model
	}
	if sandbox != "" {
		s.sandbox = sandbox
	}
	s.mu.Unlock()
}

// SetResumeID records the AI-runtime conversation handle (called by the executor
// when a turn establishes or clears one).
func (s *Session) SetResumeID(id string) {
	s.mu.Lock()
	if id != "" && s.resumeID != "" && s.resumeID != id {
		s.historicalResumeIDs = appendUniqueResumeID(s.historicalResumeIDs, s.resumeID, id)
	}
	s.resumeID = id
	s.mu.Unlock()
}

// ClearResumeIDs intentionally forgets the entire logical history chain. Use
// only for the user-facing clear-session operation; recovery failures should
// call SetResumeID("") so archived generations remain readable.
func (s *Session) ClearResumeIDs() {
	s.mu.Lock()
	s.resumeID = ""
	s.historicalResumeIDs = nil
	s.mu.Unlock()
}

// AddHistoricalResumeID records a read-only physical thread without changing
// the active thread. It is used by native discovery when it sees an older
// rollout whose jl_* alias is also the stable logical session id.
func (s *Session) AddHistoricalResumeID(id string) {
	if id == "" {
		return
	}
	s.mu.Lock()
	s.historicalResumeIDs = appendUniqueResumeID(s.historicalResumeIDs, id, s.resumeID)
	s.mu.Unlock()
}

// ResumeIDs returns physical threads in chronological generation order, with
// the active writable thread last.
func (s *Session) ResumeIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.historicalResumeIDs...)
	if s.resumeID != "" {
		out = appendUniqueResumeID(out, s.resumeID, "")
	}
	return out
}

func appendUniqueResumeID(ids []string, id, active string) []string {
	if id == "" || id == active {
		return ids
	}
	for _, existing := range ids {
		if existing == id {
			return ids
		}
	}
	return append(ids, id)
}

// SetContext records backend-reported context usage for summaries/status views.
func (s *Session) SetContext(used, max int) {
	s.mu.Lock()
	if used >= 0 {
		s.contextUsed = used
	}
	if max >= 0 {
		s.contextMax = max
	}
	s.mu.Unlock()
}

// SetLastActivity records externally observed activity, such as a native CLI
// JSONL transcript update. It never moves activity backwards.
func (s *Session) SetLastActivity(ts float64) {
	if ts <= 0 {
		return
	}
	s.mu.Lock()
	if ts > s.lastActivity {
		s.lastActivity = ts
	}
	s.mu.Unlock()
}

// Registry is the in-memory session store, owned by the Go connection core.
type Registry struct {
	mu         sync.RWMutex
	mutationMu sync.Mutex
	sessions   map[string]*Session
	store      *Store
}

func NewRegistry() *Registry {
	return &Registry{sessions: make(map[string]*Session)}
}

// AttachStore wires persistence and restores any previously saved sessions.
func (r *Registry) AttachStore(store *Store) {
	r.store = store
	loaded := store.Load()
	canonicalIDs := canonicalSavedSessionIDs(loaded)
	for _, id := range canonicalIDs {
		e := loaded[id]
		resume := e.ResumeID
		if resume == "" {
			resume = e.ClaudeUUID
		}
		created := e.CreatedAt
		if created == 0 {
			created = float64(e.LastUsed)
		}
		s := &Session{
			configRevision: e.ConfigRevision,
			ID:             id, CreatedAt: created,
			name: e.Name, cwd: e.Cwd, backend: e.Backend,
			metadataRevision: e.MetadataRevision, nameUpdatedAt: e.NameUpdatedAt,
			nameUpdatedBy: e.NameUpdatedBy, lastNameMutationID: e.LastNameMutationID,
			model: e.Model, sandbox: e.Sandbox, resumeID: resume,
			historicalResumeIDs: append([]string(nil), e.HistoricalResumeIDs...),
			effort:              e.Effort,
			serviceTier:         e.ServiceTier, collaborationMode: e.CollaborationMode, personality: e.Personality,
			pinned: e.Pinned, hidden: e.Hidden,
			lastActivity: float64(e.LastUsed),
			previewText:  e.PreviewText, previewRole: e.PreviewRole,
			previewUpdatedAt: e.PreviewUpdatedAt, previewRevision: e.PreviewRevision,
			state: Idle,
		}
		if previewActivity := float64(e.PreviewUpdatedAt) / 1000; previewActivity > s.lastActivity {
			s.lastActivity = previewActivity
		}
		r.mu.Lock()
		r.sessions[id] = s
		r.mu.Unlock()
	}
	if len(canonicalIDs) != len(loaded) {
		if err := store.Save(r.List()); err != nil {
			log.Printf("duplicate session migration persist failed: %v", err)
		} else {
			log.Printf("collapsed %d duplicate saved session(s) by resume id", len(loaded)-len(canonicalIDs))
		}
	}
}

// canonicalSavedSessionIDs collapses legacy duplicate rows that point at the
// same native Claude/Codex conversation. Older bridge builds allowed two app
// session IDs to retain one resume ID, which is unsafe for single-thread
// runtimes and also produces duplicate dashboard rows. Prefer explicit app
// sessions over native-watcher jl_ aliases, then pinned and most-recent rows.
// Store.Load already records every loaded ID as known, so the next Persist
// removes discarded duplicates from disk as part of the migration.
func canonicalSavedSessionIDs(loaded map[string]savedEntry) []string {
	ids := make([]string, 0, len(loaded))
	for id := range loaded {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	chosen := make(map[string]string)
	keep := make(map[string]bool, len(ids))
	for _, id := range ids {
		e := loaded[id]
		resumeID := firstNonEmptyRegistry(e.ResumeID, e.ClaudeUUID)
		if resumeID == "" {
			keep[id] = true
			continue
		}
		if previousID, ok := chosen[resumeID]; !ok {
			chosen[resumeID] = id
			keep[id] = true
		} else if preferSavedSession(id, e, previousID, loaded[previousID]) {
			delete(keep, previousID)
			chosen[resumeID] = id
			keep[id] = true
		}
	}

	out := make([]string, 0, len(keep))
	for id := range keep {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func preferSavedSession(id string, e savedEntry, previousID string, previous savedEntry) bool {
	appSession := !strings.HasPrefix(id, "jl_")
	previousAppSession := !strings.HasPrefix(previousID, "jl_")
	if appSession != previousAppSession {
		return appSession
	}
	if e.Pinned != previous.Pinned {
		return e.Pinned
	}
	if e.LastUsed != previous.LastUsed {
		return e.LastUsed > previous.LastUsed
	}
	if e.CreatedAt != previous.CreatedAt {
		return e.CreatedAt > previous.CreatedAt
	}
	return id < previousID
}

func firstNonEmptyRegistry(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// Persist writes the current sessions to the attached store (no-op if none).
// Safe to call from a goroutine; writes are serialized inside the Store.
func (r *Registry) Persist() {
	if err := r.PersistDurably(); err != nil {
		log.Printf("session persist failed: %v", err)
	}
}

// PersistDurably lets durable queues refuse ownership if their Session identity
// could not be saved. It uses the same mutation boundary as preview/rename.
func (r *Registry) PersistDurably() error {
	r.mutationMu.Lock()
	defer r.mutationMu.Unlock()
	if r.store == nil {
		return nil
	}
	return r.store.Save(r.List())
}

var (
	ErrRenameConflict = errors.New("session metadata revision conflict")
	ErrRenameEmpty    = errors.New("session name is empty")
	ErrPreviewInvalid = errors.New("session preview is invalid")
	ErrSessionMissing = errors.New("session not found")
)

// CommitPreviewAndPersist atomically advances the server-authoritative row
// preview. The timestamp is part of the same projection as the text and role,
// so clients never have to combine a fresh activity time with stale content.
// Older observations are ignored; equal retries are idempotent.
func (r *Registry) CommitPreviewAndPersist(id, text, role string, updatedAt int64) (Snapshot, bool, error) {
	text = strings.TrimSpace(text)
	role = strings.TrimSpace(role)
	if text == "" || (role != "user" && role != "assistant") || updatedAt <= 0 {
		return Snapshot{}, false, ErrPreviewInvalid
	}

	r.mutationMu.Lock()
	defer r.mutationMu.Unlock()
	s, ok := r.Get(id)
	if !ok {
		return Snapshot{}, false, ErrSessionMissing
	}
	before := s.Snapshot()
	if updatedAt < before.PreviewUpdatedAt ||
		(updatedAt == before.PreviewUpdatedAt && before.PreviewText == text && before.PreviewRole == role) {
		return before, false, nil
	}

	s.mu.Lock()
	s.previewText = text
	s.previewRole = role
	s.previewUpdatedAt = updatedAt
	s.previewRevision++
	activity := float64(updatedAt) / 1000
	if activity > s.lastActivity {
		s.lastActivity = activity
	}
	after := s.snapshotLocked()
	s.mu.Unlock()

	if r.store != nil {
		if err := r.store.Save(r.List()); err != nil {
			s.mu.Lock()
			if s.previewRevision == after.PreviewRevision {
				s.previewText = before.PreviewText
				s.previewRole = before.PreviewRole
				s.previewUpdatedAt = before.PreviewUpdatedAt
				s.previewRevision = before.PreviewRevision
				if s.lastActivity == after.LastActivity {
					s.lastActivity = before.LastActivity
				}
			}
			s.mu.Unlock()
			return before, false, err
		}
	}
	return after, true, nil
}

// RepairPreviewAndPersist replaces a projection only when it still exactly
// matches expectedInvalidText. This narrow compare-and-swap is used to migrate
// previews that history can prove came from a tool-only card. It deliberately
// permits an older human message timestamp while never reducing LastActivity.
func (r *Registry) RepairPreviewAndPersist(id, expectedInvalidText, text, role string, updatedAt int64) (Snapshot, bool, error) {
	text = strings.TrimSpace(text)
	role = strings.TrimSpace(role)
	if expectedInvalidText == "" || text == "" || (role != "user" && role != "assistant") || updatedAt <= 0 {
		return Snapshot{}, false, ErrPreviewInvalid
	}

	r.mutationMu.Lock()
	defer r.mutationMu.Unlock()
	s, ok := r.Get(id)
	if !ok {
		return Snapshot{}, false, ErrSessionMissing
	}
	before := s.Snapshot()
	if before.PreviewText != expectedInvalidText || before.PreviewRole != "assistant" {
		return before, false, nil
	}

	s.mu.Lock()
	s.previewText = text
	s.previewRole = role
	s.previewUpdatedAt = updatedAt
	s.previewRevision++
	after := s.snapshotLocked()
	s.mu.Unlock()

	if r.store != nil {
		if err := r.store.Save(r.List()); err != nil {
			s.mu.Lock()
			if s.previewRevision == after.PreviewRevision {
				s.previewText = before.PreviewText
				s.previewRole = before.PreviewRole
				s.previewUpdatedAt = before.PreviewUpdatedAt
				s.previewRevision = before.PreviewRevision
			}
			s.mu.Unlock()
			return before, false, err
		}
	}
	return after, true, nil
}

// RenameAndPersist is the authoritative Session-name commit. The mutation is
// serialized with every Registry persist, written atomically to disk, and only
// then returned to the caller for broadcast. A failed disk write rolls memory
// back, so session_renamed always means the new name survived a restart.
func (r *Registry) RenameAndPersist(id, name, updatedBy, mutationID string, expectedRevision *uint64) (Snapshot, bool, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Snapshot{}, false, ErrRenameEmpty
	}
	r.mutationMu.Lock()
	defer r.mutationMu.Unlock()

	s, ok := r.Get(id)
	if !ok {
		return Snapshot{}, false, ErrSessionMissing
	}
	before := s.Snapshot()
	if mutationID != "" && before.LastNameMutationID == mutationID {
		return before, false, nil
	}
	if expectedRevision != nil && *expectedRevision != before.MetadataRevision {
		return before, false, ErrRenameConflict
	}
	if before.Name == name {
		return before, false, nil
	}

	now := time.Now().UnixMilli()
	s.mu.Lock()
	s.name = name
	s.metadataRevision++
	s.nameUpdatedAt = now
	s.nameUpdatedBy = strings.TrimSpace(updatedBy)
	s.lastNameMutationID = mutationID
	s.mu.Unlock()
	after := s.Snapshot()
	if r.store != nil {
		if err := r.store.Save(r.List()); err != nil {
			s.mu.Lock()
			s.name = before.Name
			s.metadataRevision = before.MetadataRevision
			s.nameUpdatedAt = before.NameUpdatedAt
			s.nameUpdatedBy = before.NameUpdatedBy
			s.lastNameMutationID = before.LastNameMutationID
			s.mu.Unlock()
			return before, false, err
		}
	}
	return after, true, nil
}

// PruneCodexSessions removes persisted and in-memory Codex rows excluded by
// source policy. Native transcript files remain untouched.
func (r *Registry) PruneCodexSessions(ignore func(cwd, name string) bool) int {
	if ignore == nil {
		return 0
	}
	r.mu.Lock()
	removed := 0
	for id, s := range r.sessions {
		snap := s.Snapshot()
		if snap.Backend == "codex" && ignore(snap.Cwd, snap.Name) {
			delete(r.sessions, id)
			removed++
		}
	}
	r.mu.Unlock()
	if removed > 0 {
		r.Persist()
	}
	return removed
}

// Create registers a session under the client-supplied id. If the id already
// exists the existing session is returned (idempotent — the app may resend
// new_session on reconnect).
func (r *Registry) Create(id, name, cwd, backend, model, sandbox, resumeID string) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sessions[id]; ok {
		return s
	}
	now := nowSeconds()
	s := &Session{
		ID: id, CreatedAt: now,
		name: name, cwd: cwd, backend: backend, model: model,
		sandbox: sandbox, resumeID: resumeID, lastActivity: now,
		state: Idle,
	}
	r.sessions[id] = s
	return s
}

// HasResumeID reports whether any registered session already represents the
// native runtime conversation handle.
func (r *Registry) HasResumeID(resumeID string) bool {
	_, ok := r.FindByResumeID(resumeID)
	return ok
}

// FindByResumeID returns the canonical registered session for a native runtime
// conversation handle. It lets command routing make resume idempotent instead
// of creating a second bridge session that would compete for the same thread.
func (r *Registry) FindByResumeID(resumeID string) (*Session, bool) {
	if resumeID == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var best *Session
	for _, s := range r.sessions {
		if s.ResumeID() != resumeID {
			continue
		}
		if best == nil {
			best = s
			continue
		}
		current := s.Snapshot()
		previous := best.Snapshot()
		if current.LastActivity > previous.LastActivity ||
			(current.LastActivity == previous.LastActivity && current.ID < previous.ID) {
			best = s
		}
	}
	return best, best != nil
}

// UpsertExternal registers or refreshes a session discovered from the native
// Claude/Codex JSONL stores. It dedupes by resumeID so a bridge-created session
// and a native watcher event cannot produce two dashboard rows for one thread.
// The returned bool is true when the registry changed enough to merit a client
// sessions_list broadcast.
func (r *Registry) UpsertExternal(id, name, cwd, backend, resumeID string, lastUsed int64) (*Session, bool) {
	if id == "" || resumeID == "" {
		return nil, false
	}
	if name == "" {
		name = resumeID
		if len(name) > 8 {
			name = name[:8]
		}
	}
	now := nowSeconds()
	activity := float64(lastUsed)
	if activity <= 0 {
		activity = now
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, s := range r.sessions {
		if s.ResumeID() != resumeID {
			continue
		}
		before := s.Snapshot()
		s.mu.Lock()
		// Once a user-authored metadata revision exists, the Bridge registry is
		// the name authority. Native JSONL discovery may keep refreshing runtime
		// location/activity, but must never restore its derived transcript title.
		if s.metadataRevision == 0 && (s.name == "" || (s.ID == id && strings.HasPrefix(s.ID, "jl_"))) {
			s.name = name
		}
		if s.cwd == "" || (s.ID == id && strings.HasPrefix(s.ID, "jl_")) {
			s.cwd = cwd
		}
		if s.backend == "" {
			s.backend = backend
		}
		if activity > s.lastActivity {
			s.lastActivity = activity
		}
		after := s.snapshotLocked()
		s.mu.Unlock()
		// NOTE: lastActivity changes do NOT count as "changed". A session being
		// actively written by its CLI bumps mtime constantly; treating that as a
		// change would re-broadcast sessions_list every tick. It's also pointless:
		// the broadcast's last_activity comes from the search index's last_ts, not
		// this field. Only structural changes (name/cwd/backend) warrant a refresh.
		return s, before.Name != after.Name || before.Cwd != after.Cwd ||
			before.Backend != after.Backend
	}

	if s, ok := r.sessions[id]; ok {
		before := s.Snapshot()
		s.mu.Lock()
		// A jl_* id is derived from the first physical rollout that created the
		// logical session. After cold-recovery starts a new Codex thread, the
		// native watcher will continue observing that old rollout. Never let that
		// observation roll the active mapping backwards; retain it as history.
		if s.resumeID == "" {
			s.resumeID = resumeID
			if s.metadataRevision == 0 {
				s.name = name
			}
			s.cwd = cwd
			s.backend = backend
		} else if s.resumeID != resumeID {
			beforeCount := len(s.historicalResumeIDs)
			s.historicalResumeIDs = appendUniqueResumeID(s.historicalResumeIDs, resumeID, s.resumeID)
			if activity > s.lastActivity {
				s.lastActivity = activity
			}
			historyChanged := len(s.historicalResumeIDs) != beforeCount
			s.mu.Unlock()
			return s, historyChanged
		} else {
			if s.metadataRevision == 0 {
				s.name = name
			}
			s.cwd = cwd
			s.backend = backend
		}
		if activity > s.lastActivity {
			s.lastActivity = activity
		}
		after := s.snapshotLocked()
		s.mu.Unlock()
		// lastActivity excluded from "changed" — see note above.
		return s, before.Name != after.Name || before.Cwd != after.Cwd ||
			before.Backend != after.Backend || before.ResumeID != after.ResumeID
	}

	s := &Session{
		ID: id, CreatedAt: activity,
		name: name, cwd: cwd, backend: backend, resumeID: resumeID,
		lastActivity: activity, state: Idle,
	}
	r.sessions[id] = s
	return s, true
}

func (r *Registry) Get(id string) (*Session, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sessions[id]
	return s, ok
}

func (r *Registry) Delete(id string) {
	r.mu.Lock()
	s := r.sessions[id]
	delete(r.sessions, id)
	r.mu.Unlock()
	if s != nil {
		s.Close() // stop the turn worker so the goroutine doesn't leak
	}
}

func (r *Registry) List() []*Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		out = append(out, s)
	}
	return out
}
