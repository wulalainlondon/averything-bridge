// Package runtimejournal persists the authoritative, compact state of Bridge
// sessions. Streaming text remains canonical in the native rollout history;
// this journal records only lifecycle, terminal and per-device delivery/read
// cursors so reconnecting clients can deterministically repair their UI.
package runtimejournal

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	maxSessions        = 2000
	progressFlushDelay = 200 * time.Millisecond
)

type Terminal struct {
	Revision  uint64 `json:"revision"`
	RequestID string `json:"request_id,omitempty"`
	Status    string `json:"status"`
	At        int64  `json:"at"`
	Source    string `json:"source,omitempty"` // bridge_recovery is uncertain, not a native terminal
}

type Record struct {
	SessionID       string            `json:"session_id"`
	Revision        uint64            `json:"revision"`
	Phase           string            `json:"phase"`
	Stage           string            `json:"stage,omitempty"`
	StageMessage    string            `json:"stage_message,omitempty"`
	StageStartedAt  int64             `json:"stage_started_at,omitempty"`
	ActiveStartedAt int64             `json:"active_started_at,omitempty"`
	ActiveRequestID string            `json:"active_request_id,omitempty"`
	QueueLength     int               `json:"queue_length"`
	LastTerminal    string            `json:"last_terminal_status,omitempty"`
	LastError       string            `json:"last_error,omitempty"`
	UpdatedAt       int64             `json:"updated_at"`
	CompletedAt     int64             `json:"completed_at,omitempty"`
	Terminals       []Terminal        `json:"terminals,omitempty"`
	AckedByDevice   map[string]uint64 `json:"acked_by_device,omitempty"`
	ReadByDevice    map[string]uint64 `json:"read_by_device,omitempty"`
}

// View is safe to send to one device; internal device maps are never exposed.
type View struct {
	SessionID        string `json:"session_id"`
	Revision         uint64 `json:"revision"`
	Phase            string `json:"phase"`
	Stage            string `json:"stage,omitempty"`
	StageMessage     string `json:"stage_message,omitempty"`
	StageStartedAt   int64  `json:"stage_started_at,omitempty"`
	ActiveStartedAt  int64  `json:"active_started_at,omitempty"`
	ActiveRequestID  string `json:"active_request_id,omitempty"`
	QueueLength      int    `json:"queue_length"`
	LastTerminal     string `json:"last_terminal_status,omitempty"`
	LastError        string `json:"last_error,omitempty"`
	UpdatedAt        int64  `json:"updated_at"`
	CompletedAt      int64  `json:"completed_at,omitempty"`
	Unread           int    `json:"unread"`
	DeliveryPending  bool   `json:"delivery_pending"`
	HistoryReconcile bool   `json:"history_reconcile"`
}

type snapshot struct {
	Records map[string]*Record `json:"records"`
}

type cursorEntry struct {
	SessionID string `json:"session_id"`
	DeviceID  string `json:"device_id"`
	Acked     uint64 `json:"acked"`
	Read      uint64 `json:"read"`
	At        int64  `json:"at"`
}

type Store struct {
	mu         sync.Mutex
	path       string
	cursorPath string
	records    map[string]*Record
	now        func() time.Time
	flushTimer *time.Timer
	dirty      bool
	writes     uint64
}

func New(dataDir string) *Store {
	s := &Store{records: make(map[string]*Record), now: time.Now}
	if dataDir != "" {
		s.path = filepath.Join(dataDir, "session_runtime.json")
		s.cursorPath = filepath.Join(dataDir, "session_runtime_cursors.jsonl")
	}
	s.load()
	return s
}

func (s *Store) Update(sessionID, phase, requestID string, queueLength int, terminal, lastError string) (View, bool) {
	if sessionID == "" {
		return View{}, false
	}
	if queueLength < 0 {
		queueLength = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	if phase == "" {
		phase = r.Phase
	}
	previousRequestID := r.ActiveRequestID
	// A native turn can continue after the Bridge transport was interrupted.
	// Its exact request identity stays the same, but the interruption must no
	// longer be presented as the current result once live execution is observed.
	newTurn := terminal == "" && requestID != "" && (requestID != previousRequestID || r.Phase == "interrupted") &&
		(phase == "queued" || phase == "running")
	if r.Phase == phase && r.ActiveRequestID == requestID && r.QueueLength == queueLength &&
		r.LastTerminal == terminal && r.LastError == lastError {
		return viewLocked(r, ""), false
	}
	now := s.now().UnixMilli()
	r.Revision++
	r.Phase = phase
	r.Stage = defaultStageForPhase(phase)
	r.StageMessage = ""
	r.StageStartedAt = now
	if newTurn || (r.ActiveStartedAt == 0 && (phase == "queued" || phase == "running" || phase == "waiting" || phase == "stopping")) {
		r.ActiveStartedAt = now
	}
	r.ActiveRequestID = requestID
	r.QueueLength = queueLength
	r.UpdatedAt = now
	if terminal != "" {
		r.LastTerminal = terminal
		r.LastError = lastError
		r.CompletedAt = r.UpdatedAt
		r.Terminals = append(r.Terminals, Terminal{Revision: r.Revision, RequestID: requestID, Status: terminal, At: r.CompletedAt})
		if len(r.Terminals) > 128 {
			r.Terminals = append([]Terminal(nil), r.Terminals[len(r.Terminals)-128:]...)
		}
	} else if newTurn {
		// LastTerminal describes the previous request. Once a distinct request is
		// accepted it must no longer drive the current row's status. Keep the
		// terminal ledger intact for per-device unread/reconciliation cursors.
		r.LastTerminal = ""
		r.LastError = ""
		r.CompletedAt = 0
	} else if phase == "running" || phase == "queued" || phase == "waiting" || phase == "stopping" {
		r.LastError = ""
	}
	s.enforceCapLocked()
	s.saveLocked()
	return viewLocked(r, ""), true
}

// UpdateQueueLength changes only the number of turns waiting behind the active
// request. Accepting a follow-up must not replace ActiveRequestID or regress
// the active phase to queued; clients use those fields to render the turn that
// is actually producing output.
func (s *Store) UpdateQueueLength(sessionID string, queueLength int) (View, bool) {
	if sessionID == "" {
		return View{}, false
	}
	if queueLength < 0 {
		queueLength = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	if r.QueueLength == queueLength {
		return viewLocked(r, ""), false
	}
	r.Revision++
	r.QueueLength = queueLength
	r.UpdatedAt = s.now().UnixMilli()
	s.saveLocked()
	return viewLocked(r, ""), true
}

// Flush synchronously persists any coalesced progress. Lifecycle transitions,
// terminal results and device cursors remain synchronous and do not require it.
func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dirty {
		s.saveLocked()
	}
}

// Progress advances the fine-grained, server-authoritative activity within an
// active phase. It is separate from Update so a cold thread resume can expose
// each meaningful step without pretending the turn itself changed phase.
func (s *Store) Progress(sessionID, requestID, stage, message string) (View, bool) {
	if sessionID == "" || stage == "" {
		return View{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	sameRequest := requestID == "" || r.ActiveRequestID == "" || r.ActiveRequestID == requestID
	if sameRequest && stageRank(stage) < stageRank(r.Stage) {
		return viewLocked(r, ""), false
	}
	if r.Stage == stage && r.StageMessage == message && (requestID == "" || r.ActiveRequestID == requestID) {
		return viewLocked(r, ""), false
	}
	now := s.now().UnixMilli()
	requestChanged := requestID != "" && requestID != r.ActiveRequestID
	r.Revision++
	if requestID != "" {
		r.ActiveRequestID = requestID
	}
	if requestChanged || r.ActiveStartedAt == 0 {
		r.ActiveStartedAt = now
	}
	r.Stage = stage
	r.StageMessage = message
	r.StageStartedAt = now
	r.UpdatedAt = r.StageStartedAt
	s.scheduleSaveLocked()
	return viewLocked(r, ""), true
}

func stageRank(stage string) int {
	switch stage {
	case "transmitting":
		return 1
	case "received":
		return 2
	case "queued":
		return 3
	case "preparing":
		return 4
	case "resuming_thread", "starting_thread":
		return 5
	case "submitting_turn":
		return 6
	case "waiting_model":
		return 7
	case "thinking", "composing", "running_tool", "waiting_user":
		return 8
	case "stopping":
		return 9
	case "completed", "failed", "interrupted", "closed":
		return 10
	default:
		return 0
	}
}

func (s *Store) Ensure(sessionID, phase string, queueLength int) View {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	if r.Revision == 0 {
		r.Revision = 1
		r.Phase = phase
		if r.Phase == "" {
			r.Phase = "idle"
		}
		r.QueueLength = queueLength
		r.UpdatedAt = s.now().UnixMilli()
		r.Stage = defaultStageForPhase(r.Phase)
		r.StageStartedAt = r.UpdatedAt
		s.saveLocked()
	}
	return viewLocked(r, "")
}

// Recover converts a stale active phase left by a Bridge process crash into an
// explicit terminal state. It never fabricates completion; clients reconcile
// native history before marking the result read.
func (s *Store) Recover(sessionID string, actuallyActive bool) (View, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	active := r.Phase == "queued" || r.Phase == "running" || r.Phase == "stopping" || r.Phase == "waiting"
	if !active || actuallyActive {
		return viewLocked(r, ""), false
	}
	r.Revision++
	r.Phase = "interrupted"
	r.QueueLength = 0
	r.LastTerminal = "interrupted"
	r.LastError = "Bridge restarted before the terminal event was observed"
	r.UpdatedAt = s.now().UnixMilli()
	r.Stage = "interrupted"
	r.StageMessage = r.LastError
	r.StageStartedAt = r.UpdatedAt
	r.CompletedAt = r.UpdatedAt
	r.Terminals = append(r.Terminals, Terminal{Revision: r.Revision, RequestID: r.ActiveRequestID, Status: "interrupted", At: r.CompletedAt, Source: "bridge_recovery"})
	r.ActiveRequestID = ""
	r.ActiveStartedAt = 0
	s.saveLocked()
	return viewLocked(r, ""), true
}

// RecoverAllStale is called once while a new Hub is being constructed, before
// it can accept client commands. Any active phase loaded from the previous
// process therefore represents a turn whose terminal event was not observed.
// Keeping this out of reconnect snapshots avoids racing a live worker that is
// transitioning between its in-memory and persisted states.
func (s *Store) RecoverAllStale() []View {
	s.mu.Lock()
	defer s.mu.Unlock()
	var recovered []View
	for _, r := range s.records {
		active := r.Phase == "queued" || r.Phase == "running" || r.Phase == "stopping" || r.Phase == "waiting"
		if !active {
			continue
		}
		r.Revision++
		r.Phase = "interrupted"
		r.QueueLength = 0
		r.LastTerminal = "interrupted"
		r.LastError = "Bridge restarted before the terminal event was observed"
		r.UpdatedAt = s.now().UnixMilli()
		r.Stage = "interrupted"
		r.StageMessage = r.LastError
		r.StageStartedAt = r.UpdatedAt
		r.CompletedAt = r.UpdatedAt
		r.Terminals = append(r.Terminals, Terminal{Revision: r.Revision, RequestID: r.ActiveRequestID, Status: "interrupted", At: r.CompletedAt, Source: "bridge_recovery"})
		r.ActiveRequestID = ""
		r.ActiveStartedAt = 0
		recovered = append(recovered, viewLocked(r, ""))
	}
	if len(recovered) > 0 {
		s.saveLocked()
	}
	return recovered
}

// ReconcileHistoryActivity clears only a restart-generated interruption when
// canonical assistant history proves the logical Session advanced afterwards.
// User-requested stops have no restart marker and remain visible.
func (s *Store) ReconcileHistoryActivity(sessionID string, latestAssistantAt int64, observedPhase string) (View, bool) {
	if sessionID == "" || latestAssistantAt <= 0 {
		return View{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	if r.Phase != "interrupted" || r.LastTerminal != "interrupted" ||
		r.LastError != "Bridge restarted before the terminal event was observed" ||
		r.CompletedAt == 0 || latestAssistantAt <= r.CompletedAt {
		return viewLocked(r, ""), false
	}
	phase := observedPhase
	if phase != "queued" && phase != "running" && phase != "stopping" && phase != "waiting" {
		phase = "idle"
	}
	r.Revision++
	r.Phase = phase
	r.Stage = defaultStageForPhase(phase)
	r.StageMessage = ""
	r.StageStartedAt = s.now().UnixMilli()
	r.ActiveRequestID = ""
	r.ActiveStartedAt = 0
	r.QueueLength = 0
	r.LastTerminal = ""
	r.LastError = ""
	r.CompletedAt = 0
	r.UpdatedAt = r.StageStartedAt
	s.saveLocked()
	return viewLocked(r, ""), true
}

func (s *Store) Snapshot(deviceID string, sessionIDs []string) []View {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]View, 0, len(sessionIDs))
	for _, id := range sessionIDs {
		r := s.ensureLocked(id)
		out = append(out, viewLocked(r, deviceID))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })
	return out
}

// WidgetTerminals retains request identities independently from a session-wide
// read cursor. The device's native widget ledger applies its exact result reads.
func (s *Store) WidgetTerminals(sessionID string, since int64) []Terminal {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[sessionID]
	if r == nil {
		return nil
	}
	var result []Terminal
	for _, terminal := range r.Terminals {
		if terminal.At >= since && terminal.RequestID != "" {
			result = append(result, terminal)
		}
	}
	return result
}

// Ack advances only the named device. read=true additionally marks terminal
// results through revision as viewed; another device's unread count is intact.
func (s *Store) Ack(deviceID, sessionID string, revision uint64, read bool) (View, bool) {
	if deviceID == "" || sessionID == "" {
		return View{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensureLocked(sessionID)
	if revision == 0 || revision > r.Revision {
		revision = r.Revision
	}
	changed := false
	if revision > r.AckedByDevice[deviceID] {
		r.AckedByDevice[deviceID] = revision
		changed = true
	}
	if read && revision > r.ReadByDevice[deviceID] {
		r.ReadByDevice[deviceID] = revision
		changed = true
	}
	if changed {
		entry := cursorEntry{SessionID: sessionID, DeviceID: deviceID,
			Acked: r.AckedByDevice[deviceID], Read: r.ReadByDevice[deviceID], At: s.now().UnixMilli()}
		if s.appendCursorLocked(entry) != nil {
			// A cursor is delivery reliability state. If the compact append path is
			// unavailable, retain the previous synchronous full-snapshot fallback.
			s.saveLocked()
		}
	}
	return viewLocked(r, deviceID), changed
}

func (s *Store) Remove(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[sessionID]; ok {
		delete(s.records, sessionID)
		s.saveLocked()
	}
}

func (s *Store) ensureLocked(sessionID string) *Record {
	r := s.records[sessionID]
	if r == nil {
		r = &Record{SessionID: sessionID, Phase: "idle", AckedByDevice: map[string]uint64{}, ReadByDevice: map[string]uint64{}}
		s.records[sessionID] = r
	}
	if r.AckedByDevice == nil {
		r.AckedByDevice = map[string]uint64{}
	}
	if r.ReadByDevice == nil {
		r.ReadByDevice = map[string]uint64{}
	}
	if r.Stage == "" {
		r.Stage = defaultStageForPhase(r.Phase)
		r.StageStartedAt = r.UpdatedAt
	}
	if r.ActiveStartedAt == 0 && (r.Phase == "queued" || r.Phase == "running" || r.Phase == "waiting" || r.Phase == "stopping") {
		r.ActiveStartedAt = r.StageStartedAt
		if r.ActiveStartedAt == 0 {
			r.ActiveStartedAt = r.UpdatedAt
		}
	}
	return r
}

func defaultStageForPhase(phase string) string {
	switch phase {
	case "queued":
		return "queued"
	case "running":
		return "preparing"
	case "stopping":
		return "stopping"
	case "waiting":
		return "waiting_user"
	case "completed":
		return "completed"
	case "failed":
		return "failed"
	case "interrupted":
		return "interrupted"
	case "closed":
		return "closed"
	default:
		return "idle"
	}
}

func viewLocked(r *Record, deviceID string) View {
	unread := 0
	readRevision := r.ReadByDevice[deviceID]
	historyReconcile := false
	for _, terminal := range r.Terminals {
		if terminal.Revision > readRevision {
			historyReconcile = true
		}
		if terminal.Status == "completed" && terminal.Revision > readRevision {
			unread++
		}
	}
	lastTerminal, lastError, completedAt := r.LastTerminal, r.LastError, r.CompletedAt
	active := r.Phase == "queued" || r.Phase == "running" || r.Phase == "waiting" || r.Phase == "stopping"
	if active && r.ActiveRequestID != "" && len(r.Terminals) > 0 && r.Terminals[len(r.Terminals)-1].RequestID != r.ActiveRequestID {
		// Backward-compatible self-heal for journals written before new requests
		// cleared the previous terminal presentation. The ledger remains intact.
		lastTerminal, lastError, completedAt = "", "", 0
	}
	return View{SessionID: r.SessionID, Revision: r.Revision, Phase: r.Phase,
		Stage: r.Stage, StageMessage: r.StageMessage, StageStartedAt: r.StageStartedAt, ActiveStartedAt: r.ActiveStartedAt,
		ActiveRequestID: r.ActiveRequestID, QueueLength: r.QueueLength,
		LastTerminal: lastTerminal, LastError: lastError, UpdatedAt: r.UpdatedAt,
		CompletedAt: completedAt, Unread: unread,
		DeliveryPending:  deviceID != "" && r.AckedByDevice[deviceID] < r.Revision,
		HistoryReconcile: deviceID != "" && historyReconcile}
}

func (s *Store) enforceCapLocked() {
	if len(s.records) <= maxSessions {
		return
	}
	all := make([]*Record, 0, len(s.records))
	for _, r := range s.records {
		all = append(all, r)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].UpdatedAt < all[j].UpdatedAt })
	for _, r := range all[:len(all)-maxSessions] {
		delete(s.records, r.SessionID)
	}
}

func (s *Store) load() {
	if s.path != "" {
		data, err := os.ReadFile(s.path)
		if err == nil {
			var snap snapshot
			if json.Unmarshal(data, &snap) == nil && snap.Records != nil {
				s.records = snap.Records
			}
		}
	}
	for id := range s.records {
		s.ensureLocked(id)
	}
	s.loadCursorLog()
	s.enforceCapLocked()
}

func (s *Store) loadCursorLog() {
	if s.cursorPath == "" {
		return
	}
	f, err := os.Open(s.cursorPath)
	if err != nil {
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		var entry cursorEntry
		if json.Unmarshal(scanner.Bytes(), &entry) != nil || entry.SessionID == "" || entry.DeviceID == "" {
			continue
		}
		r := s.records[entry.SessionID]
		if r == nil {
			// Cursor records never resurrect a Session removed from the canonical
			// lifecycle snapshot.
			continue
		}
		s.ensureLocked(entry.SessionID)
		if entry.Acked > r.AckedByDevice[entry.DeviceID] {
			r.AckedByDevice[entry.DeviceID] = entry.Acked
		}
		if entry.Read > r.ReadByDevice[entry.DeviceID] {
			r.ReadByDevice[entry.DeviceID] = entry.Read
		}
	}
}

func (s *Store) appendCursorLocked(entry cursorEntry) error {
	if s.cursorPath == "" {
		return os.ErrInvalid
	}
	if err := os.MkdirAll(filepath.Dir(s.cursorPath), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.cursorPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(data, '\n'))
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func (s *Store) saveLocked() {
	if s.flushTimer != nil {
		s.flushTimer.Stop()
		s.flushTimer = nil
	}
	if !s.dirty {
		// Callers of synchronous state transitions have already modified memory,
		// so they must still write even when no progress flush is pending.
		s.writeLocked()
		return
	}
	s.dirty = false
	s.writeLocked()
}

func (s *Store) scheduleSaveLocked() {
	s.dirty = true
	if s.path == "" || s.flushTimer != nil {
		return
	}
	s.flushTimer = time.AfterFunc(progressFlushDelay, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.flushTimer = nil
		if !s.dirty {
			return
		}
		s.dirty = false
		s.writeLocked()
	})
}

func (s *Store) writeLocked() {
	if s.path == "" || os.MkdirAll(filepath.Dir(s.path), 0o700) != nil {
		return
	}
	data, err := json.Marshal(snapshot{Records: s.records})
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, data, 0o600) == nil {
		if os.Rename(tmp, s.path) == nil {
			s.writes++
			// The canonical snapshot now contains every cursor. Only touch the
			// append journal when it has content, avoiding an idle truncate event.
			if info, err := os.Stat(s.cursorPath); err == nil && info.Size() > 0 {
				_ = os.WriteFile(s.cursorPath, nil, 0o600)
			}
		}
	}
}
