package toolenv

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type record struct {
	Operation Operation `json:"operation"`
	Identity  Identity  `json:"identity"`
}

// Store serializes preparation and durable intent BEFORE any external mutation.
// Plans expire across restarts; interrupted mutations remain indeterminate.
type Store struct {
	mu    sync.Mutex
	path  string
	err   error
	plans map[string]Plan
	rows  map[string]record
	now   func() time.Time
}

func Open(dir string) *Store {
	s := &Store{plans: map[string]Plan{}, rows: map[string]record{}, now: time.Now}
	if dir == "" {
		s.err = Error("journal_unavailable")
		return s
	}
	s.path = filepath.Join(dir, "tool-environment-operations-v1.json")
	b, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return s
	}
	if err != nil || len(b) > 2<<20 || json.Unmarshal(b, &s.rows) != nil || s.rows == nil {
		s.err = Error("journal_unavailable")
		return s
	}
	changed := false
	for k, r := range s.rows {
		r.Operation.Identity = r.Identity
		r.Operation.ThreadID = r.Identity.Thread
		if r.Operation.Active() {
			if r.Operation.Phase == "waiting_idle" {
				r.Operation.Phase = "cancelled"
				r.Operation.Reason = "restart_before_apply"
			} else {
				r.Operation.Phase = "indeterminate"
				r.Operation.Reason = "restart_after_apply"
			}
			r.Operation.Revision++
			r.Operation.UpdatedAt = s.now().UnixMilli()
			changed = true
		}
		s.rows[k] = r
	}
	if changed {
		s.err = s.saveLocked()
	}
	return s
}

func (s *Store) saveLocked() error {
	if s.err != nil {
		return s.err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0700); err != nil {
		return Error("journal_unavailable")
	}
	b, err := json.Marshal(s.rows)
	if err != nil || len(b) > 2<<20 {
		return Error("journal_full")
	}
	f, err := os.CreateTemp(filepath.Dir(s.path), ".tool-environment-*")
	if err != nil {
		return Error("journal_unavailable")
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(f.Name(), s.path)
	}
	if err == nil {
		if d, e := os.Open(filepath.Dir(s.path)); e == nil {
			err = d.Sync()
			d.Close()
		} else {
			err = e
		}
	}
	if err != nil {
		return Error("journal_unavailable")
	}
	return nil
}

func (s *Store) Prepare(id Identity, action string, snapshot Snapshot) (Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return Plan{}, s.err
	}
	if id.Device == "" || id.Thread == "" || snapshot.ThreadID != id.Thread || snapshot.Generation == "" {
		return Plan{}, Error("identity_unavailable")
	}
	if action != "reload" && action != "fork" {
		return Plan{}, Error("unsupported_action")
	}
	if action == "reload" && !snapshot.ReloadAllowed {
		return Plan{}, Error("maintenance_window_required")
	}
	if action == "fork" && !snapshot.ForkAllowed {
		return Plan{}, Error("fork_unsupported")
	}
	if s.now().UnixMilli()-snapshot.CheckedAt > 60_000 {
		return Plan{}, Error("stale_snapshot")
	}
	for token, p := range s.plans {
		if p.ExpiresAt <= s.now().UnixMilli() {
			delete(s.plans, token)
		}
	}
	if len(s.plans) >= 128 {
		return Plan{}, Error("too_many_plans")
	}
	for _, r := range s.rows {
		if r.Operation.Active() || r.Operation.Phase == "indeterminate" {
			return Plan{}, Error("operation_pending")
		}
	}
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return Plan{}, Error("token_unavailable")
	}
	p := Plan{Token: hex.EncodeToString(b), Action: action, Scope: "thread", Generation: snapshot.Generation, ExpiresAt: s.now().Add(2 * time.Minute).UnixMilli(), Identity: id}
	p.Binding = snapshot.Binding
	if action == "reload" {
		p.Scope = "daemon"
	}
	s.plans[p.Token] = p
	return p, nil
}

func (s *Store) Begin(id Identity, req Request, generation, binding string) (Operation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return Operation{}, false, s.err
	}
	if !ValidID(req.OperationID) {
		return Operation{}, false, Error("invalid_operation_id")
	}
	if r, ok := s.rows[req.OperationID]; ok {
		if r.Identity != id || r.Operation.Action != req.Action {
			return Operation{}, false, Error("operation_conflict")
		}
		return r.Operation, false, nil
	}
	p, ok := s.plans[req.Token]
	if !ok || p.Identity != id || p.Action != req.Action || p.Generation != generation || p.Binding != binding || p.ExpiresAt <= s.now().UnixMilli() {
		return Operation{}, false, Error("plan_expired")
	}
	if p.Action == "reload" && !req.MaintenanceConfirmed {
		return Operation{}, false, Error("maintenance_confirmation_required")
	}
	for _, r := range s.rows {
		if r.Operation.Active() || r.Operation.Phase == "indeterminate" {
			return Operation{}, false, Error("operation_pending")
		}
	}
	// Bound the journal without evicting replay-protection tombstones.
	if len(s.rows) >= 1000 {
		return Operation{}, false, Error("journal_full")
	}
	o := Operation{ID: req.OperationID, Action: p.Action, Phase: "waiting_idle", Generation: generation, Revision: 1, UpdatedAt: s.now().UnixMilli(), Evidence: "none", Identity: id}
	o.ThreadID = id.Thread
	s.rows[o.ID] = record{o, id}
	if err := s.saveLocked(); err != nil {
		delete(s.rows, o.ID)
		s.err = err
		return Operation{}, false, err
	}
	delete(s.plans, req.Token)
	return o, true, nil
}

func (s *Store) Get(id Identity, operationID string) (Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return Operation{}, s.err
	}
	if operationID == "" {
		var latest Operation
		for _, r := range s.rows {
			if r.Identity == id && r.Operation.UpdatedAt >= latest.UpdatedAt {
				latest = r.Operation
			}
		}
		if latest.ID == "" {
			return Operation{}, Error("no_operation")
		}
		return latest, nil
	}
	r, ok := s.rows[operationID]
	if !ok || r.Identity != id {
		return Operation{}, Error("no_operation")
	}
	return r.Operation, nil
}

func (s *Store) Transition(id Identity, operationID, from, phase, reason, evidence, newThread, newSession string) (Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[operationID]
	if !ok || r.Identity != id {
		return Operation{}, Error("no_operation")
	}
	if r.Operation.Phase != from {
		return r.Operation, Error("operation_changed")
	}
	old := r
	r.Operation.Phase, r.Operation.Reason, r.Operation.Evidence = phase, reason, evidence
	if newThread != "" {
		r.Operation.NewThread = newThread
	}
	if newSession != "" {
		r.Operation.NewSession = newSession
	}
	r.Operation.Revision++
	r.Operation.UpdatedAt = s.now().UnixMilli()
	s.rows[operationID] = r
	if err := s.saveLocked(); err != nil {
		s.rows[operationID] = old
		s.err = err
		return old.Operation, err
	}
	return r.Operation, nil
}
