package linebridge

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// A service-owner reconnect of already-enabled, previously bound accounts. It
// never creates a Caller, grants access, pairs/logs in a new identity or sends.
type RestoreStatus struct {
	PID       int      `json:"server_pid"`
	State     string   `json:"state"`
	CheckedAt int64    `json:"checked_at"`
	Ready     int      `json:"ready_accounts"`
	Verified  int      `json:"verified_accounts"`
	Codes     []string `json:"codes,omitempty"`
	ReadOnly  bool     `json:"read_only_network"`
}

func (s *Service) restoreReady(ctx context.Context) RestoreStatus {
	status := RestoreStatus{PID: os.Getpid(), State: "complete", CheckedAt: nowMS(), ReadOnly: true}
	rows, e := s.store.db.QueryContext(ctx, "SELECT id FROM line_accounts WHERE enabled=1 AND principal IS NOT NULL AND principal!='' AND auth_state IN ('ready','stored_login') ORDER BY created_at LIMIT 17")
	if e != nil {
		status.State = "failed"
		status.Codes = []string{SafeCode(e)}
		return status
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			break
		}
		ids = append(ids, id)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil || len(ids) > 16 {
		status.State = "failed"
		status.Codes = []string{"line_restore_bound_or_store_error"}
		return status
	}
	status.Ready = len(ids)
	for _, id := range ids {
		a, err := s.store.Account(ctx, id)
		if err != nil {
			status.Codes = append(status.Codes, SafeCode(err))
			continue
		}
		d, err := s.ensure(ctx, a)
		if err == nil {
			var identity Identity
			err = d.Invoke(ctx, "resume", nil, &identity)
			if err == nil {
				err = s.store.BindIdentity(ctx, id, identity)
			}
			if err == nil {
				err = d.Invoke(ctx, "health", nil, &identity)
			}
			if err == nil {
				err = s.store.BindIdentity(ctx, id, identity)
			}
		}
		if err != nil {
			status.Codes = append(status.Codes, SafeCode(err))
			continue
		}
		status.Verified++
	}
	if status.Verified != status.Ready {
		status.State = "partial"
	}
	status.CheckedAt = nowMS()
	return status
}
func writeRestoreReport(path string, value RestoreStatus) error {
	raw, e := json.Marshal(value)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".line-health-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	e = f.Chmod(0600)
	if e == nil {
		_, e = f.Write(raw)
	}
	if e == nil {
		e = f.Sync()
	}
	f.Close()
	if e != nil {
		return e
	}
	return os.Rename(name, path)
}
func (s *Service) StartRestoreReport(path string) error {
	if !filepath.IsAbs(path) {
		return Fail("line_health_report_invalid")
	}
	if e := writeRestoreReport(path, RestoreStatus{PID: os.Getpid(), State: "checking", CheckedAt: nowMS(), ReadOnly: true}); e != nil {
		return e
	}
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return Fail("line_service_closed")
	}
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
		defer cancel()
		status := s.restoreReady(ctx)
		_ = writeRestoreReport(path, status)
	}()
	return nil
}
