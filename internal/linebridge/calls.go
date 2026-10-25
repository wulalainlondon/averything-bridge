package linebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type CallReceipt struct {
	BindingRevision uint64 `json:"-"`
	ID              string `json:"call_id"`
	Account         string `json:"account_ref"`
	Target          string `json:"target_ref"`
	State           string `json:"state"`
	Code            string `json:"code,omitempty"`
	CreatedAt       int64  `json:"created_at"`
	UpdatedAt       int64  `json:"updated_at"`
	Subject         string `json:"-"`
}
type CallMaterial struct {
	Route     json.RawMessage `json:"route"`
	Principal string          `json:"principal"`
	Target    string          `json:"target"`
}

// PrepareCall consumes the exact durable intent before obtaining any route.
// Materials are private IPC input, never a tool/API response or an on-disk file.
func (s *Service) PrepareCall(ctx context.Context, c Caller, account, target, intent string) (CallReceipt, CallMaterial, error) {
	a, err := s.account(ctx, c, account, "call")
	if err != nil {
		return CallReceipt{}, CallMaterial{}, err
	}
	t, err := s.store.Target(ctx, account, target)
	if err != nil {
		return CallReceipt{}, CallMaterial{}, err
	}
	if t.Kind != "user" || len(intent) < 8 || len(intent) > 128 {
		return CallReceipt{}, CallMaterial{}, Fail("line_call_invalid")
	}
	fingerprint := s.store.digest(string(raw([]any{a.ID, a.Principal, a.Revision, t.Ref})))
	id := "line_" + s.store.digest(account+"\x00call\x00"+intent)
	receipt := CallReceipt{ID: id, Account: account, Target: target, Subject: c.Subject, BindingRevision: a.Revision, State: "preparing", CreatedAt: nowMS(), UpdatedAt: nowMS()}
	_, err = s.store.db.ExecContext(ctx, "INSERT INTO line_calls VALUES(?,?,?,?,?,?,?,?,?,?,?)", id, account, target, c.Subject, intent, fingerprint, receipt.State, "", receipt.CreatedAt, receipt.UpdatedAt, a.Revision)
	if err != nil {
		if existing, e := s.CallReceipt(ctx, c, id); e == nil {
			return existing, CallMaterial{}, Fail("line_call_intent_consumed")
		}
		return CallReceipt{}, CallMaterial{}, Fail("line_call_busy")
	}
	return s.prepareCallRoute(ctx, c, receipt, t)
}
func (s *Service) prepareCallRoute(ctx context.Context, c Caller, receipt CallReceipt, t Target) (CallReceipt, CallMaterial, error) {
	id := receipt.ID
	fail := func(err error) (CallReceipt, CallMaterial, error) {
		s.UpdateCall(context.Background(), id, "failed_before_effect", SafeCode(err))
		receipt.State = "failed_before_effect"
		receipt.Code = SafeCode(err)
		return receipt, CallMaterial{}, err
	}
	_, driver, err := s.driver(ctx, c, receipt.Account, "call")
	if err != nil {
		return fail(err)
	}
	var material CallMaterial
	if err = driver.Invoke(ctx, "prepare_call", map[string]string{"target": t.ExternalID}, &material); err != nil {
		return fail(err)
	}
	if material.Target != t.ExternalID || material.Principal == "" || len(material.Route) == 0 || len(material.Route) > 32768 {
		return fail(Fail("line_call_material_invalid"))
	}
	if err = s.store.BindPrincipal(ctx, receipt.Account, material.Principal); err != nil {
		return fail(err)
	}
	current, err := s.account(ctx, c, receipt.Account, "call")
	if err != nil {
		return fail(err)
	}
	if current.Revision != receipt.BindingRevision {
		return fail(Fail("line_binding_changed"))
	}
	if err = s.UpdateCall(ctx, id, "prepared", ""); err != nil {
		return fail(err)
	}
	receipt.State = "prepared"
	return receipt, material, nil
}
func (s *Service) UpdateCall(ctx context.Context, id, state, code string) error {
	valid := map[string]bool{"prepared": true, "dispatching": true, "ringing": true, "connected": true, "closed": true, "unknown": true, "failed_before_effect": true, "error": true}
	if !valid[state] {
		return Fail("line_call_state_invalid")
	}
	result, err := s.store.db.ExecContext(ctx, "UPDATE line_calls SET state=?,code=?,updated_at=? WHERE id=? AND state IN ('handoff_pending','preparing','prepared','dispatching','ringing','connected')", state, code, nowMS(), id)
	if err == nil {
		n, _ := result.RowsAffected()
		if n != 1 && (state == "prepared" || state == "dispatching") {
			return Fail("line_call_terminal")
		}
	}
	return err
}
func (s *Service) CallReceipt(ctx context.Context, c Caller, id string) (CallReceipt, error) {
	var r CallReceipt
	err := s.store.db.QueryRowContext(ctx, "SELECT id,account,target,subject,state,code,created_at,updated_at,binding FROM line_calls WHERE id=?", id).Scan(&r.ID, &r.Account, &r.Target, &r.Subject, &r.State, &r.Code, &r.CreatedAt, &r.UpdatedAt, &r.BindingRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return r, Fail("line_call_not_found")
	}
	if err != nil {
		return r, err
	}
	a, err := s.store.Account(ctx, r.Account)
	if err != nil {
		return CallReceipt{}, err
	}
	phoneBound := false
	if c.Admin {
		var count int
		_ = s.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM line_call_handoffs WHERE call_id=? AND device=?", id, c.Subject).Scan(&count)
		phoneBound = count == 1
	}
	if !s.store.Allowed(ctx, c, r.Account, "call") || (r.Subject != c.Subject && !(c.Admin && (a.Owner == c.Subject || phoneBound))) {
		return CallReceipt{}, Fail("line_forbidden")
	}
	return r, nil
}

func (s *Service) ValidatePreparedCall(ctx context.Context, c Caller, id string) error {
	r, err := s.CallReceipt(ctx, c, id)
	if err != nil {
		return err
	}
	a, err := s.account(ctx, c, r.Account, "call")
	if err != nil {
		return err
	}
	if a.Revision != r.BindingRevision || r.State != "prepared" || nowMS()-r.CreatedAt > 60000 {
		return Fail("line_call_lease_expired")
	}
	return s.UpdateCall(ctx, id, "dispatching", "")
}

// RecordCallTelemetry accepts a typed, content-free projection supplied by the
// private call adapter. It cannot store raw frames or arbitrary SDK payloads.
func (s *Service) RecordCallTelemetry(ctx context.Context, id string, event CallTelemetry) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if len(value) > 2048 {
		return Fail("line_call_event_limit")
	}
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO line_call_events(call_id,event,created_at) VALUES(?,?,?)", id, value, nowMS()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM line_call_events WHERE call_id=? AND seq NOT IN (SELECT seq FROM line_call_events WHERE call_id=? ORDER BY seq DESC LIMIT 1000)", id, id); err != nil {
		return err
	}
	return tx.Commit()
}

type CallTelemetry struct {
	TXFrames       int    `json:"tx_20ms_frames,omitempty"`
	RXFrames       int    `json:"rx_20ms_frames,omitempty"`
	Code           string `json:"code"`
	Stage          string `json:"stage,omitempty"`
	Status         string `json:"status,omitempty"`
	ElapsedMS      int64  `json:"elapsed_ms,omitempty"`
	DurationMS     int64  `json:"duration_ms,omitempty"`
	ReleaseCode    *int   `json:"release_code,omitempty"`
	DroppedPackets int    `json:"dropped_packets,omitempty"`
	PayloadLength  int    `json:"payload_length,omitempty"`
}

// CallIntentReceipt recovers the original source-bound call without placing another call.
func (s *Service) CallIntentReceipt(ctx context.Context, c Caller, account, intent string) (*CallReceipt, bool, error) {
	if _, e := s.account(ctx, c, account, "status"); e != nil {
		return nil, false, e
	}
	var id string
	e := s.store.db.QueryRowContext(ctx, "SELECT id FROM line_calls WHERE account=? AND subject=? AND intent=?", account, c.Subject, intent).Scan(&id)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, false, nil
	}
	if e != nil {
		return nil, false, e
	}
	r, e := s.CallReceipt(ctx, c, id)
	if e != nil {
		return nil, false, e
	}
	return &r, true, nil
}
