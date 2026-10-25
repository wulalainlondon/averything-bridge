package linebridge

import (
	"context"
	"strings"
)

// ReserveHandoff records the verified original native caller and exact phone
// before asking that phone to retire AI audio. No route or invitation is sent.
func (s *Service) ReserveHandoff(ctx context.Context, c Caller, device, session, voice, account, target, intent string) (CallReceipt, error) {
	if c.Subject != "session:"+session || !strings.HasPrefix(device, "device:") || voice == "" || len(intent) < 8 || len(intent) > 128 {
		return CallReceipt{}, Fail("line_handoff_invalid")
	}
	a, err := s.account(ctx, c, account, "call")
	if err != nil {
		return CallReceipt{}, err
	}
	if _, err = s.account(ctx, Caller{Subject: device}, account, "call"); err != nil {
		return CallReceipt{}, err
	}
	t, err := s.store.Target(ctx, account, target)
	if err != nil {
		return CallReceipt{}, err
	}
	if t.Kind != "user" {
		return CallReceipt{}, Fail("line_call_invalid")
	}
	id := "line_" + s.store.digest(account+"\x00call\x00"+intent)
	if existing, e := s.CallReceipt(ctx, c, id); e == nil {
		if existing.Target != target || existing.BindingRevision != a.Revision {
			return CallReceipt{}, Fail("line_intent_conflict")
		}
		return existing, nil
	}
	fingerprint := s.store.digest(string(raw([]any{a.ID, a.Principal, a.Revision, t.Ref})))
	r := CallReceipt{ID: id, Account: account, Target: target, Subject: c.Subject, State: "handoff_pending", BindingRevision: a.Revision, CreatedAt: nowMS(), UpdatedAt: nowMS()}
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return CallReceipt{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "INSERT INTO line_calls VALUES(?,?,?,?,?,?,?,?,?,?,?)", id, account, target, c.Subject, intent, fingerprint, r.State, "", r.CreatedAt, r.UpdatedAt, a.Revision)
	if err != nil {
		return CallReceipt{}, Fail("line_call_busy")
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO line_call_handoffs VALUES(?,?,?,?,?)", id, device, session, voice, r.CreatedAt+60000); err != nil {
		return CallReceipt{}, err
	}
	return r, tx.Commit()
}

func (s *Service) PrepareHandoffCall(ctx context.Context, c Caller, id, session, account, target string) (CallReceipt, CallMaterial, error) {
	r, err := s.CallReceipt(ctx, c, id)
	if err != nil {
		return CallReceipt{}, CallMaterial{}, err
	}
	var device, sid, voice string
	var expires int64
	err = s.store.db.QueryRowContext(ctx, "SELECT device,session_id,voice_id,expires_at FROM line_call_handoffs WHERE call_id=?", id).Scan(&device, &sid, &voice, &expires)
	if err != nil || device != c.Subject || sid != session || r.Account != account || r.Target != target || expires <= nowMS() || r.State != "handoff_pending" {
		return CallReceipt{}, CallMaterial{}, Fail("line_handoff_binding_invalid")
	}
	// This is execution of the stored authorization, not a new native caller.
	original := Caller{Subject: r.Subject}
	a, err := s.account(ctx, original, account, "call")
	if err != nil {
		return CallReceipt{}, CallMaterial{}, err
	}
	if _, err = s.account(ctx, c, account, "call"); err != nil {
		return CallReceipt{}, CallMaterial{}, err
	}
	if a.Revision != r.BindingRevision {
		return CallReceipt{}, CallMaterial{}, Fail("line_binding_changed")
	}
	result, err := s.store.db.ExecContext(ctx, "UPDATE line_calls SET state='preparing',updated_at=? WHERE id=? AND state='handoff_pending'", nowMS(), id)
	if err != nil {
		return CallReceipt{}, CallMaterial{}, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return CallReceipt{}, CallMaterial{}, Fail("line_call_intent_consumed")
	}
	t, err := s.store.Target(ctx, account, target)
	if err != nil {
		_ = s.UpdateCall(ctx, id, "failed_before_effect", SafeCode(err))
		return CallReceipt{}, CallMaterial{}, err
	}
	return s.prepareCallRoute(ctx, original, r, t)
}
func (s *Service) CancelCall(ctx context.Context, c Caller, id string) (CallReceipt, error) {
	r, err := s.CallReceipt(ctx, c, id)
	if err != nil {
		return CallReceipt{}, err
	}
	if r.State == "handoff_pending" || r.State == "preparing" || r.State == "prepared" {
		_ = s.UpdateCall(ctx, id, "closed", "line_call_cancelled_before_invite")
	}
	s.mu.Lock()
	owner := s.calls[id]
	s.mu.Unlock()
	if owner != nil {
		owner.cancel()
	}
	return s.CallReceipt(ctx, c, id)
}
func (s *Service) expireHandoffs(ctx context.Context) {
	_, _ = s.store.db.ExecContext(ctx, "UPDATE line_calls SET state='closed',code='line_handoff_expired',updated_at=? WHERE state='handoff_pending' AND id IN (SELECT call_id FROM line_call_handoffs WHERE expires_at<=?)", nowMS(), nowMS())
}
