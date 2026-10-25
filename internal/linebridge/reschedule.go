package linebridge

import (
	"context"
	"time"
)

// Reschedule is an explicit new decision about a known, unexecuted operation.
// An unknown/dispatching/accepted operation is never eligible for this path.
func (s *Service) Reschedule(ctx context.Context, c Caller, id string, revision uint64, due int64, zone string) (Operation, error) {
	op, err := s.Receipt(ctx, c, id)
	if err != nil {
		return Operation{}, err
	}
	a, err := s.account(ctx, Caller{Subject: op.Subject}, op.Account, "schedule")
	if err != nil {
		return Operation{}, err
	}
	if !s.store.Allowed(ctx, Caller{Subject: op.Subject}, op.Account, "messages.send") {
		return Operation{}, Fail("line_forbidden")
	}
	if op.Content.ResourceRef != "" {
		r, e := s.store.Resource(ctx, Caller{Subject: op.Subject}, op.Account, op.Content.ResourceRef)
		if e != nil {
			return Operation{}, e
		}
		if due >= r.ExpiresAt {
			return Operation{}, Fail("line_resource_expired")
		}
	}
	if _, err = time.LoadLocation(zone); err != nil || due <= nowMS() {
		return Operation{}, Fail("line_schedule_invalid")
	}
	result, err := s.store.db.ExecContext(ctx, "UPDATE line_ops SET state='scheduled',effect='not_dispatched',code='',due_at=?,timezone=?,binding=?,updated_at=?,revision=revision+1 WHERE id=? AND revision=? AND state IN ('queued','scheduled','held_late','held_binding','held_unavailable','failed_before_effect')", due, zone, a.Revision, nowMS(), id, revision)
	if err != nil {
		return Operation{}, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return Operation{}, Fail("line_operation_revision_conflict")
	}
	return s.Receipt(ctx, c, id)
}
