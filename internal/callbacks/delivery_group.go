package callbacks

import (
	"context"
	"database/sql"
	"encoding/json"
)

// SameDelivery preserves individual requests and cancellation scopes while
// coalescing only the same verified execution/result for the same recipient.
func deliveryKey(r Record) string {
	if r.Report == nil {
		return ""
	}
	execution := r.Report.ExecutionRequest
	if execution == "" {
		execution = r.Request
	}
	raw, _ := json.Marshal([]any{r.Authority, r.Session, r.Epoch, r.Config, r.Thread, execution, r.Report.Turn, r.Report.Hash, r.Report.SourceKind, r.Report.SourceID, r.Report.SourceRevision, r.Recipient.Device, r.Recipient.Binding})
	return Digest(string(raw))
}
func SameDelivery(a, b Record) bool {
	return a.Report != nil && b.Report != nil && deliveryKey(a) == deliveryKey(b)
}
func HoldsDelivery(r Record) bool {
	if r.Invite == nil || r.Invite.State == "group_waiting" {
		return false
	}
	// Presentation/claim is durable evidence of an attempted report. Cancellation
	// after presentation must never make a second automatic phone call.
	if r.Invite.PresentedAt > 0 || r.Invite.Claim != "" || r.Invite.State == "declined" {
		return true
	}
	return r.Enabled && !Terminal(r.Invite.State)
}
func GroupBlocked(r Record, peers []Record) bool {
	for _, p := range peers {
		if p.ID != r.ID && SameDelivery(r, p) && HoldsDelivery(p) {
			return true
		}
	}
	return false
}

// Runs inside the same transaction as each invitation and its original receipt.
// A concurrent seal cannot publish two automatic delivery_pending invitations.
func groupAutomaticInvitation(ctx context.Context, tx *sql.Tx, r *Record) error {
	if r.Invite == nil || r.Report == nil {
		return nil
	}
	key := deliveryKey(*r)
	if r.Invite.PresentedAt > 0 || r.Invite.Claim != "" || r.Invite.State == "declined" {
		if _, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO callback_delivery_history VALUES(?,?)", key, r.Invite.ID); e != nil {
			return e
		}
	}
	if r.Invite.Manual || r.Invite.State != "delivery_pending" || !r.Enabled {
		return nil
	}
	var attempted bool
	if e := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM callback_delivery_history WHERE group_key=?)", key).Scan(&attempted); e != nil {
		return e
	}
	if attempted {
		r.Invite.State = "group_waiting"
		r.Invite.Reason = "此結果已發出匯報，請明確選擇現在來電匯報才重播"
		r.State = "report_grouped"
		return nil
	}
	rows, e := tx.QueryContext(ctx, "SELECT record FROM callbacks WHERE session=? AND id<>?", r.Session, r.ID)
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if e = rows.Scan(&raw); e != nil {
			return e
		}
		peer, e := decode(raw)
		if e != nil {
			return e
		}
		if SameDelivery(*r, peer) && HoldsDelivery(peer) {
			r.Invite.State = "group_waiting"
			r.Invite.Reason = "此執行的匯報已合併，同一裝置不重複來電"
			r.State = "report_grouped"
			break
		}
	}
	return rows.Err()
}

func (s *Store) DeliveryAttempted(ctx context.Context, r Record) (bool, error) {
	var found bool
	e := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM callback_delivery_history WHERE group_key=?)", deliveryKey(r)).Scan(&found)
	return found, e
}
