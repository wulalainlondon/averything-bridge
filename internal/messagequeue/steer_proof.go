package messagequeue

// Forward-only proof bound to the exact canonical supplement. Old state tuples
// remain history, never retroactively promoted from malformed legacy ACKs.
func (s *Store) StrictSteerProven(e Entry) bool {
	var found bool
	err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM task_native_steer_proofs WHERE session_id=? AND request_id=? AND payload_hash=? AND active_request_id=? AND turn_id=? AND strict_ack_version=1)`, e.SessionID, e.RequestID, e.PayloadHash, e.ActiveRequestID, e.TurnID).Scan(&found)
	return err == nil && found && e.State == Steered
}
