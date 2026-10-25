package runtimejournal

// RestoreUnconfirmedStop is a compare-and-swap, not a new turn. A failed RPC
// cannot resurrect a terminal or overwrite a newer run that won the race.
func (s *Store) RestoreUnconfirmedStop(sessionID, requestID, previousPhase string) (View, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[sessionID]
	if record == nil || record.Phase != "stopping" || record.ActiveRequestID != requestID {
		return View{}, false
	}
	switch previousPhase {
	case "queued", "running", "waiting":
	default:
		previousPhase = "running"
	}
	now := s.now().UnixMilli()
	record.Phase = previousPhase
	record.Stage = defaultStageForPhase(previousPhase)
	record.StageMessage = "停止尚未確認，工作仍保持鎖定"
	record.StageStartedAt = now
	record.UpdatedAt = now
	record.Revision++
	s.saveLocked()
	return viewLocked(record, ""), true
}
