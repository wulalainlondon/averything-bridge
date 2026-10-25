package runtimejournal

// WaitForInteraction changes presentation only for an already running turn.
// The question's identity never replaces execution ownership or queue length.
func (s *Store) WaitForInteraction(sessionID, interactionID string) (View, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[sessionID]
	if r == nil || interactionID == "" || (r.Phase != "running" && r.Phase != "waiting") {
		return View{}, false
	}
	if owner, exists := r.WaitingInteractions[interactionID]; exists && owner == r.ActiveRequestID {
		return viewLocked(r, ""), false
	}
	if r.WaitingInteractions == nil {
		r.WaitingInteractions = make(map[string]string)
	}
	r.WaitingInteractions[interactionID] = r.ActiveRequestID
	changed := r.Phase != "waiting"
	if changed {
		r.Revision++
		r.Phase, r.Stage, r.StageMessage = "waiting", "waiting_user", ""
		r.UpdatedAt = s.now().UnixMilli()
		r.StageStartedAt = r.UpdatedAt
	}
	s.saveLocked()
	return viewLocked(r, ""), changed
}

// ResolveInteraction resumes only the same waiting turn, after its final
// blocking question has been resolved. Async/unknown/stale resolutions do not
// imply that the model started work.
func (s *Store) ResolveInteraction(sessionID, interactionID string) (View, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[sessionID]
	if r == nil || interactionID == "" || r.Phase != "waiting" {
		return View{}, false
	}
	owner, exists := r.WaitingInteractions[interactionID]
	if !exists || owner != r.ActiveRequestID {
		return viewLocked(r, ""), false
	}
	delete(r.WaitingInteractions, interactionID)
	if len(r.WaitingInteractions) > 0 {
		s.saveLocked()
		return viewLocked(r, ""), false
	}
	r.Revision++
	r.Phase, r.Stage, r.StageMessage = "running", "thinking", ""
	r.UpdatedAt = s.now().UnixMilli()
	r.StageStartedAt = r.UpdatedAt
	s.saveLocked()
	return viewLocked(r, ""), true
}
