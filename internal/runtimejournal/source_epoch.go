package runtimejournal

import "errors"

// SourceEpoch is the durable session generation, without changing any device's
// read cursor or shared unread state. A persisted callback must not outlive it.
func (s *Store) SourceEpoch(id string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" {
		return "", errors.New("session_required")
	}
	r := s.ensureLocked(id)
	if e := s.persistReadBoundaryLocked(r, r.Revision); e != nil {
		return "", e
	}
	return r.ReadEpoch, nil
}
