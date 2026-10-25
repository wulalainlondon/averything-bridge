package core

import "everything-go/internal/session"

// Absence means unknown/last-known at the client; an explicit known zero must
// remain present so the existing client can replace a cached positive value.
func knownContextUsed(s session.Snapshot) *int {
	if !s.ContextUsedKnown || s.ContextUsed < 0 {
		return nil
	}
	value := s.ContextUsed
	return &value
}
func knownContextMax(s session.Snapshot) *int {
	if s.ContextMax <= 0 {
		return nil
	}
	value := s.ContextMax
	return &value
}
