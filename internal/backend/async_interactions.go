package backend

import "everything-go/internal/session"

// NativeAsyncInteractions prepares a correlated ordinary user message. It does
// not deliver it: the Hub retains control/admission and durable queue ownership.
type NativeAsyncInteractions interface {
	PrepareAsyncReply(id string, answers map[string]any, cancelled bool) (sessionID, content string, handled bool, err error)
	ReconcileAsyncQuestions(s *session.Session)
}
