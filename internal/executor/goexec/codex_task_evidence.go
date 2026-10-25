package goexec

import (
	"errors"
	"everything-go/internal/session"
)

// Read only the exact durable provider-owned map. Queue tuples alone cannot
// prove input consumption, and an explicit steer keeps its original owner.
func (c *Codex) ValidateNativeAcceptance(s *session.Session, thread, request, turn string) error {
	allowed := false
	for _, id := range s.ResumeIDs() {
		if id == thread {
			allowed = true
		}
	}
	if !allowed {
		return errors.New("native_thread_mismatch")
	}
	return c.mappedExactTurn(thread, request, turn)
}

func (c *Codex) NativeTurnForRequest(s *session.Session, request string) (string, error) {
	c.historyRequestMu.Lock()
	defer c.historyRequestMu.Unlock()
	r, err := c.loadTurnRequests(s.ResumeID())
	if err != nil {
		return "", err
	}
	found := ""
	for turn, id := range r.Requests {
		if id == request {
			if found != "" && found != turn {
				return "", errors.New("ambiguous native request mapping")
			}
			found = turn
		}
	}
	return found, nil
}
