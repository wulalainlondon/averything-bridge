package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/recap"
	"time"
)

type recapEvent struct {
	Type      string          `json:"type"`
	SessionID string          `json:"session_id"`
	Authority string          `json:"authority_instance_id"`
	RequestID string          `json:"request_id"`
	Snapshot  *recap.Snapshot `json:"snapshot,omitempty"`
	Stale     bool            `json:"stale"`
	Error     string          `json:"error_code,omitempty"`
}

func (h *Hub) handleSessionRecap(c *Client, cmd clientproto.Command) {
	e := recapEvent{Type: "session_recap", SessionID: cmd.SessionID, Authority: h.cfg.InstanceID, RequestID: cmd.RequestID}
	fail := func(code string) { e.Error = code; c.enqueueEventUnlogged(e) }
	if c.enrollmentOnly || c.deviceID == "" {
		fail("pairing_required")
		return
	}
	if cmd.RequestID == "" || len(cmd.RequestID) > 128 {
		fail("invalid_request")
		return
	}
	s, ok := h.registry.Get(cmd.SessionID)
	if !ok {
		fail("no_session")
		return
	}
	if s.Backend() != backend.Codex || s.ResumeID() == "" {
		fail("backend_unsupported")
		return
	}
	p, ok := h.exec.(backend.RecapExecutor)
	if !ok {
		fail("backend_unsupported")
		return
	}
	generate := cmd.Kind == "generate_session_recap"
	threadID := s.ResumeID()
	if generate && (s.IsStreaming() || s.QueueLen() > 0 || h.toolRepairRunning.Load()) {
		fail("session_busy")
		return
	}
	go func() {
		if !c.live() {
			return
		}
		// Keep recap history scans/model waits off the ordinary history semaphore
		// so a burst cannot starve opening a chat, and never queue unbounded work.
		select {
		case h.recapJobs <- struct{}{}:
			defer func() { <-h.recapJobs }()
		default:
			fail("recap_busy")
			return
		}
		ctx, cancel := context.WithTimeout(c.ctx, 95*time.Second)
		defer cancel()
		result, err := p.SessionRecap(ctx, s, generate, cmd.Force)
		e.Snapshot, e.Stale = result.Snapshot, result.Stale
		if current, exists := h.registry.Get(s.ID); !exists || current != s || current.ResumeID() != threadID || e.Snapshot != nil && e.Snapshot.ThreadID != threadID {
			e.Snapshot = nil
			fail("recap_source_changed")
			return
		}
		if s.IsStreaming() || s.QueueLen() > 0 {
			e.Stale = e.Snapshot != nil
		}
		if err != nil {
			// Adapters return fixed reason codes only, never raw model/tool errors.
			switch err.Error() {
			case "backend_unsupported", "daemon_not_connected", "recap_history_empty", "recap_history_unavailable", "recap_busy", "recap_timeout", "recap_source_changed", "recap_cache_unavailable", "recap_cache_invalid", "recap_invalid", "recap_tools_unavailable":
				e.Error = err.Error()
			default:
				e.Error = "recap_generation_failed"
			}
		}
		c.enqueueEventUnlogged(e)
	}()
}
