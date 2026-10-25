package core

import (
	"everything-go/internal/nativewatch"
	"everything-go/internal/session"
)

// Require two consecutive complete catalogs before retiring imported rows.
// Incomplete/offline scans clear the candidate and can never establish loss.
func (h *Hub) reconcileNativeCatalog(opts nativewatch.Options, previous map[string]bool) (map[string]bool, int) {
	catalog := nativewatch.ScanTranscriptCatalog(map[string]string{
		nativewatch.BackendClaude: opts.ClaudeProjectsDir,
		nativewatch.BackendCodex:  opts.CodexSessionsDir,
	})
	candidates := map[string]bool{}
	for _, s := range h.registry.List() {
		snap := s.Snapshot()
		missing := catalog.MissingSession(snap.Backend, snap.ResumeID)
		// A logical conversation may own older physical threads. Never retire
		// it while any original thread remains available.
		for _, id := range snap.HistoricalResumeIDs {
			if !catalog.MissingSession(snap.Backend, id) {
				missing = false
				break
			}
		}
		if missing {
			candidates[snap.ID+"\x00"+snap.ResumeID] = true
		}
	}
	// Same lock order as message admission: durable queue -> registry -> session.
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	removed := h.registry.PruneMissingNativeSessions(func(s session.Snapshot) bool {
		key := s.ID + "\x00" + s.ResumeID
		if !candidates[key] || !previous[key] {
			return false
		}
		if h.messageQueue != nil {
			pending, err := h.messageQueue.HasUnsettled(s.ID)
			if err != nil || pending {
				return false
			}
		}
		return true
	})
	return candidates, removed
}
