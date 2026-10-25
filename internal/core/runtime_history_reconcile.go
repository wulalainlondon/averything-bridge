package core

import (
	"log"
	"time"

	"everything-go/internal/search"
)

// reconcileRestartMarkersFromSearch retries for a short bounded window because
// the out-of-process search indexer may commit the surviving assistant tail a
// few seconds after Hub startup. It creates no watcher and exits after 52s.
func (h *Hub) reconcileRestartMarkersFromSearch() {
	for _, delay := range []time.Duration{0, 2 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second} {
		if delay > 0 {
			time.Sleep(delay)
		}
		h.reconcileRestartMarkersOnce()
	}
}

func (h *Hub) reconcileRestartMarkersOnce() int {
	if h.search == nil {
		return 0
	}
	registered := h.registry.List()
	ids := make([]string, 0, len(registered))
	byID := make(map[string]struct {
		backend string
		uid     string
		phase   string
	}, len(registered))
	for _, session := range registered {
		snapshot := session.Snapshot()
		ids = append(ids, snapshot.ID)
		phase := snapshot.State.String()
		if phase == "streaming" {
			phase = "running"
		}
		byID[snapshot.ID] = struct {
			backend string
			uid     string
			phase   string
		}{backend: snapshot.Backend, uid: snapshot.ResumeID, phase: phase}
	}
	views := h.runtimes.Snapshot("", ids)
	uids := make([]search.SessionUID, 0)
	for _, view := range views {
		identity := byID[view.SessionID]
		if view.Phase != "interrupted" || view.LastTerminal != "interrupted" ||
			view.LastError != "Bridge restarted before the terminal event was observed" ||
			identity.backend == "" || identity.uid == "" {
			continue
		}
		uids = append(uids, search.SessionUID{HubID: view.SessionID, Backend: identity.backend, UID: identity.uid})
	}
	if len(uids) == 0 {
		return 0
	}
	previews := h.search.RecentMessagesByUID(uids, 12)
	healed := 0
	for _, uid := range uids {
		preview := previews[uid.HubID]
		if preview == nil || preview.LastAssistantTS <= 0 {
			continue
		}
		identity := byID[uid.HubID]
		view, changed := h.runtimes.ReconcileHistoryActivity(uid.HubID, preview.LastAssistantTS*1000, identity.phase)
		if !changed {
			continue
		}
		healed++
		h.broadcastRuntime(view)
	}
	if healed > 0 {
		log.Printf("[runtime] reconciled %d restart interruption(s) from canonical history", healed)
	}
	return healed
}
