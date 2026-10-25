package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/agentvisibility"
	"everything-go/internal/session"
	"net/http"
	"strings"
)

type internalAgentObserver interface {
	ObserveInternalAgents(context.Context, *session.Session, string) agentvisibility.Snapshot
}

// handleInternalAgentObservation is an ordinary authenticated read. The parent
// session and authority are resolved from the Hub, not accepted from JSON.
// Root-orchestration-only team actors remain unsupported until their original
// native owner supplies a Reader; they are not Bridge top-level sessions.
func (h *Hub) handleInternalAgentObservation(ctx context.Context, c *Client, sessionID string, readIDs ...string) {
	proof := c.readIdentity.Load()
	if proof == nil || h.pairedTaskDevice(c) == "" {
		c.enqueueEvent(h.client.Error(sessionID, "observation_forbidden", "Original paired reader required"))
		return
	}
	readID := ""
	if len(readIDs) == 1 {
		readID = readIDs[0]
	}
	out, ok := h.observeInternalAgents(ctx, sessionID)
	if c.readIdentity.Load() != proof || h.pairedTaskDevice(c) == "" {
		c.enqueueEvent(h.client.Error(sessionID, "observation_forbidden", "Original paired reader changed"))
		return
	}
	if ok {
		// Only this authenticated owner route creates the transport scope binding.
		// It confers no canonical relation, human action, or Task API authority.
		if strings.HasPrefix(readID, "obs_") && len(readID) >= 8 && len(readID) <= 80 {
			c.enqueueEvent(map[string]any{"type": "internal_agent_owner_binding", "request_id": readID, "authority": out.Authority, "source_epoch": out.SourceEpoch, "session_id": out.SessionID, "root_actor_id": out.RootActorID, "supported": out.Availability == "live", "max_age_ms": 60000, "coverage": out.Coverage, "root_inventory_callback": "unsupported"})
		}
		c.enqueueEvent(out)
	}
}

func (h *Hub) observeInternalAgents(ctx context.Context, sessionID string) (agentvisibility.Snapshot, bool) {
	s, ok := h.registry.Get(sessionID)
	if !ok || s.Snapshot().Hidden || !h.controllerInScope(s) {
		return agentvisibility.Snapshot{}, false
	}
	before := s.Snapshot()
	scope := agentvisibility.Scope{Authority: h.cfg.InstanceID, SessionID: s.ID, RootActorID: s.ResumeID()}
	out := agentvisibility.Unsupported(scope)
	if hr, ok := h.exec.(historyRouter); ok {
		if provider, found := hr.ProviderFor(s); found {
			if reader, supported := provider.(internalAgentObserver); supported {
				out = reader.ObserveInternalAgents(ctx, s, h.cfg.InstanceID)
			}
		}
	}
	after := s.Snapshot()
	current, exists := h.registry.Get(sessionID)
	// Preserve parent config/source scope throughout the read; no client can
	// attach a prior probe to a switched session/model/thread/authority.
	if !exists || current != s || before.ConfigRevision != after.ConfigRevision || before.ResumeID != after.ResumeID || before.Backend != after.Backend || after.Hidden || !h.controllerInScope(s) || out.Authority != scope.Authority || out.SessionID != scope.SessionID || out.RootActorID != scope.RootActorID || (out.Availability == "live" && (out.SourceEpoch == "" || (out.Coverage != "partial_provider_child_cache" && out.Coverage != "partial_selected_native_root"))) {
		out = agentvisibility.Unsupported(scope)
		out.Availability = "unknownstale"
		out.ProbeError = "scope_changed"
	}
	// No canonical resolver is wired in this subset. A future provider cannot
	// silently widen this read into a task relation, native final or QA publisher.
	for _, child := range out.Children {
		if child.Binding != nil || child.Report != nil || child.Association != "unmatched" || child.Role != "implementation" || child.Axes.NativeFinal != "unverified" || child.Axes.Review != "unverified" || child.Axes.Delivery != "unverified" {
			out = agentvisibility.Unsupported(scope)
			out.Availability = "unknownstale"
			out.ProbeError = "canonical_relation_unavailable"
			break
		}
	}
	return out, true
}

// ServeInternalAgentObservation is a bounded metadata read through the original
// paired Hub authorizer. It creates no device connection, queue or caller and
// cannot displace an active application. There is no ingest or install route.
func (h *Hub) ServeInternalAgentObservation(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !h.HTTPAuthorized(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if len(r.URL.RawQuery) > 256 || len(r.URL.Query()) != 1 || len(r.URL.Query()["session_id"]) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	id := r.URL.Query().Get("session_id")
	if id == "" || len(id) > 128 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	out, ok := h.observeInternalAgents(r.Context(), id)
	if !h.HTTPAuthorized(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if out.Availability == "unsupported" && out.ProbeError == "" {
		out.ProbeError = "original_runtime_reader_unavailable"
	}
	_ = json.NewEncoder(w).Encode(out)
}
