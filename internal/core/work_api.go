package core

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"

	"everything-go/internal/workitems"
)

type agentWorkEvent struct {
	Action          string `json:"action"`
	ExpectedVersion uint64 `json:"expected_version"`
	Body            string `json:"body,omitempty"`
	NextStep        string `json:"next_step,omitempty"`
	ReasonCode      string `json:"reason_code,omitempty"`
	Note            string `json:"note,omitempty"`
}

// ServeWorkAPI exposes a loopback-first, versioned API for local agents. Remote
// callers still need the Bridge bearer token; no endpoint can accept/done a
// WorkItem because acceptance is a human-only transition.
func (h *Hub) ServeWorkAPI(w http.ResponseWriter, r *http.Request) {
	if h.work == nil {
		http.Error(w, "work coordination unavailable", http.StatusServiceUnavailable)
		return
	}
	if !loopbackWorkAPIRequest(r) {
		token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if !h.authValid(token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/api/work/v1/items/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" {
		http.Error(w, "expected /api/work/v1/items/{id}/context or /events", http.StatusNotFound)
		return
	}
	itemID, operation := parts[0], parts[1]
	managed, err := h.work.IsCollaborationItem(r.Context(), itemID)
	if err != nil || managed {
		http.Error(w, "v2 managed work requires run-scoped collaboration tools", http.StatusForbidden)
		return
	}
	switch {
	case r.Method == http.MethodGet && operation == "context":
		maxCharacters, _ := strconv.Atoi(r.URL.Query().Get("max_characters"))
		pack, err := h.work.BuildContextPack(r.Context(), itemID, maxCharacters)
		if err != nil {
			h.writeWorkAPIError(w, err)
			return
		}
		for i := range pack.Attachments {
			pack.Attachments[i] = h.materializeWorkAttachment(pack.Attachments[i])
		}
		pack.Prompt, pack.Truncated = workitems.RenderContextPrompt(pack, maxCharacters, pack.Truncated)
		_ = json.NewEncoder(w).Encode(pack)
	case r.Method == http.MethodPost && operation == "events":
		var event agentWorkEvent
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			http.Error(w, "invalid event: "+err.Error(), http.StatusBadRequest)
			return
		}
		if event.ExpectedVersion == 0 {
			http.Error(w, "expected_version is required", http.StatusBadRequest)
			return
		}
		item, err := h.applyAgentWorkEvent(r, itemID, event)
		if err != nil {
			h.writeWorkAPIError(w, err)
			return
		}
		h.broadcastWorkRevision(item.ActivityRevision)
		// An agent may request review before the enclosing Session emits Done.
		// Notify at the authoritative attention transition itself; otherwise the
		// later run completion sees an already-review item and cannot surface it.
		if event.Action == "request_review" {
			h.notifyWorkAttention(item, "review_ready")
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(item)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func loopbackWorkAPIRequest(r *http.Request) bool {
	if r.Header.Get("CF-Connecting-IP") != "" || r.Header.Get("X-Forwarded-For") != "" {
		return false
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func (h *Hub) applyAgentWorkEvent(r *http.Request, itemID string, event agentWorkEvent) (workitems.WorkItem, error) {
	actor := workitems.Actor{Type: workitems.ActorAgent, DeviceID: "local-work-api"}
	switch event.Action {
	case "comment", "progress":
		_, item, err := h.work.AddComment(r.Context(), workitems.AddCommentInput{
			WorkItemID: itemID, ExpectedVersion: event.ExpectedVersion, Body: event.Body, Actor: actor,
		})
		return item, err
	case "update_next_step":
		next := event.NextStep
		return h.work.UpdateItem(r.Context(), workitems.UpdateItemInput{ID: itemID,
			ExpectedVersion: event.ExpectedVersion, NextStep: &next, Actor: actor})
	case "blocked":
		reason, note := event.ReasonCode, event.Note
		if strings.TrimSpace(reason) == "" {
			reason = "agent_blocked"
		}
		return h.work.UpdateItem(r.Context(), workitems.UpdateItemInput{ID: itemID,
			ExpectedVersion: event.ExpectedVersion, BlockedReasonCode: &reason, BlockedNote: &note, Actor: actor})
	case "clear_blocked":
		empty := ""
		return h.work.UpdateItem(r.Context(), workitems.UpdateItemInput{ID: itemID,
			ExpectedVersion: event.ExpectedVersion, BlockedReasonCode: &empty, BlockedNote: &empty, Actor: actor})
	case "request_review":
		return h.work.MoveItem(r.Context(), workitems.MoveItemInput{ID: itemID,
			ExpectedVersion: event.ExpectedVersion, Lifecycle: workitems.LifecycleReview, Actor: actor})
	default:
		return workitems.WorkItem{}, errors.New("unsupported agent action")
	}
}

func (h *Hub) writeWorkAPIError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, workitems.ErrNotFound) {
		status = http.StatusNotFound
	}
	var conflict *workitems.ConflictError
	if errors.As(err, &conflict) {
		status = http.StatusConflict
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
}
