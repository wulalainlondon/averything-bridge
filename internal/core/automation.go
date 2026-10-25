package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"everything-go/internal/automation"
	"everything-go/internal/eventinbox"
	"everything-go/internal/relay"
	"everything-go/internal/workitems"
)

const automationSchedulerInterval = 5 * time.Second

func (h *Hub) SetAutomation(store *automation.Store) {
	h.automation = store
	h.automationManager = automation.NewManager(store, automation.EnvSecretResolver{}, automation.MetaFacebookConnector{}, automation.MetaThreadsConnector{}, automation.SentryConnector{})
}

func (h *Hub) StartAutomationScheduler(ctx context.Context) {
	if h.automation == nil || h.work == nil || !h.automationScheduler.CompareAndSwap(false, true) {
		return
	}
	go func() {
		ticker := time.NewTicker(automationSchedulerInterval)
		defer ticker.Stop()
		for {
			h.drainExternalAttachmentQueue(ctx)
			h.drainEventAssignments(ctx)
			h.drainAutomationQueue(ctx)
			h.drainConnectorWork(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-h.automationWake:
			}
		}
	}()
}

func (h *Hub) drainConnectorWork(ctx context.Context) {
	if h.automationManager == nil || h.events == nil {
		return
	}
	owner := h.cfg.InstanceID + ":" + h.gen
	for count := 0; count < 2; count++ {
		processed, err := h.automationManager.PollOnce(ctx, owner, func(publishCtx context.Context, input eventinbox.Input) error {
			event, deduped, insertErr := h.events.Insert(publishCtx, input)
			if insertErr != nil {
				return insertErr
			}
			return h.publishExternalEvent(event, deduped)
		})
		if err != nil {
			log.Printf("[automation] connector poll: %v", err)
			break
		}
		if !processed {
			break
		}
		h.broadcastAutomationSnapshot()
	}
	if processed, err := h.automationManager.ExecuteOnce(ctx); err != nil {
		log.Printf("[automation] approved action: %v", err)
	} else if processed {
		h.broadcastAutomationSnapshot()
	}
}

func (h *Hub) WakeAutomationScheduler() {
	select {
	case h.automationWake <- struct{}{}:
	default:
	}
}

func (h *Hub) routeExternalEvent(event eventinbox.Event) {
	if h.automation == nil {
		return
	}
	bindings, _, err := h.automation.RouteEvent(context.Background(), event)
	if err != nil {
		log.Printf("[automation] route event=%s: %v", event.ID, err)
		return
	}
	if len(bindings) == 0 {
		return
	}
	h.broadcastAutomationSnapshot()
	h.WakeAutomationScheduler()
}

func (h *Hub) drainAutomationQueue(ctx context.Context) {
	for dispatched := 0; dispatched < 4; dispatched++ {
		binding, ok, err := h.automation.ClaimNextBinding(ctx, time.Now().UnixMilli())
		if err != nil {
			log.Printf("[automation] claim binding: %v", err)
			return
		}
		if !ok {
			return
		}
		if !h.dispatchAutomationBinding(ctx, binding) {
			return
		}
	}
}

func (h *Hub) dispatchAutomationBinding(ctx context.Context, binding automation.Binding) bool {
	if binding.WorkItemID == "" && binding.TargetWorkItemID == "" {
		return h.dispatchSessionBinding(ctx, binding)
	}
	if binding.TargetInstanceID != "" {
		return h.dispatchRelayBinding(ctx, binding)
	}
	if binding.WorkItemID == "" {
		return h.dispatchSessionBinding(ctx, binding)
	}
	item, err := h.work.GetItem(ctx, binding.WorkItemID)
	if err != nil {
		h.deferAutomation(binding, "work_item_unavailable", time.Minute)
		return false
	}
	if item.Lifecycle != workitems.LifecycleReady && item.Lifecycle != workitems.LifecycleActive {
		h.deferAutomation(binding, "work_item_"+string(item.Lifecycle), 30*time.Second)
		return false
	}
	if active, activeErr := h.work.HasActiveRun(ctx, item.ID); activeErr != nil || active {
		h.deferAutomation(binding, "work_item_busy", 10*time.Second)
		return false
	}
	if _, ok := h.registry.Get(binding.SessionID); !ok {
		h.deferAutomation(binding, "session_unavailable", time.Minute)
		return false
	}
	runID := "aer_" + strings.TrimPrefix(binding.ID, "aeb_")
	requestID := "auto_" + strings.TrimPrefix(binding.ID, "aeb_")
	run, updated, err := h.work.StartRun(ctx, workitems.StartRunInput{
		ID: runID, WorkItemID: item.ID, SessionID: binding.SessionID, RequestID: requestID,
		Kind: "research", Instruction: binding.Instruction, ExpectedVersion: item.Version,
		Actor: workitems.Actor{Type: workitems.ActorSystem},
	})
	if err != nil {
		if errors.Is(err, workitems.ErrConflict) || errors.Is(err, workitems.ErrInvalidTransition) {
			h.deferAutomation(binding, "work_item_changed", 10*time.Second)
		} else {
			h.deferAutomation(binding, "run_create_failed", time.Minute)
		}
		return false
	}
	if err := h.automation.BindRun(ctx, binding.ID, run.ID, run.RequestID); err != nil {
		_, _ = h.work.AdvanceRun(ctx, run.SessionID, run.RequestID, "failed", "automation binding receipt failed")
		log.Printf("[automation] bind run binding=%s: %v", binding.ID, err)
		return false
	}
	h.broadcastWorkRevision(updated.ActivityRevision)
	h.broadcastAutomationSnapshot()
	h.WakeWorkScheduler()
	return true
}

func (h *Hub) dispatchRelayBinding(ctx context.Context, binding automation.Binding) bool {
	if h.relay == nil || h.events == nil {
		h.deferAutomation(binding, "relay_unavailable", time.Minute)
		return false
	}
	if _, ok := h.relayPeers[binding.TargetInstanceID]; !ok {
		h.deferAutomation(binding, "relay_peer_unavailable", time.Minute)
		return false
	}
	eventIDs, err := h.automation.EventIDsForBinding(ctx, binding)
	if err != nil || len(eventIDs) == 0 {
		h.deferAutomation(binding, "event_unavailable", time.Minute)
		return false
	}
	var event eventinbox.Event
	descriptors := make([]relay.Attachment, 0)
	missing := 0
	for eventIndex, eventID := range eventIDs {
		current, getErr := h.events.Get(ctx, eventID)
		if getErr != nil {
			h.deferAutomation(binding, "event_unavailable", time.Minute)
			return false
		}
		if eventIndex == 0 {
			event = current
		}
		for _, attachment := range current.Attachments {
			switch attachment.Status {
			case "available":
				descriptors = append(descriptors, relay.Attachment{ID: attachment.ID, Ordinal: len(descriptors),
					MIMEType: attachment.MIMEType, DisplayName: attachment.DisplayName,
					SizeBytes: attachment.SizeBytes, SHA256: attachment.SHA256})
			case "missing":
				missing++
			default:
				h.deferAutomation(binding, "attachments_materializing", 10*time.Second)
				return false
			}
		}
	}
	instruction := binding.Instruction
	if missing > 0 {
		instruction += "\nEvidence warning: " + fmt.Sprint(missing) + " attachment(s) could not be materialized; continue with available evidence and report the limitation."
	}
	now := time.Now().UnixMilli()
	jobID := "rjob_" + shortRelayID(binding.ID)
	job := relay.Job{SchemaVersion: 1, ID: jobID, OriginInstanceID: h.cfg.InstanceID,
		TargetInstanceID: binding.TargetInstanceID, EventID: event.ID, EventKey: event.EventKey,
		EventIDs: eventIDs,
		Source:   event.Source, Kind: event.Kind, Title: event.Title, Body: event.Body, Data: event.Data,
		TargetWorkItemID: binding.TargetWorkItemID, TargetSessionID: binding.TargetSessionID,
		Instruction: instruction, ReviewOnly: binding.ReviewOnly, Attachments: descriptors,
		CreatedAt: now, ExpiresAt: now + int64(30*24*time.Hour/time.Millisecond), TraceID: binding.ID}
	if _, err := h.relay.EnqueueOutbound(ctx, job); err != nil {
		h.deferAutomation(binding, "relay_enqueue_failed", time.Minute)
		return false
	}
	if err := h.automation.BindRun(ctx, binding.ID, job.ID, job.ID); err != nil {
		log.Printf("[relay] bind automation binding=%s job=%s: %v", binding.ID, job.ID, err)
		return false
	}
	h.broadcastAutomationSnapshot()
	h.WakeRelayScheduler()
	return true
}

func (h *Hub) deferAutomation(binding automation.Binding, reason string, delay time.Duration) {
	if err := h.automation.DeferBinding(context.Background(), binding.ID, reason, time.Now().Add(delay).UnixMilli()); err != nil {
		log.Printf("[automation] defer binding=%s reason=%s: %v", binding.ID, reason, err)
		return
	}
	h.broadcastAutomationSnapshot()
}

func (h *Hub) projectAutomationRun(requestID, status, reason string) {
	if h.automation == nil || requestID == "" {
		return
	}
	changed, err := h.automation.AdvanceRun(context.Background(), requestID, status, reason)
	if err != nil {
		log.Printf("[automation] project request=%s status=%s: %v", requestID, status, err)
		return
	}
	if changed {
		h.broadcastAutomationSnapshot()
	}
}

func (h *Hub) automationSnapshotEvent() map[string]any {
	snapshot, err := h.automation.Snapshot(context.Background())
	if err != nil {
		return map[string]any{"type": "automation_error", "message": err.Error()}
	}
	assignments, _ := h.automation.EventAssignments(context.Background())
	for i := range snapshot.Proposals {
		p := &snapshot.Proposals[i]
		for _, a := range assignments {
			if p.RunID == a.ID && p.WorkItemID == "" {
				p.SessionID = a.SessionID
				p.OwnerInstanceID = a.OwnerInstanceID
			}
		}
		if p.RunID == "" {
			continue
		}
		if run, ok := h.findWorkRun(context.Background(), p.RunID); ok && run.WorkItemID == p.WorkItemID {
			p.SessionID = run.SessionID
			p.OwnerInstanceID = h.cfg.InstanceID
		}
	}
	return map[string]any{"type": "automation_snapshot", "snapshot": snapshot}
}

func (h *Hub) broadcastAutomationSnapshot() {
	if h.automation != nil {
		_ = h.broadcastOnline(h.automationSnapshotEvent())
	}
}

func (h *Hub) ServeAutomationAPI(w http.ResponseWriter, r *http.Request) {
	if automationAppCORS(w, r) {
		return
	}
	if h.automation == nil {
		http.Error(w, "automation unavailable", http.StatusServiceUnavailable)
		return
	}
	if !loopbackWorkAPIRequest(r) && !h.HTTPAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/automation/v1/"), "/")
	if h.serveInboxAPI(w, r, path) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet && path == "snapshot" {
		_ = json.NewEncoder(w).Encode(h.automationSnapshotEvent())
		return
	}
	if r.Method == http.MethodPost && path == "accounts" {
		var input struct {
			Account automation.Account `json:"account"`
		}
		if !decodeAutomationRequest(w, r, &input) {
			return
		}
		account, revision, err := h.automation.UpsertAccount(r.Context(), input.Account, 0)
		if err != nil {
			writeAutomationError(w, err)
			return
		}
		h.broadcastAutomationSnapshot()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"account": account, "revision": revision})
		return
	}
	if r.Method == http.MethodPost && path == "routes" {
		var input struct {
			Route           automation.Route `json:"route"`
			ExpectedVersion uint64           `json:"expected_version"`
		}
		if !decodeAutomationRequest(w, r, &input) {
			return
		}
		var sessionPolicy *automation.EventAssignment
		if input.Route.WorkItemID == "" && input.Route.TargetWorkItemID == "" && input.Route.HandlingMode != automation.NotifyOnly {
			device := h.inboxDevice(r)
			if device == "" {
				http.Error(w, "paired device required", 403)
				return
			}
			policy := automation.EventAssignment{ID: input.Route.ID, OwnerDevice: device, Mode: "analyze", RuleID: input.Route.ID, RuleVersion: input.ExpectedVersion + 1, SourceInstanceID: h.cfg.InstanceID}
			if input.Route.TargetInstanceID != "" {
				peer, ok := h.relayPeers[input.Route.TargetInstanceID]
				if !ok {
					http.Error(w, "original peer unavailable", 409)
					return
				}
				response, err := h.relayRequest(r.Context(), peer, http.MethodGet, "/api/relay/v1/session-target/"+url.PathEscape(input.Route.TargetSessionID), nil)
				if err != nil {
					http.Error(w, "peer target unavailable", 409)
					return
				}
				var target automation.EventAssignment
				decodeErr := json.NewDecoder(io.LimitReader(response.Body, 8192)).Decode(&target)
				response.Body.Close()
				if response.StatusCode != 200 || decodeErr != nil || target.ThreadID == "" {
					http.Error(w, "peer target not granted or initialized", 409)
					return
				}
				policy.SessionID = target.SessionID
				policy.OwnerInstanceID = target.OwnerInstanceID
				policy.ThreadID = target.ThreadID
				policy.ConfigRevision = target.ConfigRevision
			} else {
				target, ok := h.registry.Get(input.Route.SessionID)
				if !ok || !h.eventSessionAllowed(target) || target.ResumeID() == "" {
					http.Error(w, "initialized local target required", 409)
					return
				}
				if managed, err := h.PMConfiguration(target.ID); err != nil || managed != nil {
					http.Error(w, "managed target unavailable", 409)
					return
				}
				policy.SessionID = target.ID
				policy.OwnerInstanceID = h.cfg.InstanceID
				policy.ThreadID = target.ResumeID()
				policy.ConfigRevision = target.SettingsSnapshot().ConfigRevision
			}
			if input.Route.HandlingMode == automation.DraftForReview {
				policy.Mode = "draft"
			}
			if input.Route.HandlingMode == automation.ApprovedAction {
				policy.Mode = "action_draft"
			}
			sessionPolicy = &policy
		}

		var route automation.Route
		var revision uint64
		var err error
		if sessionPolicy != nil {
			route, revision, err = h.automation.UpsertSessionRoute(r.Context(), input.Route, input.ExpectedVersion, *sessionPolicy)
		} else {
			route, revision, err = h.automation.UpsertRoute(r.Context(), input.Route, input.ExpectedVersion)
		}
		if err != nil {
			writeAutomationError(w, err)
			return
		}
		h.broadcastAutomationSnapshot()
		h.WakeAutomationScheduler()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"route": route, "revision": revision})
		return
	}
	if r.Method == http.MethodPost && path == "proposals" {
		if !loopbackWorkAPIRequest(r) {
			http.Error(w, "proposal creation is loopback-only", http.StatusForbidden)
			return
		}
		var input struct {
			ID             string          `json:"id"`
			AccountID      string          `json:"connector_account_id"`
			WorkItemID     string          `json:"work_item_id"`
			RunID          string          `json:"run_id"`
			ActionType     string          `json:"action_type"`
			TargetID       string          `json:"target_id"`
			Payload        json.RawMessage `json:"payload"`
			DisplayPreview string          `json:"display_preview"`
			ExpiresAt      int64           `json:"expires_at"`
		}
		if !decodeAutomationRequest(w, r, &input) {
			return
		}
		if input.WorkItemID == "" {
			assignments, err := h.automation.EventAssignments(r.Context())
			if err != nil {
				writeAutomationError(w, err)
				return
			}
			verified := false
			for _, assignment := range assignments {
				if assignment.ID != input.RunID || assignment.OwnerInstanceID != h.cfg.InstanceID {
					continue
				}
				target, ok := h.registry.Get(assignment.SessionID)
				if ok && h.nativeTaskTurn(target, assignment.ID) != "" {
					verified = true
				}
			}
			if !verified {
				http.Error(w, "original event native execution is unconfirmed", 409)
				return
			}
		}
		proposal, revision, err := h.automation.CreateProposal(r.Context(), automation.ProposalInput{
			ID: input.ID, AccountID: input.AccountID, WorkItemID: input.WorkItemID, RunID: input.RunID,
			ActionType: input.ActionType, TargetID: input.TargetID, Payload: input.Payload,
			DisplayPreview: input.DisplayPreview, ExpiresAt: input.ExpiresAt,
		})
		if err != nil {
			writeAutomationError(w, err)
			return
		}
		h.broadcastAutomationSnapshot()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"proposal": proposal, "revision": revision})
		return
	}
	parts := strings.Split(path, "/")
	if r.Method == http.MethodPost && len(parts) == 3 && parts[0] == "proposals" && (parts[2] == "approve" || parts[2] == "reject") {
		deviceID := strings.TrimSpace(r.Header.Get("X-Bridge-Device-ID"))
		if deviceID == "" {
			http.Error(w, "X-Bridge-Device-ID is required", http.StatusBadRequest)
			return
		}
		var input struct {
			ExpectedVersion uint64 `json:"expected_version"`
			PayloadHash     string `json:"payload_hash"`
		}
		if !decodeAutomationRequest(w, r, &input) {
			return
		}
		decision := "approved"
		if parts[2] == "reject" {
			decision = "rejected"
		}
		proposal, revision, err := h.automation.DecideProposal(r.Context(), parts[1], input.ExpectedVersion, deviceID, decision, input.PayloadHash)
		if err != nil {
			writeAutomationError(w, err)
			return
		}
		h.broadcastAutomationSnapshot()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"proposal": proposal, "revision": revision})
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

func decodeAutomationRequest(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		http.Error(w, "invalid automation request: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeAutomationError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, automation.ErrNotFound) {
		status = http.StatusNotFound
	} else if errors.Is(err, automation.ErrConflict) {
		status = http.StatusConflict
	}
	http.Error(w, err.Error(), status)
}
