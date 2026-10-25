package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/sessiondispatch"
	"io"
	"net/http"
	"strings"
)

type controllerEnvelope struct {
	Interaction *interactionPeerPayload       `json:"interaction,omitempty"`
	ParentID    string                        `json:"parent_session_id"`
	Input       backend.SessionControlRequest `json:"input"`
	Record      sessiondispatch.Record        `json:"dispatch"`
}
type controllerRemoteFailure struct {
	Status  int
	Message string
}

func (e controllerRemoteFailure) Error() string { return e.Message }

func remoteControllerID(origin, id string) string {
	sum := sha256.Sum256([]byte(origin + "\x00" + id))
	return "scr_" + hex.EncodeToString(sum[:16])
}
func (h *Hub) ServeSessionControlAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if h.dispatches == nil || r.Method != http.MethodPost || r.Header.Get("Origin") != "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	body, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 128*1024))
	if e != nil {
		http.Error(w, "invalid body", 400)
		return
	}
	origin, ok := h.authorizeRelayRequest(r, body)
	if !ok {
		http.Error(w, "unauthorized", 401)
		return
	}
	if r.URL.Path == "/api/session-control/v1/interaction" {
		h.serveInteractionPeer(w, r, origin, body)
		return
	}
	grant, e := h.dispatches.Grant(r.Context(), "peer:"+origin)
	if e != nil || !grant.Enabled || !grant.Local {
		http.Error(w, "peer controller access not granted", 403)
		return
	}
	var envelope controllerEnvelope
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || decoder.Decode(&struct{}{}) != io.EOF || envelope.ParentID == "" || len(envelope.ParentID) > 160 {
		http.Error(w, "invalid envelope", 400)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/session-control/v1/")
	var result any
	switch path {
	case "read":
		input := envelope.Input
		if input.Action == "list_sessions" {
			result = h.controllerCatalogPage(grant, input)
		} else if input.Action == "get_session_status" && grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, input.SessionID) {
			result, e = h.controllerStatus(input.SessionID, input.ExpectedThreadID)
		} else {
			e = errors.New("peer_read_forbidden")
		}
	case "dispatch":
		job := envelope.Record
		if job.ID == "" || len(job.ID) > 160 || job.ParentID != envelope.ParentID || job.InstanceID != h.cfg.InstanceID || job.SessionID == "" || job.ThreadID == "" || strings.TrimSpace(job.Content) == "" || len(job.Content) > 32000 || job.IntentHash == "" {
			e = errors.New("remote_dispatch_invalid")
			break
		}
		if !grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, job.SessionID) || (job.Mode != "queue" && job.Mode != "steer") || (job.Mode == "steer" && !grant.Steer) {
			e = errors.New("remote_dispatch_not_granted")
			break
		}
		originalID := job.ID
		// Never trust the caller's digest: bind the persisted idempotency key to
		// the actual immutable instruction and target received over authenticated transport.
		intent, _ := json.Marshal(backend.SessionControlRequest{Action: "dispatch_to_session", InstanceID: job.InstanceID, SessionID: job.SessionID, ExpectedThreadID: job.ThreadID, ExpectedConfigRevision: job.ConfigRevision, Content: job.Content, Mode: job.Mode})
		digest := sha256.Sum256(intent)
		job.IntentHash = hex.EncodeToString(digest[:])
		job.ID = remoteControllerID(origin, originalID)
		job.ParentID = "peer:" + origin + ":" + envelope.ParentID
		job.RequestID = "scjob_" + job.ID
		job.OriginRequestID = originalID
		job.ToolCallID = "remote_dispatch"
		job.State = "prepared"
		job.DeliveryState = "inbound"
		job.Result = ""
		job.Error = ""
		if previous, found, err := h.dispatches.Get(r.Context(), job.ID); err != nil {
			e = err
			break
		} else if found {
			if previous.IntentHash != job.IntentHash {
				e = errors.New("remote_dispatch_intent_conflict")
				break
			}
			result = h.refreshSessionDispatch(r.Context(), previous)
			break
		}
		if _, e = h.dispatches.Create(r.Context(), job); e == nil {
			job, _, e = h.dispatches.Get(r.Context(), job.ID)
			if e == nil && job.IntentHash != hex.EncodeToString(digest[:]) {
				e = errors.New("remote_dispatch_intent_conflict")
			}
			if e == nil {
				result = h.submitLocalSessionDispatch(r.Context(), job)
			}
		}
	case "receipt", "cancel":
		job, found, err := h.dispatches.Get(r.Context(), remoteControllerID(origin, envelope.Record.ID))
		if err != nil || !found || job.ParentID != "peer:"+origin+":"+envelope.ParentID {
			e = errors.New("remote_dispatch_not_found")
			break
		}
		if path == "cancel" {
			result, e = h.cancelSessionDispatch(r.Context(), backend.SessionControlCaller{}, job)
		} else {
			result = h.refreshSessionDispatch(r.Context(), job)
		}
	default:
		e = errors.New("controller_path_unknown")
	}
	w.Header().Set("Content-Type", "application/json")
	if e != nil {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": e.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}
func (h *Hub) controllerRemoteRequest(ctx context.Context, instance, path string, envelope controllerEnvelope, result any) error {
	if path == "receipt" || path == "cancel" {
		envelope.Record = sessiondispatch.Record{ID: envelope.Record.ID}
	}
	peer, ok := h.relayPeers[instance]
	if !ok {
		return errors.New("controller_peer_unconfigured")
	}
	raw, e := json.Marshal(envelope)
	if e != nil {
		return e
	}
	response, e := h.relayRequest(ctx, peer, http.MethodPost, "/api/session-control/v1/"+path, raw)
	if e != nil {
		return errors.New("controller_remote_unreachable")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&failure)
		if failure.Error != "" {
			return controllerRemoteFailure{response.StatusCode, failure.Error}
		}
		return controllerRemoteFailure{response.StatusCode, "controller_remote_not_authorized_or_unsupported"}
	}
	return json.NewDecoder(io.LimitReader(response.Body, 2*1024*1024)).Decode(result)
}
func (h *Hub) remoteControllerRead(ctx context.Context, c backend.SessionControlCaller, input backend.SessionControlRequest) (any, error) {
	var result any
	e := h.controllerRemoteRequest(ctx, input.InstanceID, "read", controllerEnvelope{ParentID: c.Parent.ID, Input: input}, &result)
	return result, e
}
func (h *Hub) submitRemoteSessionDispatch(ctx context.Context, r sessiondispatch.Record) sessiondispatch.Record {
	grant, e := h.dispatches.Grant(ctx, r.ParentID)
	if e != nil || !grant.Allows(h.cfg.InstanceID, r.InstanceID, r.SessionID) {
		r.Error = "controller_grant_revoked_no_resubmit"
		_ = h.dispatches.Put(ctx, r)
		return r
	}
	// Query first after any ambiguous response. Only the exact same ID/payload is ever resent.
	if r.State == "uncertain" || r.State == "pending" {
		var previous sessiondispatch.Record
		if e := h.controllerRemoteRequest(ctx, r.InstanceID, "receipt", controllerEnvelope{ParentID: r.ParentID, Record: r}, &previous); e == nil {
			r.State, r.Result, r.Error = previous.State, previous.Result, previous.Error
			r.ExecutionRequestID, r.ExecutionTurnID = previous.ExecutionRequestID, previous.ExecutionTurnID
			_ = h.dispatches.Put(ctx, r)
			return r
		} else if e.Error() != "remote_dispatch_not_found" {
			// Loss of access is not proof that an earlier write did not execute.
			r.Error = e.Error()
			_ = h.dispatches.Put(ctx, r)
			return r
		}
	}
	var remote sessiondispatch.Record
	e = h.controllerRemoteRequest(ctx, r.InstanceID, "dispatch", controllerEnvelope{ParentID: r.ParentID, Record: r}, &remote)
	if e != nil {
		var refused controllerRemoteFailure
		if r.State != "uncertain" && errors.As(e, &refused) && refused.Status >= 400 && refused.Status < 500 {
			r.State = "rejected"
		} else {
			r.State = "uncertain"
		}
		r.Error = e.Error()
	} else {
		r.State, r.Result, r.Error = remote.State, remote.Result, remote.Error
		r.ExecutionRequestID, r.ExecutionTurnID = remote.ExecutionRequestID, remote.ExecutionTurnID
	}
	_ = h.dispatches.Put(ctx, r)
	return r
}
func (h *Hub) pollRemoteSessionDispatch(ctx context.Context, r sessiondispatch.Record) sessiondispatch.Record {
	if r.DeliveryState == "inbound" {
		return r
	}
	var remote sessiondispatch.Record
	e := h.controllerRemoteRequest(ctx, r.InstanceID, "receipt", controllerEnvelope{ParentID: r.ParentID, Record: r}, &remote)
	if e != nil {
		if r.State == "uncertain" {
			return h.submitRemoteSessionDispatch(ctx, r)
		}
		r.Error = e.Error()
	} else {
		r.State, r.Result, r.Error = remote.State, remote.Result, remote.Error
		r.ExecutionRequestID, r.ExecutionTurnID = remote.ExecutionRequestID, remote.ExecutionTurnID
	}
	_ = h.dispatches.Put(ctx, r)
	return r
}
func (h *Hub) remoteControllerCancel(ctx context.Context, c backend.SessionControlCaller, r sessiondispatch.Record) (any, error) {
	var remote sessiondispatch.Record
	e := h.controllerRemoteRequest(ctx, r.InstanceID, "cancel", controllerEnvelope{ParentID: r.ParentID, Record: r}, &remote)
	if e != nil {
		return nil, e
	}
	r.State, r.Result, r.Error = remote.State, remote.Result, remote.Error
	_ = h.dispatches.Put(ctx, r)
	return r, nil
}
