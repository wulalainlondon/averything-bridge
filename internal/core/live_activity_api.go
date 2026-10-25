package core

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"everything-go/internal/liveactivity"
	"everything-go/internal/widgetaccess"
)

// ServeLiveActivityAPI accepts enrollment only from the already paired host
// app. Widget read grants cannot enroll, send, or run commands.
func (h *Hub) ServeLiveActivityAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	reject := func(status int, code string) {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": code})
	}
	if r.Method != http.MethodPost {
		reject(405, "method_not_allowed")
		return
	}
	register := r.URL.Path == "/api/live-activities/v1/register"
	if !register && r.URL.Path != "/api/live-activities/v1/unregister" {
		reject(404, "not_found")
		return
	}
	var input struct {
		DeviceID   string `json:"deviceId"`
		ActivityID string `json:"activityId"`
		Token      string `json:"token,omitempty"`
		Authority  string `json:"authority,omitempty"`
		SessionID  string `json:"sessionId,omitempty"`
		RequestID  string `json:"requestId,omitempty"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.DeviceID == "" || len(input.DeviceID) > 160 || input.ActivityID == "" || len(input.ActivityID) > 160 {
		reject(400, "invalid_request")
		return
	}
	authorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, "Bearer ") || !h.pairedPushDevices()[input.DeviceID] {
		reject(401, "device_auth_required")
		return
	}
	credentialDigest := widgetaccess.CredentialDigest(strings.TrimPrefix(authorization, "Bearer "))
	if !h.widgetCredentialValid(input.DeviceID, credentialDigest) {
		reject(401, "device_auth_required")
		return
	}
	if h.fcm == nil {
		reject(503, "push_unavailable")
		return
	}
	if !register {
		h.fcm.UnregisterLiveActivity(input.DeviceID, input.ActivityID)
		_ = json.NewEncoder(w).Encode(map[string]bool{"removed": true})
		return
	}
	if input.Authority != h.cfg.InstanceID || input.RequestID == "" || len(input.RequestID) > 160 || !liveactivity.ValidToken(input.Token) {
		reject(400, "invalid_request")
		return
	}
	if _, exists := h.registry.Get(input.SessionID); !exists {
		reject(404, "session_not_found")
		return
	}
	views := h.runtimes.Snapshot(input.DeviceID, []string{input.SessionID})
	if len(views) != 1 || views[0].ActiveRequestID != input.RequestID {
		reject(409, "request_changed")
		return
	}
	view := views[0]
	state := liveactivity.State{Phase: view.Phase, Stage: view.Stage, Revision: view.Revision, UpdatedAt: view.UpdatedAt, StartedAt: view.ActiveStartedAt, ResultPending: view.HistoryReconcile}
	if !state.Valid() || state.Terminal() {
		reject(409, "task_not_active")
		return
	}
	expires, err := h.fcm.RegisterLiveActivity(r.Context(), input.DeviceID, input.ActivityID, input.Authority, input.SessionID, input.RequestID, input.Token, credentialDigest, state)
	if err != nil {
		reject(503, "push_unavailable")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"authority": h.cfg.InstanceID, "activityId": input.ActivityID, "registered": true, "expiresAt": expires * 1000})
}
