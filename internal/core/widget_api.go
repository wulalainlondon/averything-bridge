package core

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"everything-go/internal/identity"
	"everything-go/internal/widgetaccess"
)

func (h *Hub) widgetCapabilities() (*widgetaccess.Signer, error) {
	h.widgetMu.Lock()
	defer h.widgetMu.Unlock()
	if h.widgetSigner != nil {
		return h.widgetSigner, nil
	}
	signer, err := widgetaccess.New(h.cfg.DataDir, h.cfg.InstanceID)
	if err == nil {
		h.widgetSigner = signer
	}
	return signer, err
}

func (h *Hub) widgetCredentialValid(deviceID, digest string) bool {
	for _, binding := range h.pairing.DeviceBindings() {
		if binding.DeviceID == deviceID && subtle.ConstantTimeCompare([]byte(widgetaccess.CredentialDigest(binding.Token)), []byte(digest)) == 1 {
			return true
		}
	}
	if configured := strings.TrimSpace(os.Getenv("BRIDGE_AUTH_TOKEN")); configured != "" {
		return subtle.ConstantTimeCompare([]byte(widgetaccess.CredentialDigest(configured)), []byte(digest)) == 1
	}
	return false
}

// ServeWidgetAPI exposes only runtime metadata. Its grant cannot authenticate
// WebSockets, file APIs, notification replies, or any command submission route.
func (h *Hub) ServeWidgetAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	reject := func(code int, message string) {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
	}
	grantPath := r.URL.Path == "/api/widgets/v1/access"
	snapshotPath := r.URL.Path == "/api/widgets/v1/snapshot"
	if !grantPath && !snapshotPath {
		reject(404, "not_found")
		return
	}
	if (grantPath && r.Method != http.MethodPost) || (snapshotPath && r.Method != http.MethodGet) {
		reject(405, "method_not_allowed")
		return
	}
	signer, err := h.widgetCapabilities()
	if err != nil {
		reject(503, "widget_access_unavailable")
		return
	}
	if grantPath {
		var input struct {
			DeviceID      string `json:"deviceId"`
			PreviousToken string `json:"previousToken"`
		}
		decoder := json.NewDecoder(io.LimitReader(r.Body, 8192))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil || input.DeviceID == "" || len(input.DeviceID) > 160 {
			reject(400, "invalid_request")
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			reject(401, "device_auth_required")
			return
		}
		credential := strings.TrimPrefix(auth, "Bearer ")
		if !h.widgetCredentialValid(input.DeviceID, widgetaccess.CredentialDigest(credential)) {
			reject(401, "device_auth_required")
			return
		}
		token, claims := signer.Issue(input.DeviceID, credential, input.PreviousToken, time.Now())
		if token == "" {
			reject(503, "widget_access_unavailable")
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"authority": h.cfg.InstanceID, "readToken": token, "expiresAt": claims.ExpiresAt})
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Widget ") {
		reject(401, "widget_grant_required")
		return
	}
	claims, err := signer.Parse(strings.TrimPrefix(auth, "Widget "), time.Now())
	if err != nil || !h.widgetCredentialValid(claims.DeviceID, claims.CredentialDigest) {
		reject(401, "invalid_widget_grant")
		return
	}
	rows := h.registry.List()
	names := map[string]string{}
	ids := make([]string, 0, len(rows))
	for _, session := range rows {
		snapshot := session.Snapshot()
		ids = append(ids, snapshot.ID)
		names[snapshot.ID] = truncateGraphemes(snapshot.Name, 100)
	}
	views := h.runtimes.Snapshot(claims.DeviceID, ids)
	sort.Slice(views, func(i, j int) bool { return views[i].UpdatedAt > views[j].UpdatedAt })
	if len(views) > 40 {
		views = views[:40]
	}
	tasks := make([]map[string]any, 0, len(views))
	completions := []map[string]any{}
	for _, view := range views {
		key, _ := identity.MakeSessionKey(h.cfg.InstanceID, view.SessionID)
		tasks = append(tasks, map[string]any{"id": key, "sessionId": view.SessionID, "requestId": view.ActiveRequestID,
			"revision": view.Revision, "name": names[view.SessionID], "phase": view.Phase, "stage": view.Stage,
			"startedAt": view.ActiveStartedAt, "updatedAt": view.UpdatedAt, "completedAt": view.CompletedAt,
			"unread": view.Unread, "resultPending": false})
		for _, terminal := range h.runtimes.WidgetTerminals(view.SessionID, claims.Since) {
			completions = append(completions, map[string]any{"id": key, "sessionId": view.SessionID, "requestId": terminal.RequestID,
				"revision": terminal.Revision, "name": names[view.SessionID], "phase": terminal.Status, "stage": terminal.Status,
				"startedAt": 0, "updatedAt": terminal.At, "completedAt": terminal.At, "unread": 1, "resultPending": false})
		}
	}
	sort.Slice(completions, func(i, j int) bool { return completions[i]["updatedAt"].(int64) > completions[j]["updatedAt"].(int64) })
	if len(completions) > 64 {
		completions = completions[:64]
	}
	snapshot := map[string]any{"schemaVersion": 1, "updatedAt": time.Now().UnixMilli(), "completions": completions,
		"bridges": []map[string]any{{"id": h.cfg.InstanceID, "name": h.cfg.InstanceName, "connected": true, "authoritative": true, "tasks": tasks, "quotas": []any{}}}}
	_ = json.NewEncoder(w).Encode(snapshot)
}
