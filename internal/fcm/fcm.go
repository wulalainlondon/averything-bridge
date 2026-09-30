// Package fcm sends Firebase Cloud Messaging push notifications via the HTTP v1
// API, authenticated with a Google service account (the project's
// serviceAccountKey.json). It mirrors the Python bridge's notify_fcm: a
// "task done" push when a turn completes, with a markdown-cleaned summary.
//
// Using the raw HTTP v1 endpoint + golang.org/x/oauth2/google keeps the
// dependency footprint small (no full firebase-admin equivalent) while reusing
// the project's existing credentials, per the user's instruction to share the
// bridge's service token.
package fcm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"everything-go/internal/identity"
	"everything-go/internal/protocol"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const messagingScope = "https://www.googleapis.com/auth/firebase.messaging"

const (
	registryVersion = 1
	legacyDeviceID  = "legacy"
	maxDevices      = 64
)

type deviceRegistration struct {
	Token       string                            `json:"token"`
	Platform    string                            `json:"platform,omitempty"`
	UpdatedAt   int64                             `json:"updated_at"`
	Preferences *protocol.NotificationPreferences `json:"notification_preferences,omitempty"`
}

type tokenRegistry struct {
	Version int                           `json:"version"`
	Devices map[string]deviceRegistration `json:"devices"`
}

type target struct {
	deviceID string
	token    string
}

type ReplyAction struct {
	URL         string
	FallbackURL string
	Capability  string
	ExpiresAt   int64
}

// Notifier holds the OAuth2 token source and a per-device token registry.
type Notifier struct {
	projectID    string
	tokenSource  oauth2.TokenSource
	registryPath string
	endpoint     string
	http         *http.Client

	mu      sync.RWMutex
	devices map[string]deviceRegistration

	statusMu          sync.Mutex
	statusLastSent    map[string]time.Time
	statusLastPhase   map[string]string
	statusPending     map[string]v1message
	statusTimers      map[string]*time.Timer
	statusMinInterval time.Duration
}

// New loads the service account from serviceAccountPath and the persisted
// per-device token registry from registryPath (if present). Returns (nil, err) if the credentials are
// missing or invalid — the caller treats a nil Notifier as "FCM disabled".
func New(serviceAccountPath, registryPath string) (*Notifier, error) {
	data, err := os.ReadFile(serviceAccountPath)
	if err != nil {
		return nil, fmt.Errorf("read service account: %w", err)
	}
	return NewFromBytes(data, registryPath)
}

// NewFromBytes parses private runtime credentials already read by the caller.
// Service-account private keys must never be compiled into an application.
func NewFromBytes(data []byte, registryPath string) (*Notifier, error) {
	var sa struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(data, &sa); err != nil || sa.ProjectID == "" {
		return nil, fmt.Errorf("service account missing project_id")
	}
	creds, err := google.CredentialsFromJSON(context.Background(), data, messagingScope)
	if err != nil {
		return nil, fmt.Errorf("credentials: %w", err)
	}
	n := &Notifier{
		projectID: sa.ProjectID,
		// Fan-out calls Token concurrently. ReuseTokenSource both caches the
		// OAuth token and serializes refreshes around the underlying source.
		tokenSource:       oauth2.ReuseTokenSource(nil, creds.TokenSource),
		registryPath:      registryPath,
		endpoint:          "https://fcm.googleapis.com/v1/projects/" + sa.ProjectID + "/messages:send",
		http:              &http.Client{Timeout: 15 * time.Second},
		devices:           make(map[string]deviceRegistration),
		statusLastSent:    make(map[string]time.Time),
		statusLastPhase:   make(map[string]string),
		statusPending:     make(map[string]v1message),
		statusTimers:      make(map[string]*time.Timer),
		statusMinInterval: 15 * time.Second,
	}
	n.loadRegistry()
	return n, nil
}

// SetToken registers/updates one stable device's token and persists the whole
// registry. Reusing a token under a new device id moves it instead of creating
// duplicate pushes (e.g. after an app reinstall changes the local device id).
func (n *Notifier) SetToken(deviceID, token string, platform ...string) {
	devicePlatform := ""
	if len(platform) > 0 {
		devicePlatform = platform[0]
	}
	n.RegisterDevice(deviceID, token, devicePlatform, nil)
}

// RegisterDevice installs token and preferences atomically: a newly registered
// iPhone must never briefly receive an unredacted legacy push before opt-out.
func (n *Notifier) RegisterDevice(deviceID, token, platform string, preferences *protocol.NotificationPreferences) {
	deviceID, token = strings.TrimSpace(deviceID), strings.TrimSpace(token)
	devicePlatform := strings.ToLower(strings.TrimSpace(platform))
	if devicePlatform != "android" && devicePlatform != "ios" {
		devicePlatform = ""
	}
	if deviceID == "" || (token == "" && preferences == nil) {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.devices == nil {
		n.devices = make(map[string]deviceRegistration)
	}
	for id, registration := range n.devices {
		if token != "" && id != deviceID && registration.Token == token {
			delete(n.devices, id)
		}
	}
	current, exists := n.devices[deviceID]
	unchangedPreferences := preferences == nil || (current.Preferences != nil && *current.Preferences == *preferences)
	if exists && (token == "" || current.Token == token) && (devicePlatform == "" || current.Platform == devicePlatform) && unchangedPreferences {
		return
	}
	if token != "" {
		current.Token = token
	}
	if devicePlatform != "" {
		current.Platform = devicePlatform
	}
	if preferences != nil {
		copy := *preferences
		current.Preferences = &copy
	}
	current.UpdatedAt = time.Now().UnixMilli()
	n.devices[deviceID] = current
	n.enforceCapLocked()
	n.persistLocked()
	log.Printf("[fcm] device registration updated device=%s preferences=%t", deviceID, current.Preferences != nil)
}

func (n *Notifier) targets() []target {
	return n.targetsForPlatform("")
}

func (n *Notifier) targetsForPlatform(platform string) []target {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]target, 0, len(n.devices))
	seen := make(map[string]struct{}, len(n.devices))
	for deviceID, registration := range n.devices {
		if registration.Token == "" {
			continue
		}
		if platform != "" && registration.Platform != platform {
			continue
		}
		if _, duplicate := seen[registration.Token]; duplicate {
			continue
		}
		seen[registration.Token] = struct{}{}
		out = append(out, target{deviceID: deviceID, token: registration.Token})
	}
	return out
}

func (n *Notifier) targetsForUnspecifiedPlatform() []target {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]target, 0, len(n.devices))
	seen := make(map[string]struct{}, len(n.devices))
	for deviceID, registration := range n.devices {
		if registration.Token == "" || registration.Platform != "" {
			continue
		}
		if _, duplicate := seen[registration.Token]; duplicate {
			continue
		}
		seen[registration.Token] = struct{}{}
		out = append(out, target{deviceID: deviceID, token: registration.Token})
	}
	return out
}

// NotifyTaskDone sends the turn-complete push. No-op if no device token yet.
func (n *Notifier) NotifyTaskDone(sessionName, lastText, sessionID string) {
	n.NotifyTaskDoneWithReply("", sessionName, lastText, sessionID, "", ReplyAction{})
}

func (n *Notifier) NotifyTaskDoneWithReply(instanceID, sessionName, lastText, sessionID, requestID string, reply ReplyAction) {
	n.NotifyTaskDoneWithAuthority(instanceID, "", sessionName, lastText, sessionID, requestID, reply)
}

// TerminalProjection makes a completion independently orderable when iOS did
// not receive intermediate lifecycle pushes while the host was suspended.
type TerminalProjection struct {
	Revision                                uint64
	UpdatedAt, ActiveStartedAt, CompletedAt int64
}

func (n *Notifier) NotifyTaskDoneWithAuthority(instanceID, instanceName, sessionName, lastText, sessionID, requestID string, reply ReplyAction, terminal ...TerminalProjection) {
	summary := summarize(lastText)
	sessionKey, _ := identity.MakeSessionKey(instanceID, sessionID)
	data := map[string]string{
		"type": "task_done", "session_id": sessionID, "session_name": sessionName,
		"title": "✓ " + sessionName, "body": summary, "authority_instance_id": instanceID,
		"request_id": requestID, "session_key": sessionKey, "schema_version": fcmPayloadSchemaVersion,
		"event_id": "task_done:" + sessionKey + ":" + requestID,
	}
	if len(terminal) > 0 && terminal[0].Revision > 0 {
		t := terminal[0]
		data["revision"] = fmt.Sprint(t.Revision)
		data["updated_at"] = fmt.Sprint(t.UpdatedAt)
		data["active_started_at"] = fmt.Sprint(t.ActiveStartedAt)
		data["completed_at"] = fmt.Sprint(t.CompletedAt)
		data["stage"] = "completed"
	}
	if instanceName != "" {
		data["authority_name"] = instanceName
	}
	addReplyAction(data, reply)
	android := v1message{}
	android.Message.Data = data
	android.Message.Android = &v1androidConfig{Priority: "high", TTL: "86400s", CollapseKey: "task-done-" + instanceID + "-" + sessionID}
	n.sendTargets(android, "task_done", n.targetsForPlatform("android"))

	legacy := v1message{}
	legacy.Message.Notification = &v1notification{Title: "✓ " + sessionName, Body: summary}
	legacy.Message.Data = data
	ios := legacy
	ios.Message.APNS = visibleAPNSConfig(
		"BRIDGE_SESSION_REPLY",
		"bridge-session-"+shortStableID(instanceID, sessionID),
		// Use the status notification's collapse ID so completion replaces the
		// stale running/waiting card for this same session on iOS.
		"bridge-status-"+shortStableID(instanceID, sessionID),
		legacy.Message.Notification,
	)
	n.sendTargets(ios, "task_done", n.targetsForPlatform("ios"))
	n.sendTargets(legacy, "task_done", n.targetsForUnspecifiedPlatform())
}

// NotifyFilePush mirrors notify_fcm_file_push.
func (n *Notifier) NotifyFilePush(fileID, filename string) {
	msg := v1message{}
	msg.Message.Notification = &v1notification{Title: "📎 新檔案", Body: filename}
	msg.Message.Data = map[string]string{
		"type": "file_push", "schema_version": fcmPayloadSchemaVersion, "file_id": fileID, "filename": filename, "deep_link": "bridge://inbox",
		"event_id": "file_push:" + fileID,
	}
	n.sendAll(msg, "file_push")
}

// NotifyTunnelURL mirrors push_registry.send_tunnel_fcm_once.
// Sends a data-only (silent) push so the app can update its tunnel URL.
func (n *Notifier) NotifyTunnelURL(wsURL, instanceID string) {
	// cloudflared reports HTTPS origins; clients consume WebSocket endpoints.
	// Normalize at the push boundary so native receivers can require WSS.
	if strings.HasPrefix(wsURL, "https://") {
		wsURL = "wss://" + strings.TrimPrefix(wsURL, "https://")
	}
	msg := v1message{}
	msg.Message.Data = map[string]string{
		"type": "tunnel_url", "schema_version": fcmPayloadSchemaVersion, "url": wsURL, "instance_id": instanceID,
		"event_id":  "tunnel_url:" + instanceID,
		"issued_at": fmt.Sprint(time.Now().UnixMilli()),
	}
	msg.Message.APNS = &v1apnsConfig{
		Headers: map[string]string{
			"apns-push-type":   "background",
			"apns-priority":    "5",
			"apns-collapse-id": "tunnel-" + shortStableID(instanceID),
			"apns-expiration":  fmt.Sprint(time.Now().Add(24 * time.Hour).Unix()),
		},
		Payload: v1apnsPayload{APS: v1aps{ContentAvailable: 1}},
	}
	n.sendAll(msg, "tunnel_url")
}

// NotifyFeedNew mirrors feed_ops.notify_fcm_feed_new.
func (n *Notifier) NotifyFeedNew(feedID, title string) {
	msg := v1message{}
	msg.Message.Notification = &v1notification{Title: "新文章", Body: title}
	msg.Message.Data = map[string]string{
		"type": "feed_new", "schema_version": fcmPayloadSchemaVersion, "feed_id": feedID, "title": title, "deep_link": "bridge://feed",
		"event_id": "feed_new:" + feedID,
	}
	n.sendAll(msg, "feed_new")
}

func (n *Notifier) NotifyWorkAttention(instanceID, workItemID, title string, revision uint64, kind string) {
	msg := v1message{}
	msg.Message.Notification = &v1notification{Title: "工作需要你的注意", Body: title}
	msg.Message.Data = map[string]string{
		"type": "work_attention", "schema_version": fcmPayloadSchemaVersion, "authority_instance_id": instanceID,
		"work_item_id": workItemID, "activity_revision": fmt.Sprint(revision),
		"kind": kind, "title": title, "deep_link": "bridge://inbox/work/" + workItemID,
		"event_id": "work_attention:" + instanceID + ":" + workItemID + ":" + fmt.Sprint(revision) + ":" + kind,
	}
	n.sendAll(msg, "work_attention")
}

// NotifySessionStatus delivers a compact, revisioned lifecycle projection for
// Android's single ongoing Bridge status card. It is deliberately data-only:
// the native receiver owns rendering and can merge work from multiple Bridge
// instances without producing one system notification per Session.
//
// Frequent tool/thinking transitions are coalesced per Session. Waiting and
// terminal transitions bypass the interval so a request for human attention or
// removal of a stale card is never delayed.
func (n *Notifier) NotifySessionStatus(instanceID, sessionID, sessionName, phase, stage, stageMessage string,
	revision uint64, updatedAt, activeStartedAt int64, activeRequestID string, queueLength int, reply ReplyAction) {
	n.NotifySessionStatusWithAuthority(instanceID, "", sessionID, sessionName, phase, stage, stageMessage,
		revision, updatedAt, activeStartedAt, activeRequestID, queueLength, reply)
}

func (n *Notifier) NotifySessionStatusWithAuthority(instanceID, instanceName, sessionID, sessionName, phase, stage, stageMessage string,
	revision uint64, updatedAt, activeStartedAt int64, activeRequestID string, queueLength int, reply ReplyAction) {
	if instanceID == "" || sessionID == "" || revision == 0 {
		return
	}
	sessionKey, _ := identity.MakeSessionKey(instanceID, sessionID)
	msg := v1message{}
	msg.Message.Data = map[string]string{
		"type": "session_status", "authority_instance_id": instanceID,
		"session_id": sessionID, "session_key": sessionKey, "session_name": sessionName,
		"schema_version": fcmPayloadSchemaVersion,
		"phase":          phase, "stage": stage, "stage_message": summarizeStatus(stageMessage),
		"revision": fmt.Sprint(revision), "updated_at": fmt.Sprint(updatedAt),
		"active_started_at": fmt.Sprint(activeStartedAt), "active_request_id": activeRequestID,
		"queue_length": fmt.Sprint(queueLength),
		"deep_link":    "bridge://chat?session_id=" + sessionID,
		"event_id":     "session_status:" + sessionKey + ":" + fmt.Sprint(revision),
	}
	if instanceName != "" {
		msg.Message.Data["authority_name"] = instanceName
	}
	addReplyAction(msg.Message.Data, reply)
	msg.Message.Android = &v1androidConfig{
		Priority: "high", TTL: "300s", CollapseKey: "session-status-" + instanceID + "-" + sessionID,
	}
	n.enqueueSessionStatus(instanceID+"\x00"+sessionID, phase, msg, phase == "waiting" || !activeSessionPhase(phase))
}

func addReplyAction(data map[string]string, reply ReplyAction) {
	if reply.URL == "" || reply.Capability == "" || reply.ExpiresAt <= 0 {
		return
	}
	data["reply_url"] = reply.URL
	if reply.FallbackURL != "" && reply.FallbackURL != reply.URL {
		data["reply_fallback_url"] = reply.FallbackURL
	}
	data["reply_capability"] = reply.Capability
	data["reply_expires_at"] = fmt.Sprint(reply.ExpiresAt)
}

func activeSessionPhase(phase string) bool {
	switch phase {
	case "queued", "running", "stopping", "waiting":
		return true
	default:
		return false
	}
}

func summarizeStatus(value string) string {
	value = strings.TrimSpace(strings.Join(strings.Fields(value), " "))
	runes := []rune(value)
	if len(runes) > 80 {
		value = string(runes[:80]) + "…"
	}
	return value
}

func (n *Notifier) enqueueSessionStatus(key, phase string, msg v1message, immediate bool) {
	n.statusMu.Lock()
	n.ensureStatusStateLocked()
	now := time.Now()
	interval := n.statusMinInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	last := n.statusLastSent[key]
	phaseChanged := n.statusLastPhase[key] != "" && n.statusLastPhase[key] != phase
	if immediate || phaseChanged || last.IsZero() || now.Sub(last) >= interval {
		if timer := n.statusTimers[key]; timer != nil {
			timer.Stop()
			delete(n.statusTimers, key)
		}
		delete(n.statusPending, key)
		n.statusLastSent[key] = now
		n.statusLastPhase[key] = phase
		n.statusMu.Unlock()
		n.sendSessionStatus(msg)
		return
	}
	n.statusPending[key] = msg
	if n.statusTimers[key] == nil {
		delay := interval - now.Sub(last)
		n.statusTimers[key] = time.AfterFunc(delay, func() { n.flushSessionStatus(key) })
	}
	n.statusMu.Unlock()
}

func (n *Notifier) ensureStatusStateLocked() {
	if n.statusLastSent == nil {
		n.statusLastSent = make(map[string]time.Time)
	}
	if n.statusLastPhase == nil {
		n.statusLastPhase = make(map[string]string)
	}
	if n.statusPending == nil {
		n.statusPending = make(map[string]v1message)
	}
	if n.statusTimers == nil {
		n.statusTimers = make(map[string]*time.Timer)
	}
}

func (n *Notifier) flushSessionStatus(key string) {
	n.statusMu.Lock()
	n.ensureStatusStateLocked()
	msg, ok := n.statusPending[key]
	delete(n.statusPending, key)
	delete(n.statusTimers, key)
	if ok {
		n.statusLastSent[key] = time.Now()
		n.statusLastPhase[key] = msg.Message.Data["phase"]
	}
	n.statusMu.Unlock()
	if ok {
		n.sendSessionStatus(msg)
	}
}

func (n *Notifier) sendSessionStatus(msg v1message) {
	n.sendTargets(msg, "session_status", n.targetsForPlatform("android"))
	if !shouldSurfaceIOSSessionStatus(msg.Message.Data["phase"], msg.Message.Data["stage"]) {
		return
	}
	title := msg.Message.Data["session_name"]
	body := iosSessionStatusBody(msg.Message.Data["phase"], msg.Message.Data["stage"], msg.Message.Data["stage_message"])
	alert := &v1notification{Title: title, Body: body}
	ios := msg
	ios.Message.Android = nil
	ios.Message.Notification = alert
	ios.Message.APNS = visibleAPNSConfig(
		"BRIDGE_SESSION_REPLY",
		"bridge-session-"+shortStableID(msg.Message.Data["authority_instance_id"], msg.Message.Data["session_id"]),
		"bridge-status-"+shortStableID(msg.Message.Data["authority_instance_id"], msg.Message.Data["session_id"]),
		alert,
	)
	n.sendTargets(ios, "session_status", n.targetsForPlatform("ios"))
}

// NotifyExternalEvent surfaces a durable Event Inbox record. FCM is only a
// wake-up hint; the app always reconciles canonical content from the Bridge.
func (n *Notifier) NotifyExternalEvent(instanceID, eventID, source, kind, severity, title, body string) {
	msg := v1message{}
	notificationBody := summarize(body)
	if notificationBody == "" {
		notificationBody = source
	}
	msg.Message.Notification = &v1notification{Title: title, Body: notificationBody}
	msg.Message.Data = map[string]string{
		"type": "external_event", "schema_version": fcmPayloadSchemaVersion, "event_id": eventID, "source": source,
		"kind": kind, "severity": severity, "title": title,
		"authority_instance_id": instanceID,
		"deep_link":             "bridge://inbox/events/" + eventID,
	}
	n.sendAll(msg, "external_event")
}

type v1message struct {
	Message struct {
		Token        string            `json:"token"`
		Notification *v1notification   `json:"notification,omitempty"`
		Data         map[string]string `json:"data"`
		Android      *v1androidConfig  `json:"android,omitempty"`
		APNS         *v1apnsConfig     `json:"apns,omitempty"`
	} `json:"message"`
}

type v1notification struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

type v1androidConfig struct {
	CollapseKey string `json:"collapse_key,omitempty"`
	Priority    string `json:"priority,omitempty"`
	TTL         string `json:"ttl,omitempty"`
}

type v1apnsConfig struct {
	Headers map[string]string `json:"headers,omitempty"`
	Payload v1apnsPayload     `json:"payload"`
}

type v1apnsPayload struct {
	APS v1aps `json:"aps"`
}

type v1aps struct {
	Alert             *v1notification `json:"alert,omitempty"`
	Category          string          `json:"category,omitempty"`
	ThreadID          string          `json:"thread-id,omitempty"`
	ContentAvailable  int             `json:"content-available,omitempty"`
	InterruptionLevel string          `json:"interruption-level,omitempty"`
}

func visibleAPNSConfig(category, threadID, collapseID string, alert *v1notification) *v1apnsConfig {
	return &v1apnsConfig{
		Headers: map[string]string{
			"apns-priority":    "10",
			"apns-expiration":  fmt.Sprint(time.Now().Add(24 * time.Hour).Unix()),
			"apns-collapse-id": collapseID,
		},
		Payload: v1apnsPayload{APS: v1aps{
			Alert: alert, Category: category, ThreadID: threadID,
			ContentAvailable: 1, InterruptionLevel: "active",
		}},
	}
}

func shortStableID(parts ...string) string {
	joined := strings.Join(parts, "\x00")
	sum := sha256.Sum256([]byte(joined))
	return hex.EncodeToString(sum[:8])
}

func shouldSurfaceIOSSessionStatus(phase, stage string) bool {
	switch phase {
	case "waiting", "stopping", "failed":
		return true
	case "running":
		// One visible status per request. Fine-grained thinking/tool progress is
		// still delivered over WebSocket and rendered inside the app; repeatedly
		// alerting for every stage would turn iOS Notification Center into a log.
		return stage == "" || stage == "running" || stage == "preparing"
	default:
		return false
	}
}

func iosSessionStatusBody(phase, stage, message string) string {
	detail := summarizeStatus(message)
	var label string
	switch phase {
	case "waiting":
		label = "需要你的回覆"
	case "stopping":
		label = "正在停止"
	case "failed":
		label = "執行失敗"
	default:
		label = "正在處理"
	}
	if detail != "" {
		return label + " · " + detail
	}
	return label
}

// sendAll fans a notification out concurrently so a slow or invalid device
// cannot delay the remaining devices.
func (n *Notifier) sendAll(msg v1message, kind string) {
	n.sendTargets(msg, kind, n.targets())
}

func (n *Notifier) sendTargets(msg v1message, kind string, targets []target) {
	if len(targets) == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(targets))
	for _, dst := range targets {
		dst := dst
		go func() {
			defer wg.Done()
			copy := msg
			copy.Message.Token = dst.token
			n.send(copy, kind, dst)
		}()
	}
	wg.Wait()
}

// send POSTs one device's message with 3 attempts + exponential backoff,
// matching the Python retry policy. A fatal response removes only the token
// that failed; other device registrations remain intact.
func (n *Notifier) send(msg v1message, kind string, dst target) {
	for attempt := 0; attempt < 3; attempt++ {
		// Re-read preferences for each actual send/retry, not when a status
		// first entered the coalescing buffer. Never mutate the shared payload.
		filtered, allowed := n.messageForDevice(msg, kind, dst)
		if !allowed {
			return
		}
		body, _ := json.Marshal(filtered)
		tok, err := n.tokenSource.Token()
		if err != nil {
			log.Printf("[fcm] oauth token error: %v", err)
			return
		}
		req, _ := http.NewRequest(http.MethodPost, n.endpoint, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := n.http.Do(req)
		if err == nil {
			respBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				log.Printf("[fcm] %s notification sent device=%s", kind, dst.deviceID)
				return
			}
			if tokenFatal(resp.StatusCode, respBody) {
				log.Printf("[fcm] device token invalid device=%s (%d) — clearing; resp=%s", dst.deviceID, resp.StatusCode, truncate(string(respBody), 300))
				n.invalidate(dst)
				return
			}
			err = fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(respBody), 200))
		}
		if attempt < 2 {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		log.Printf("[fcm] %s send failed device=%s after 3 attempts: %v", kind, dst.deviceID, err)
	}
}

func (n *Notifier) invalidate(dst target) {
	n.mu.Lock()
	current, ok := n.devices[dst.deviceID]
	if ok && current.Token == dst.token {
		// A refreshed token must not resurrect notifications the user disabled.
		current.Token = ""
		n.devices[dst.deviceID] = current
		n.persistLocked()
	}
	n.mu.Unlock()
}

func (n *Notifier) loadRegistry() {
	if n.registryPath == "" {
		return
	}
	data, err := os.ReadFile(n.registryPath)
	if err == nil {
		var registry tokenRegistry
		if json.Unmarshal(data, &registry) == nil && registry.Devices != nil {
			n.devices = registry.Devices
			before := len(n.devices)
			n.enforceCapLocked()
			if len(n.devices) != before {
				n.persistLocked()
			}
			return
		}
		// Backward compatibility when the constructor still points at the old
		// plain-text file during a rolling upgrade.
		if tok := strings.TrimSpace(string(data)); tok != "" && !strings.HasPrefix(tok, "{") {
			n.devices[legacyDeviceID] = deviceRegistration{Token: tok, UpdatedAt: time.Now().UnixMilli()}
			n.persistLocked()
			return
		}
	}
	legacyPath := filepath.Join(filepath.Dir(n.registryPath), "fcm_token.txt")
	if tok, legacyErr := os.ReadFile(legacyPath); legacyErr == nil {
		if token := strings.TrimSpace(string(tok)); token != "" {
			n.devices[legacyDeviceID] = deviceRegistration{Token: token, UpdatedAt: time.Now().UnixMilli()}
			n.persistLocked()
		}
	}
}

func (n *Notifier) persistLocked() {
	if n.registryPath == "" || os.MkdirAll(filepath.Dir(n.registryPath), 0o700) != nil {
		return
	}
	data, err := json.Marshal(tokenRegistry{Version: registryVersion, Devices: n.devices})
	if err != nil {
		return
	}
	tmp := n.registryPath + ".tmp"
	if os.WriteFile(tmp, data, 0o600) == nil {
		_ = os.Rename(tmp, n.registryPath)
	}
}

func (n *Notifier) enforceCapLocked() {
	for len(n.devices) > maxDevices {
		oldestID := ""
		var oldestAt int64
		for deviceID, registration := range n.devices {
			if oldestID == "" || registration.UpdatedAt < oldestAt {
				oldestID = deviceID
				oldestAt = registration.UpdatedAt
			}
		}
		delete(n.devices, oldestID)
	}
}

// tokenFatal reports whether the FCM error means the device token is permanently
// invalid (so it should be cleared rather than retried).
func tokenFatal(status int, body []byte) bool {
	if status == 404 {
		return true
	}
	s := string(body)
	return strings.Contains(s, "UNREGISTERED") || strings.Contains(s, "INVALID_ARGUMENT")
}

var (
	mdMarks = regexp.MustCompile("[*`#_~>]+")
	mdLinks = regexp.MustCompile(`\[([^\]]+)\]\([^)]+\)`)
	wsRun   = regexp.MustCompile(`\s+`)
	sentEnd = regexp.MustCompile(`[。！？!?.]\s+`)
)

// summarize mirrors notify_fcm's body shaping: take the last non-empty
// paragraph, strip markdown, keep the first sentence, cap at 120 runes.
func summarize(lastText string) string {
	clean := func(s string) string {
		s = mdMarks.ReplaceAllString(s, "")
		s = mdLinks.ReplaceAllString(s, "$1")
		return strings.TrimSpace(wsRun.ReplaceAllString(s, " "))
	}
	var paras []string
	for _, p := range strings.Split(lastText, "\n\n") {
		if strings.TrimSpace(p) != "" {
			paras = append(paras, p)
		}
	}
	source := lastText
	if len(paras) > 0 {
		source = paras[len(paras)-1]
	}
	summary := clean(source)
	if loc := sentEnd.FindStringIndex(summary); loc != nil {
		summary = strings.TrimSpace(summary[:loc[1]])
	}
	if summary == "" {
		summary = clean(lastText)
	}
	if r := []rune(summary); len(r) > 120 {
		summary = strings.TrimRight(string(r[:120]), " ") + "…"
	}
	return summary
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
