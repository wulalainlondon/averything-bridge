package core

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode"

	"everything-go/internal/eventinbox"
)

const maxAppStoreWebhookBytes = 1024 * 1024

type appStoreWebhookApp struct {
	Slug        string
	DisplayName string
	SecretEnv   string
}

var appStoreWebhookApps = map[string]appStoreWebhookApp{
	"lucky3":    {Slug: "lucky3", DisplayName: "Lucky3", SecretEnv: "APPSTORE_WEBHOOK_SECRET_LUCKY3"},
	"salon":     {Slug: "salon", DisplayName: "職人日誌 Salon", SecretEnv: "APPSTORE_WEBHOOK_SECRET_SALON"},
	"sudokuzen": {Slug: "sudokuzen", DisplayName: "SudokuZen 數獨修行", SecretEnv: "APPSTORE_WEBHOOK_SECRET_SUDOKUZEN"},
}

type appStoreRelationship struct {
	Data struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	} `json:"data"`
}

type appStoreWebhookPayload struct {
	Data struct {
		Type          string                          `json:"type"`
		ID            string                          `json:"id"`
		Version       int64                           `json:"version"`
		Attributes    map[string]any                  `json:"attributes"`
		Relationships map[string]appStoreRelationship `json:"relationships"`
	} `json:"data"`
}

// ServeAppStoreWebhook verifies App Store Connect's x-apple-signature for one
// configured app and maps the provider payload into the canonical Event Inbox.
// The public Funnel paths remain app-specific so one leaked secret cannot
// authenticate deliveries for another app.
func (h *Hub) ServeAppStoreWebhook(w http.ResponseWriter, r *http.Request) {
	if h.events == nil {
		http.Error(w, "event inbox unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	app, ok := appStoreWebhookAppForPath(r.URL.Path)
	if !ok {
		http.Error(w, "unknown App Store webhook", http.StatusNotFound)
		return
	}
	secret := strings.TrimSpace(os.Getenv(app.SecretEnv))
	if secret == "" {
		http.Error(w, "App Store webhook unavailable", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAppStoreWebhookBytes))
	if err != nil {
		http.Error(w, "invalid webhook body", http.StatusBadRequest)
		return
	}
	if !validAppStoreSignature([]byte(secret), body, r.Header.Get("x-apple-signature")) {
		http.Error(w, "invalid App Store signature", http.StatusUnauthorized)
		return
	}
	input, err := normalizeAppStoreDelivery(app, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	event, deduped, err := h.events.Insert(r.Context(), input)
	if err != nil {
		http.Error(w, "store App Store event: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.publishExternalEvent(event, deduped); err != nil {
		http.Error(w, "encode App Store event: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeWebhookResponse(w, http.StatusAccepted, map[string]any{
		"status": "accepted", "event_id": event.ID, "deduplicated": deduped,
	})
}

func appStoreWebhookAppForPath(path string) (appStoreWebhookApp, bool) {
	trimmed := strings.Trim(strings.TrimPrefix(path, "/hooks/apple-app-store"), "/")
	if trimmed == "" {
		trimmed = "lucky3"
	}
	app, ok := appStoreWebhookApps[trimmed]
	return app, ok
}

func validAppStoreSignature(secret, body []byte, raw string) bool {
	const prefix = "hmacsha256="
	raw = strings.ToLower(strings.TrimSpace(raw))
	if !strings.HasPrefix(raw, prefix) {
		return false
	}
	provided, err := hex.DecodeString(strings.TrimPrefix(raw, prefix))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return hmac.Equal(provided, mac.Sum(nil))
}

func normalizeAppStoreDelivery(app appStoreWebhookApp, body []byte) (eventinbox.Input, error) {
	var payload appStoreWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return eventinbox.Input{}, errors.New("invalid App Store webhook payload")
	}
	eventType := strings.TrimSpace(payload.Data.Type)
	if eventType == "" {
		return eventinbox.Input{}, errors.New("App Store webhook data.type is required")
	}
	eventID := strings.TrimSpace(payload.Data.ID)
	if eventID == "" {
		digest := sha256.Sum256(append([]byte(app.Slug+"\x00"), body...))
		eventID = "payload:" + hex.EncodeToString(digest[:16])
	}
	eventDigest := sha256.Sum256([]byte(app.Slug + "\x00" + eventID))
	eventKey := "apple_event:" + hex.EncodeToString(eventDigest[:16])
	attributes := payload.Data.Attributes
	oldState := firstAppStoreString(attributes, "oldValue", "oldExternalBuildState", "oldInternalBuildState", "oldState")
	newState := firstAppStoreString(attributes, "newValue", "newExternalBuildState", "newInternalBuildState", "newState")
	timestamp := firstAppStoreString(attributes, "timestamp", "createdDate")
	occurredAt := time.Now().UnixMilli()
	if parsed, err := time.Parse(time.RFC3339Nano, timestamp); err == nil {
		occurredAt = parsed.UnixMilli()
	}
	instance := payload.Data.Relationships["instance"].Data
	severity := appStoreStateSeverity(newState)
	kind := "appstore." + appStoreKind(eventType)
	title := app.DisplayName + " · " + appStoreHumanize(eventType)
	bodyText := "App Store Connect event received."
	if isAppStorePing(eventType) {
		kind = "appstore.webhook_ping"
		severity = "success"
		title = app.DisplayName + " App Store webhook connected"
		bodyText = "Apple can now deliver App Store Connect events to Bridge."
	} else if oldState != "" || newState != "" {
		if oldState == "" {
			oldState = "unknown"
		}
		if newState == "" {
			newState = "unknown"
		}
		title = fmt.Sprintf("%s · %s → %s", app.DisplayName, appStoreHumanize(oldState), appStoreHumanize(newState))
		bodyText = appStoreHumanize(eventType)
	}
	data, _ := json.Marshal(map[string]any{
		"app": app.Slug, "display_name": app.DisplayName,
		"provider_event_id": payload.Data.ID, "provider_event_type": eventType,
		"provider_version": payload.Data.Version, "old_state": oldState,
		"new_state": newState, "timestamp": timestamp,
		"instance_type": instance.Type, "instance_id": instance.ID,
	})
	return eventinbox.Input{
		Source: "apple.appstore." + app.Slug, EventKey: eventKey, Kind: kind,
		Severity: severity, Title: clipUTF8Bytes(title, 240), Body: bodyText,
		URL: "https://appstoreconnect.apple.com/apps", Data: data, OccurredAt: occurredAt,
	}, nil
}

func firstAppStoreString(attributes map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := attributes[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func isAppStorePing(eventType string) bool {
	lower := strings.ToLower(eventType)
	return strings.Contains(lower, "ping")
}

func appStoreStateSeverity(state string) string {
	normalized := strings.ToUpper(strings.TrimSpace(state))
	switch {
	case normalized == "":
		return "info"
	case strings.Contains(normalized, "REJECT"), strings.Contains(normalized, "FAIL"),
		strings.Contains(normalized, "INVALID"), strings.Contains(normalized, "ERROR"):
		return "error"
	case strings.Contains(normalized, "READY_FOR_DISTRIBUTION"), normalized == "READY_FOR_SALE",
		normalized == "COMPLETE", normalized == "ACCEPTED", normalized == "TESTING":
		return "success"
	case strings.Contains(normalized, "ACTION"), strings.Contains(normalized, "MISSING"),
		strings.Contains(normalized, "EXPIRED"), strings.Contains(normalized, "CANCEL"),
		strings.Contains(normalized, "DEVELOPER_RELEASE"), strings.Contains(normalized, "COMPLIANCE"):
		return "warning"
	default:
		return "info"
	}
}

func appStoreKind(value string) string {
	var out strings.Builder
	for index, current := range value {
		if unicode.IsUpper(current) {
			if index > 0 {
				out.WriteByte('_')
			}
			out.WriteRune(unicode.ToLower(current))
			continue
		}
		if unicode.IsLetter(current) || unicode.IsDigit(current) || current == '_' || current == '-' || current == '.' {
			out.WriteRune(unicode.ToLower(current))
		} else {
			out.WriteByte('_')
		}
	}
	return strings.Trim(out.String(), "_")
}

func appStoreHumanize(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	if value == strings.ToUpper(value) {
		value = strings.ToLower(value)
	} else {
		value = appStoreKind(value)
	}
	value = strings.NewReplacer("_", " ", "-", " ", ".", " ").Replace(value)
	return strings.Join(strings.Fields(value), " ")
}
