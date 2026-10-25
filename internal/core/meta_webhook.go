package core

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"everything-go/internal/eventinbox"
)

const maxMetaWebhookBytes = 2 * 1024 * 1024

var metaKindSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

type metaWebhookPayload struct {
	Object string `json:"object"`
	Entry  []struct {
		ID      string `json:"id"`
		Time    int64  `json:"time"`
		Changes []struct {
			Field string          `json:"field"`
			Value json.RawMessage `json:"value"`
		} `json:"changes"`
		Messaging []json.RawMessage `json:"messaging"`
	} `json:"entry"`
}

func (h *Hub) ServeMetaWebhook(w http.ResponseWriter, r *http.Request) {
	if h.events == nil || h.automation == nil || h.automationManager == nil {
		http.Error(w, "meta webhook unavailable", http.StatusServiceUnavailable)
		return
	}
	accountID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/hooks/meta/"), "/")
	if accountID == "" || strings.Contains(accountID, "/") {
		http.Error(w, "unknown Meta account", http.StatusNotFound)
		return
	}
	account, err := h.automation.GetAccount(r.Context(), accountID)
	if err != nil || !account.Enabled || !account.WebhookEnabled || !strings.HasPrefix(account.Provider, "meta.") {
		http.Error(w, "unknown Meta account", http.StatusNotFound)
		return
	}
	if r.Method == http.MethodGet {
		h.serveMetaChallenge(w, r, account.VerifyTokenRef)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	secret, err := h.automationManager.ResolveSecret(account.AppSecretRef)
	if err != nil || secret == "" {
		http.Error(w, "meta webhook unavailable", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxMetaWebhookBytes))
	if err != nil {
		http.Error(w, "invalid webhook body", http.StatusBadRequest)
		return
	}
	if !validMetaSignature(body, secret, r.Header.Get("X-Hub-Signature-256")) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	inputs, err := normalizeMetaWebhook(account.ID, account.Provider, account.DisplayName, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	accepted, deduplicated := 0, 0
	for _, input := range inputs {
		event, deduped, insertErr := h.events.InsertWithAttachments(r.Context(), input, input.Attachments)
		if insertErr != nil {
			http.Error(w, "store Meta event", http.StatusInternalServerError)
			return
		}
		if deduped {
			deduplicated++
		} else if publishErr := h.publishExternalEvent(event, false); publishErr != nil {
			http.Error(w, "publish Meta event", http.StatusInternalServerError)
			return
		}
		accepted++
	}
	writeWebhookResponse(w, http.StatusAccepted, map[string]any{"status": "accepted", "events": accepted, "deduplicated": deduplicated})
}

func (h *Hub) serveMetaChallenge(w http.ResponseWriter, r *http.Request, verifyRef string) {
	if r.URL.Query().Get("hub.mode") != "subscribe" {
		http.Error(w, "invalid challenge", http.StatusBadRequest)
		return
	}
	expected, err := h.automationManager.ResolveSecret(verifyRef)
	provided := r.URL.Query().Get("hub.verify_token")
	if err != nil || expected == "" || len(expected) != len(provided) || subtle.ConstantTimeCompare([]byte(expected), []byte(provided)) != 1 {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	challenge := r.URL.Query().Get("hub.challenge")
	if challenge == "" || len(challenge) > 1024 {
		http.Error(w, "invalid challenge", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = io.WriteString(w, challenge)
}

func validMetaSignature(body []byte, secret, header string) bool {
	provided := strings.TrimPrefix(strings.TrimSpace(header), "sha256=")
	decoded, err := hex.DecodeString(provided)
	if err != nil || len(decoded) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return hmac.Equal(decoded, mac.Sum(nil))
}

func normalizeMetaWebhook(accountID, provider, displayName string, body []byte) ([]eventinbox.Input, error) {
	var payload metaWebhookPayload
	if json.Unmarshal(body, &payload) != nil || payload.Object == "" || len(payload.Entry) == 0 {
		return nil, errors.New("invalid Meta webhook payload")
	}
	var inputs []eventinbox.Input
	for _, entry := range payload.Entry {
		occurredAt := entry.Time * 1000
		for index, change := range entry.Changes {
			input, err := normalizeMetaChange(accountID, provider, displayName, entry.ID, occurredAt, index, change.Field, change.Value)
			if err == nil {
				inputs = append(inputs, input)
			}
		}
		for index, raw := range entry.Messaging {
			input, err := normalizeMetaMessage(accountID, provider, displayName, entry.ID, occurredAt, index, raw)
			if err == nil {
				inputs = append(inputs, input)
			}
		}
	}
	if len(inputs) == 0 {
		return nil, errors.New("Meta webhook contains no supported events")
	}
	return inputs, nil
}

func normalizeMetaChange(accountID, provider, displayName, entryID string, occurredAt int64, index int, field string, raw json.RawMessage) (eventinbox.Input, error) {
	var value map[string]any
	if json.Unmarshal(raw, &value) != nil {
		return eventinbox.Input{}, errors.New("invalid change")
	}
	field = strings.ToLower(strings.Trim(metaKindSanitizer.ReplaceAllString(field, "_"), "._-"))
	if field == "" {
		field = "change"
	}
	kind := "webhook." + field
	title := displayName + " Meta update"
	body := stringValue(value, "text", "message")
	objectID := stringValue(value, "id", "comment_id", "post_id", "media_id")
	verb := strings.ToLower(stringValue(value, "verb"))
	item := strings.ToLower(stringValue(value, "item"))
	switch {
	case field == "comments" || field == "live_comments" || item == "comment":
		if verb == "" {
			verb = "created"
		}
		switch verb {
		case "add", "create":
			verb = "created"
		case "edit":
			verb = "updated"
		case "remove":
			verb = "deleted"
		}
		kind = "comment." + metaKindSanitizer.ReplaceAllString(verb, "_")
		title = displayName + " received a Meta comment"
	case field == "mentions":
		kind, title = "mention.created", displayName+" was mentioned"
	case field == "feed":
		if verb == "" {
			verb = "updated"
		}
		kind, title = "feed."+metaKindSanitizer.ReplaceAllString(verb, "_"), displayName+" feed changed"
	}
	data := safeMetaData(value)
	keyPart := objectID
	if keyPart == "" {
		sum := sha256.Sum256(raw)
		keyPart = hex.EncodeToString(sum[:12])
	}
	eventKey := fmt.Sprintf("entry:%s:%s:%s:%d:%d", entryID, field, keyPart, occurredAt, index)
	return eventinbox.Input{Source: provider + "." + accountID, EventKey: eventKey, Kind: kind,
		Severity: "info", Title: title, Body: body, Data: data, OccurredAt: occurredAt}, nil
}

func normalizeMetaMessage(accountID, provider, displayName, entryID string, occurredAt int64, index int, raw json.RawMessage) (eventinbox.Input, error) {
	var value struct {
		Sender struct {
			ID string `json:"id"`
		} `json:"sender"`
		Recipient struct {
			ID string `json:"id"`
		} `json:"recipient"`
		Timestamp int64 `json:"timestamp"`
		Message   struct {
			MID         string `json:"mid"`
			Text        string `json:"text"`
			Attachments []struct {
				Type    string `json:"type"`
				Payload struct {
					URL string `json:"url"`
				} `json:"payload"`
			} `json:"attachments"`
		} `json:"message"`
	}
	if json.Unmarshal(raw, &value) != nil || value.Message.MID == "" {
		return eventinbox.Input{}, errors.New("unsupported message")
	}
	if value.Timestamp > 0 {
		occurredAt = value.Timestamp
	}
	data, _ := json.Marshal(map[string]string{"sender_id": value.Sender.ID, "recipient_id": value.Recipient.ID,
		"message_id": value.Message.MID, "conversation_id": value.Sender.ID})
	attachments := make([]eventinbox.AttachmentInput, 0, len(value.Message.Attachments))
	for index, attachment := range value.Message.Attachments {
		if strings.TrimSpace(attachment.Payload.URL) == "" {
			continue
		}
		attachments = append(attachments, eventinbox.AttachmentInput{ExternalID: fmt.Sprintf("%s:%d", value.Message.MID, index),
			SourceURL: attachment.Payload.URL, MIMEType: metaAttachmentMIME(attachment.Type),
			DisplayName: fmt.Sprintf("Messenger %s %d", attachment.Type, index+1), Ordinal: index})
	}
	return eventinbox.Input{Source: provider + "." + accountID, EventKey: "message:" + value.Message.MID,
		Kind: "message.received", Severity: "info", Title: displayName + " received a message",
		Body: value.Message.Text, Data: data, OccurredAt: occurredAt, Attachments: attachments}, nil
}

func metaAttachmentMIME(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "image":
		return "image/jpeg"
	case "video":
		return "video/mp4"
	case "audio":
		return "audio/mpeg"
	default:
		return "application/octet-stream"
	}
}

func stringValue(value map[string]any, keys ...string) string {
	for _, key := range keys {
		if raw, ok := value[key]; ok {
			if text, ok := raw.(string); ok && strings.TrimSpace(text) != "" {
				return strings.TrimSpace(text)
			}
		}
	}
	return ""
}

func safeMetaData(value map[string]any) json.RawMessage {
	allowed := map[string]any{}
	for _, key := range []string{"id", "comment_id", "post_id", "media_id", "item", "verb", "from", "media"} {
		if raw, ok := value[key]; ok {
			allowed[key] = raw
		}
	}
	data, _ := json.Marshal(allowed)
	if len(data) > 16*1024 {
		return json.RawMessage(`{}`)
	}
	return data
}

func metaSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
