package core

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"everything-go/internal/eventinbox"
)

const maxExternalEventRequestBytes = 64 * 1024

var eventNameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)

type externalEventRequest struct {
	SchemaVersion int             `json:"schema_version,omitempty"`
	Source        string          `json:"source"`
	EventKey      string          `json:"event_key,omitempty"`
	Kind          string          `json:"kind"`
	Severity      string          `json:"severity,omitempty"`
	Title         string          `json:"title"`
	Body          string          `json:"body,omitempty"`
	URL           string          `json:"url,omitempty"`
	Data          json.RawMessage `json:"data,omitempty"`
	OccurredAt    string          `json:"occurred_at,omitempty"`
	ExpiresAt     string          `json:"expires_at,omitempty"`
}

// ServeEventAPI accepts only a canonical, inert event envelope. Provider
// adapters normalize their payloads before this boundary; no event body is
// executed, interpolated into a shell command, or submitted to an agent.
func (h *Hub) ServeEventAPI(w http.ResponseWriter, r *http.Request) {
	if h.events == nil {
		http.Error(w, "event inbox unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.eventIngressAuthorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxExternalEventRequestBytes))
	decoder.DisallowUnknownFields()
	var input externalEventRequest
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "invalid event: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "invalid event: request body must contain exactly one JSON object", http.StatusBadRequest)
		return
	}
	if input.SchemaVersion != 0 && input.SchemaVersion != 1 {
		http.Error(w, "unsupported schema_version", http.StatusBadRequest)
		return
	}
	eventKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if eventKey == "" {
		eventKey = strings.TrimSpace(input.EventKey)
	}
	input.Source = strings.TrimSpace(input.Source)
	input.Kind = strings.TrimSpace(input.Kind)
	input.Severity = strings.ToLower(strings.TrimSpace(input.Severity))
	input.Title = strings.TrimSpace(input.Title)
	if input.Severity == "" {
		input.Severity = "info"
	}
	if err := validateExternalEvent(input, eventKey); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	occurredAt, err := parseEventTimestamp(input.OccurredAt)
	if err != nil {
		http.Error(w, "invalid occurred_at: "+err.Error(), http.StatusBadRequest)
		return
	}
	expiresAt, err := parseEventTimestamp(input.ExpiresAt)
	if err != nil {
		http.Error(w, "invalid expires_at: "+err.Error(), http.StatusBadRequest)
		return
	}
	if occurredAt > 0 && expiresAt > 0 && expiresAt <= occurredAt {
		http.Error(w, "expires_at must be later than occurred_at", http.StatusBadRequest)
		return
	}

	event, deduped, err := h.events.Insert(r.Context(), eventinbox.Input{
		Source: input.Source, EventKey: eventKey, Kind: input.Kind, Severity: input.Severity,
		Title: input.Title, Body: input.Body, URL: input.URL, Data: input.Data,
		OccurredAt: occurredAt, ExpiresAt: expiresAt,
	})
	if err != nil {
		http.Error(w, "store event: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.publishExternalEvent(event, deduped); err != nil {
		http.Error(w, "encode event: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"event_id": event.ID, "sequence": event.Sequence, "deduplicated": deduped, "status": "accepted",
	})
}

func (h *Hub) publishExternalEvent(event eventinbox.Event, deduped bool) error {
	if deduped {
		return nil
	}
	h.materializeExternalEvent(&event)
	if err := h.broadcastOnline(h.client.ExternalEventUpsert(eventinbox.View{Event: event})); err != nil {
		return err
	}
	if h.fcm != nil {
		go h.fcm.NotifyExternalEvent(h.cfg.InstanceID, event.ID, event.Source, event.Kind, event.Severity, event.Title, event.Body)
	}
	h.routeExternalEvent(event)
	return nil
}

func (h *Hub) publishExternalEventUpdate(event eventinbox.Event) error {
	h.materializeExternalEvent(&event)
	return h.broadcastOnline(h.client.ExternalEventUpsert(eventinbox.View{Event: event}))
}

func (h *Hub) materializeExternalEvent(event *eventinbox.Event) {
	for index := range event.Attachments {
		attachment := &event.Attachments[index]
		if attachment.Status == "available" && attachment.LocalPath != "" {
			attachment.URL = h.mediaScan.LocalURL(attachment.LocalPath)
		}
	}
}

func (h *Hub) eventIngressAuthorized(r *http.Request) bool {
	provided := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if expected := strings.TrimSpace(os.Getenv("EVENT_INGRESS_TOKEN")); expected != "" {
		return len(provided) == len(expected) && subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1
	}
	if loopbackWorkAPIRequest(r) {
		return true
	}
	return h.authValid(provided)
}

func validateExternalEvent(input externalEventRequest, eventKey string) error {
	if !eventNameRE.MatchString(input.Source) {
		return errors.New("source must match [a-zA-Z0-9_.-] and be at most 64 characters")
	}
	if !eventNameRE.MatchString(input.Kind) {
		return errors.New("kind must match [a-zA-Z0-9_.-] and be at most 64 characters")
	}
	if eventKey == "" || len(eventKey) > 128 {
		return errors.New("Idempotency-Key or event_key is required and must be at most 128 characters")
	}
	if input.Title == "" || len(input.Title) > 240 {
		return errors.New("title is required and must be at most 240 bytes")
	}
	if len(input.Body) > 32*1024 {
		return errors.New("body must be at most 32 KB")
	}
	switch input.Severity {
	case "info", "success", "warning", "error", "critical":
	default:
		return errors.New("severity must be info, success, warning, error, or critical")
	}
	if input.URL != "" {
		parsed, err := url.Parse(input.URL)
		if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
			return errors.New("url must be an absolute http or https URL")
		}
	}
	return nil
}

func parseEventTimestamp(raw string) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, err
	}
	return parsed.UnixMilli(), nil
}
