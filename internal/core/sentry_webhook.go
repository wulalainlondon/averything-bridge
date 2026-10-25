package core

import (
	"io"
	"net/http"
	"strings"
	"time"

	"everything-go/internal/automation"
)

const maxSentryWebhookBytes = 2 * 1024 * 1024

// ServeSentryWebhook accepts Integration Platform callbacks for one configured
// connector account. Provider JSON is verified and normalized before it can
// enter the source-neutral Event Inbox.
func (h *Hub) ServeSentryWebhook(w http.ResponseWriter, r *http.Request) {
	if h.events == nil || h.automation == nil || h.automationManager == nil {
		http.Error(w, "sentry webhook unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	accountID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/hooks/sentry/"), "/")
	if accountID == "" || strings.Contains(accountID, "/") {
		http.Error(w, "unknown Sentry account", http.StatusNotFound)
		return
	}
	account, err := h.automation.GetAccount(r.Context(), accountID)
	if err != nil || !account.Enabled || !account.WebhookEnabled || account.Provider != "sentry" {
		http.Error(w, "unknown Sentry account", http.StatusNotFound)
		return
	}
	secret, err := h.automationManager.ResolveSecret(account.AppSecretRef)
	if err != nil || secret == "" {
		http.Error(w, "sentry webhook unavailable", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSentryWebhookBytes))
	if err != nil {
		http.Error(w, "invalid webhook body", http.StatusBadRequest)
		return
	}
	if !automation.ValidSentryWebhookSignature(body, secret, r.Header.Get("Sentry-Hook-Signature")) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	requestID := strings.TrimSpace(r.Header.Get("Request-ID"))
	if requestID == "" || len(requestID) > 128 {
		http.Error(w, "missing or invalid Request-ID", http.StatusBadRequest)
		return
	}
	input, ignored, err := automation.NormalizeSentryWebhook(account, r.Header.Get("Sentry-Hook-Resource"), body, time.Now().UnixMilli())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if ignored {
		writeWebhookResponse(w, http.StatusAccepted, map[string]any{"status": "ignored"})
		return
	}
	event, deduped, err := h.events.Insert(r.Context(), input)
	if err != nil {
		http.Error(w, "store Sentry event", http.StatusInternalServerError)
		return
	}
	if err := h.publishExternalEvent(event, deduped); err != nil {
		http.Error(w, "publish Sentry event", http.StatusInternalServerError)
		return
	}
	writeWebhookResponse(w, http.StatusAccepted, map[string]any{
		"status": "accepted", "event_id": event.ID, "deduplicated": deduped,
	})
}
