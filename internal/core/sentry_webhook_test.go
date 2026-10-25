package core

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"everything-go/internal/automation"
)

func setupSentryWebhook(t *testing.T) *Hub {
	t.Helper()
	h, _ := newTestHub(t)
	attachEventStore(t, h)
	store := attachAutomationStore(t, h, t.TempDir())
	t.Setenv("SENTRY_TEST_SECRET", "client-secret")
	_, _, err := store.UpsertAccount(t.Context(), automation.Account{
		ID: "primary", Provider: "sentry", ExternalAccountID: "acme/mobile", DisplayName: "Mobile",
		CredentialRef: "env:SENTRY_TEST_TOKEN", AppSecretRef: "env:SENTRY_TEST_SECRET",
		Enabled: true, WebhookEnabled: true, PollEnabled: false,
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestSentryWebhookVerifiesNormalizesAndDeduplicates(t *testing.T) {
	h := setupSentryWebhook(t)
	body := []byte(`{"action":"created","data":{"issue":{"id":"9","shortId":"APP-9","title":"failure for user@example.com","culprit":"worker 192.168.1.20","count":"1","level":"error","status":"unresolved","substatus":"new","firstSeen":"2026-08-30T01:00:00Z","lastSeen":"2026-08-30T01:00:00Z","web_url":"https://acme.sentry.io/issues/9/","project":{"slug":"mobile"}}}}`)
	post := func(signature string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/hooks/sentry/primary", strings.NewReader(string(body)))
		request.Header.Set("Request-ID", "delivery-1")
		request.Header.Set("Sentry-Hook-Resource", "issue")
		request.Header.Set("Sentry-Hook-Signature", signature)
		response := httptest.NewRecorder()
		h.ServeSentryWebhook(response, request)
		return response
	}
	if invalid := post("00"); invalid.Code != http.StatusUnauthorized {
		t.Fatalf("invalid signature status=%d", invalid.Code)
	}
	first := post(sentryWebhookSignature("client-secret", body))
	if first.Code != http.StatusAccepted || !strings.Contains(first.Body.String(), `"deduplicated":false`) {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	second := post(sentryWebhookSignature("client-secret", body))
	if second.Code != http.StatusAccepted || !strings.Contains(second.Body.String(), `"deduplicated":true`) {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
	snapshot, err := h.events.Snapshot(t.Context(), "phone", 20)
	if err != nil || len(snapshot.Items) != 1 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	item := snapshot.Items[0]
	if item.Source != "sentry.primary" || item.Kind != "issue.created" || strings.Contains(item.Title, "user@example.com") || strings.Contains(item.Body, "192.168.1.20") {
		t.Fatalf("item=%+v", item)
	}
}

func TestSentryWebhookRejectsMissingDeliveryAndIgnoresOtherProject(t *testing.T) {
	h := setupSentryWebhook(t)
	body := []byte(`{"action":"created","data":{"issue":{"id":"9","title":"other","count":"1","status":"unresolved","project":{"slug":"backend"}}}}`)
	request := httptest.NewRequest(http.MethodPost, "/hooks/sentry/primary", strings.NewReader(string(body)))
	request.Header.Set("Sentry-Hook-Resource", "issue")
	request.Header.Set("Sentry-Hook-Signature", sentryWebhookSignature("client-secret", body))
	response := httptest.NewRecorder()
	h.ServeSentryWebhook(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("missing request id status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodPost, "/hooks/sentry/primary", strings.NewReader(string(body)))
	request.Header.Set("Request-ID", "delivery-2")
	request.Header.Set("Sentry-Hook-Resource", "issue")
	request.Header.Set("Sentry-Hook-Signature", sentryWebhookSignature("client-secret", body))
	response = httptest.NewRecorder()
	h.ServeSentryWebhook(response, request)
	if response.Code != http.StatusAccepted || !strings.Contains(response.Body.String(), `"status":"ignored"`) {
		t.Fatalf("other project status=%d body=%s", response.Code, response.Body.String())
	}
}

func sentryWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
