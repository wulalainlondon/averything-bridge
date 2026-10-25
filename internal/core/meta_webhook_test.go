package core

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"everything-go/internal/automation"
)

func setupMetaWebhook(t *testing.T) (*Hub, *automation.Store) {
	t.Helper()
	h, _ := newTestHub(t)
	attachEventStore(t, h)
	store := attachAutomationStore(t, h, t.TempDir())
	t.Setenv("META_TEST_APP_SECRET", "app-secret")
	t.Setenv("META_TEST_VERIFY_TOKEN", "verify-secret")
	_, _, err := store.UpsertAccount(t.Context(), automation.Account{ID: "judge", Provider: "meta.facebook",
		ExternalAccountID: "page-id", DisplayName: "Judge", CredentialRef: "env:META_TEST_PAGE_TOKEN",
		AppSecretRef: "env:META_TEST_APP_SECRET", VerifyTokenRef: "env:META_TEST_VERIFY_TOKEN",
		Enabled: true, WebhookEnabled: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return h, store
}

func TestMetaWebhookChallengeSignatureNormalizationAndDedupe(t *testing.T) {
	h, _ := setupMetaWebhook(t)
	challenge := httptest.NewRequest(http.MethodGet, "/hooks/meta/judge?hub.mode=subscribe&hub.verify_token=verify-secret&hub.challenge=12345", nil)
	response := httptest.NewRecorder()
	h.ServeMetaWebhook(response, challenge)
	if response.Code != http.StatusOK || response.Body.String() != "12345" {
		t.Fatalf("challenge status=%d body=%q", response.Code, response.Body.String())
	}
	body := []byte(`{"object":"page","entry":[{"id":"page-id","time":1800000000,"changes":[{"field":"feed","value":{"item":"comment","verb":"add","comment_id":"comment-1","post_id":"post-1","message":"Please review this"}}],"messaging":[{"sender":{"id":"user-1"},"recipient":{"id":"page-id"},"timestamp":1800000000123,"message":{"mid":"mid-1","text":"Hello","attachments":[{"type":"image","payload":{"url":"https://cdn.example/proof.jpg"}}]}}]}]}`)
	post := func(signature string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/hooks/meta/judge", strings.NewReader(string(body)))
		request.Header.Set("X-Hub-Signature-256", signature)
		result := httptest.NewRecorder()
		h.ServeMetaWebhook(result, request)
		return result
	}
	if invalid := post("sha256=00"); invalid.Code != http.StatusUnauthorized {
		t.Fatalf("invalid signature status=%d", invalid.Code)
	}
	first := post(metaSignature("app-secret", body))
	if first.Code != http.StatusAccepted || !strings.Contains(first.Body.String(), `"events":2`) {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	second := post(metaSignature("app-secret", body))
	if second.Code != http.StatusAccepted || !strings.Contains(second.Body.String(), `"deduplicated":2`) {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
	snapshot, err := h.events.Snapshot(t.Context(), "phone", 20)
	if err != nil || len(snapshot.Items) != 2 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	if snapshot.Items[0].Kind != "message.received" || snapshot.Items[1].Kind != "comment.created" ||
		strings.Contains(string(snapshot.Items[0].Data), "app-secret") {
		t.Fatalf("items=%+v", snapshot.Items)
	}
	if len(snapshot.Items[0].Attachments) != 1 || snapshot.Items[0].Attachments[0].Status != "pending" ||
		!strings.Contains(string(snapshot.Items[0].Data), `"conversation_id":"user-1"`) {
		t.Fatalf("message attachment/conversation not normalized: %+v", snapshot.Items[0])
	}
}

func TestMetaWebhookRejectsDisabledOrUnknownAccount(t *testing.T) {
	h, store := setupMetaWebhook(t)
	_, _, err := store.UpsertAccount(t.Context(), automation.Account{ID: "disabled", Provider: "meta.facebook",
		ExternalAccountID: "page-2", DisplayName: "Disabled", CredentialRef: "env:TOKEN", Enabled: true, WebhookEnabled: false}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/hooks/meta/disabled", "/hooks/meta/unknown"} {
		response := httptest.NewRecorder()
		h.ServeMetaWebhook(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
		if response.Code != http.StatusNotFound {
			t.Fatalf("path=%s status=%d", path, response.Code)
		}
	}
}
