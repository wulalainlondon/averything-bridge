package core

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func githubSignature(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func postGitHubWebhook(t *testing.T, h *Hub, secret, eventType, deliveryID, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/hooks/github", strings.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", githubSignature(secret, body))
	req.Header.Set("X-GitHub-Event", eventType)
	req.Header.Set("X-GitHub-Delivery", deliveryID)
	response := httptest.NewRecorder()
	h.ServeGitHubWebhook(response, req)
	return response
}

func TestGitHubWebhookRejectsInvalidSignature(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "github-secret")
	h, _ := newTestHub(t)
	attachEventStore(t, h)
	req := httptest.NewRequest(http.MethodPost, "/hooks/github", strings.NewReader(`{"zen":"hi"}`))
	req.Header.Set("X-Hub-Signature-256", "sha256=00")
	req.Header.Set("X-GitHub-Event", "ping")
	req.Header.Set("X-GitHub-Delivery", "delivery-1")
	response := httptest.NewRecorder()
	h.ServeGitHubWebhook(response, req)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestGitHubSignatureMatchesOfficialVector(t *testing.T) {
	const expected = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17"
	if !validGitHubSignature([]byte("It's a Secret to Everybody"), []byte("Hello, World!"), expected) {
		t.Fatal("official GitHub HMAC-SHA256 vector did not validate")
	}
}

func TestGitHubPingPersistsAndDeduplicates(t *testing.T) {
	const secret = "github-secret"
	t.Setenv("GITHUB_WEBHOOK_SECRET", secret)
	h, _ := newTestHub(t)
	store := attachEventStore(t, h)
	body := `{"zen":"Keep it logically awesome.","hook_id":42,"repository":{"full_name":"wulalainlondon/everything-go"},"sender":{"login":"wulalainlondon"}}`
	first := postGitHubWebhook(t, h, secret, "ping", "delivery-ping", body)
	second := postGitHubWebhook(t, h, secret, "ping", "delivery-ping", body)
	if first.Code != http.StatusAccepted || !strings.Contains(first.Body.String(), `"deduplicated":false`) {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	if second.Code != http.StatusAccepted || !strings.Contains(second.Body.String(), `"deduplicated":true`) {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
	snapshot, err := store.Snapshot(context.Background(), "phone", 0)
	if err != nil || len(snapshot.Items) != 1 || snapshot.Items[0].Kind != "webhook.ping" {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
}

func TestGitHubWorkflowRunCompletedNormalizesCanonicalEvent(t *testing.T) {
	const secret = "github-secret"
	t.Setenv("GITHUB_WEBHOOK_SECRET", secret)
	h, _ := newTestHub(t)
	store := attachEventStore(t, h)
	body := `{"action":"completed","workflow_run":{"id":99,"name":"release","run_number":12,"run_attempt":2,"event":"push","status":"completed","conclusion":"failure","head_branch":"main","head_sha":"1234567890abcdef","html_url":"https://github.com/wulalainlondon/everything-go/actions/runs/99","actor":{"login":"wulala"},"updated_at":"2026-08-29T00:00:00Z"},"repository":{"full_name":"wulalainlondon/everything-go"},"sender":{"login":"wulala"}}`
	response := postGitHubWebhook(t, h, secret, "workflow_run", "delivery-run", body)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	snapshot, err := store.Snapshot(context.Background(), "phone", 0)
	if err != nil || len(snapshot.Items) != 1 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	item := snapshot.Items[0]
	if item.Source != "github.actions" || item.Kind != "workflow_run.completed" || item.Severity != "error" || !strings.Contains(item.Title, "release #12 failed") || item.URL == "" {
		t.Fatalf("item=%+v", item)
	}
	redelivery := postGitHubWebhook(t, h, secret, "workflow_run", "different-delivery-id", body)
	if redelivery.Code != http.StatusAccepted || !strings.Contains(redelivery.Body.String(), `"deduplicated":true`) {
		t.Fatalf("redelivery status=%d body=%s", redelivery.Code, redelivery.Body.String())
	}
}

func TestGitHubWebhookIgnoresUnsupportedAndNonCompletedEvents(t *testing.T) {
	const secret = "github-secret"
	t.Setenv("GITHUB_WEBHOOK_SECRET", secret)
	h, _ := newTestHub(t)
	store := attachEventStore(t, h)
	unsupported := postGitHubWebhook(t, h, secret, "issues", "delivery-issues", `{}`)
	requested := postGitHubWebhook(t, h, secret, "workflow_run", "delivery-requested", `{"action":"requested"}`)
	if unsupported.Code != http.StatusAccepted || requested.Code != http.StatusAccepted {
		t.Fatalf("unsupported=%d requested=%d", unsupported.Code, requested.Code)
	}
	snapshot, err := store.Snapshot(context.Background(), "phone", 0)
	if err != nil || len(snapshot.Items) != 0 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
}
