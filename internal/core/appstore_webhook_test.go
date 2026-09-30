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

func appStoreSignature(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(body))
	return "hmacsha256=" + hex.EncodeToString(mac.Sum(nil))
}

func postAppStoreWebhook(t *testing.T, h *Hub, path, secret, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("x-apple-signature", appStoreSignature(secret, body))
	response := httptest.NewRecorder()
	h.ServeAppStoreWebhook(response, req)
	return response
}

func TestAppStoreSignatureMatchesOfficialVector(t *testing.T) {
	const expected = "hmacsha256=7f062172b01cb00b53ca068614674a3d982a34062a0f5d37687d5e3377e54657"
	if !validAppStoreSignature([]byte("This is my secret"), []byte("Hello, World!"), expected) {
		t.Fatal("official App Store HMAC-SHA256 vector did not validate")
	}
}

func TestAppStoreWebhookRejectsInvalidSignatureAndUnknownApp(t *testing.T) {
	t.Setenv("APPSTORE_WEBHOOK_SECRET_LUCKY3", "lucky-secret")
	h, _ := newTestHub(t)
	attachEventStore(t, h)
	req := httptest.NewRequest(http.MethodPost, "/hooks/apple-app-store/lucky3", strings.NewReader(`{}`))
	req.Header.Set("x-apple-signature", "hmacsha256=00")
	response := httptest.NewRecorder()
	h.ServeAppStoreWebhook(response, req)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("invalid signature status=%d", response.Code)
	}
	unknown := postAppStoreWebhook(t, h, "/hooks/apple-app-store/unknown", "lucky-secret", `{}`)
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown app status=%d", unknown.Code)
	}
}

func TestAppStoreStatusPersistsPerAppAndDeduplicates(t *testing.T) {
	const secret = "salon-secret"
	t.Setenv("APPSTORE_WEBHOOK_SECRET_SALON", secret)
	h, _ := newTestHub(t)
	store := attachEventStore(t, h)
	body := `{"data":{"type":"appStoreVersionAppVersionStateUpdated","id":"apple-event-1","version":1,"attributes":{"oldValue":"WAITING_FOR_REVIEW","newValue":"READY_FOR_DISTRIBUTION","timestamp":"2026-08-29T01:00:00.000Z"},"relationships":{"instance":{"data":{"type":"appStoreVersions","id":"version-1"}}}}}`
	first := postAppStoreWebhook(t, h, "/hooks/apple-app-store/salon", secret, body)
	second := postAppStoreWebhook(t, h, "/hooks/apple-app-store/salon", secret, body)
	if first.Code != http.StatusAccepted || !strings.Contains(first.Body.String(), `"deduplicated":false`) {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	if second.Code != http.StatusAccepted || !strings.Contains(second.Body.String(), `"deduplicated":true`) {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
	snapshot, err := store.Snapshot(context.Background(), "phone", 0)
	if err != nil || len(snapshot.Items) != 1 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	item := snapshot.Items[0]
	if item.Source != "apple.appstore.salon" || item.Severity != "success" || !strings.Contains(item.Title, "waiting for review → ready for distribution") || !strings.Contains(item.Kind, "app_store_version") {
		t.Fatalf("item=%+v", item)
	}
}

func TestAppStoreBuildVariantAndPingNormalize(t *testing.T) {
	t.Setenv("APPSTORE_WEBHOOK_SECRET_LUCKY3", "lucky-secret")
	t.Setenv("APPSTORE_WEBHOOK_SECRET_SUDOKUZEN", "sudoku-secret")
	h, _ := newTestHub(t)
	store := attachEventStore(t, h)
	build := `{"data":{"type":"buildBetaDetailExternalBuildStateUpdated","id":"build-event","attributes":{"oldExternalBuildState":"WAITING_FOR_REVIEW","newExternalBuildState":"REJECTED","timestamp":"2026-08-29T01:00:00Z"}}}`
	ping := `{"data":{"type":"webhookPings","relationships":{"webhook":{"data":{"type":"webhooks","id":"hook-1"}}}}}`
	if response := postAppStoreWebhook(t, h, "/hooks/apple-app-store", "lucky-secret", build); response.Code != http.StatusAccepted {
		t.Fatalf("legacy lucky route status=%d body=%s", response.Code, response.Body.String())
	}
	if response := postAppStoreWebhook(t, h, "/hooks/apple-app-store/sudokuzen", "sudoku-secret", ping); response.Code != http.StatusAccepted {
		t.Fatalf("ping status=%d body=%s", response.Code, response.Body.String())
	}
	snapshot, err := store.Snapshot(context.Background(), "phone", 0)
	if err != nil || len(snapshot.Items) != 2 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	if snapshot.Items[0].Kind != "appstore.webhook_ping" || snapshot.Items[0].Severity != "success" {
		t.Fatalf("ping=%+v", snapshot.Items[0])
	}
	if snapshot.Items[1].Severity != "error" {
		t.Fatalf("build=%+v", snapshot.Items[1])
	}
}

func TestAppStoreWebhookRequiresConfiguredSecret(t *testing.T) {
	h, _ := newTestHub(t)
	attachEventStore(t, h)
	req := httptest.NewRequest(http.MethodPost, "/hooks/apple-app-store/salon", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	h.ServeAppStoreWebhook(response, req)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", response.Code)
	}
}
