package fcm

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestTunnelPushIncludesAppleBackgroundEnvelope(t *testing.T) {
	var received v1message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "tokens.json"), server.URL, server.Client())
	n.SetToken("iphone", "token", "ios")
	n.NotifyTunnelURL("https://example.com", "morrie")
	msg := received.Message
	if msg.APNS == nil || msg.APNS.Payload.APS.ContentAvailable != 1 {
		t.Fatal("missing silent push payload")
	}
	if msg.APNS.Headers["apns-push-type"] != "background" || msg.APNS.Headers["apns-priority"] != "5" {
		t.Fatal("invalid background headers")
	}
	if msg.Notification != nil || msg.APNS.Payload.APS.Alert != nil {
		t.Fatal("routine endpoint update must stay silent")
	}
	if msg.Data["url"] != "wss://example.com" || msg.Data["instance_id"] != "morrie" || msg.Data["issued_at"] == "" {
		t.Fatal("missing endpoint identity")
	}
}
