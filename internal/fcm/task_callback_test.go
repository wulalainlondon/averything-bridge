package fcm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCallbackRegistrationEligibilityIsNotDelivery(t *testing.T) {
	var absent *Notifier
	if absent.HasRegisteredTaskCallbackDevice("ios") {
		t.Fatal("nil notifier eligible")
	}
	n := testNotifier("", "http://unused.invalid", http.DefaultClient)
	n.SetToken("ios", "ios-token", "ios")
	n.SetToken("android", "android-token", "android")
	n.SetToken("unknown-platform", "legacy-token", "")
	if !n.HasRegisteredTaskCallbackDevice("ios") || !n.HasRegisteredTaskCallbackDevice("android") || n.HasRegisteredTaskCallbackDevice("unknown-platform") || n.HasRegisteredTaskCallbackDevice("absent") || n.HasRegisteredTaskCallbackDevice("") {
		t.Fatal("registration eligibility is not exact supported device")
	}
	n.SetDeviceFilter(func(id string) bool { return id != "ios" })
	if n.HasRegisteredTaskCallbackDevice("ios") {
		t.Fatal("revoked device eligible")
	}
	n.RemoveDevice("android")
	if n.HasRegisteredTaskCallbackDevice("android") {
		t.Fatal("withdrawn registration eligible")
	}
}

func TestCallbackPushTargetsOneDeviceAndCarriesNoResultOrCredentials(t *testing.T) {
	var messages []v1message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m v1message
		if e := json.NewDecoder(r.Body).Decode(&m); e != nil {
			t.Error(e)
		}
		messages = append(messages, m)
		w.Write([]byte(`{"name":"sent"}`))
	}))
	defer server.Close()
	n := testNotifier("", server.URL, server.Client())
	n.SetToken("s10", "token-s10", "android")
	n.SetToken("note20", "token-note20", "android")
	n.SetToken("ios", "token-ios", "ios")
	n.NotifyTaskCallback("authority", "session", "request", "cb_policy", "cb_invitation", "s10", time.Now().Add(time.Minute).UnixMilli())
	if len(messages) != 1 {
		t.Fatal(len(messages))
	}
	m := messages[0]
	if m.Message.Token != "token-s10" || m.Message.Android.TTL != "60s" || m.Message.Notification != nil {
		t.Fatal(m)
	}
	for _, key := range []string{"body", "report", "text", "auth_token", "reply_capability"} {
		if _, ok := m.Message.Data[key]; ok {
			t.Fatal("sensitive push field", key)
		}
	}
}

func TestIOSCallbackIsOnePrivateExpiringAlertNotVoIP(t *testing.T) {
	var messages []v1message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m v1message
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			t.Error(err)
		}
		messages = append(messages, m)
		w.Write([]byte(`{"name":"sent"}`))
	}))
	defer server.Close()
	n := testNotifier("", server.URL, server.Client())
	n.SetToken("original-ios", "original-token", "ios")
	n.SetToken("other-ios", "other-token", "ios")
	n.SetToken("android", "android-token", "android")
	expires := time.Now().Add(30 * time.Second).UnixMilli()
	n.NotifyTaskCallback("authority", "session", "request", "cb_policy", "inv_original", "original-ios", expires)
	if len(messages) != 1 {
		t.Fatalf("sent %d, want exactly original recipient", len(messages))
	}
	m := messages[0]
	if m.Message.Token != "original-token" || m.Message.Android != nil || m.Message.Notification != nil || m.Message.APNS == nil {
		t.Fatal(m)
	}
	apns := m.Message.APNS
	if apns.Headers["apns-push-type"] != "alert" || apns.Headers["apns-expiration"] != fmt.Sprint(expires/1000) || apns.Payload.APS.Category != "BRIDGE_TASK_CALL" || apns.Payload.APS.Sound != "default" {
		t.Fatal(apns)
	}
	if apns.Payload.APS.Alert == nil || apns.Payload.APS.ContentAvailable != 1 {
		t.Fatal(apns)
	}
	for _, key := range []string{"text", "body", "report", "auth_token", "reply_capability", "voice_id"} {
		if _, ok := m.Message.Data[key]; ok {
			t.Fatal("sensitive or premature authority", key)
		}
	}
	n.NotifyTaskCallback("authority", "session", "request", "cb_policy", "expired", "original-ios", time.Now().Add(-time.Second).UnixMilli())
	if len(messages) != 1 {
		t.Fatal("expired invitation sent")
	}
}
