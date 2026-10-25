package fcm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/liveactivity"
)

type capturedLiveEnvelope struct {
	Message struct {
		Token        string `json:"token"`
		Notification any    `json:"notification"`
		APNS         struct {
			Token   string            `json:"live_activity_token"`
			Headers map[string]string `json:"headers"`
			Payload struct {
				APS struct {
					Event     string             `json:"event"`
					Timestamp int64              `json:"timestamp"`
					State     liveactivity.State `json:"content-state"`
					Alert     any                `json:"alert"`
				} `json:"aps"`
			} `json:"payload"`
		} `json:"apns"`
	} `json:"message"`
}

func receiveLive(t *testing.T, events <-chan capturedLiveEnvelope) capturedLiveEnvelope {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(4 * time.Second):
		t.Fatal("live update missing")
		return capturedLiveEnvelope{}
	}
}
func TestLiveActivityDeviceRunIsolationAndTerminalOrdering(t *testing.T) {
	events := make(chan capturedLiveEnvelope, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var value capturedLiveEnvelope
		_ = json.NewDecoder(r.Body).Decode(&value)
		events <- value
		w.WriteHeader(200)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm.json"), server.URL, server.Client())
	defer n.Close()
	n.RegisterDevice("phone", "verified-fcm-token", "ios", nil)
	n.RegisterDevice("android", "android-token", "android", nil)
	var authorized atomic.Bool
	authorized.Store(true)
	n.SetLiveActivityAuthorizer(func(id, digest string) bool {
		return authorized.Load() && id == "phone" && digest == "credential-digest"
	})
	state := liveactivity.State{Phase: "running", Stage: "thinking", Revision: 1, UpdatedAt: time.Now().UnixMilli()}
	token := strings.Repeat("ab", 32)
	_, err := n.RegisterLiveActivity(context.Background(), "phone", "activity-a", "bridge-a", "session-a", "run-a", token, "credential-digest", state)
	if err != nil {
		t.Fatal(err)
	}
	first := receiveLive(t, events)
	if first.Message.Token != "verified-fcm-token" || first.Message.APNS.Token != token || first.Message.APNS.Headers["apns-topic"] != "com.morrie.text.push-type.liveactivity" || first.Message.Notification != nil || first.Message.APNS.Payload.APS.Alert != nil {
		t.Fatal("device-scoped, silent payload changed")
	}
	state.Revision = 20
	n.NotifyLiveActivity("other-bridge", "session-a", "run-a", state)
	n.NotifyLiveActivity("bridge-a", "other-session", "run-a", state)
	n.NotifyLiveActivity("bridge-a", "session-a", "other-run", state)
	select {
	case <-events:
		t.Fatal("unrelated run updated pinned activity")
	case <-time.After(25 * time.Millisecond):
	}
	state.Phase = "completed"
	n.NotifyLiveActivity("bridge-a", "session-a", "run-a", state)
	terminal := receiveLive(t, events)
	if terminal.Message.APNS.Payload.APS.Event != "end" || terminal.Message.APNS.Payload.APS.Timestamp <= first.Message.APNS.Payload.APS.Timestamp {
		t.Fatal("terminal ordering/event")
	}
	state.Revision++
	state.Phase = "running"
	n.NotifyLiveActivity("bridge-a", "session-a", "run-a", state)
	select {
	case <-events:
		t.Fatal("ended run resurrected")
	case <-time.After(25 * time.Millisecond):
	}
	n.UnregisterLiveActivity("phone", "activity-a")
	_, err = n.RegisterLiveActivity(context.Background(), "android", "activity-b", "bridge-a", "session-a", "run-b", token, "credential-digest", state)
	if err == nil {
		t.Fatal("Android registration accepted")
	}
	authorized.Store(false)
	_, err = n.RegisterLiveActivity(context.Background(), "phone", "activity-b", "bridge-a", "session-a", "run-b", token, "credential-digest", state)
	if err == nil {
		t.Fatal("unpaired device accepted")
	}
}
func TestLiveActivityRetriesOnlyExplicitProviderRejection(t *testing.T) {
	events := make(chan capturedLiveEnvelope, 8)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var value capturedLiveEnvelope
		_ = json.NewDecoder(r.Body).Decode(&value)
		events <- value
		if attempts.Add(1) == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(200)
		}
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm.json"), server.URL, server.Client())
	defer n.Close()
	n.RegisterDevice("phone", "fcm-token", "ios", nil)
	n.SetLiveActivityAuthorizer(func(string, string) bool { return true })
	state := liveactivity.State{Phase: "running", Revision: 1, UpdatedAt: time.Now().UnixMilli()}
	if _, err := n.RegisterLiveActivity(context.Background(), "phone", "a", "bridge", "s", "r", strings.Repeat("ab", 32), "digest", state); err != nil {
		t.Fatal(err)
	}
	first := receiveLive(t, events)
	second := receiveLive(t, events)
	if first.Message.APNS.Payload.APS.Timestamp != second.Message.APNS.Payload.APS.Timestamp || first.Message.APNS.Payload.APS.State.Revision != second.Message.APNS.Payload.APS.State.Revision {
		t.Fatal("retry changed update identity")
	}
}
