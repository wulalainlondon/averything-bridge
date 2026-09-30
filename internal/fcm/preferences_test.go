package fcm

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"everything-go/internal/protocol"
)

func TestPreferencesSurviveRegistrationRefreshAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tokens.json")
	n := testNotifier(path, "", http.DefaultClient)
	p := protocol.NotificationPreferences{ErrorEnabled: true}
	n.SetPreferences("iphone", p)
	n.SetToken("iphone", "token-1", "ios")
	n.SetToken("iphone", "token-2", "ios")
	n.invalidate(target{deviceID: "iphone", token: "token-2"})
	n.SetToken("iphone", "token-3", "ios")
	n.SetToken("ipad", "other-token", "ios")
	loaded := testNotifier(path, "", http.DefaultClient)
	loaded.loadRegistry()
	if got := loaded.devices["iphone"]; got.Preferences == nil || *got.Preferences != p || got.Token != "token-3" {
		t.Fatalf("preferences lost: %+v", got)
	}
	if loaded.devices["ipad"].Preferences != nil {
		t.Fatal("iPhone preferences leaked to iPad")
	}
}

func TestPerDevicePushPreferencesAndPrivacy(t *testing.T) {
	var mu sync.Mutex
	var received []v1message
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg v1message
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			t.Error(err)
		}
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "tokens.json"), s.URL, s.Client())
	for _, id := range []string{"disabled", "private", "visible", "legacy"} {
		n.SetToken(id, id, "ios")
	}
	n.SetPreferences("disabled", protocol.NotificationPreferences{})
	n.SetPreferences("private", protocol.NotificationPreferences{TaskDoneEnabled: true, ErrorEnabled: true, AlertWhenWaiting: true})
	n.SetPreferences("visible", protocol.NotificationPreferences{TaskDoneEnabled: true, ErrorEnabled: true, AlertWhenWaiting: true, ShowLockscreenDetails: true})
	n.NotifyTaskDoneWithAuthority("wulala", "private machine", "secret session", "confidential result", "s1", "r1", ReplyAction{})
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("wanted three devices, got %d", len(received))
	}
	for _, msg := range received {
		switch msg.Message.Token {
		case "private":
			raw, _ := json.Marshal(msg)
			for _, secret := range []string{"secret session", "confidential result", "private machine"} {
				if strings.Contains(string(raw), secret) {
					t.Fatalf("private payload leaked %q", secret)
				}
			}
			if msg.Message.Notification.Title != "Averything" || msg.Message.APNS.Payload.APS.Alert.Body != "任務已完成" {
				t.Fatal("missing generic alert")
			}
		case "visible", "legacy":
			if msg.Message.Notification.Title != "✓ secret session" {
				t.Fatal("per-device privacy mutated another payload")
			}
		default:
			t.Fatal("disabled device received push")
		}
	}
}

func TestIOSPreferenceTypesAreIndependent(t *testing.T) {
	n := testNotifier("", "", http.DefaultClient)
	n.SetToken("iphone", "token", "ios")
	dst := target{deviceID: "iphone", token: "token"}
	for _, tc := range []struct {
		name, kind, phase string
		p                 protocol.NotificationPreferences
		want              bool
	}{
		{"all off running", "session_status", "running", protocol.NotificationPreferences{}, false},
		{"completion off", "task_done", "", protocol.NotificationPreferences{ErrorEnabled: true}, false},
		{"waiting on", "session_status", "waiting", protocol.NotificationPreferences{AlertWhenWaiting: true}, true},
		{"waiting off", "session_status", "waiting", protocol.NotificationPreferences{TaskDoneEnabled: true}, false},
		{"error on", "session_status", "failed", protocol.NotificationPreferences{ErrorEnabled: true}, true},
		{"error off", "session_status", "failed", protocol.NotificationPreferences{TaskDoneEnabled: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n.SetPreferences("iphone", tc.p)
			var msg v1message
			msg.Message.Data = map[string]string{"phase": tc.phase}
			if _, allowed := n.messageForDevice(msg, tc.kind, dst); allowed != tc.want {
				t.Fatalf("allowed=%v", allowed)
			}
		})
	}
}

func TestOptOutAppliesToQueuedSendAndPreservesAndroidTerminalUpdates(t *testing.T) {
	n := testNotifier("", "", http.DefaultClient)
	n.SetToken("iphone", "token", "ios")
	dst := n.targets()[0]
	n.SetPreferences("iphone", protocol.NotificationPreferences{})
	var msg v1message
	msg.Message.Data = map[string]string{"phase": "running"}
	if _, allowed := n.messageForDevice(msg, "session_status", dst); allowed {
		t.Fatal("old target snapshot bypassed opt-out")
	}
	n.SetToken("android", "android-token", "android")
	n.SetPreferences("android", protocol.NotificationPreferences{})
	msg.Message.Data["phase"] = "completed"
	if _, allowed := n.messageForDevice(msg, "session_status", target{deviceID: "android", token: "android-token"}); !allowed {
		t.Fatal("Android needs terminal data to remove ongoing card")
	}
}

func TestIOSCompletionReplacesStatusCollapseID(t *testing.T) {
	var got []v1message
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg v1message
		_ = json.NewDecoder(r.Body).Decode(&msg)
		got = append(got, msg)
	}))
	defer s.Close()
	n := testNotifier("", s.URL, s.Client())
	n.SetToken("iphone", "token", "ios")
	n.NotifySessionStatusWithAuthority("wulala", "Wulala", "s1", "name", "waiting", "", "", 1, 1, 1, "r1", 0, ReplyAction{})
	n.NotifyTaskDoneWithAuthority("wulala", "Wulala", "name", "done", "s1", "r1", ReplyAction{})
	if len(got) != 2 || got[0].Message.APNS.Headers["apns-collapse-id"] != got[1].Message.APNS.Headers["apns-collapse-id"] {
		t.Fatal("completion and status no longer replace the same iOS notification")
	}
}
