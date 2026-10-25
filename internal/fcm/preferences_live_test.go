package fcm

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"everything-go/internal/protocol"
)

type livePreferenceTransport struct {
	base     http.RoundTripper
	mu       sync.Mutex
	statuses []int
}

func (t *livePreferenceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(r)
	t.mu.Lock()
	defer t.mu.Unlock()
	status := 0
	if response != nil {
		status = response.StatusCode
	}
	t.statuses = append(t.statuses, status)
	return response, err
}

// Explicitly opt-in: uses only the selected iOS registration and a fresh test
// registry. Never loads other targets or modifies the running Bridge's state.
func TestLiveIOSNotificationPreferences(t *testing.T) {
	if os.Getenv("BRIDGE_FCM_QA") != "1" {
		t.Skip("requires an explicitly selected physical iPhone")
	}
	var input struct {
		DeviceID    string                           `json:"device_id"`
		Preferences protocol.NotificationPreferences `json:"notification_preferences"`
	}
	raw, err := os.ReadFile(os.Getenv("BRIDGE_FCM_QA_PREFERENCES"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &input); err != nil || input.DeviceID == "" {
		t.Fatal("invalid explicit QA device")
	}
	raw, err = os.ReadFile(os.Getenv("BRIDGE_FCM_QA_REGISTRY"))
	if err != nil {
		t.Fatal(err)
	}
	var registry tokenRegistry
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatal(err)
	}
	device, ok := registry.Devices[input.DeviceID]
	if !ok || device.Platform != "ios" || device.Token == "" {
		t.Fatal("selected device has no iOS registration")
	}
	n, err := New(os.Getenv("BRIDGE_FCM_QA_SERVICE_ACCOUNT"), filepath.Join(t.TempDir(), "qa-tokens.json"))
	if err != nil {
		t.Fatal(err)
	}
	n.RegisterDevice(input.DeviceID, device.Token, "ios", &input.Preferences)
	transport := &livePreferenceTransport{base: n.http.Transport}
	if transport.base == nil {
		transport.base = http.DefaultTransport
	}
	n.http.Transport = transport
	n.NotifyTaskDoneWithAuthority("qa-only", "Notification QA", "i11 preference QA", "Private QA result must be hidden", "qa-ios-preferences", "qa-request", ReplyAction{})
	want := 0
	if input.Preferences.TaskDoneEnabled {
		want = 1
	}
	if !input.Preferences.TaskDoneEnabled {
		if input.Preferences.ErrorEnabled || input.Preferences.AlertWhenWaiting {
			t.Fatal("off test must disable all task notification types")
		}
		for _, phase := range []string{"running", "waiting", "failed"} {
			n.NotifySessionStatusWithAuthority("qa-only", "Notification QA", "qa-ios-preferences-"+phase, "i11 preference QA", phase, "", "Private QA detail", 1, 1, 1, "qa-request", 0, ReplyAction{})
		}
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.statuses) != want {
		t.Fatalf("FCM network calls=%d, want=%d", len(transport.statuses), want)
	}
	for _, status := range transport.statuses {
		if status != http.StatusOK {
			t.Fatalf("FCM did not accept QA push: HTTP %d", status)
		}
	}
	t.Logf("selected iPhone taskDone=%t details=%t: FCM requests=%d", input.Preferences.TaskDoneEnabled, input.Preferences.ShowLockscreenDetails, len(transport.statuses))
}
