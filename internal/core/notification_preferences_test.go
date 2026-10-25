package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"everything-go/internal/fcm"
	"everything-go/internal/protocol"
)

func TestFCMPreferencesUseAuthenticatedDeviceIdentity(t *testing.T) {
	h, _ := newTestHub(t)
	path := filepath.Join(t.TempDir(), "tokens.json")
	n, err := fcm.NewFromBytes([]byte(`{"type":"service_account","project_id":"qa","client_email":"qa@example.invalid","private_key":"unused-in-this-no-network-test"}`), path)
	if err != nil {
		t.Fatal(err)
	}
	h.fcm = n
	n.SetToken("ipad", "ipad-token", "ios")
	c := &Client{deviceID: "iphone"}
	route(h, c, `{"type":"fcm_token","device_id":"ipad","token":"iphone-token","platform":"ios","notification_preferences":{"task_done_enabled":false,"error_enabled":false,"alert_when_waiting":false,"show_lockscreen_details":false}}`)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		Devices map[string]struct {
			Token       string                            `json:"token"`
			Preferences *protocol.NotificationPreferences `json:"notification_preferences"`
		} `json:"devices"`
	}
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Devices["iphone"].Token != "iphone-token" || saved.Devices["iphone"].Preferences == nil {
		t.Fatal("connection device did not receive preferences")
	}
	if saved.Devices["ipad"].Token != "ipad-token" || saved.Devices["ipad"].Preferences != nil {
		t.Fatal("payload device_id overwrote another device")
	}
}
