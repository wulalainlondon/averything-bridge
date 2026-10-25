package clientproto

import (
	"everything-go/internal/protocol"
	"testing"
)

func TestNotificationPreferencesSurviveCommandParsing(t *testing.T) {
	in, err := protocol.ParseInbound([]byte(`{"type":"fcm_token","token":"","platform":"ios","notification_preferences":{"task_done_enabled":false,"error_enabled":true,"alert_when_waiting":false,"show_lockscreen_details":false}}`))
	if err != nil {
		t.Fatal(err)
	}
	cmd := (AppV1{}).ParseCommand(in)
	if cmd.NotificationPreferences == nil || cmd.NotificationPreferences.TaskDoneEnabled || !cmd.NotificationPreferences.ErrorEnabled || cmd.NotificationPreferences.ShowLockscreenDetails {
		t.Fatalf("preferences lost: %+v", cmd.NotificationPreferences)
	}
}
