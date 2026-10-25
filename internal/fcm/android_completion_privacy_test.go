package fcm

import (
	"testing"

	"everything-go/internal/protocol"
)

func TestAndroidCompletionRetainsUnlockedContentAndReply(t *testing.T) {
	n := testNotifier("", "", nil)
	n.SetToken("android", "android-token", "android")
	n.SetPreferences("android", protocol.NotificationPreferences{TaskDoneEnabled: true})
	var msg v1message
	msg.Message.Data = map[string]string{
		"type": "task_done", "session_name": "Report task", "authority_name": "Work Mac",
		"title": "Report task", "body": "Report is ready", "session_id": "s1", "request_id": "r1",
		"reply_url":        "http://100.64.0.1:8766/api/notification/v1/replies",
		"reply_capability": "test-capability", "reply_expires_at": "9999999999999",
	}
	got, allowed := n.messageForDevice(msg, "task_done", target{deviceID: "android", token: "android-token"})
	if !allowed || got.Message.Notification != nil {
		t.Fatal("completion must remain data-only")
	}
	for key, want := range msg.Message.Data {
		if got.Message.Data[key] != want {
			t.Fatalf("Android data field %s was redacted", key)
		}
	}
	n.SetPreferences("android", protocol.NotificationPreferences{})
	if _, allowed := n.messageForDevice(msg, "task_done", target{deviceID: "android", token: "android-token"}); allowed {
		t.Fatal("native privacy handling must not bypass task notification opt-out")
	}
}

func TestAndroidVisibleLegacyPayloadStillHonorsPrivacy(t *testing.T) {
	n := testNotifier("", "", nil)
	n.SetToken("android", "android-token", "android")
	n.SetPreferences("android", protocol.NotificationPreferences{TaskDoneEnabled: true})
	var msg v1message
	msg.Message.Notification = &v1notification{Title: "Private name", Body: "Private result"}
	msg.Message.Data = map[string]string{"session_name": "Private name", "body": "Private result"}
	got, allowed := n.messageForDevice(msg, "task_done", target{deviceID: "android", token: "android-token"})
	if !allowed || got.Message.Notification.Title != "Averything" || got.Message.Data["body"] != "任務已完成" {
		t.Fatal("system-rendered visible payload lost lock-screen privacy")
	}
}
