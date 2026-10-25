package fcm

import (
	"fmt"
	"time"
)

// HasRegisteredTaskCallbackDevice checks current notification registration,
// including the pairing filter. It does not imply successful push delivery or
// permission to acquire audio. Only supported native platforms qualify.
func (n *Notifier) HasRegisteredTaskCallbackDevice(device string) bool {
	if n == nil || device == "" {
		return false
	}
	for _, platform := range []string{"ios", "android"} {
		for _, dst := range n.targetsForPlatform(platform) {
			if dst.deviceID == device {
				return true
			}
		}
	}
	return false
}

// NotifyTaskCallback addresses exactly one enrolled device. Stable event IDs
// retain the broker's idempotency receipt across uncertain network delivery.
func (n *Notifier) NotifyTaskCallback(authority, session, request, callback, invitation, device string, expires int64) {
	if expires <= time.Now().UnixMilli() {
		return
	}
	msg := v1message{allowedDevice: func(id string) bool { return id == device }}
	msg.Message.Data = map[string]string{"type": "task_callback_invite", "schema_version": fcmPayloadSchemaVersion, "authority_instance_id": authority, "session_id": session, "task_request_id": request, "callback_id": callback, "invitation_id": invitation, "expires_at": fmt.Sprint(expires), "event_id": invitation}
	msg.Message.Android = &v1androidConfig{Priority: "high", TTL: "60s", CollapseKey: invitation}
	targets := []target{}
	for _, dst := range n.targetsForPlatform("android") {
		if dst.deviceID == device {
			targets = append(targets, dst)
		}
	}
	n.sendTargets(msg, "task_callback_invite", targets)

	// iOS presents an ordinary visible, actionable notification. This is not a
	// VoIP/PushKit push and must never start audio in the background. The native
	// answer action opens the original app and verifies the invitation first.
	ios := v1message{allowedDevice: msg.allowedDevice}
	ios.Message.Data = msg.Message.Data
	ios.Message.APNS = visibleAPNSConfig("BRIDGE_TASK_CALL", "task-call-"+shortStableID(authority, session), shortStableID(invitation), &v1notification{Title: "Averything 任務匯報", Body: "任務結果已準備好，接聽後可查看原對話。"})
	ios.Message.APNS.Headers["apns-push-type"] = "alert"
	ios.Message.APNS.Headers["apns-expiration"] = fmt.Sprint(expires / 1000)
	ios.Message.APNS.Payload.APS.Sound = "default"
	iosTargets := []target{}
	for _, dst := range n.targetsForPlatform("ios") {
		if dst.deviceID == device {
			iosTargets = append(iosTargets, dst)
		}
	}
	n.sendTargets(ios, "task_callback_invite", iosTargets)
}
