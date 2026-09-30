package fcm

import "everything-go/internal/protocol"

func (n *Notifier) SetPreferences(deviceID string, preferences protocol.NotificationPreferences) {
	n.RegisterDevice(deviceID, "", "", &preferences)
}

func (n *Notifier) messageForDevice(msg v1message, kind string, dst target) (v1message, bool) {
	n.mu.RLock()
	registration, exists := n.devices[dst.deviceID]
	n.mu.RUnlock()
	if !exists || registration.Token != dst.token {
		return msg, false
	}
	p := registration.Preferences
	if p == nil {
		return msg, true
	}
	// Android's native renderer consumes even suppressed/terminal runtime
	// updates to reconcile its ongoing card, and applies privacy locally.
	if kind == "session_status" && registration.Platform == "android" {
		return msg, true
	}
	phase := msg.Message.Data["phase"]
	privateBody := ""
	switch kind {
	case "task_done":
		if !p.TaskDoneEnabled {
			return msg, false
		}
		privateBody = "任務已完成"
	case "session_status":
		switch phase {
		case "waiting":
			if !p.AlertWhenWaiting {
				return msg, false
			}
		case "failed":
			if !p.ErrorEnabled {
				return msg, false
			}
		default:
			// Android still needs data-only runtime updates to remove finished
			// cards. iOS visible task activity follows the task notification toggle.
			if registration.Platform == "ios" && !p.TaskDoneEnabled {
				return msg, false
			}
		}
		privateBody = iosSessionStatusBody(phase, "", "")
	case "work_attention":
		if msg.Message.Data["kind"] == "failed" {
			if !p.ErrorEnabled {
				return msg, false
			}
		} else if !p.TaskDoneEnabled {
			return msg, false
		}
		privateBody = "工作狀態已更新"
	}
	// Android completion pushes are data-only and rendered by the app. Keep
	// their private content intact: the native notification already supplies a
	// generic publicVersion with VISIBILITY_PRIVATE for the lock screen.
	// Redacting the transport here also erases the unlocked conversation name
	// and summary. Still honor TaskDoneEnabled above, and leave iOS/legacy
	// visible-payload privacy unchanged.
	if registration.Platform == "android" && kind == "task_done" && msg.Message.Notification == nil {
		return msg, true
	}
	if p.ShowLockscreenDetails || privateBody == "" {
		return msg, true
	}
	// Deep-copy only the mutable presentation fields. Identifiers and reply
	// capabilities remain intact; names, result text and tool details do not.
	data := make(map[string]string, len(msg.Message.Data))
	for key, value := range msg.Message.Data {
		data[key] = value
	}
	for _, key := range []string{"session_name", "authority_name", "title", "work_item_title"} {
		if _, ok := data[key]; ok {
			data[key] = "Averything"
		}
	}
	for _, key := range []string{"body", "stage_message"} {
		if _, ok := data[key]; ok {
			data[key] = privateBody
		}
	}
	msg.Message.Data = data
	alert := &v1notification{Title: "Averything", Body: privateBody}
	if msg.Message.Notification != nil {
		msg.Message.Notification = alert
	}
	if msg.Message.APNS != nil {
		apns := *msg.Message.APNS
		apns.Payload.APS.Alert = alert
		msg.Message.APNS = &apns
	}
	return msg, true
}
