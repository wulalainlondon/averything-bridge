package protocol

// NotificationPreferences is a complete per-device snapshot. A nil snapshot
// means a legacy client and retains its prior notification behavior.
type NotificationPreferences struct {
	TaskDoneEnabled       bool `json:"task_done_enabled"`
	ErrorEnabled          bool `json:"error_enabled"`
	AlertWhenWaiting      bool `json:"alert_when_waiting"`
	ShowLockscreenDetails bool `json:"show_lockscreen_details"`
}
