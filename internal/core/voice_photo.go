package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"time"
)

// Phone admission must match the actual paired owner, not a display name or guessed client id.
func (h *Hub) voicePhotoOwner(c *Client, sessionID, voice, thread string) bool {
	owner := h.pairedTaskDevice(c)
	if owner == "" || voice == "" || thread == "" {
		return false
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	for device, call := range h.voiceClients {
		if device.live() && !call.stopping && call.kind != "line" && call.session.ID == sessionID && call.voiceID == voice && call.session.ResumeID() == thread && h.pairedTaskDevice(device) == owner {
			return true
		}
	}
	return false
}
func (h *Hub) voicePhotoStillCurrent(sessionID, voice, thread string) bool {
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	for device, call := range h.voiceClients {
		if device.live() && !call.stopping && call.kind != "line" && call.session.ID == sessionID && call.voiceID == voice && call.session.ResumeID() == thread {
			return true
		}
	}
	return false
}

// The image reaches the original backing conversation as real image input. The voice
// receives a context notification, never a fabricated claim that it has inspected pixels.
func (h *Hub) notifyVoicePhoto(s *session.Session, voice, thread, request string) {
	if !h.voicePhotoStillCurrent(s.ID, voice, thread) {
		return
	}
	exec, ok := h.exec.(backend.RealtimeVoiceExecutor)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = exec.AppendRealtimeVoiceText(ctx, s, voice, fmt.Sprintf("我剛傳了圖片到目前這個原對話（訊息 %s）。請依該圖片與目前通話脈絡回應；若你目前看不到圖片，請讓原對話代理檢視後回報，不要在未檢視前聲稱已看過。", request))
}
