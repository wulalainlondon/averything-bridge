package goexec

import (
	"context"
	"errors"
	"everything-go/internal/session"
	"strings"
	"time"
)

// Only classified diagnostics: never log a native error body, headers or URLs.
func realtimeFailureCategory(message string) string {
	m := strings.ToLower(message)
	for _, category := range []string{"逾時", "timeout", "timed out", "deadline", "prompt", "instructions", "unauthorized", "forbidden", "rate limit", "connection", "400", "403", "429", "500", "502", "503"} {
		if strings.Contains(m, category) {
			return strings.ReplaceAll(category, " ", "_")
		}
	}
	return "native_error_unclassified"
}

// AppendRealtimeVoiceSpeech is the pinned daemon's speakable-output operation.
// It is deliberately separate from role=user appendText / task submission.
func (c *Codex) AppendRealtimeVoiceSpeech(ctx context.Context, s *session.Session, voiceID, text string) error {
	if len(text) > 16000 || strings.TrimSpace(text) == "" {
		return errors.New("無效的匯報語音文字")
	}
	call := c.ownedVoice(s, voiceID)
	if call == nil || call.threadID != s.ResumeID() {
		return errors.New("語音連線已失效")
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	_, e := c.rpcCall("thread/realtime/appendSpeech", map[string]any{"threadId": call.threadID, "text": text}, 10*time.Second)
	return e
}
