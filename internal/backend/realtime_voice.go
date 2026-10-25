package backend

import (
	"context"
	"everything-go/internal/session"
)

// RealtimeVoice is separate from Send/Stop: ending audio must never interrupt a turn.
type RealtimeVoiceStart struct {
	MediaGeneration     string
	MediaInstructions   string
	SpeakerInstructions string
	VoiceID             string
	VoiceName           string
	SDP                 string
	OnEvent             func(RealtimeVoiceEvent)
}
type RealtimeVoiceAnswer struct{ ThreadID, VoiceID, SDP string }
type RealtimeVoiceEvent struct {
	State, Text, Role, Message, TranscriptVoiceID string
}
type RealtimeVoiceExecutor interface {
	StartRealtimeVoice(context.Context, *session.Session, RealtimeVoiceStart) (RealtimeVoiceAnswer, error)
	StopRealtimeVoice(context.Context, *session.Session, string) error
	AppendRealtimeVoiceText(context.Context, *session.Session, string, string) error
}

// Codex rust-v0.160.0 maps realtime v3 to this built-in voice set.
func ValidRealtimeVoiceName(name string) bool {
	switch name {
	case "", "juniper", "maple", "spruce", "ember", "vale", "breeze", "arbor", "sol", "cove":
		return true
	}
	return false
}

// RealtimeSpeechExecutor speaks output text without admitting a new user task.
type RealtimeSpeechExecutor interface {
	AppendRealtimeVoiceSpeech(context.Context, *session.Session, string, string) error
}

type RealtimeSpeechEvidence struct{ ThreadID, VoiceID, Text string }

// Generation-pinned provider readback; this never starts or repeats speech.
type RealtimeSpeechReader interface {
	ReadRealtimeVoiceSpeech(context.Context, *session.Session, string, int64) (RealtimeSpeechEvidence, error)
}
