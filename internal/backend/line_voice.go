package backend

import "context"

// Uses a verified native tool caller and its exact active voice, never a
// model-supplied session or device identifier.
type LineVoiceProvider interface {
	LineVoiceEnabled() bool
	HandoffLineVoice(context.Context, SessionControlCaller, string, string) (any, error)
}
