package backend

import (
	"context"
	"everything-go/internal/protocol"
)

// Controls only the original caller's active voice endpoint. No caller/voice
// identity is accepted in model arguments.
type VoiceMediaRequest struct {
	Delta      *int   `json:"delta,omitempty"`
	Query      string `json:"query,omitempty"`
	Artist     string `json:"artist,omitempty"`
	PositionMS *int64 `json:"position_ms,omitempty"`
	IntentID   string `json:"intent,omitempty"`
	Action     string `json:"action,omitempty"`
	URL        string `json:"url,omitempty"`
	Title      string `json:"title,omitempty"`
	Volume     *int   `json:"volume,omitempty"`
}

// MediaOrigin is populated from provider events, never model arguments.
type MediaOrigin struct {
	SessionID, ThreadID, TurnID, RequestID, VoiceID, Generation string
}
type MediaOriginResolver interface {
	ResolveMediaOrigin(thread, operation string) (MediaOrigin, error)
}
type UnifiedVoiceMediaProvider interface {
	UnifiedVoiceMediaEnabled() bool
	ExecuteScopedVoiceMedia(context.Context, SessionControlCaller, MediaOrigin, string, string, VoiceMediaRequest) (any, error)
}
type VoiceMediaProvider interface {
	VoiceMediaEnabled() bool
	ExecuteVoiceMedia(context.Context, SessionControlCaller, string, VoiceMediaRequest) (any, error)
}

// VoiceMediaNotDispatchedError is issued only by the media executor before it
// queues a device command. Ordinary errors (including lost delivery outcomes)
// must never be interpreted as proof that no effect occurred.
type VoiceMediaNotDispatchedError struct{ Cause error }

func (e *VoiceMediaNotDispatchedError) Error() string { return e.Cause.Error() }
func (e *VoiceMediaNotDispatchedError) Unwrap() error { return e.Cause }

type VoiceMediaResult struct {
	SystemMediaAudio  *protocol.SystemMediaAudioState `json:"system_media_audio,omitempty"`
	VoiceAudio        *protocol.VoiceAudioState       `json:"voice_audio,omitempty"`
	HandoffOpened     bool                            `json:"handoff_opened,omitempty"`
	ExternalMusic     *protocol.ExternalMusicState    `json:"external_music,omitempty"`
	Provider          string                          `json:"provider,omitempty"`
	Endpoint          *MediaEndpoint                  `json:"endpoint,omitempty"`
	LastObservedState string                          `json:"last_observed_state,omitempty"`
	ObservationAgeMS  *int64                          `json:"observation_age_ms,omitempty"`
	Audio             *protocol.MediaAudioInfo        `json:"audio,omitempty"`
	AudioCheck        string                          `json:"audio_check"`
	Transport         string                          `json:"transport,omitempty"`
	Action            string                          `json:"action,omitempty"`
	ActionConfirmed   bool                            `json:"action_confirmed"`
	CommandID         string                          `json:"command_id,omitempty"`
	Sequence          uint64                          `json:"sequence,omitempty"`
	State             string                          `json:"state"`
	Title             string                          `json:"title,omitempty"`
	VideoURL          string                          `json:"video_url,omitempty"`
	Volume            int                             `json:"volume"`
	Observed          bool                            `json:"observed"`
	PlaybackConfirmed bool                            `json:"playback_confirmed"`
	NeedsUserAction   bool                            `json:"needs_user_action"`
	ErrorCode         string                          `json:"error_code,omitempty"`
	LastObservedMS    int64                           `json:"last_observed_ms,omitempty"`
}

// Diagnostic identity of the actual bound client, never an address for routing.
type MediaEndpoint struct {
	VoiceID           string `json:"voice_id"`
	BridgeInstanceID  string `json:"bridge_instance_id"`
	DeviceInventoryID string `json:"device_inventory_id,omitempty"`
	ClientID          string `json:"client_id,omitempty"`
	Platform          string `json:"platform"`
	Name              string `json:"name,omitempty"`
	Model             string `json:"model,omitempty"`
	AppVersion        string `json:"app_version,omitempty"`
	AppBuild          string `json:"app_build,omitempty"`
	Verification      string `json:"verification"`
}
