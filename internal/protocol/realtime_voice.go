package protocol

// Ephemeral, owner-only signaling. Never put this event in offline replay or history.
type RealtimeVoiceEvent struct {
	Media          *VoiceMediaCommand `json:"media,omitempty"`
	LineCallID     string             `json:"line_call_id,omitempty"`
	LineAccountRef string             `json:"line_account_ref,omitempty"`
	LineTargetRef  string             `json:"line_target_ref,omitempty"`
	LinePeerName   string             `json:"line_peer_name,omitempty"`
	Type           string             `json:"type"`
	SessionID      string             `json:"session_id"`
	RequestID      string             `json:"request_id"`
	VoiceID        string             `json:"voice_id"`
	ThreadID       string             `json:"thread_id,omitempty"`
	State          string             `json:"state"`
	SDP            string             `json:"sdp,omitempty"`
	Text           string             `json:"text,omitempty"`
	Role           string             `json:"role,omitempty"`
	Message        string             `json:"message,omitempty"`
}
