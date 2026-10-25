package core

import (
	"encoding/json"
	"everything-go/internal/protocol"
	"log"
	"os"
)

// frameLog gates per-frame protocol logging for Phase 2 E2E observation. It is
// the protocol-level source of truth during fixed-endpoint testing: every
// inbound command and outbound event is logged with direction, so a transcript
// can be diffed against the Python bridge. Off by default (zero overhead in
// prod); enable with EG_FRAME_LOG=1.
var frameLog = os.Getenv("EG_FRAME_LOG") != ""

const frameLogMax = 240

// logInbound records a received command frame ("<<").
func logInbound(typ, sessionID string) {
	if !frameLog {
		return
	}
	if sessionID != "" {
		log.Printf("<< %s session=%s", typ, sessionID)
	} else {
		log.Printf("<< %s", typ)
	}
}

// logOutbound records an emitted event frame (">>"). It marshals the event for
// full visibility (type + session_id + payload snippet); the cost is only paid
// when EG_FRAME_LOG is on.
func logOutbound(event any) {
	if !frameLog {
		return
	}
	if _, private := event.(interface{ PrivatePayload() }); private {
		log.Printf(">> private frame [private payload redacted]")
		return
	}
	if voice, ok := event.(protocol.RealtimeVoiceEvent); ok {
		log.Printf(">> codex_voice_event session=%s state=%s signaling=[redacted]", voice.SessionID, voice.State)
		return
	}
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf(">> <unmarshalable %T>", event)
		return
	}
	if workspaceFrameHasReservedSession(data) {
		log.Printf(">> workspace native frame [private payload redacted]")
		return
	}
	if len(data) > frameLogMax {
		log.Printf(">> %s…(%d bytes)", data[:frameLogMax], len(data))
	} else {
		log.Printf(">> %s", data)
	}
}
