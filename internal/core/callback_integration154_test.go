package core

import (
	"context"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/session"
	"github.com/coder/websocket"
)

func TestIntegrated154HelloPreservesCallbackLINEHistoryAndIOSMusic(t *testing.T) {
	for _, paired := range []bool{true, false} {
		name := "enrollment"
		if paired {
			name = "paired"
		}
		t.Run(name, func(t *testing.T) {
			h, base := newControlTestHub(t, t.TempDir())
			testLineCatalog(h, t) // Temporary encrypted store; no network driver.
			h.lineControl = &lineLocalControl{launcher: "/fixture/bridge-line"}
			e := &appleMusic153Exec{voiceExec: &voiceExec{fakeExec: base}}
			h.SetExecutor(e)
			if paired {
				if err := h.pairing.Claim("integrated-token", "integrated-phone"); err != nil {
					t.Fatal(err)
				}
			}
			conn, ctx, cleanup := dialWS(t, h)
			defer cleanup()
			if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"integrated-phone","auth_token":"integrated-token","client_info":{"platform":"ios","app_id":"com.morrie.text","build":"312"}}`)); err != nil {
				t.Fatal(err)
			}
			ack := readEvent(t, ctx, conn)
			if ack["type"] != "hello_ack" {
				t.Fatal(ack)
			}
			caps := map[string]bool{}
			for _, value := range ack["capabilities"].([]any) {
				caps[value.(string)] = true
			}
			for _, cap := range []string{"task_callback_v1", "line_accounts_v1", "line_service_config_v1", "line_control_cli_v1", "history_continuity_v1", "codex_voice_external_music_v1", "codex_voice_external_music_ios_v1", "codex_voice_apple_music_ios_v1"} {
				if caps[cap] != paired {
					t.Fatalf("paired=%v capability=%s advertised=%v", paired, cap, caps[cap])
				}
			}
			if e.starts.Load() != 0 {
				t.Fatal("capability discovery started a call")
			}
		})
	}
}

type integrated154CallbackExec struct {
	*callbackExec
	inputs chan backend.RealtimeVoiceStart
}

func (e *integrated154CallbackExec) StartRealtimeVoice(ctx context.Context, s *session.Session, in backend.RealtimeVoiceStart) (backend.RealtimeVoiceAnswer, error) {
	e.inputs <- in
	return e.callbackExec.StartRealtimeVoice(ctx, s, in)
}

func TestIntegrated154CallbackSpeechKeepsSeparateLINEControlInstructions(t *testing.T) {
	h, c, base, r := callbackReady(t)
	h.lineControl = &lineLocalControl{launcher: "/fixture/bridge-line"}
	e := &integrated154CallbackExec{callbackExec: base, inputs: make(chan backend.RealtimeVoiceStart, 1)}
	h.SetExecutor(e)
	const claim = "integrated_callback_claim"
	got := callbackAction(t, h, c, callbackInput{Action: "claim", Operation: claim, Session: r.Session, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID})
	if got["accepted"] != true {
		t.Fatal(got)
	}
	h.handleVoiceStart(c, clientproto.Command{SessionID: r.Session, ThreadID: r.Thread, VoiceID: "voice-integrated-callback", RequestID: "request-integrated-callback", SDP: "v=0\r\n", CallbackID: r.ID, CallbackInvitation: r.Invite.ID, CallbackClaim: claim})
	nextVoiceState(t, c, "answer")
	in := <-e.inputs
	if !strings.Contains(in.MediaInstructions, "/fixture/bridge-line") || !strings.Contains(in.MediaInstructions, "original current conversation") {
		t.Fatal("LINE control instructions lost during callback startup")
	}
	if !strings.Contains(in.SpeakerInstructions, base.final) || strings.Contains(in.SpeakerInstructions, "/fixture/bridge-line") {
		t.Fatal("sealed callback speech mixed with tool instructions")
	}
	if e.starts.Load() != 1 || e.appends.Load() != 0 || e.cancels.Load() != 0 {
		t.Fatal("callback startup replayed speech or affected original work")
	}
}
