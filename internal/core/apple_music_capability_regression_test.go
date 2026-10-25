package core

import (
	"everything-go/internal/backend"
	"github.com/coder/websocket"
	"testing"
)

type appleMusic153Exec struct{ *voiceExec }

func (e *appleMusic153Exec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{&historyRangeFixtureProvider{}}
}

func TestAppleMusic153AuthenticatedHelloAndNoEnrollmentLeak(t *testing.T) {
	for _, paired := range []bool{true, false} {
		name := "unpaired"
		if paired {
			name = "paired"
		}
		t.Run(name, func(t *testing.T) {
			h, base := newControlTestHub(t, t.TempDir())
			f := &appleMusic153Exec{voiceExec: &voiceExec{fakeExec: base}}
			h.SetExecutor(f)
			if paired {
				if err := h.pairing.Claim("fixture-token", "fixture-ipad"); err != nil {
					t.Fatal(err)
				}
			}
			conn, ctx, cleanup := dialWS(t, h)
			defer cleanup()
			err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"fixture-ipad","auth_token":"fixture-token","client_info":{"platform":"ios","app_id":"com.morrie.text","build":"312"}}`))
			if err != nil {
				t.Fatal(err)
			}
			ack := readEvent(t, ctx, conn)
			if ack["type"] != "hello_ack" {
				t.Fatal(ack)
			}
			caps := map[string]bool{}
			if values, ok := ack["capabilities"].([]any); ok {
				for _, v := range values {
					if text, ok := v.(string); ok {
						caps[text] = true
					}
				}
			}
			for _, capability := range []string{"codex_voice_apple_music_ios_v1", "codex_voice_external_music_ios_v1"} {
				if caps[capability] != paired {
					t.Fatalf("paired=%v capability=%s advertised=%v", paired, capability, caps[capability])
				}
			}
			if paired && !caps["history_continuity_v1"] {
				t.Fatal("151 history continuity capability lost")
			}
			if f.starts.Load() != 0 {
				t.Fatal("hello started voice")
			}
		})
	}
}
