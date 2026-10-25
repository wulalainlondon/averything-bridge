package goexec

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type mediaCatalogProvider struct{ controllerTestProvider }

func (mediaCatalogProvider) VoiceMediaEnabled() bool { return true }
func (mediaCatalogProvider) ExecuteVoiceMedia(context.Context, backend.SessionControlCaller, string, backend.VoiceMediaRequest) (any, error) {
	return backend.VoiceMediaResult{State: "queued"}, nil
}
func TestVoiceMediaCatalogIsStrictAndDoesNotAcceptCallerIdentityArguments(t *testing.T) {
	c := &Codex{}
	c.SetSessionControlProvider(mediaCatalogProvider{})
	s := session.NewRegistry().Create("media-session", "media", t.TempDir(), backend.Codex, "", "", "")
	params := map[string]any{}
	c.applyVoiceMediaTools(s, params)
	groups := params["dynamicTools"].([]map[string]any)
	if len(groups) != 1 || groups[0]["name"] != "bridge_media" {
		t.Fatal(groups)
	}
	tools := groups[0]["tools"].([]map[string]any)
	if len(tools) != 3 {
		t.Fatal(tools)
	}
	for _, tool := range tools {
		schema := tool["inputSchema"].(map[string]any)
		if schema["additionalProperties"] != false {
			t.Fatal(schema)
		}
		fields := schema["properties"].(map[string]any)
		for _, name := range []string{"session_id", "voice_id", "device_id", "thread_id", "authority"} {
			if _, ok := fields[name]; ok {
				t.Fatal("model can substitute identity", name)
			}
		}
	}
}

// Opt-in native schema acceptance only: no model turn, playback or production
// conversation mutation. The ephemeral thread belongs to this test.
func TestVoiceMediaInstalledNativeCatalogAcceptance(t *testing.T) {
	if os.Getenv("BRIDGE_CODEX_VOICE_MEDIA_PROBE") != "1" {
		t.Skip("opt-in installed native catalog schema acceptance")
	}
	c := NewCodex(&caplessSink{}, "codex")
	c.remoteReconnect = false
	c.appServerMode = "daemon"
	c.SetDataDir(t.TempDir())
	if err := c.startRemoteServerLocked(filepath.Dir(c.sessionsRoot)); err != nil {
		t.Fatal(err)
	}
	defer func() { c.remoteCancel(); c.remoteConn.CloseNow() }()
	c.SetSessionControlProvider(mediaCatalogProvider{})
	s := session.NewRegistry().Create("music-native-schema-fixture", "schema fixture", t.TempDir(), backend.Codex, "", "", "")
	params := map[string]any{"cwd": t.TempDir(), "ephemeral": true, "approvalPolicy": "never", "sandbox": "read-only"}
	c.applyVoiceMediaTools(s, params)
	raw, err := c.rpc.request("thread/start", params, 25*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := extractThreadID(raw, "")
	if id == "" {
		t.Fatal("missing fixture native thread")
	}
	t.Logf("NATIVE_SCHEMA_ACCEPTED thread=%s namespace=bridge_media tools=3 model_turn=false playback=false", id)
}

func TestVoiceMediaInstructionsUseExistingToolsWithoutPermissionOverrides(t *testing.T) {
	input := backend.RealtimeVoiceStart{VoiceID: "voice-original", SDP: "v=0", MediaInstructions: "exact private channel instructions"}
	params := realtimeVoiceStartParams("thread-original", input)
	if params["realtimeStartInstructions"] != input.MediaInstructions || params["threadId"] != "thread-original" || params["realtimeSessionId"] != "voice-original" {
		t.Fatal(params)
	}
	for _, key := range []string{"sandbox", "permissions", "modelProvider", "dynamicTools"} {
		if _, ok := params[key]; ok {
			t.Fatal("voice overrides nativeprofile", key)
		}
	}
}
