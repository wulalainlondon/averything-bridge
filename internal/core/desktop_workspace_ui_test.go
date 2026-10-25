package core

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// A real loopback WS/upload/queue server with a deterministic executor. Only
// this synthetic Bridge is mutated during native Finder/paste/preview QA.
func TestDesktopWorkspaceNativeUIHarness(t *testing.T) {
	manifestPath := os.Getenv("BRIDGE_WORKSPACE_UI_MANIFEST")
	if manifestPath == "" {
		t.Skip("opt-in native UI fixture")
	}
	h, fe := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.cfg.InstanceID, h.cfg.InstanceName = "desktop_workspace_qa", "Workspace QA"
	h.cfg.Backends = []backend.Definition{{ID: "codex", Label: "Codex fixture", DefaultModel: "gpt-6-astra", Models: []backend.Model{{ID: "gpt-6-astra", Label: "GPT-6-Astra", SupportedReasoningEfforts: []string{"low", "medium", "high", "xhigh"}, ServiceTiers: []backend.ServiceTier{{ID: "fast", Name: "Fast"}}}}}}
	if err := h.pairing.Claim("workspace-fixture-token", "workspace-ui-fixture"); err != nil {
		t.Fatal(err)
	}
	h.registry.Create("attachment-ui", "Attachment QA", h.cfg.DataDir, "codex", "gpt-6-astra", "read-only", "")
	fe.onSend = func(s *session.Session, id, content string) {
		entry, ok, err := h.messageQueue.Get(s.ID, id)
		if err != nil || !ok {
			return
		}
		var payload queuedPayload
		_ = json.Unmarshal(entry.Payload, &payload)
		text := "WORKSPACE_UI_ACK\n" + content
		if len(payload.Images) > 0 {
			text += "\nIMAGE_BYTES_RECEIVED"
		}
		h.Emit(protocol.NewTextChunk(s.ID, id, text))
		h.Emit(protocol.NewDone(s.ID, id))
	}
	server := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	defer server.Close()
	raw, _ := json.Marshal(map[string]string{"url": "ws" + strings.TrimPrefix(server.URL, "http"), "token": "workspace-fixture-token", "session_id": "attachment-ui", "authority": h.cfg.InstanceID})
	if err := os.WriteFile(manifestPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("DESKTOP_WORKSPACE_UI_READY")
	deadline := time.Now().Add(20 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(manifestPath + ".stop"); err == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("native UI fixture timed out")
}
