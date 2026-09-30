package core

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type floatingHistoryExec struct {
	*fakeExec
	provider *floatingHistoryProvider
}

type floatingHistoryProvider struct {
	mu       sync.Mutex
	byResume map[string][]map[string]any
}

func (p *floatingHistoryProvider) LoadHistory(id string, opts history.Opts) (*history.Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return history.Slice(p.byResume[id], opts), nil
}
func (p *floatingHistoryProvider) ResumableSessions(int) ([]history.ResumableSession, error) {
	return nil, nil
}
func (p *floatingHistoryProvider) append(id string, message map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.byResume[id] = append(p.byResume[id], message)
}

func (e *floatingHistoryExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.provider, true
}
func (e *floatingHistoryExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.provider}
}

// Opt-in, isolated device fixture. No production executor, session, credentials,
// or daemon is used. The transcript says MOCK explicitly, never a real AI answer.
func TestMobileFloatingUIHarness(t *testing.T) {
	path := os.Getenv("BRIDGE_FLOATING_QA_MANIFEST")
	if path == "" {
		t.Skip("opt-in S10+ UI fixture")
	}
	h, executor := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.cfg.InstanceID, h.cfg.InstanceName = "mobile_floating_qa", "S10 Floating QA"
	h.cfg.Backends = []backend.Definition{{ID: "codex", Label: "Codex fixture", DefaultModel: "fixture",
		Models:             []backend.Model{{ID: "fixture", Label: "Fixture", SupportedReasoningEfforts: []string{"low", "medium", "high"}, DefaultReasoningEffort: "medium"}},
		Capabilities:       backend.Capabilities{History: true, Usage: true, Images: true, Files: true, Sandbox: true, Interactions: true, Steering: true},
		CollaborationModes: []backend.CollaborationMode{{Name: "Default", Mode: "default"}, {Name: "Plan", Mode: "plan"}},
	}}
	if err := h.pairing.Claim("floating-fixture-token", "floating-qa-device"); err != nil {
		t.Fatal(err)
	}
	projectRoot := h.cfg.DataDir
	if value := os.Getenv("BRIDGE_FLOATING_QA_PROJECT_ROOT"); value != "" {
		projectRoot = value
	}
	alpha, beta := filepath.Join(projectRoot, "Project-Alpha"), filepath.Join(projectRoot, "Project-Beta")
	for _, dir := range []string{alpha, beta} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	h.registry.Create("floating-ui", "QA History 90", alpha, "codex", "fixture", "read-only", "")
	h.registry.Create("floating-active", "QA Processing", alpha, "codex", "fixture", "read-only", "")
	h.registry.Create("floating-unread", "QA Unread", alpha, "codex", "fixture", "read-only", "")
	h.registry.Create("floating-beta", "QA Beta Draft", beta, "codex", "fixture", "read-only", "")
	provider := &floatingHistoryProvider{byResume: map[string][]map[string]any{}}
	for _, id := range []string{"floating-ui", "floating-active", "floating-unread", "floating-beta"} {
		s, _ := h.registry.Get(id)
		s.SetResumeID(id)
		for i := 1; i <= 90; i++ {
			role := "user"
			content := fmt.Sprintf("QA %s history %03d", id, i)
			message := map[string]any{"role": role, "content": content, "source_message_id": fmt.Sprintf("%s-%03d", id, i), "request_id": fmt.Sprintf("qa-history-%03d", (i+1)/2), "timestamp": float64(i)}
			if i%2 == 0 {
				message["role"] = "assistant"
				message["content"] = content + "\n\n**Markdown works**\n\n```js\nconst answer = 42;\n```"
			}
			if i == 90 {
				message["blocks"] = []map[string]any{{"type": "text", "text": message["content"]}, {"type": "thinking", "thinking": "QA visible summary fixture"}, {"type": "tool_call", "tool_use_id": "qa-tool", "name": "fixture-check", "command": "check sample", "output": "passed without executing a real tool"}}
			}
			provider.byResume[id] = append(provider.byResume[id], message)
		}
	}
	h.SetExecutor(&floatingHistoryExec{fakeExec: executor, provider: provider})
	h.updateRuntime("floating-active", "running", "qa-pending", 0, "", "")
	h.Emit(protocol.NewDone("floating-unread", "qa-unread-done"))
	var mu sync.Mutex
	records := []map[string]any{}
	executor.onSend = func(s *session.Session, id, content string) {
		entry, ok, err := h.messageQueue.Get(s.ID, id)
		if err != nil || !ok {
			return
		}
		var payload queuedPayload
		if json.Unmarshal(entry.Payload, &payload) != nil {
			return
		}
		images := []string{}
		for _, image := range payload.Images {
			raw, err := base64.StdEncoding.DecodeString(image.Data)
			if err != nil {
				continue
			}
			digest := sha256.Sum256(raw)
			images = append(images, hex.EncodeToString(digest[:]))
		}
		mu.Lock()
		records = append(records, map[string]any{"session_id": s.ID, "request_id": id, "image_sha256": images, "content": content})
		data, _ := json.Marshal(records)
		_ = os.WriteFile(path+".receipts.json", data, 0600)
		mu.Unlock()
		provider.append(s.ID, map[string]any{"role": "user", "content": content, "source_message_id": id + "-user", "request_id": id, "timestamp": float64(time.Now().Unix())})
		h.Emit(protocol.NewThinkingChunk(s.ID, id, "QA fixture is preparing a deterministic response."))
		if strings.Contains(content, "QA_WAIT") {
			time.Sleep(20 * time.Second)
		} else {
			time.Sleep(2 * time.Second)
		}
		h.Emit(protocol.NewTextChunk(s.ID, id, "MOCK_NATIVE_QA_RECEIVED: "+content))
		time.Sleep(time.Second)
		h.Emit(protocol.NewTextChunk(s.ID, id, "\nIMAGE_COUNT="+string(rune('0'+len(images)))))
		provider.append(s.ID, map[string]any{"role": "assistant", "content": "MOCK_NATIVE_QA_RECEIVED: " + content + "\nIMAGE_COUNT=" + string(rune('0'+len(images))), "source_message_id": id + "-assistant", "request_id": id, "timestamp": float64(time.Now().Unix())})
		h.Emit(protocol.NewDone(s.ID, id))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/media/", func(w http.ResponseWriter, r *http.Request) {
		// Device verification can read only this fixture's generated assets.
		file := filepath.Clean(strings.TrimPrefix(r.URL.Path, "/media"))
		relative, err := filepath.Rel(h.cfg.DataDir, file)
		if err != nil || !filepath.IsLocal(relative) {
			http.Error(w, "fixture assets only", http.StatusForbidden)
			return
		}
		http.ServeFile(w, r, file)
	})
	mux.HandleFunc("/", h.ServeWS)
	server := httptest.NewUnstartedServer(mux)
	if host := os.Getenv("BRIDGE_FLOATING_QA_BIND_HOST"); host != "" {
		ip := net.ParseIP(host)
		_, tailnet, _ := net.ParseCIDR("100.64.0.0/10")
		if ip == nil || !(ip.IsLoopback() || tailnet.Contains(ip)) {
			t.Fatal("fixture listener must be loopback or a Tailnet address")
		}
		listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
		if err != nil {
			t.Fatal(err)
		}
		_ = server.Listener.Close()
		server.Listener = listener
	}
	server.Start()
	defer server.Close()
	raw, _ := json.Marshal(map[string]string{"url": "ws" + strings.TrimPrefix(server.URL, "http"), "token": "floating-fixture-token", "session_id": "floating-ui", "authority": h.cfg.InstanceID})
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("MOBILE_FLOATING_QA_READY")
	deadline := time.Now().Add(90 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path + ".stop"); err == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("S10 fixture deadline reached")
}
