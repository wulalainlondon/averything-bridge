package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// Opt-in real Go journal/pairing/WebSocket browser fixture. All mutations and
// transcripts are synthetic, all storage is t.TempDir, and no model is invoked.
func TestSessionReadUIHarness(t *testing.T) {
	manifest := os.Getenv("BRIDGE_READ_SYNC_QA_MANIFEST")
	if manifest == "" {
		t.Skip("opt-in shared-read browser fixture")
	}
	servers := []map[string]any{}
	for _, owner := range []string{"wulala", "morrie"} {
		dir := t.TempDir()
		pairing := governance.NewPairing(filepath.Join(dir, "pairing.json"))
		for _, device := range []string{"browser", "observer"} {
			pairing.OpenEnrollment(time.Minute)
			if err := pairing.Claim("read-qa-"+owner+"-"+device, "read-qa-"+device); err != nil {
				t.Fatal(err)
			}
		}
		h := NewHub(session.NewRegistry(), Config{DataDir: dir, InstanceID: "read-qa-" + owner, InstanceName: "Read QA " + owner,
			Backends: []backend.Definition{{ID: "codex", Label: "Fixture", DefaultModel: "fixture", Models: []backend.Model{{ID: "fixture", Label: "Fixture"}}, Capabilities: backend.Capabilities{History: true}}}}, pairing, 0)
		t.Cleanup(func() { h.messageQueue.Close() })
		provider := &floatingHistoryProvider{byResume: map[string][]map[string]any{}}
		h.SetExecutor(&floatingHistoryExec{fakeExec: &fakeExec{sink: h}, provider: provider})
		var mu sync.Mutex
		sequence := map[string]int{}
		latest := map[string]string{}
		appendBody := func(id, request string) {
			provider.append(id, map[string]any{"role": "assistant", "content": fmt.Sprintf("MOCK read-sync %s %s result %d", owner, id, sequence[id]),
				"source_message_id": request + "-assistant", "request_id": request, "timestamp": float64(time.Now().UnixMilli()) / 1000, "history_read_result_verified": true})
		}
		complete := func(id string, body bool) {
			sequence[id]++
			request := fmt.Sprintf("read-qa-%s-%s-%d", owner, id, sequence[id])
			latest[id] = request
			h.updateRuntime(id, "running", request, 0, "", "")
			if body {
				appendBody(id, request)
			}
			h.Emit(protocol.NewDone(id, request))
		}
		for _, id := range []string{"same", "peer", "race"} {
			h.registry.Create(id, "Read QA "+id, dir, "codex", "fixture", "read-only", id)
			provider.byResume[id] = []map[string]any{{"role": "user", "content": "MOCK fixture task, no model", "source_message_id": owner + "-" + id + "-user", "timestamp": float64(time.Now().UnixMilli()) / 1000}}
			complete(id, true)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/qa/state", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Content-Type", "application/json")
			views, err := h.runtimes.SharedSnapshot("read-qa-observer", []string{"same", "peer", "race"}, h.sharedReadDevices())
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			_ = json.NewEncoder(w).Encode(views)
		})
		mux.HandleFunc("/qa/complete", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "POST only", 405)
				return
			}
			id := r.URL.Query().Get("session")
			if _, ok := h.registry.Get(id); !ok {
				http.Error(w, "fixture session only", 404)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			complete(id, r.URL.Query().Get("body") != "false")
			w.WriteHeader(204)
		})
		mux.HandleFunc("/qa/flush", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "POST only", 405)
				return
			}
			id := r.URL.Query().Get("session")
			mu.Lock()
			defer mu.Unlock()
			if latest[id] == "" {
				http.Error(w, "fixture session only", 404)
				return
			}
			appendBody(id, latest[id])
			w.WriteHeader(204)
		})
		mux.HandleFunc("/qa/progress", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "POST only", 405)
				return
			}
			id := r.URL.Query().Get("session")
			mu.Lock()
			defer mu.Unlock()
			if latest[id] == "" {
				http.Error(w, "fixture session only", 404)
				return
			}
			provider.append(id, map[string]any{"role": "assistant", "content": "MOCK same-turn progress, completed answer is still flushing", "source_message_id": latest[id] + "-progress", "request_id": latest[id], "history_read_result_verified": false, "timestamp": float64(time.Now().UnixMilli()) / 1000})
			w.WriteHeader(204)
		})
		mux.HandleFunc("/", h.ServeWS)
		server := httptest.NewServer(mux)
		t.Cleanup(server.Close)
		servers = append(servers, map[string]any{"owner": owner, "authority": h.cfg.InstanceID, "url": "ws" + strings.TrimPrefix(server.URL, "http"), "http": server.URL,
			"token": "read-qa-" + owner + "-browser", "observer_token": "read-qa-" + owner + "-observer", "device_id": "read-qa-browser", "observer_device_id": "read-qa-observer", "data_dir": dir})
	}
	data, err := json.Marshal(map[string]any{"servers": servers})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("SESSION_READ_UI_QA_READY")
	deadline := time.NewTimer(20 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("shared-read fixture deadline reached")
		case <-ticker.C:
			if _, err := os.Stat(manifest + ".stop"); err == nil {
				return
			}
		}
	}
}
