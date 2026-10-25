package goexec

import (
	"bufio"
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Opt-in real native executor with an owned local mock provider. No paid model,
// real voice, playback, credential write or production-thread change is made.
func TestInstalledNativeVoicePermissionInheritance(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_RUN_VOICE_PERMISSION_PROBE") != "1" {
		t.Skip("opt-in owned native settings probe")
	}
	binary := os.Getenv("EVERYTHING_GO_VOICE_PERMISSION_PROBE_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("explicit installed binary required")
	}
	var counter atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := counter.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		item := map[string]any{"type": "message", "id": fmt.Sprintf("msg_permission_%d", n), "role": "assistant", "status": "completed", "content": []map[string]string{{"type": "output_text", "text": "Owned fixture OK."}}}
		data, _ := json.Marshal(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item})
		fmt.Fprintf(w, "event: response.output_item.done\ndata: %s\n\n", data)
		fmt.Fprintf(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_permission_%d\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n", n)
	}))
	defer server.Close()
	completed := make(chan string, 8)
	var target atomic.Value
	target.Store("")
	c := NewCodex(&capSink{}, binary)
	c.nativeFixtureObserver = func(raw json.RawMessage) {
		var e struct {
			Method string
			Params struct {
				ThreadID string `json:"threadId"`
				Turn     struct {
					ID string `json:"id"`
				} `json:"turn"`
			}
		}
		if json.Unmarshal(raw, &e) == nil && e.Method == "turn/completed" && e.Params.ThreadID == target.Load().(string) {
			completed <- e.Params.Turn.ID
		}
	}
	c.appServerMode = "daemon"
	c.remoteReconnect = false
	c.SetDataDir(t.TempDir())
	if err := c.startRemoteServerLocked(filepath.Dir(c.sessionsRoot)); err != nil {
		t.Fatal(err)
	}
	conn, done, cancel := c.remoteConn, c.remoteDone, c.remoteCancel
	t.Cleanup(func() {
		cancel()
		_ = conn.CloseNow()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("native probe connection did not close")
		}
	})
	c.runtimeDiagnostics = map[string]any{"codex": map[string]any{"running_version": "0.162.1"}}
	cwd := t.TempDir()
	cfgRaw, err := c.rpcCall("config/read", map[string]any{"includeLayers": false}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var loaded struct {
		Config struct {
			MCP map[string]json.RawMessage `json:"mcp_servers"`
		} `json:"config"`
	}
	json.Unmarshal(cfgRaw, &loaded)
	names := []string{}
	for name := range loaded.Config.MCP {
		names = append(names, name)
	}
	config := codexPMConfig(names)
	config["features"].(map[string]any)["enable_request_compression"] = false
	config["model_providers"] = map[string]any{"voice_permission_fixture": map[string]any{"name": "Owned local permission fixture", "base_url": server.URL, "wire_api": "responses", "requires_openai_auth": false}}
	raw, err := c.rpcCall("thread/start", map[string]any{"cwd": cwd, "ephemeral": false, "historyMode": "legacy", "sandbox": "workspace-write", "approvalPolicy": "never", "approvalsReviewer": "user", "model": "gpt-6.1-sol", "modelProvider": "voice_permission_fixture", "config": config, "baseInstructions": "Owned local permission fixture. No tools or real model requests."}, 25*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := extractThreadID(raw, "")
	if id == "" {
		t.Fatal("no owned fixture id")
	}
	target.Store(id)
	var initial struct {
		Provider string `json:"modelProvider"`
	}
	json.Unmarshal(raw, &initial)
	if initial.Provider != "voice_permission_fixture" {
		t.Fatal("local mock provider not selected")
	}
	archived := false
	t.Cleanup(func() {
		if !archived {
			if _, e := c.rpcCall("thread/archive", map[string]any{"threadId": id}, 15*time.Second); e != nil {
				t.Error("owned fixture cleanup", e)
			}
		}
	})
	runFixtureTurn := func() string {
		response, e := c.rpcCall("turn/start", map[string]any{"threadId": id, "input": []map[string]string{{"type": "text", "text": "Owned local fixture. Return OK."}}}, 15*time.Second)
		if e != nil {
			t.Fatal(e)
		}
		var v struct {
			Turn struct {
				ID string `json:"id"`
			} `json:"turn"`
		}
		json.Unmarshal(response, &v)
		if v.Turn.ID == "" {
			t.Fatal("no native fixture turn")
		}
		timer := time.NewTimer(25 * time.Second)
		defer timer.Stop()
		for {
			select {
			case actual := <-completed:
				if actual == v.Turn.ID {
					return actual
				}
			case <-timer.C:
				t.Fatal("local mock turn did not complete")
				return ""
			}
		}
	}
	s := session.NewRegistry().Create("voice-permission-native-probe", "owned settings fixture", cwd, backend.Codex, "", "read-only", id)
	c.state(s.ID).threadID = id
	c.mu.Lock()
	c.threadToSession[id] = s
	c.mu.Unlock()
	if e := c.syncVoiceSessionPermissions(context.Background(), s, id); e != nil {
		t.Fatal("fresh voice-first permission verification", e)
	}
	runFixtureTurn()            // Persist only after verifying the new thread works without a rollout.
	c.state(s.ID).threadID = "" // Exercise exact cold restoration on the same persisted identity.
	results := []map[string]any{}
	for i, mode := range []string{"danger-full-access", "read-only", "workspace-write"} {
		s.ApplyConfig("", "", mode)
		s.SetConfigRevision(uint64(i + 1))
		if i > 0 {
			if e := c.UpdateSessionSettings(context.Background(), s); e != nil {
				t.Fatal("UI settings policy sync", e)
			}
			check, e := c.rpcCall("thread/resume", map[string]any{"threadId": id, "excludeTurns": true}, 15*time.Second)
			if e != nil || !voicePermissionMetadataMatches(check, c.sessionPermissionSettings(id, s.Snapshot())) {
				t.Fatal("UI settings did not apply the session policy", e)
			}
		}
		if actual, e := c.voiceThreadContext(context.Background(), s); e != nil || actual != id {
			t.Fatal("voice preparation", mode, e)
		}
		read, e := c.rpcCall("thread/resume", map[string]any{"threadId": id, "excludeTurns": true}, 15*time.Second)
		if e != nil {
			t.Fatal(e)
		}
		var response struct {
			Sandbox  map[string]any `json:"sandbox"`
			Approval string         `json:"approvalPolicy"`
			Provider string         `json:"modelProvider"`
			Thread   struct {
				ID string `json:"id"`
			} `json:"thread"`
		}
		if json.Unmarshal(read, &response) != nil || response.Thread.ID != id {
			t.Fatal("fixture identity changed")
		}
		want := map[string]string{"danger-full-access": "dangerFullAccess", "read-only": "readOnly", "workspace-write": "workspaceWrite"}[mode]
		if response.Sandbox["type"] != want || response.Approval != "never" {
			t.Fatalf("native policy mismatch for %s: %v", mode, response.Sandbox)
		}
		if response.Provider != "voice_permission_fixture" {
			t.Fatal("provider changed away from local fixture")
		}
		if mode != "danger-full-access" && response.Sandbox["networkAccess"] != false {
			t.Fatal("restricted mode lost network boundary", response.Sandbox)
		}
		turn := runFixtureTurn()
		path := c.findCodexSessionFile(id)
		if path == "" {
			t.Fatal("fixture rollout unavailable")
		}
		f, e := os.Open(path)
		if e != nil {
			t.Fatal(e)
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 4096), 8<<20)
		var policy map[string]any
		for scanner.Scan() {
			var row struct {
				Type    string
				Payload map[string]json.RawMessage
			}
			if json.Unmarshal(scanner.Bytes(), &row) != nil || row.Type != "turn_context" {
				continue
			}
			var tid string
			json.Unmarshal(row.Payload["turn_id"], &tid)
			if tid == turn {
				json.Unmarshal(row.Payload["sandbox_policy"], &policy)
			}
		}
		scanErr := scanner.Err()
		f.Close()
		if scanErr != nil {
			t.Fatal(scanErr)
		}
		typ, _ := policy["type"].(string)
		normalized := strings.NewReplacer("-", "", "_", "").Replace(strings.ToLower(typ))
		if normalized != strings.ToLower(want) {
			t.Fatalf("native backing turn used wrong permissions for %s: %v", mode, policy)
		}
		results = append(results, map[string]any{"backing_turn_policy": policy, "session_mode": mode, "native_policy": response.Sandbox, "approval_policy": response.Approval, "same_thread": true, "warm": i > 0, "ui_settings_verified": i > 0})
	}
	if _, err = c.rpcCall("thread/archive", map[string]any{"threadId": id}, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	archived = true
	if out := os.Getenv("EVERYTHING_GO_VOICE_PERMISSION_PROBE_RESULT"); out != "" {
		b, _ := json.MarshalIndent(map[string]any{"thread_id": id, "results": results, "archived": archived, "fresh_voice_first_verified": true, "local_mock_model_requests": counter.Load(), "paid_model_or_realtime_calls": false}, "", "  ")
		if !filepath.IsAbs(out) || !strings.Contains(out, "voice-session-permissions-") {
			t.Fatal("scoped result path required")
		}
		if e := os.WriteFile(out, append(b, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Log("installed daemon readback preserved exact session modes across cold/warm voice preparation")
}
