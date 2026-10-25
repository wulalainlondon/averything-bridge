package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/mediaservice"
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

// Owned mock-model fixture. Real installed native executor + real CLI + private
// socket are exercised; there is no paid model or production playback effect.
func TestUnifiedMediaInstalledNativeOrigin(t *testing.T) {
	if os.Getenv("BRIDGE_MEDIA_EXECUTOR_PROBE") != "1" {
		t.Skip("opt-in native executor/CLI fixture")
	}
	binary := os.Getenv("BRIDGE_MEDIA_PROBE_EXECUTABLE")
	if !filepath.IsAbs(binary) {
		t.Fatal("explicit fixture binary required")
	}
	var called atomic.Int32
	var c *Codex
	manifest := filepath.Join(t.TempDir(), "private", "manifest.json")
	channel, err := mediaservice.OpenServer(t.TempDir(), "fixture-instance", manifest, func(ctx context.Context, r mediaservice.Request) (mediaservice.Result, error) {
		deadline := time.NewTimer(3 * time.Second)
		defer deadline.Stop()
		for {
			if _, e := c.ResolveMediaOrigin(r.Thread, r.ID); e == nil {
				called.Add(1)
				return mediaservice.Result{VoiceMediaResult: backend.VoiceMediaResult{State: "fixture_bound_cli", Transport: "local_user_socket"}}, nil
			}
			select {
			case <-ctx.Done():
				return mediaservice.Result{}, ctx.Err()
			case <-deadline.C:
				return mediaservice.Result{}, fmt.Errorf("real native user/command origin was not observed")
			case <-time.After(20 * time.Millisecond):
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	wrapper := filepath.Join(t.TempDir(), "bridge-media")
	if err = os.WriteFile(wrapper, []byte("#!/bin/sh\nexec "+quote(binary)+" media --manifest "+quote(manifest)+" \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	command := quote(wrapper) + " --id vop_real_executor_origin --method status"
	cmdJSON, _ := json.Marshal(command)
	code := "const r=await tools.exec_command({cmd:" + string(cmdJSON) + ",max_output_tokens:1000});text(r.output);"
	var requests atomic.Int32
	var warmup atomic.Bool
	warmup.Store(true)
	observed := make(chan bool, 1)
	toolsSeen := make(chan bool, 1)
	fixedInstructionsSeen := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			http.Error(w, "invalid", 400)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if warmup.Load() {
			item := map[string]any{"type": "message", "id": "msg_fixture_warmup", "role": "assistant", "status": "completed", "content": []map[string]string{{"type": "output_text", "text": "Owned fixture initialized."}}}
			data, _ := json.Marshal(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item})
			fmt.Fprintf(w, "event: response.output_item.done\ndata: %s\n\n", data)
			fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fixture_warmup\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n")
			return
		}

		if requests.Add(1) == 1 {
			fixedInstructionsSeen <- strings.Contains(string(body["input"]), "VOICE_MEDIA_FIXED_ENTRY_FIXTURE")
			toolsSeen <- strings.Contains(string(body["input"]), "bridge_media") || strings.Contains(string(body["tools"]), "bridge_media")
			item := map[string]any{"type": "custom_tool_call", "id": "call_voice_media_fixture", "call_id": "call_voice_media_fixture", "namespace": "functions", "name": "exec", "input": code}
			data, _ := json.Marshal(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item})
			fmt.Fprintf(w, "event: response.output_item.done\ndata: %s\n\n", data)
		} else {
			observed <- strings.Contains(string(body["input"]), "fixture_bound_cli")
		}
		fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_voice_media_fixture\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n")
	}))
	defer server.Close()
	c = NewCodex(&caplessSink{}, "codex")
	c.mediaOrigins.launcher = wrapper
	c.nativeFixtureObserver = func(raw json.RawMessage) {
		var event struct {
			Method string
			Params map[string]json.RawMessage
		}
		if json.Unmarshal(raw, &event) != nil {
			return
		}
		if strings.Contains(event.Method, "item/") || strings.Contains(event.Method, "exec") || strings.Contains(event.Method, "tool") {
			keys := []string{}
			for k := range event.Params {
				keys = append(keys, k)
			}
			t.Logf("owned fixture event method=%s keys=%v item=%s", event.Method, keys, event.Params["item"])
		}
	}
	c.remoteReconnect = false
	c.appServerMode = "daemon"
	c.SetDataDir(t.TempDir())
	if err = c.startRemoteServerLocked(filepath.Dir(c.sessionsRoot)); err != nil {
		t.Fatal(err)
	}
	defer func() { c.remoteCancel(); c.remoteConn.CloseNow() }()
	raw, err := c.rpc.request("config/read", map[string]any{"includeLayers": false}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Config struct {
			MCP map[string]json.RawMessage `json:"mcp_servers"`
		} `json:"config"`
	}
	json.Unmarshal(raw, &cfg)
	names := []string{}
	for name := range cfg.Config.MCP {
		names = append(names, name)
	}
	config := codexPMConfig(names)
	features := config["features"].(map[string]any)
	features["code_mode"] = map[string]any{"enabled": true, "excluded_tool_namespaces": []string{}}
	features["unified_exec"] = true
	features["shell_tool"] = true
	features["enable_request_compression"] = false
	config["model_providers"] = map[string]any{"voice_media_fixture": map[string]any{"name": "Owned local voice media fixture", "base_url": server.URL, "wire_api": "responses", "requires_openai_auth": false}}
	params := map[string]any{"cwd": t.TempDir(), "ephemeral": false, "historyMode": "legacy", "model": "gpt-5.6-sol", "modelProvider": "voice_media_fixture", "approvalPolicy": "never", "sandbox": "danger-full-access", "config": config, "baseInstructions": "Owned protocol fixture only; no real model request. Execute the supplied local fixture command."}
	fixtureCwd := params["cwd"].(string)
	if err = os.WriteFile(filepath.Join(fixtureCwd, "AGENTS.md"), []byte("Owned fixture initial project instructions.\n"), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err = c.rpc.request("thread/start", params, 25*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := extractThreadID(raw, "")
	if id == "" {
		t.Fatal("missing fixturethread")
	}
	defer c.rpc.request("thread/archive", map[string]any{"threadId": id}, 5*time.Second)
	// A persisted original thread requires one actual (owned local mock) turn.
	initial, err := c.rpc.request("turn/start", map[string]any{"threadId": id, "input": []map[string]string{{"type": "text", "text": "Initialize owned fixture only."}}}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var warm struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	json.Unmarshal(initial, &warm)
	deadline := time.Now().Add(20 * time.Second)
	completed := false
	for time.Now().Before(deadline) {
		raw, readErr := c.rpc.request("thread/read", map[string]any{"threadId": id, "includeTurns": true}, 5*time.Second)
		if readErr != nil {
			t.Fatal(readErr)
		}
		var state struct {
			Thread struct {
				Turns []struct {
					ID     string `json:"id"`
					Status string `json:"status"`
				} `json:"turns"`
			} `json:"thread"`
		}
		json.Unmarshal(raw, &state)
		for _, turn := range state.Thread.Turns {
			if turn.ID == warm.Turn.ID && turn.Status == "completed" {
				completed = true
			}
		}
		if completed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !completed {
		t.Fatal("warmup did not finish")
	}
	warmup.Store(false)
	if err = os.WriteFile(filepath.Join(fixtureCwd, "AGENTS.md"), []byte("VOICE_MEDIA_FIXED_ENTRY_FIXTURE\nUse the fixed bridge-media entry through existing exec_command.\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// Reproduce the previous, silently ignored late dynamicTools registration.
	s := session.NewRegistry().Create("fixture", "fixture", t.TempDir(), backend.Codex, "", "", id)
	c.SetSessionControlProvider(mediaCatalogProvider{})
	resume := map[string]any{"threadId": id, "excludeTurns": true}
	c.applyVoiceMediaTools(s, resume)
	if _, err = c.rpc.request("thread/resume", resume, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	if err = c.claimActiveThread(id, s); err != nil {
		t.Fatal(err)
	}
	st := c.state(s.ID)
	st.mu.Lock()
	st.threadID = id
	st.mu.Unlock()
	// Synthetic provider handoff seed; the native userMessage and actual command
	// execution event below are real. This does NOT prove live voice handoff.
	mediaHandoff(c, s, "fixture-voice", "fixture-generation", "probe", "fixture-handoff")
	turn, err := c.rpc.request("turn/start", map[string]any{"threadId": id, "input": []map[string]string{{"type": "text", "text": "<realtime_delegation><input>probe</input></realtime_delegation>"}}}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var started struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	json.Unmarshal(turn, &started)
	defer c.rpc.request("turn/interrupt", map[string]any{"threadId": id, "turnId": started.Turn.ID}, 5*time.Second)
	select {
	case present := <-toolsSeen:
		if present {
			t.Fatal("native late registration behaviour changed; inspect baseline")
		}
		t.Log("LEGACY_DYNAMIC_TOOLS_UPDATE_ABSENT: real native model input has no bridge_media after resume ACK")
	case <-time.After(30 * time.Second):
		t.Fatal("no native model request")
	}
	select {
	case present := <-fixedInstructionsSeen:
		// Observe this separately: the fixed CLI execution does not depend on
		// hot reloading AGENTS.md. Voice startup instructions carry its stable
		// entry point and the original conversation can retain it in context.
		t.Logf("LEGACY_THREAD_AGENTS_REFRESH_AVAILABLE=%v (not relied on)", present)
	case <-time.After(5 * time.Second):
		t.Fatal("no fixed instruction observation")
	}
	select {
	case ok := <-observed:
		if !ok || called.Load() != 1 {
			c.mediaOrigins.mu.Lock()
			t.Logf("origin handoffs=%d turns=%d commands=%d blocked=%v", len(c.mediaOrigins.handoffs), len(c.mediaOrigins.turns), len(c.mediaOrigins.commands), c.mediaOrigins.blocked)
			c.mediaOrigins.mu.Unlock()
			fixtureRead, readErr := c.rpc.request("thread/read", map[string]any{"threadId": id, "includeTurns": true}, 5*time.Second)
			if readErr == nil {
				t.Logf("owned fixture timeline: %s", fixtureRead)
			}
			t.Fatal("actual native exec did not return bound CLI result", called.Load())
		}
		t.Log("NATIVE_COMMAND_ORIGIN_TO_FIXED_CLI: PASS; actual user/command events, synthetic handoff seed, no production playback")
	case <-time.After(30 * time.Second):
		t.Fatal("native CLI result not observed")
	}
}
