package goexec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Captures only the tool catalog sent to an OWNED loopback mock model. It
// connects to the existing daemon; no daemon lifecycle/config writes occur.
func TestCodexPMToolCatalogProbe(t *testing.T) {
	if os.Getenv("BRIDGE_CODEX_PM_PROBE") != "1" {
		t.Skip("opt-in installed daemon tool policy probe")
	}
	seen := make(chan json.RawMessage, 8)
	var count atomic.Int32
	executed := make(chan bool, 1)
	directDenied := make(chan bool, 1)
	patchDenied := make(chan bool, 1)
	marker := filepath.Join(t.TempDir(), "PM_MUST_NOT_WRITE.txt")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		keys := []string{}
		for key := range body {
			keys = append(keys, key)
		}
		t.Logf("MOCK_REQUEST path=%s keys=%v", r.URL.Path, keys)
		t.Logf("MOCK_POLICY choice=%s prompt_has_pm=%v prompt_has_patch=%v prompt_has_exec=%v", body["tool_choice"], bytes.Contains(body["input"], []byte("bridge_pm")), bytes.Contains(body["input"], []byte("apply_patch")), bytes.Contains(body["input"], []byte("exec_command")))
		for _, name := range []string{"bridge_pm", "exec_command", "apply_patch"} {
			if i := bytes.Index(body["input"], []byte(name)); i >= 0 {
				end := i + 420
				if end > len(body["input"]) {
					end = len(body["input"])
				}
				t.Logf("TOOL_CONTEXT %s %s", name, body["input"][i:end])
			}
		}
		select {
		case seen <- body["input"]:
		default:
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if count.Add(1) == 1 {
			item := map[string]any{"type": "custom_tool_call", "id": "call_pm_boundary", "call_id": "call_pm_boundary", "namespace": "functions", "name": "exec", "input": `text({boundary:"PM_BOUNDARY", exec:typeof tools.exec_command, patch:typeof tools.apply_patch, fs:typeof require, process:typeof process, fetch:typeof fetch, allowed:ALL_TOOLS.map(t=>t.name)});`}
			event, _ := json.Marshal(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item})
			fmt.Fprintf(w, "event: response.output_item.done\ndata: %s\n\n", event)
			direct := map[string]any{"type": "function_call", "id": "call_pm_direct", "call_id": "call_pm_direct", "namespace": "functions", "name": "exec_command", "arguments": `{"cmd":"true"}`}
			event, _ = json.Marshal(map[string]any{"type": "response.output_item.done", "output_index": 1, "item": direct})
			fmt.Fprintf(w, "event: response.output_item.done\ndata: %s\n\n", event)
			patch := map[string]any{"type": "custom_tool_call", "id": "call_pm_patch", "call_id": "call_pm_patch", "namespace": "functions", "name": "apply_patch", "input": "*** Begin Patch\n*** Add File: " + marker + "\n+probe\n*** End Patch"}
			event, _ = json.Marshal(map[string]any{"type": "response.output_item.done", "output_index": 2, "item": patch})
			fmt.Fprintf(w, "event: response.output_item.done\ndata: %s\n\n", event)
		} else {
			var items []map[string]any
			_ = json.Unmarshal(body["input"], &items)
			for _, item := range items {
				if item["type"] == "function_call_output" && item["call_id"] == "call_pm_direct" {
					b, _ := json.Marshal(item["output"])
					t.Logf("DIRECT_RESULT %s", b)
					directDenied <- bytes.Contains(b, []byte("unsupported")) || bytes.Contains(b, []byte("Unknown")) || bytes.Contains(b, []byte("unknown"))
				}
				if item["type"] == "custom_tool_call_output" && item["call_id"] == "call_pm_patch" {
					b, _ := json.Marshal(item["output"])
					t.Logf("DIRECT_PATCH_RESULT %s", b)
					patchDenied <- bytes.Contains(b, []byte("unsupported")) || bytes.Contains(b, []byte("writing is blocked by read-only sandbox"))
				}
				if item["type"] == "custom_tool_call_output" && item["call_id"] == "call_pm_boundary" {
					b, _ := json.Marshal(item["output"])
					t.Logf("EXECUTOR_RESULT %s", b)
					executed <- bytes.Contains(b, []byte("PM_BOUNDARY")) && bytes.Contains(b, []byte(`exec\":\"undefined`)) && bytes.Contains(b, []byte(`patch\":\"undefined`))
				}
			}
		}
		fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_pm_probe\",\"object\":\"response\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n")
	}))
	defer server.Close()
	c := NewCodex(&caplessSink{}, "codex")
	c.remoteReconnect = false
	c.appServerMode = "daemon"
	c.SetDataDir(t.TempDir())
	if err := c.startRemoteServerLocked(filepath.Dir(c.sessionsRoot)); err != nil {
		t.Fatal(err)
	}
	defer func() { c.remoteCancel(); c.remoteConn.CloseNow() }()
	raw, err := c.rpc.request("config/read", map[string]any{"includeLayers": false}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var loaded struct {
		Config struct {
			MCP map[string]json.RawMessage `json:"mcp_servers"`
		} `json:"config"`
	}
	if err = json.Unmarshal(raw, &loaded); err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for name := range loaded.Config.MCP {
		names = append(names, name)
	}
	config := codexPMConfig(names)
	config["features"].(map[string]any)["enable_request_compression"] = false
	config["model_providers"] = map[string]any{"pm_probe": map[string]any{"name": "PM tool capture", "base_url": server.URL, "wire_api": "responses", "requires_openai_auth": false}}
	params := map[string]any{"model": "gpt-5.6-sol", "modelProvider": "pm_probe", "cwd": t.TempDir(), "ephemeral": true, "approvalPolicy": "never", "sandbox": "read-only", "config": config, "baseInstructions": "Reply OK. Do not call any tools.", "dynamicTools": []map[string]any{{"type": "function", "name": "pm_read", "description": "Read PM context", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{}}}}}
	params["dynamicTools"] = []map[string]any{{"type": "namespace", "name": "bridge_pm", "description": "Only project coordination capabilities", "tools": []map[string]any{{"type": "function", "name": "project_get_context", "description": "Read PM context", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{}}}}}}
	raw, err = c.rpc.request("thread/start", params, 25*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := extractThreadID(raw, "")
	if id == "" {
		t.Fatal("no probe thread")
	}
	turn, err := c.rpc.request("turn/start", map[string]any{"threadId": id, "input": []map[string]string{{"type": "text", "text": "Reply OK."}}}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	_ = json.Unmarshal(turn, &response)
	defer func() {
		_, _ = c.rpc.request("turn/interrupt", map[string]string{"threadId": id, "turnId": response.Turn.ID}, 5*time.Second)
	}()
	select {
	case tools := <-seen:
		var list []map[string]any
		if err = json.Unmarshal(tools, &list); err != nil {
			t.Fatal(err)
		}
		for _, tool := range list {
			t.Logf("ASSEMBLED_TOOL type=%v name=%v", tool["type"], tool["name"])
			if tool["type"] == "additional_tools" {
				for _, ns := range tool["tools"].([]any) {
					n := ns.(map[string]any)
					t.Logf("NAMESPACE %v", n["name"])
					for _, item := range n["tools"].([]any) {
						t.Logf("TOOL %v", item.(map[string]any)["name"])
					}
				}
			}
		}
		if len(list) == 0 {
			t.Fatal("no tools captured")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("mock model did not receive a request")
	}
	select {
	case ok := <-executed:
		if !ok {
			t.Fatal("executor boundary failed")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("executor probe did not run")
	}
	select {
	case ok := <-directDenied:
		if !ok {
			t.Fatal("direct native execution was not denied")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no direct denial result")
	}
	select {
	case ok := <-patchDenied:
		if !ok {
			t.Fatal("direct patch capability not denied")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no direct patch denial")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("probe wrote project file", err)
	}
}
