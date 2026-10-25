package goexec

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func TestCodexCatalogAndSettingsPreserveSol6(t *testing.T) {
	socket, _ := healthServer(t, func(method, threadID string) (any, bool) {
		switch method {
		case "model/list":
			return map[string]any{"data": []map[string]any{
				{"id": "gpt-6-astra", "model": "gpt-6-astra", "displayName": "GPT-6-Astra", "isDefault": true},
				{"id": "gpt-6-sol", "model": "gpt-6-sol", "displayName": "GPT-6-Sol", "defaultReasoningEffort": "medium", "inputModalities": []string{"text", "image"}, "supportedReasoningEfforts": []map[string]string{{"reasoningEffort": "medium"}, {"reasoningEffort": "ultra"}}},
			}}, true
		default:
			return map[string]any{}, true
		}
	})
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.appServerSocket = socket
	c.remoteReconnect = false
	if err := c.startRemoteServerLocked(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.startMu.Lock(); defer c.startMu.Unlock(); _ = c.stopServerLocked() })
	catalog, err := c.Catalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if catalog.DefaultModel != "gpt-6-astra" || len(catalog.Models) != 2 {
		t.Fatalf("runtime default/catalog was replaced: %+v", catalog)
	}
	sol := catalog.Models[1]
	if sol.ID != "gpt-6-sol" || sol.Label != "GPT-6-Sol" || !reflect.DeepEqual(sol.SupportedReasoningEfforts, []string{"medium", "ultra"}) {
		t.Fatalf("runtime capabilities not preserved: %+v", sol)
	}
	s := session.NewRegistry().Create("sol6-settings", "Sol 6 QA", t.TempDir(), backend.Codex, "gpt-6-sol", "read-only", "test-thread")
	s.SetEffort("medium")
	settings := NewCodex(&capSink{}, "codex")
	writer := &rpcCaptureWriter{writes: make(chan []byte, 1)}
	settings.rpc.setWriter(writer)
	done := make(chan error, 1)
	go func() { done <- settings.UpdateSessionSettings(context.Background(), s) }()
	select {
	case raw := <-writer.writes:
		var frame struct {
			ID     int            `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
		if frame.Method != "thread/settings/update" || frame.Params["model"] != "gpt-6-sol" || frame.Params["effort"] != "medium" {
			t.Fatalf("selection not sent to Codex: %+v", frame)
		}
		response, _ := json.Marshal(map[string]any{"id": frame.ID, "result": map[string]any{}})
		settings.rpc.dispatchResponse(response)
	case <-time.After(time.Second):
		t.Fatal("settings RPC missing")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
