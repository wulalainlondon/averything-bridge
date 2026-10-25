package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"os"
	"testing"
	"time"
)

type lineCatalogProvider struct{ controllerTestProvider }

func (lineCatalogProvider) LineServiceEnabled() bool { return true }
func (lineCatalogProvider) ExecuteLineTool(context.Context, backend.SessionControlCaller, string, json.RawMessage) (any, error) {
	return nil, nil
}
func TestLineCatalogIsTypedAndExcludedFromDelegation(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.SetSessionControlProvider(lineCatalogProvider{})
	s := session.NewRegistry().Create("line-catalog", "LINE catalog fixture", t.TempDir(), backend.Codex, "", "read-only", "")
	p := map[string]any{}
	c.applyLineServiceTools(s, p)
	groups := p["dynamicTools"].([]map[string]any)
	if len(groups) != 1 || groups[0]["name"] != "bridge_line" {
		t.Fatal("namespace missing")
	}
	encoded, _ := json.Marshal(groups)
	for _, bad := range []string{"keychain_helper", "authToken", "data_dir", "rpc_method", "file_path"} {
		if containsJSONField(encoded, bad) {
			t.Fatalf("unrestricted SDK input exposed: %s", bad)
		}
	}
	s.ID = "s_dg_fixture"
	p = map[string]any{}
	c.applyLineServiceTools(s, p)
	if p["dynamicTools"] != nil {
		t.Fatal("delegation received LINE tools")
	}
}
func containsJSONField(encoded []byte, name string) bool {
	var nodes any
	_ = json.Unmarshal(encoded, &nodes)
	var walk func(any) bool
	walk = func(v any) bool {
		switch x := v.(type) {
		case map[string]any:
			for k, value := range x {
				if k == name || walk(value) {
					return true
				}
			}
		case []any:
			for _, value := range x {
				if walk(value) {
					return true
				}
			}
		}
		return false
	}
	return walk(nodes)
}

// This opt-in creates and archives an empty thread. It starts no model turn,
// sends no LINE message and is catalog acceptance, not a native tool invocation.
func TestLineCatalogActualNativeProtocol(t *testing.T) {
	if os.Getenv("LINE_NATIVE_CATALOG_TEST") != "1" {
		t.Skip("opt-in empty fresh native thread only")
	}
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.SetDataDir(t.TempDir())
	c.SetSessionControlProvider(lineCatalogProvider{})
	if err := c.ensureServer(); err != nil {
		t.Fatal(err)
	}
	defer func() { c.startMu.Lock(); _ = c.stopServerLocked(); c.startMu.Unlock() }()
	s := session.NewRegistry().Create("line-empty-native-contract", "LINE empty catalog contract", t.TempDir(), backend.Codex, "", "read-only", "")
	if err := c.ensureThread(s, c.state(s.ID)); err != nil {
		t.Fatal(err)
	}
	if s.ResumeID() == "" {
		t.Fatal("native thread ID missing")
	}
	if _, err := c.rpcCall("thread/archive", map[string]any{"threadId": s.ResumeID()}, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	t.Log("actual native protocol accepted and archived LINE catalog; no model turn or LINE effect")
}
