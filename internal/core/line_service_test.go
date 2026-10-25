package core

import (
	"bytes"
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/linebridge"
	"everything-go/internal/session"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testLineCatalog(h *Hub, t *testing.T) (*linebridge.Store, linebridge.Account) {
	t.Helper()
	store, err := linebridge.OpenStore(t.TempDir(), "fixture-authority", bytes.Repeat([]byte{2}, 32))
	if err != nil {
		t.Fatal(err)
	}
	service := linebridge.NewService(store, nil, linebridge.Config{})
	h.SetLineService(service)
	t.Cleanup(func() { service.Close(); store.Close() })
	a, err := store.CreateAccount(context.Background(), "fixture LINE", "device:fixture-device", "/fixture/profile", "/fixture/helper")
	if err != nil {
		t.Fatal(err)
	}
	return store, a
}
func TestLineHTTPRequiresMatchingPairedDeviceAndBearer(t *testing.T) {
	h, _ := newTestHub(t)
	_, a := testLineCatalog(h, t)
	h.pairing.OpenEnrollment(0)
	if err := h.pairing.Claim("fixture-bearer", "fixture-device"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		token, device string
		status        int
	}{{"fixture-bearer", "fixture-device", 200}, {"fixture-bearer", "other", 401}, {"wrong", "fixture-device", 401}, {"", "fixture-device", 401}} {
		r := httptest.NewRequest(http.MethodPost, "/api/line/v1/request", strings.NewReader(`{"method":"accounts"}`))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		r.Header.Set("X-Bridge-Device-ID", tc.device)
		w := httptest.NewRecorder()
		h.ServeLineAPI(w, r)
		if w.Code != tc.status {
			t.Fatalf("status %d want %d", w.Code, tc.status)
		}
		if w.Code == 200 && !strings.Contains(w.Body.String(), a.ID) {
			t.Fatal("owner cannot see account")
		}
	}
}
func TestLineToolUsesRegisteredConversationGrantAndRejectsControlArguments(t *testing.T) {
	h, _ := newControlTestHub(t, t.TempDir())
	store, a := testLineCatalog(h, t)
	ctx := context.Background()
	s := h.registry.Create("line-parent", "parent", t.TempDir(), backend.Codex, "", "read-only", "fixture-thread")
	caller := backend.SessionControlCaller{Parent: s, ToolCallID: "fixture-native-call"}
	result, err := h.ExecuteLineTool(ctx, caller, "accounts", json.RawMessage(`{}`))
	if err != nil || len(result.([]linebridge.Account)) != 0 {
		t.Fatal("ungranted conversation sees accounts", err)
	}
	if err = store.Grant(ctx, linebridge.Caller{Subject: "device:fixture-device"}, a.ID, "session:"+s.ID, []string{"status"}); err != nil {
		t.Fatal(err)
	}
	result, err = h.ExecuteLineTool(ctx, caller, "accounts", json.RawMessage(`{}`))
	if err != nil || len(result.([]linebridge.Account)) != 1 {
		t.Fatal(err)
	}
	for _, method := range []string{"grant", "pair", "set_enabled", "create_account"} {
		if _, err = h.ExecuteLineTool(ctx, caller, method, json.RawMessage(`{}`)); err == nil {
			t.Fatalf("model management method allowed: %s", method)
		}
	}
	if _, err = h.ExecuteLineTool(ctx, caller, "accounts", json.RawMessage(`{"method":"send_message"}`)); err == nil {
		t.Fatal("method smuggling accepted")
	}
	// Reconstruct an equal-looking identity instead of copying Session's mutex.
	// A different object with the same logical/native IDs must still be refused.
	snap := s.Snapshot()
	copy := session.NewRegistry().Create(s.ID, snap.Name, snap.Cwd, snap.Backend, snap.Model, snap.Sandbox, snap.ResumeID)
	caller.Parent = copy
	if _, err = h.ExecuteLineTool(ctx, caller, "accounts", json.RawMessage(`{}`)); err == nil {
		t.Fatal("copied caller authorized")
	}
	child := h.registry.Create("s_dg_fixture", "child", t.TempDir(), backend.Codex, "", "read-only", "fixture-child-thread")
	caller.Parent = child
	if _, err = h.ExecuteLineTool(ctx, caller, "accounts", json.RawMessage(`{}`)); err == nil {
		t.Fatal("delegated child authorized")
	}
}
