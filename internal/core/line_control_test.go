package core

import (
	"bytes"
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/linebridge"
	"everything-go/internal/linecontrol"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fixtureLineOrigin struct{ caller backend.SessionControlCaller }

func (f fixtureLineOrigin) ResolveLineCaller(_, _, _ string) (backend.SessionControlCaller, error) {
	return f.caller, nil
}
func TestLINEFixedEntryPreservesOriginalConversationGrants(t *testing.T) {
	h, _ := newControlTestHub(t, t.TempDir())
	store, a := testLineCatalog(h, t)
	ctx := context.Background()
	s := h.registry.Create("line-cli-parent", "parent", t.TempDir(), backend.Codex, "", "read-only", "native-line-cli")
	origin := fixtureLineOrigin{backend.SessionControlCaller{Parent: s, RequestID: "original-request", TurnID: "original-turn", ToolCallID: "actual-native-command"}}
	q := linecontrol.Request{ID: "lop_original_accounts", Thread: s.ResumeID(), Method: "accounts", Params: json.RawMessage(`{}`)}
	got, e := h.executeLineControl(ctx, origin, q)
	if e != nil || !got.OK || len(got.Result.([]linebridge.Account)) != 0 {
		t.Fatal("ungranted account leaked", got, e)
	}
	if e = store.Grant(ctx, linebridge.Caller{Subject: "device:fixture-device"}, a.ID, "session:"+s.ID, []string{"status"}); e != nil {
		t.Fatal(e)
	}
	got, e = h.executeLineControl(ctx, origin, q)
	if e != nil || !got.OK || len(got.Result.([]linebridge.Account)) != 1 {
		t.Fatal("existing grant unavailable", got, e)
	}
	q.Params = json.RawMessage(`{"method":"send_message"}`)
	got, e = h.executeLineControl(ctx, origin, q)
	if e != nil || got.OK {
		t.Fatal("method smuggling accepted", got, e)
	}
	q.Thread = "wrong-native-thread"
	if _, e = h.executeLineControl(ctx, origin, q); e == nil {
		t.Fatal("foreign native thread accepted")
	}
}
func TestLINEFixedEntryWillNotAcceptClientChosenWriteIntent(t *testing.T) {
	h, _ := newControlTestHub(t, t.TempDir())
	testLineCatalog(h, t)
	s := h.registry.Create("line-cli-parent", "parent", t.TempDir(), backend.Codex, "", "read-only", "native-line-cli")
	origin := fixtureLineOrigin{backend.SessionControlCaller{Parent: s, RequestID: "original", TurnID: "turn", ToolCallID: "native-command"}}
	q := linecontrol.Request{ID: "lop_original_write", Thread: s.ResumeID(), Method: "send_message", Params: json.RawMessage(`{"intent_key":"foreign","content":{"text":"unit fixture only"}}`)}
	if _, e := h.executeLineControl(context.Background(), origin, q); e == nil {
		t.Fatal("caller-chosen cross-source intent accepted")
	}
}

// This driver is an in-memory fixture; no LINE connection or message is made.
type lineCLITestDriver struct{ sends atomic.Int32 }

func (d *lineCLITestDriver) Close() error { return nil }
func (d *lineCLITestDriver) Invoke(_ context.Context, method string, _ any, out any) error {
	var value any
	switch method {
	case "resume":
		value = linebridge.Identity{Principal: "u" + strings.Repeat("a", 32), Name: "fixture"}
	case "send_message":
		d.sends.Add(1)
		value = map[string]string{"message_id": "unit-only"}
	default:
		return linebridge.Fail("line_fixture_method_invalid")
	}
	raw, _ := json.Marshal(value)
	return json.Unmarshal(raw, out)
}

type lineCLITestFactory struct{ driver *lineCLITestDriver }

func (f lineCLITestFactory) Start(context.Context, linebridge.Account, string, func(linebridge.RuntimeEvent)) (linebridge.Driver, error) {
	return f.driver, nil
}
func TestLINEFixedWriteIntentSurvivesNewNativeCommandWithoutResend(t *testing.T) {
	h, _ := newControlTestHub(t, t.TempDir())
	ctx := context.Background()
	store, e := linebridge.OpenStore(t.TempDir(), "fixture", bytes.Repeat([]byte{3}, 32))
	if e != nil {
		t.Fatal(e)
	}
	driver := &lineCLITestDriver{}
	service := linebridge.NewService(store, lineCLITestFactory{driver}, linebridge.Config{})
	h.SetLineService(service)
	t.Cleanup(func() { service.Close(); store.Close() })
	a, e := store.CreateAccount(ctx, "fixture", "device:fixture-owner", "/fixture/profile", "/fixture/helper")
	if e != nil {
		t.Fatal(e)
	}
	if e = store.BindIdentity(ctx, a.ID, linebridge.Identity{Principal: "u" + strings.Repeat("a", 32), Name: "fixture"}); e != nil {
		t.Fatal(e)
	}
	target, e := store.SaveTarget(ctx, a.ID, "unit-target", "fixture", "user")
	if e != nil {
		t.Fatal(e)
	}
	source := h.registry.Create("line-write-parent", "parent", t.TempDir(), backend.Codex, "", "read-only", "native-original")
	if e = store.Grant(ctx, linebridge.Caller{Subject: "device:fixture-owner"}, a.ID, "session:"+source.ID, []string{"status", "messages.send"}); e != nil {
		t.Fatal(e)
	}
	args, _ := json.Marshal(map[string]any{"account_ref": a.ID, "target_ref": target.Ref, "content": map[string]string{"text": "unit fixture only"}})
	q := linecontrol.Request{ID: "lop_same_original_intent", Thread: source.ResumeID(), Method: "send_message", Params: args}
	origin := fixtureLineOrigin{backend.SessionControlCaller{Parent: source, RequestID: "request", TurnID: "turn", ToolCallID: "native-first"}}
	result, e := h.executeLineControl(ctx, origin, q)
	if e != nil || !result.OK {
		t.Fatal(result, e)
	}
	first := result.Result.(linebridge.Operation)
	deadline := time.Now().Add(3 * time.Second)
	for driver.sends.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	origin.caller.ToolCallID = "native-after-restart"
	result, e = h.executeLineControl(ctx, origin, q)
	if e != nil || !result.OK || result.Result.(linebridge.Operation).ID != first.ID {
		t.Fatal(result, e)
	}
	if driver.sends.Load() != 1 {
		t.Fatal("original intent dispatched more than once")
	}

	lookup, _ := json.Marshal(map[string]string{"account_ref": a.ID, "original_intent": q.ID})
	probe := linecontrol.Request{ID: "lop_read_original_receipt", Thread: source.ResumeID(), Method: "intent_status", Params: lookup}
	observation, e := h.executeLineControl(ctx, origin, probe)
	if e != nil || !observation.OK {
		t.Fatal(observation, e)
	}
	observed := observation.Result.(map[string]any)
	if observed["found"] != true || observed["operation"].(linebridge.Operation).ID != first.ID || driver.sends.Load() != 1 {
		t.Fatal("lost receipt cannot be observed safely", observed)
	}
	changed, _ := json.Marshal(map[string]any{"account_ref": a.ID, "target_ref": target.Ref, "content": map[string]string{"text": "different fixture text"}})
	q.Params = changed
	result, e = h.executeLineControl(ctx, origin, q)
	if e != nil || result.OK || driver.sends.Load() != 1 {
		t.Fatal("intent payload changed", result, e)
	}
}
