package core

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"github.com/coder/websocket"
)

func TestIntakeAuthenticatedOnceAndFrozenSource(t *testing.T) {
	root := t.TempDir()
	pair := governance.NewPairing(filepath.Join(root, "pairing.json"))
	pair.Claim("isolated-intake-token", "fixturedevice")
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", RootDir: root, DataDir: root}, pair, 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	defer h.dispatches.Close()
	s := h.registry.Create("s_intake", "Current", root, backend.Codex, "gpt-6.1-sol", "workspace-write", "")
	s.SetResumeID("thread-original")
	original := s.SettingsSnapshot()
	started := make(chan string, 2)
	release := make(chan struct{})
	var sends atomic.Int32
	h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, r, c string) {
		sends.Add(1)
		if c != "修正清單" {
			t.Errorf("goalwaswrapped:%q", c)
		}
		started <- r
		<-release
	}})
	conn, ctx, cleanup := dialWS(t, h)
	defer cleanup()
	send := func(v any) {
		raw, _ := json.Marshal(v)
		if e := conn.Write(ctx, websocket.MessageText, raw); e != nil {
			t.Fatal(e)
		}
	}
	receive := func(kind string) map[string]any {
		for {
			v := readEvent(t, ctx, conn)
			if v["type"] == kind {
				return v
			}
		}
	}
	send(map[string]any{"type": "hello", "auth_token": "isolated-intake-token", "device_id": "fixturedevice", "protocol_version": 3})
	receive("hello_ack")
	call := func(op string, input any, key string) map[string]any {
		v := map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "intake-fixture", "operation": op, "input": input}
		if key != "" {
			v["idempotency_key"] = key
		}
		send(v)
		out := receive("task_api_response")
		raw, _ := json.Marshal(out)
		c, _ := contract.New()
		if e := c.Validate(raw, "Response"); e != nil {
			t.Fatalf("invalidresponse:%s:%v", raw, e)
		}
		return out
	}
	caps := call("capabilities", map[string]any{}, "")
	if caps["ok"] != true {
		t.Fatal(caps)
	}
	inspect := call("intake", map[string]any{"action": "inspect", "session_id": s.ID}, "")
	if inspect["ok"] != true || inspect["result"].(map[string]any)["status"] != "available" {
		t.Fatal(inspect)
	}
	target := inspect["result"].(map[string]any)["target"]
	key := "r_" + strings.Repeat("1", 32)
	input := map[string]any{"action": "submit", "submission_request_id": key, "target": target, "goal": "修正清單", "images": []any{map[string]string{"data": "AQID", "media_type": "image/png"}}}
	first := call("intake", input, key)
	if first["ok"] != true {
		t.Fatal(first)
	}
	r := first["result"].(map[string]any)
	if <-started != key {
		t.Fatal("executionrequestchanged")
	}
	again := call("intake", input, key)
	if again["ok"] != true || again["result"].(map[string]any)["receipt_id"] != r["receipt_id"] {
		t.Fatal(again)
	}
	entry, found, e := h.messageQueue.Get(s.ID, key)
	if e != nil || !found {
		t.Fatal(e)
	}
	var payload queuedPayload
	json.Unmarshal(entry.Payload, &payload)
	if payload.Content != "修正清單" || payload.Origin != nil || payload.OwnerDevice != "fixturedevice" || payload.MessagePurpose != "instruction" || len(payload.Images) != 1 {
		t.Fatal("rawinput/source/attachments lost", payload)
	}
	task := call("get", map[string]any{"task_id": r["task_id"]}, "")
	v := task["result"].(map[string]any)
	source := v["source"].(map[string]any)
	identity := source["identity"].(map[string]any)
	if source["binding_kind"] != "paired_human" || identity["request_id"] != key || identity["session_id"] != s.ID || identity["native_turn_id"] != nil {
		t.Fatal("forged/unboundsource", source)
	}
	if v["axes"].(map[string]any)["execution"] != "handoff" {
		t.Fatal("ACKassumednativeconsumed", v)
	}
	changed := map[string]any{}
	for k, v := range input {
		changed[k] = v
	}
	changed["goal"] = "other"
	conflict := call("intake", changed, key)
	if conflict["error"].(map[string]any)["code"] != "idempotency_conflict" {
		t.Fatal(conflict)
	}
	spoof := map[string]any{}
	for k, v := range input {
		spoof[k] = v
	}
	spoof["caller"] = map[string]string{"kind": "human"}
	bad := call("intake", spoof, key)
	if bad["ok"] == true {
		t.Fatal("clientprincipalaccepted")
	}
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: key, ThreadID: "thread-original", TurnID: "native-intake-turn"})
	task = call("get", map[string]any{"task_id": r["task_id"]}, "")
	if task["result"].(map[string]any)["axes"].(map[string]any)["execution"] != "native_consumed" {
		t.Fatal(task)
	}
	// Frozen queued secondinput can cancel through existing transactional barrier.
	next := "r_" + strings.Repeat("2", 32)
	waiting := map[string]any{"action": "submit", "submission_request_id": next, "target": target, "goal": "queuedgoal"}
	ack := call("intake", waiting, next)
	if ack["ok"] != true {
		t.Fatal(ack)
	}
	rr := ack["result"].(map[string]any)
	tv := call("get", map[string]any{"task_id": rr["task_id"]}, "")["result"].(map[string]any)
	cancel := call("cancel", map[string]any{"task_id": rr["task_id"], "command_receipt_id": rr["receipt_id"], "expected_revision": tv["revision"], "target": target, "mode": "queued"}, "fixture-intake-cancel")
	if cancel["ok"] != true {
		t.Fatal(cancel)
	}
	close(release)
	time.Sleep(20 * time.Millisecond)
	if sends.Load() != 1 {
		t.Fatal("duplicate/cancelledrequestexecuted", sends.Load())
	}
	if s.SettingsSnapshot().Model != original.Model || s.SettingsSnapshot().Sandbox != original.Sandbox {
		t.Fatal("currentexecutionconfigoverwritten")
	}
	again = call("get", map[string]any{"original_receipt": r["locator"]}, "")
	if again["ok"] != true {
		t.Fatal(again)
	}
}
func TestIntakeNotNativeToolOrHistoryBackfill(t *testing.T) {
	root := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", RootDir: root, DataDir: root}, governance.NewPairing(filepath.Join(root, "pair.json")), 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	defer h.dispatches.Close()
	s := h.registry.Create("s_fixture", "Fixture", root, backend.Codex, "gpt-6.1-sol", "read-only", "")
	s.SetResumeID("thread")
	input, _ := json.Marshal(map[string]any{"action": "submit", "submission_request_id": "r_" + strings.Repeat("a", 32), "target": map[string]any{"instance_id": "i1", "session_id": s.ID, "expected_native_thread_id": s.ResumeID(), "expected_config_revision": s.SettingsSnapshot().ConfigRevision}, "goal": "goal"})
	_, e := h.Authorize(context.Background(), taskapi.VerifiedContext{Authority: "i1", StableScopeID: "session:" + s.ID, BindingKind: "native_tool", Transport: "native_dynamic_tool"}, taskapi.Request{Operation: "intake", Input: input, IdempotencyKey: "r_" + strings.Repeat("a", 32)})
	if e == nil {
		t.Fatal("nativecalleracquiredpairedintake")
	}
	tools, e := contract.ToolInputs()
	if e != nil {
		t.Fatal(e)
	}
	if tools["task_intake"] != nil {
		t.Fatal("intakeexposedtomodeltool")
	}
	// No ordinarymessage may become a formal intake by retrying its oldID.
	id := "r_" + strings.Repeat("a", 32)
	h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: id, Content: "old question", Payload: []byte(`{"content":"old question"}`)})
	_, e = h.Authorize(context.Background(), taskapi.VerifiedContext{Authority: "i1", StableScopeID: "device:fixture", BindingKind: "paired_human", Transport: "authenticated_ws"}, taskapi.Request{Operation: "intake", Input: input, IdempotencyKey: id})
	if e == nil {
		t.Fatal("backfilled oldordinaryqueue")
	}
}

func TestIntakeIdentityAndOwnerPolicy(t *testing.T) {
	root := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", RootDir: root, DataDir: root}, governance.NewPairing(filepath.Join(root, "pair.json")), 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	defer h.dispatches.Close()
	s := h.registry.Create("s_policy", "Current", root, backend.Codex, "gpt-6.1-sol", "read-only", "")
	s.SetResumeID("thread")
	caller := taskapi.VerifiedContext{Authority: "i1", StableScopeID: "device:owner", BindingKind: "paired_human", Transport: "authenticated_ws"}
	key := "r_" + strings.Repeat("b", 32)
	cases := []struct {
		name              string
		authority, thread string
		revision          uint64
		request           string
		want              string
	}{
		{"authority", "foreign", "thread", 0, key, "wrong_authority"},
		{"thread", "i1", "other", 0, key, "stale_revision"},
		{"revision", "i1", "thread", 9, key, "stale_revision"},
		{"wrongkey", "i1", "thread", 0, "r_" + strings.Repeat("c", 32), "invalid_argument"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]any{"action": "submit", "submission_request_id": tc.request, "target": map[string]any{"instance_id": tc.authority, "session_id": s.ID, "expected_native_thread_id": tc.thread, "expected_config_revision": tc.revision}, "goal": "goal"})
			_, e := h.Authorize(context.Background(), caller, taskapi.Request{Operation: "intake", Input: raw, IdempotencyKey: key})
			if a, ok := e.(*taskapi.APIError); !ok || a.Code != tc.want {
				t.Fatal(e)
			}
		})
	}
	h.controls.MarkDesktopOwner(s.ID, "thread")
	raw, _ := json.Marshal(map[string]any{"action": "submit", "submission_request_id": key, "target": map[string]any{"instance_id": "i1", "session_id": s.ID, "expected_native_thread_id": "thread", "expected_config_revision": 0}, "goal": "goal"})
	_, e := h.Authorize(context.Background(), caller, taskapi.Request{Operation: "intake", Input: raw, IdempotencyKey: key})
	if a, ok := e.(*taskapi.APIError); !ok || a.Code != "permission" {
		t.Fatal("desktopowner bypass", e)
	}
	if _, found, _ := h.messageQueue.Get(s.ID, key); found {
		t.Fatal("refusedintake createdqueueeffect")
	}
}

func TestIntakeInheritedProfileProjectionAndUnicodeBound(t *testing.T) {
	root := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", RootDir: root, DataDir: root}, governance.NewPairing(filepath.Join(root, "pair.json")), 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	defer h.dispatches.Close()
	s := h.registry.Create("s_unicode", "Current", root, backend.Codex, "gpt-6.1-sol", "danger-full-access", "")
	s.SetResumeID("thread")
	goal := strings.Repeat("a\u0301", 12000)
	source := map[string]any{"instance_id": "i1", "session_id": s.ID, "request_id": "r_original", "native_thread_id": "thread", "config_revision": uint64(0)}
	meta, _ := json.Marshal(apiMetadata{Request: taskapi.Request{Operation: "intake", Input: []byte(`{}`)}, Caller: taskapi.VerifiedContext{BindingKind: "paired_human"}, Intake: &intakeMetadata{Source: source, Goal: goal, Configuration: session.ConfigurationFrom(s.SettingsSnapshot())}})
	c := taskapi.AuthorizedCommand{Caller: taskapi.VerifiedContext{Authority: "i1", StableScopeID: "device:fixture"}, Namespace: taskapi.Namespace{Authority: "i1", StableScopeID: "device:fixture", Path: "ordinary", Operation: "intake"}, Request: taskapi.Request{Version: contract.Version, Operation: "intake", Input: []byte(`{}`), IdempotencyKey: "fixture-uni-key"}, Locator: taskapi.Locator{Authority: "i1", Path: "ordinary", Operation: "intake", Key: "fixture-uni-key"}, IntentHash: strings.Repeat("a", 64)}
	r := taskapi.IntentRecord{ReceiptID: "receipt", TaskID: apiID("i1", "ordinary", s.ID+"/r_original"), NativeID: s.ID + "/r_original", SessionID: s.ID, RequestID: "r_original", Metadata: meta}
	h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: "r_original", Payload: []byte(`{"content":"original"}`), API: &messagequeue.APIAdmission{Command: c, Record: r}})
	record, e := h.messageQueue.TaskJournal().Lookup(context.Background(), c.Namespace, c.Request.IdempotencyKey)
	if e != nil {
		t.Fatal(e)
	}
	task, e := h.apiTask(context.Background(), record)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(task)
	compiler, _ := contract.New()
	if e := compiler.Validate(raw, "Task"); e != nil {
		t.Fatal("Unicode projection invalid schema", e)
	}
	if len([]rune(task["goal"].(string))) != 4096 {
		t.Fatal("wrong codepoint displaybound")
	}
	target := task["target"].(map[string]any)
	if target["native_thread_id"] != "thread" || target["config_revision"] != uint64(0) {
		t.Fatal("lost admitted target DTO", target)
	}
	var saved apiMetadata
	json.Unmarshal(record.Metadata, &saved)
	if saved.Intake.Goal != goal {
		t.Fatal("rawgoal truncated")
	}
}
