package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"fmt"
	"github.com/coder/websocket"
	"path/filepath"
	"testing"
	"time"
)

func TestLegacySourceAuthenticatedWSAuditedNoBusinessEffectAndRevocable(t *testing.T) {
	root := t.TempDir()
	pair := governance.NewPairing(filepath.Join(root, "pairing.json"))
	pair.Claim("fixture-human-token", "human")
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", RootDir: root, DataDir: root}, pair, 0)
	defer h.messageQueue.Close()
	defer h.dispatches.Close()
	defer h.delegations.Close()
	h.taskService, _ = taskapi.NewService(h, h)
	parent := h.registry.Create("parent", "Parent", root, backend.Codex, "", "", "parent-thread")
	target := h.registry.Create("target", "Target", root, backend.Codex, "", "", "target-thread")
	entry, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: target.ID, RequestID: "r_original", Content: "Existing business request", Payload: []byte(`{"content":"Existing business request"}`)})
	if err != nil {
		t.Fatal(err)
	}
	conn, ctx, cleanup := dialWS(t, h)
	conn.SetReadLimit(contract.MaxBytes)
	defer cleanup()
	send := func(value any) {
		data, _ := json.Marshal(value)
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			t.Fatal(err)
		}
	}
	read := func(kind string) map[string]any {
		for {
			value := readEvent(t, ctx, conn)
			if value["type"] == kind {
				return value
			}
		}
	}
	send(map[string]any{"type": "hello", "device_id": "human", "auth_token": "fixture-human-token", "protocol_version": 3})
	read("hello_ack")
	apiCall := func(op string, input any, key string) map[string]any {
		frame := map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "legacy-fixture", "operation": op, "input": input}
		if key != "" {
			frame["idempotency_key"] = key
		}
		send(frame)
		reply := read("task_api_response")
		raw, _ := json.Marshal(reply)
		compiler, _ := contract.New()
		if err := compiler.Validate(raw, "Response"); err != nil {
			t.Fatalf("invalid source-intake response: %v / %s", err, raw)
		}
		return reply
	}
	call := func(input any, key string) map[string]any { return apiCall("legacy_source", input, key) }
	source := messagequeue.LegacySourceIdentity{InstanceID: "i1", SessionID: parent.ID, ThreadID: parent.ResumeID(), Revision: parent.SettingsSnapshot().ConfigRevision}
	dest := messagequeue.LegacySourceIdentity{InstanceID: "i1", SessionID: target.ID, ThreadID: target.ResumeID(), Revision: target.SettingsSnapshot().ConfigRevision}
	preview := call(map[string]any{"action": "preview", "source": source, "target": dest, "request_id": entry.RequestID}, "")
	if preview["ok"] != true {
		t.Fatal(preview)
	}
	row := preview["result"].(map[string]any)["preview"].(map[string]any)
	input := map[string]any{"action": "confirm", "source": source, "target": dest, "request_id": entry.RequestID, "receipt_hash": row["receipt_hash"], "expected_queue_revision": row["queue_revision"]}
	confirmed := call(input, "persisted-intake-fixture")
	if confirmed["ok"] != true {
		t.Fatal(confirmed)
	}
	receipt := confirmed["result"].(map[string]any)
	repeated := call(input, "persisted-intake-fixture")
	if repeated["ok"] != true || repeated["result"].(map[string]any)["receipt_id"] != receipt["receipt_id"] {
		t.Fatal("receipt changed", repeated)
	}
	conflict := map[string]any{}
	for k, v := range input {
		conflict[k] = v
	}
	conflict["receipt_hash"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if call(conflict, "persisted-intake-fixture")["error"].(map[string]any)["code"] != "idempotency_conflict" {
		t.Fatal("key conflict accepted")
	}
	after, found, err := h.messageQueue.Get(target.ID, entry.RequestID)
	if err != nil || !found || after.State != entry.State || after.PayloadHash != entry.PayloadHash || string(after.Payload) != string(entry.Payload) || target.QueueLen() != 0 {
		t.Fatal("intake changed original business effect", after, err)
	}
	if _, found, err := h.messageQueue.TaskAdmission(target.ID, entry.RequestID); err != nil || found {
		t.Fatal("intake forged original admission")
	}
	send(map[string]any{"type": "request_session_tasks", "session_id": parent.ID, "request_id": "children-read"})
	projection := read("session_tasks_snapshot")
	children := projection["children"].([]any)
	if len(children) != 1 {
		t.Fatal(projection)
	}
	child := children[0].(map[string]any)
	if child["state"] != "legacy_unknown" || child["transport"] != "human_linked" || child["can_cancel"] != false {
		t.Fatal("human assertion became execution/caller", child)
	}
	list := call(map[string]any{"action": "list", "session_id": parent.ID}, "")
	links := list["result"].(map[string]any)["links"].([]any)
	if len(links) != 1 {
		t.Fatal(list)
	}
	linkID := links[0].(map[string]any)["relation_id"].(string)
	snap := apiCall("snapshot", map[string]any{"session_id": parent.ID, "views": []string{"source_children"}, "limit": 1}, "")
	if snap["ok"] != true || len(snap["result"].(map[string]any)["items"].([]any)) != 1 {
		t.Fatal("source membership missing", snap)
	}
	eventCursor := snap["result"].(map[string]any)["events_cursor"]
	revoked := call(map[string]any{"action": "revoke", "relation_id": linkID}, "persisted-revoke-fixture")
	if revoked["ok"] != true {
		t.Fatal(revoked)
	}
	if len(call(map[string]any{"action": "list", "session_id": parent.ID}, "")["result"].(map[string]any)["links"].([]any)) != 0 {
		t.Fatal("revoked relation still active")
	}
	events := apiCall("events", map[string]any{"session_id": parent.ID, "cursor": eventCursor, "limit": 100, "subscribe": false}, "")
	if events["ok"] != true {
		t.Fatal(events)
	}
	foundRetraction := false
	for _, raw := range events["result"].(map[string]any)["events"].([]any) {
		if raw.(map[string]any)["kind"] == "scope_revoked" {
			foundRetraction = true
		}
	}
	if !foundRetraction {
		t.Fatal("retraction lost", events)
	}
	controllerEvents := apiCall("events", map[string]any{"session_id": parent.ID, "path": "controller", "limit": 100, "subscribe": false}, "")
	if controllerEvents["ok"] != true || len(controllerEvents["result"].(map[string]any)["events"].([]any)) != 0 {
		t.Fatal("ordinary link escaped path filter", controllerEvents)
	}
	confirmed = call(input, "persisted-reconfirm-fixture")
	if confirmed["ok"] != true {
		t.Fatal("reconfirm depends on old revoked intent", confirmed)
	}
	if err := h.messageQueue.TaskJournal().Prune(context.Background(), time.Now().Add(91*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	listed := apiCall("list", map[string]any{"session_id": parent.ID, "limit": 100}, "")
	if listed["ok"] != true || len(listed["result"].(map[string]any)["items"].([]any)) != 1 {
		t.Fatal("pruned intent hid durable source", listed)
	}
	task := apiCall("get", map[string]any{"task_id": receipt["task_id"]}, "")
	if task["ok"] != true {
		t.Fatal("pruned intake poisoned original task", task)
	}
	// An owner may retract their own audit relation after the target identity changes.
	currentLinks := call(map[string]any{"action": "list", "session_id": parent.ID}, "")["result"].(map[string]any)["links"].([]any)
	currentID := currentLinks[0].(map[string]any)["relation_id"]
	target.SetResumeID("changed-target-thread")
	if response := call(map[string]any{"action": "revoke", "relation_id": currentID}, "stale-own-audit-retraction"); response["ok"] != true {
		t.Fatal("stale own audit cannot retract", response)
	}
	after, _, _ = h.messageQueue.Get(target.ID, entry.RequestID)
	if after.State != entry.State || after.PayloadHash != entry.PayloadHash {
		t.Fatal("revoke changed original task")
	}
}

func legacyWSFixture(t *testing.T) (*Hub, *session.Session, *session.Session, func(string, any, string) map[string]any) {
	t.Helper()
	root := t.TempDir()
	pair := governance.NewPairing(filepath.Join(root, "pairing.json"))
	if err := pair.Claim("fixture-human-token", "human"); err != nil {
		t.Fatal(err)
	}
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", RootDir: root, DataDir: root}, pair, 0)
	t.Cleanup(func() { h.messageQueue.Close(); h.dispatches.Close(); h.delegations.Close() })
	parent := h.registry.Create("parent", "Parent", root, backend.Codex, "", "", "parent-thread")
	target := h.registry.Create("target", "Target", root, backend.Codex, "", "", "target-thread")
	conn, ctx, cleanup := dialWS(t, h)
	conn.SetReadLimit(contract.MaxBytes)
	extended, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	ctx = extended
	t.Cleanup(cancel)
	t.Cleanup(cleanup)
	send := func(value any) {
		data, _ := json.Marshal(value)
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			t.Fatal(err)
		}
	}
	read := func(kind string) map[string]any {
		for {
			value := readEvent(t, ctx, conn)
			if value["type"] == kind {
				return value
			}
		}
	}
	send(map[string]any{"type": "hello", "device_id": "human", "auth_token": "fixture-human-token", "protocol_version": 3})
	read("hello_ack")
	return h, parent, target, func(op string, input any, key string) map[string]any {
		frame := map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "fixture-correction", "operation": op, "input": input}
		if key != "" {
			frame["idempotency_key"] = key
		}
		send(frame)
		reply := read("task_api_response")
		raw, _ := json.Marshal(reply)
		compiler, _ := contract.New()
		if err := compiler.Validate(raw, "Response"); err != nil {
			t.Fatalf("response invalid: %v / %s", err, raw)
		}
		return reply
	}
}
func TestLegacySourceScopedPaginationFrozenMembershipAndNoFalseCaller(t *testing.T) {
	h, parent, target, call := legacyWSFixture(t)
	source := messagequeue.LegacySourceIdentity{InstanceID: "i1", SessionID: parent.ID, ThreadID: parent.ResumeID(), Revision: parent.SettingsSnapshot().ConfigRevision}
	dest := messagequeue.LegacySourceIdentity{InstanceID: "i1", SessionID: target.ID, ThreadID: target.ResumeID(), Revision: target.SettingsSnapshot().ConfigRevision}
	var last string
	for i := 0; i < 101; i++ {
		request := fmt.Sprintf("r_original_%03d", i)
		last = request
		entry, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: target.ID, RequestID: request, Content: "Original bounded fixture", Payload: []byte(`{"content":"fixture"}`)})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, _ := h.messageQueue.Snapshot(target.ID)
		response := call("legacy_source", map[string]any{"action": "confirm", "source": source, "target": dest, "request_id": request, "receipt_hash": entry.PayloadHash, "expected_queue_revision": snapshot.Revision}, fmt.Sprintf("persisted-link-%03d", i))
		if response["ok"] != true {
			t.Fatal(response)
		}
	}
	reply := call("legacy_source", map[string]any{"action": "list", "session_id": parent.ID}, "")
	if reply["ok"] != true {
		t.Fatal(reply)
	}
	first := reply["result"].(map[string]any)
	if len(first["links"].([]any)) != 100 || first["has_more"] != true {
		t.Fatal("101st relation silently lost", first)
	}
	second := call("legacy_source", map[string]any{"action": "list", "session_id": parent.ID, "cursor": first["next_cursor"]}, "")
	if second["ok"] != true || len(second["result"].(map[string]any)["links"].([]any)) != 1 || second["result"].(map[string]any)["links"].([]any)[0].(map[string]any)["request_id"] != last {
		t.Fatal(second)
	}
	link := first["links"].([]any)[0].(map[string]any)
	if response := call("legacy_source", map[string]any{"action": "revoke", "relation_id": link["relation_id"]}, "persisted-paged-revoke"); response["ok"] != true {
		t.Fatal(response)
	}
	expired := call("legacy_source", map[string]any{"action": "list", "session_id": parent.ID, "cursor": first["next_cursor"]}, "")
	if expired["error"].(map[string]any)["code"] != "cursor_expired" {
		t.Fatal("changed membership retained cursor", expired)
	}
	page := call("snapshot", map[string]any{"session_id": parent.ID, "views": []string{"source_children"}, "limit": 1}, "")
	if page["ok"] != true {
		t.Fatal(page)
	}
	cursor := page["result"].(map[string]any)["next_page_cursor"]
	hidden := true
	target.SetMeta(nil, &hidden)
	stale := call("snapshot", map[string]any{"session_id": parent.ID, "views": []string{"source_children"}, "limit": 1, "cursor": cursor}, "")
	if stale["ok"] == true {
		t.Fatal("frozen page disclosed hidden target", stale)
	}
	defs, err := contract.ToolInputs()
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := defs["task_legacy_source"]; exists {
		t.Fatal("human intake exposed as model tool")
	}
	_, err = h.Authorize(context.Background(), taskapi.VerifiedContext{Authority: "i1", StableScopeID: "session:fixture", BindingKind: "native_tool", Transport: "native_tool"}, taskapi.Request{Operation: "legacy_source", Input: []byte(`{"action":"list"}`)})
	if err == nil {
		t.Fatal("model caller acquired human annotation policy")
	}
}
