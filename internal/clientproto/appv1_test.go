package clientproto

import (
	"encoding/json"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
)

func asMap(t *testing.T, event any) map[string]any {
	t.Helper()
	b, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func TestAppV1HelloAckIncludesRegistry(t *testing.T) {
	app := NewAppV1()
	ev := app.HelloAck(HelloInput{
		ClientID: "c1", DeviceID: "d1", DeviceName: "phone",
		InstanceID: "i1", Gen: "g1", IsLocked: true, LockedToMe: true,
		InstanceName: "bridge", RootDir: "/repo", DataDir: "/data", LanIP: "192.168.1.2",
		Backends: []backend.Definition{{
			ID: "remote-ws", Label: "Remote WS",
			Capabilities: backend.Capabilities{Remote: true},
		}},
	})
	m := asMap(t, ev)
	if m["type"] != "hello_ack" || m["client_id"] != "c1" || m["device_id"] != "d1" {
		t.Fatalf("bad hello_ack identity: %v", m)
	}
	registry, ok := m["backend_registry"].([]any)
	if !ok || len(registry) != 1 {
		t.Fatalf("backend_registry missing: %v", m["backend_registry"])
	}
	if registry[0].(map[string]any)["id"] != "remote-ws" {
		t.Fatalf("bad backend_registry: %v", registry)
	}
}

func TestAppV1SessionsListNeverNull(t *testing.T) {
	app := NewAppV1()
	m := asMap(t, app.SessionsList(nil))
	sessions, ok := m["sessions"].([]any)
	if !ok || len(sessions) != 0 {
		t.Fatalf("sessions must be empty array, got %T %v", m["sessions"], m["sessions"])
	}
}

func TestAppV1SessionCreatedFromSummary(t *testing.T) {
	app := NewAppV1()
	ev := app.SessionCreated(SessionCreatedInput{
		ID: "s1", Name: "N", CreatedAt: 12, Cwd: "/tmp",
		Backend: "remote-ws", Model: "m1", Sandbox: "danger-full-access",
	})
	m := asMap(t, ev)
	if m["type"] != "session_created" || m["session_id"] != "s1" || m["backend"] != "remote-ws" {
		t.Fatalf("bad session_created: %v", m)
	}
}

func TestAppV1UsageReportWireShape(t *testing.T) {
	app := NewAppV1()
	util := 0.42
	resets := "2026-01-01T00:00:00Z"
	ev := app.UsageReport(backend.NewUsageReport(
		&backend.UsageWindow{Utilization: &util, ResetsAt: &resets},
		nil,
		nil,
	))
	m := asMap(t, ev)
	if m["type"] != "usage_report" {
		t.Fatalf("bad usage type: %v", m)
	}
	if m["seven_day"] != nil || m["seven_day_sonnet"] != nil {
		t.Fatalf("nil windows must serialize as null: %v", m)
	}
	five, ok := m["five_hour"].(map[string]any)
	if !ok || five["utilization"] != 0.42 || five["resets_at"] != resets {
		t.Fatalf("bad five_hour usage window: %v", m["five_hour"])
	}
}

func TestAppV1ParseCommandMapsCoreFields(t *testing.T) {
	app := NewAppV1()
	in, err := protocol.ParseInbound([]byte(`{
		"type":"message",
		"session_id":"s1",
		"request_id":"r1",
		"device_id":"d1",
		"name":"N",
		"cwd":"/tmp",
		"backend":"remote-ws",
		"model":"m1",
		"sandbox":"danger-full-access",
		"resume_claude_id":"resume-1",
		"content":"hello",
		"effort":"high",
		"pinned":true,
		"hidden":false,
		"objective":"finish release",
		"status":"active",
		"token_budget":2048,
		"images":[{"data":"abc","media_type":"image/png"}],
		"files":[{"name":"a.txt","content":"x","media_type":"text/plain"}]
	}`))
	if err != nil {
		t.Fatalf("parse inbound: %v", err)
	}
	cmd := app.ParseCommand(in)
	if cmd.Kind != "message" || cmd.SessionID != "s1" || cmd.RequestID != "r1" {
		t.Fatalf("bad command envelope: %+v", cmd)
	}
	if cmd.Backend != "remote-ws" || cmd.Content != "hello" || cmd.ResumeClaudeID != "resume-1" {
		t.Fatalf("bad command fields: %+v", cmd)
	}
	if cmd.Effort != "high" || cmd.Pinned == nil || !*cmd.Pinned || cmd.Hidden == nil || *cmd.Hidden {
		t.Fatalf("meta fields not mapped: %+v", cmd)
	}
	if cmd.Objective != "finish release" || cmd.GoalStatus != "active" || cmd.TokenBudget == nil || *cmd.TokenBudget != 2048 {
		t.Fatalf("goal fields not mapped: %+v", cmd)
	}
	if len(cmd.Images) != 1 || len(cmd.Files) != 1 {
		t.Fatalf("attachments not mapped: images=%d files=%d", len(cmd.Images), len(cmd.Files))
	}
}

func TestAppV1ParseCommandMapsOperationalFields(t *testing.T) {
	app := NewAppV1()
	in, err := protocol.ParseInbound([]byte(`{
		"type":"request_search",
		"limit":42,
		"known_last_source_message_id":"line-9",
		"mode":"delta",
		"before_source_message_id":"line-1",
		"shell_id":"sh1",
		"data":"ls",
		"id":"task1",
		"pid":123,
		"force":true,
		"path":"/tmp",
		"token":"fcm",
		"query":"hello",
		"offset":5,
		"cursor":"cur",
		"project_dir":"/repo",
		"include_hidden":true,
		"include_subagents":true,
		"msg_uuid":"m1",
		"around":7,
		"sdp":"offer",
		"candidate":"cand",
		"sdpMid":"0",
		"sdpMLineIndex":1,
		"answers":{"q":"a"},
		"cancelled":true,
		"decision":"allow",
		"file_id":"f1",
		"fork_after_message_id":"line-2",
		"feed_id":"feed1",
		"title":"T",
		"html":"<p>x</p>",
		"source":"src",
		"url":"https://example.com",
		"client_dedup_key":"dedup",
		"content_type":"text/html",
		"filters":{"role":"assistant","max_per_session":3}
	}`))
	if err != nil {
		t.Fatalf("parse inbound: %v", err)
	}
	cmd := app.ParseCommand(in)
	if cmd.Limit != 42 || cmd.KnownLast != "line-9" || cmd.Mode != "delta" || cmd.Before != "line-1" {
		t.Fatalf("history fields not mapped: %+v", cmd)
	}
	if cmd.ShellID != "sh1" || cmd.Data != "ls" || cmd.ID != "task1" || cmd.PID != 123 || !cmd.Force {
		t.Fatalf("runtime fields not mapped: %+v", cmd)
	}
	if cmd.Path != "/tmp" || cmd.Token != "fcm" || cmd.FileID != "f1" {
		t.Fatalf("file fields not mapped: %+v", cmd)
	}
	if cmd.Query != "hello" || cmd.Offset != 5 || cmd.Cursor != "cur" || cmd.ProjectDir != "/repo" || !cmd.IncludeHidden || !cmd.IncludeSubagents || cmd.MsgUUID != "m1" || cmd.Around != 7 {
		t.Fatalf("search fields not mapped: %+v", cmd)
	}
	if cmd.Filters == nil || cmd.Filters.Role != "assistant" || cmd.Filters.MaxPerSession != 3 {
		t.Fatalf("filters not mapped: %+v", cmd.Filters)
	}
	if cmd.SDP != "offer" || cmd.Candidate != "cand" || cmd.SDPMid != "0" || cmd.SDPMLineIndex == nil || *cmd.SDPMLineIndex != 1 {
		t.Fatalf("webrtc fields not mapped: %+v", cmd)
	}
	if cmd.Answers["q"] != "a" || cmd.Cancelled == nil || !*cmd.Cancelled || cmd.Decision != "allow" {
		t.Fatalf("interaction fields not mapped: %+v", cmd)
	}
	if cmd.ForkAfterMessageID != "line-2" || cmd.FeedID != "feed1" || cmd.Title != "T" || cmd.HTML == "" || cmd.Source != "src" || cmd.URL == "" || cmd.ClientDedupKey != "dedup" || cmd.ContentType != "text/html" {
		t.Fatalf("fork/feed fields not mapped: %+v", cmd)
	}
}

func TestAppV1ParseCommandMapsNativeWorkItemFields(t *testing.T) {
	app := NewAppV1()
	in, err := protocol.ParseInbound([]byte(`{
		"type":"work_item_move",
		"project_id":"p1",
		"work_item_id":"wi1",
		"mutation_id":"m1",
		"expected_version":7,
		"revision":11,
		"lifecycle":"active",
		"priority":"high",
		"sort_key":2000000,
		"description":"ship it",
		"workspace_path":"/repo",
		"thread_id":"t1",
		"role":"primary",
		"depends_on_id":"wi0",
		"blocked_reason_code":"input",
		"blocked_note":"waiting",
		"delivered_revision":10,
		"acked_revision":9,
		"fields":["description","priority"]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	cmd := app.ParseCommand(in)
	if cmd.WorkItemID != "wi1" || cmd.ProjectID != "p1" || cmd.MutationID != "m1" || cmd.ExpectedVersion != 7 {
		t.Fatalf("bad work identity: %+v", cmd)
	}
	if cmd.Lifecycle != "active" || cmd.Priority != "high" || cmd.SortKey != 2_000_000 || cmd.Description != "ship it" {
		t.Fatalf("bad work mutation: %+v", cmd)
	}
	if cmd.DeliveredRevision != 10 || cmd.AckedRevision != 9 || len(cmd.Fields) != 2 {
		t.Fatalf("bad work sync fields: %+v", cmd)
	}
}
