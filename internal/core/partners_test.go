package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/partners"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"github.com/coder/websocket"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPartnerAuthenticatedWSFrozenQueuedRole(t *testing.T) {
	root := t.TempDir()
	pair := governance.NewPairing(filepath.Join(root, "pairing.json"))
	if e := pair.Claim("partner-fixture-token", "partner-device"); e != nil {
		t.Fatal(e)
	}
	h := NewHub(session.NewRegistry(), Config{InstanceID: "partner-authority", RootDir: root, DataDir: root}, pair, 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	defer h.dispatches.Close()
	s := h.registry.Create("s_partner", "Original", root, backend.Codex, "gpt-6.1-sol", "read-only", "")
	s.SetResumeID("original-thread")
	started := make(chan string, 4)
	release := make(chan struct{})
	h.SetExecutor(&fakeExec{sink: h, onSendContext: func(ctx context.Context, s *session.Session, id, content string) {
		started <- backend.PresentationContent(ctx, content)
		if id == "r_partner_first" {
			<-release
		}
		h.Emit(protocol.NewDone(s.ID, id))
	}})
	conn, _, cleanup := dialWS(t, h)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	send := func(v any) {
		b, _ := json.Marshal(v)
		if e := conn.Write(ctx, websocket.MessageText, b); e != nil {
			t.Fatal(e)
		}
	}
	receive := func(kind string) map[string]any {
		for {
			r := readEvent(t, ctx, conn)
			if r["type"] == kind {
				return r
			}
		}
	}
	send(map[string]any{"type": "hello", "device_id": "partner-device", "auth_token": "partner-fixture-token", "protocol_version": 3})
	hello := receive("hello_ack")
	b, _ := json.Marshal(hello)
	if !strings.Contains(string(b), "teammate_profile_v1") {
		t.Fatal("capability absent")
	}
	thread := "original-thread"
	revision := s.SettingsSnapshot().ConfigRevision
	call := func(in partners.Request) map[string]any {
		send(map[string]any{"type": "partner_profile_request", "request_id": "profile-read", "partner": in})
		return receive("partner_profile_result")
	}
	p := partners.Profile{SessionID: s.ID, Name: "小橋", Color: "mint", Description: "ROLE_ONE"}
	save := func(key string, expected uint64) map[string]any {
		return call(partners.Request{Action: "save", Authority: "partner-authority", MutationID: key, SessionID: s.ID, ExpectedThread: &thread, ExpectedConfigRevision: &revision, ExpectedRevision: expected, Profile: &p})
	}
	first := save("mutation-original", 0)
	if first["ok"] != true {
		t.Fatal(first)
	}
	if s.ResumeID() != thread || s.SettingsSnapshot().Sandbox != "read-only" {
		t.Fatal("profile mutated original permissions")
	}
	send(map[string]any{"type": "message", "session_id": s.ID, "request_id": "r_partner_first", "content": "first assignment"})
	select {
	case value := <-started:
		if !strings.Contains(value, "ROLE_ONE") {
			t.Fatal(value)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	p.Description = "ROLE_TWO"
	second := save("mutation-second", 1)
	if second["ok"] != true {
		t.Fatal(second)
	}
	send(map[string]any{"type": "message", "session_id": s.ID, "request_id": "r_partner_second", "content": "second assignment"})
	for {
		ack := receive("message_ack")
		if ack["request_id"] == "r_partner_second" {
			break
		}
	}
	p.Description = "ROLE_THREE"
	third := save("mutation-third", 2)
	if third["ok"] != true {
		t.Fatal(third)
	}
	e, found, err := h.messageQueue.Get(s.ID, "r_partner_second")
	if err != nil || !found {
		t.Fatal(err)
	}
	var frozen queuedPayload
	if err = json.Unmarshal(e.Payload, &frozen); err != nil {
		t.Fatal(err)
	}
	if frozen.Content != "second assignment" || frozen.PartnerRole == nil || frozen.PartnerRole.Description != "ROLE_TWO" {
		t.Fatalf("queue not frozen: %+v", frozen)
	}
	close(release)
	select {
	case value := <-started:
		if !strings.Contains(value, "ROLE_TWO") || strings.Contains(value, "ROLE_THREE") {
			t.Fatal(value)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	list := call(partners.Request{Action: "list", Authority: "partner-authority"})
	if list["ok"] != true || len(list["profiles"].([]any)) != 1 {
		t.Fatal(list)
	}
	p.Description = "ROLE_THREE"
	replay := save("mutation-third", 2)
	if replay["ok"] != true {
		t.Fatal(replay)
	}
	p.SessionID = "sk1:partner-authority:" + s.ID
	scoped := call(partners.Request{Action: "save", Authority: "partner-authority", MutationID: "mutation-scoped", SessionID: p.SessionID, ExpectedThread: &thread, ExpectedConfigRevision: &revision, ExpectedRevision: 3, Profile: &p})
	if scoped["ok"] != true {
		t.Fatal("scoped native edit rejected", scoped)
	}
	p.SessionID = "sk1:foreign:" + s.ID
	foreign := call(partners.Request{Action: "save", Authority: "partner-authority", MutationID: "mutation-foreign", SessionID: p.SessionID, ExpectedThread: &thread, ExpectedConfigRevision: &revision, ExpectedRevision: 4, Profile: &p})
	if foreign["ok"] == true {
		t.Fatal("foreign nested identity accepted")
	}
	wrong := call(partners.Request{Action: "list", Authority: "foreign"})
	if wrong["ok"] == true {
		t.Fatal("foreign authority admitted")
	}
}
func TestPartnerUnboundClientDenied(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	route(h, c, `{"type":"partner_profile_request","request_id":"unbound","partner":{"action":"list","authority":"i1"}}`)
	got := waitForType(t, c, "partner_profile_result")
	if got["ok"] == true || got["error"] != "permission_denied" {
		t.Fatal(got)
	}
}
