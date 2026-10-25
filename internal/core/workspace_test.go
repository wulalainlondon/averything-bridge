package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"everything-go/internal/workitems"
	"everything-go/internal/workspace"
	"github.com/coder/websocket"
)

// Synthetic provider oracle: these tests verify the gateway/receipt mechanics,
// not a real Codex/Claude model, device installation or independent agent QA.
type workspaceFixtureHistory struct {
	correlationHistoryFixture
	finalMu sync.Mutex
	finals  map[string][]map[string]any
}

func (p *workspaceFixtureHistory) LoadHistory(thread string, _ history.Opts) (*history.Result, error) {
	p.finalMu.Lock()
	defer p.finalMu.Unlock()
	return &history.Result{Kind: "snapshot", Messages: append([]map[string]any{}, p.finals[thread]...)}, nil
}
func (p *workspaceFixtureHistory) add(s *session.Session, request, turn, text string) {
	p.seedNativeFixture(backend.NativeTaskAccepted{SessionID: s.ID, RequestID: request, ThreadID: s.ResumeID(), TurnID: turn})
	p.finalMu.Lock()
	defer p.finalMu.Unlock()
	p.finals[s.ResumeID()] = append(p.finals[s.ResumeID()], map[string]any{"id": "message-" + request, "role": "assistant", "content": text, "request_id": request, "source_message_id": "message-" + request, "source_thread_id": s.ResumeID(), "source_turn_id": turn, "history_read_result_verified": true})
}

type workspaceFixtureExecutor struct {
	*fakeExec
	proof *workspaceFixtureHistory
}

func (e *workspaceFixtureExecutor) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.proof, true
}
func (e *workspaceFixtureExecutor) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.proof}
}

type workspaceHarness struct {
	t      *testing.T
	h      *Hub
	owner  workspace.Actor
	ctx    context.Context
	conn   *websocket.Conn
	server *httptest.Server
	n      int
}

func workspaceSetup(t *testing.T) *workspaceHarness {
	return workspaceSetupDuration(t, 20*time.Second)
}
func workspaceSetupDuration(t *testing.T, duration time.Duration) *workspaceHarness {
	t.Helper()
	root := t.TempDir()
	pair := governance.NewPairing(filepath.Join(root, "pairing.json"))
	if err := pair.Claim("fixture-workspace-device-token", "workspace-device"); err != nil {
		t.Fatal(err)
	}
	h := NewHub(session.NewRegistry(), Config{WorkspaceEnabled: true, InstanceID: "workspace-fixture", InstanceName: "Isolated workspace fixture", RootDir: root, DataDir: root, Backends: []backend.Definition{{ID: backend.Codex, DefaultModel: "fixture-model", Models: []backend.Model{{ID: "fixture-model", Label: "Synthetic model"}}}}}, pair, 0)
	service, err := workitems.OpenService(root, "workspace-fixture")
	if err != nil {
		t.Fatal(err)
	}
	h.SetWorkItems(service)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	ex := &workspaceFixtureExecutor{fakeExec: &fakeExec{sink: h}, proof: &workspaceFixtureHistory{correlationHistoryFixture: correlationHistoryFixture{Owners: map[string]string{}}, finals: map[string][]map[string]any{}}}
	h.SetExecutor(ex)
	h.StartWorkspaceScheduler(ctx)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/workspace/v1/guest", h.ServeWorkspaceGuest)
	mux.HandleFunc("/", h.ServeWS)
	server := httptest.NewServer(mux)
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	conn.SetReadLimit(16 * 1024 * 1024)
	f := &workspaceHarness{t: t, h: h, owner: h.workspace.Owner("workspace-device", workspaceHash("fixture-workspace-device-token")), ctx: ctx, conn: conn, server: server}
	f.send(conn, map[string]any{"type": "hello", "device_id": "workspace-device", "auth_token": "fixture-workspace-device-token", "protocol_version": 3})
	f.receive(conn, "hello_ack", "")
	t.Cleanup(func() {
		_ = conn.CloseNow()
		cancel()
		_ = h.CloseWorkspace()
		_ = service.Close()
		server.Close()
		_ = h.messageQueue.Close()
		_ = h.delegations.Close()
		_ = h.dispatches.Close()
	})
	return f
}
func (f *workspaceHarness) send(c *websocket.Conn, v any) {
	f.t.Helper()
	b, _ := json.Marshal(v)
	if err := c.Write(f.ctx, websocket.MessageText, b); err != nil {
		f.t.Fatal(err)
	}
}
func (f *workspaceHarness) receive(c *websocket.Conn, kind, request string) map[string]any {
	f.t.Helper()
	for {
		_, b, err := c.Read(f.ctx)
		if err != nil {
			f.t.Fatal(err)
		}
		var v map[string]any
		if err = json.Unmarshal(b, &v); err != nil {
			f.t.Fatal(err)
		}
		if v["type"] == kind && (request == "" || v["request_id"] == request) {
			return v
		}
	}
}
func (f *workspaceHarness) call(c workspace.Command) workspace.Result {
	f.t.Helper()
	f.n++
	c.Authority = f.h.cfg.InstanceID
	if c.MutationID == "" {
		c.MutationID = fmt.Sprintf("ws-mutation-%04d", f.n)
	}
	if c.RoomID != "" && c.ExpectedACL == nil {
		room, _, err := f.h.workspace.Authorize(f.ctx, f.owner, c.RoomID, "read")
		if err != nil {
			f.t.Fatal(err)
		}
		c.ExpectedACL = &room.ACLRevision
	}
	request := fmt.Sprintf("ws-request-%04d", f.n)
	f.send(f.conn, map[string]any{"type": "workspace_request", "request_id": request, "workspace": c})
	v := f.receive(f.conn, "workspace_result", request)
	if v["ok"] != true {
		f.t.Fatalf("%s: %+v", c.Action, v)
	}
	b, _ := json.Marshal(v["data"])
	var r workspace.Result
	if err := json.Unmarshal(b, &r); err != nil {
		f.t.Fatal(err)
	}
	return r
}
func wireData(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
func (f *workspaceHarness) room(name string) workspace.Room {
	w := f.call(workspace.Command{Action: "create_workspace", Data: wireData(map[string]any{"name": name})}).Workspace
	return *f.call(workspace.Command{Action: "create_room", WorkspaceID: w.ID, Data: wireData(map[string]any{"name": name})}).Room
}
func (f *workspaceHarness) agent(room workspace.Room, name string) workspace.Entity {
	return *f.call(workspace.Command{Action: "create_agent", RoomID: room.ID, Data: wireData(map[string]any{"name": name, "description": "Use pinned room sources", "backend": "codex", "model": "fixture-model"})}).Entity
}

func TestWorkspaceWSOwnerTaskUsesOriginalQueueAndSealedFixtureFinal(t *testing.T) {
	f := workspaceSetup(t)
	project, err := f.h.work.CreateProject(f.ctx, workitems.CreateProjectInput{Name: "Original linked project"})
	if err != nil {
		t.Fatal(err)
	}
	ws := f.call(workspace.Command{Action: "create_workspace", Data: wireData(map[string]any{"name": "Workspace"})})
	linked := f.call(workspace.Command{Action: "create_room", WorkspaceID: ws.Workspace.ID, Data: wireData(map[string]any{"name": "Project channel", "project_id": project.ID})})
	room := *linked.Room
	if room.ProjectID != project.ID {
		t.Fatal("original project missing")
	}
	if _, err = f.h.executeWorkspace(f.ctx, f.owner, workspace.Command{Action: "create_room", Authority: f.h.cfg.InstanceID, WorkspaceID: ws.Workspace.ID, MutationID: "invalid-project", Data: wireData(map[string]any{"name": "Invalid", "project_id": "missing"})}); err == nil {
		t.Fatal("unverified original project accepted")
	}
	agent := f.agent(room, "Researcher")
	var role workspace.Agent
	if err := json.Unmarshal(agent.Body, &role); err != nil || !role.Ready || !role.RoomExclusive {
		t.Fatal("source not ready", err, role)
	}
	var calls atomic.Int32
	started := make(chan string, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	ex := f.h.exec.(*workspaceFixtureExecutor)
	ex.onSend = func(s *session.Session, request, content string) {
		calls.Add(1)
		started <- request
		<-release
		if s.ResumeID() == "" {
			s.SetResumeID("native-" + s.ID)
			f.h.Emit(protocol.NewSessionUUID(s.ID, s.ResumeID()))
		}
		turn := "turn-" + request
		ex.proof.add(s, request, turn, "Synthetic original final, not a real model response")
		f.h.Emit(backend.NativeTaskAccepted{SessionID: s.ID, RequestID: request, ThreadID: s.ResumeID(), TurnID: turn})
		f.h.Emit(protocol.NewDone(s.ID, request))
	}
	command := workspace.Command{Action: "create_task", RoomID: room.ID, MutationID: "ws-stable-native-intent-1", Data: wireData(workspace.TaskInput{Title: "Research", Instruction: "Return a cited brief", AgentID: agent.ID})}
	first := f.call(command)
	var task workspace.Task
	_ = json.Unmarshal(first.Entity.Body, &task)
	if !strings.HasPrefix(task.RequestID, "r_") {
		t.Fatal("not an ordinary persisted request", task.RequestID)
	}
	select {
	case actual := <-started:
		if actual != task.RequestID {
			t.Fatal("replacement native request", actual)
		}
	case <-f.ctx.Done():
		t.Fatal("no execution", f.ctx.Err())
	}
	// Actor running/ACK are deliberately insufficient for a native-running badge.
	f.h.drainWorkspace(f.ctx)
	early := f.call(workspace.Command{Action: "get", RoomID: room.ID, EntityID: first.Entity.ID})
	var pending workspace.Task
	_ = json.Unmarshal(early.Entity.Body, &pending)
	if pending.Axes.Execution == "running" {
		t.Fatal("actor acceptance counted as native consumption")
	}
	policy, err := f.h.workspaceRuntimePolicy(role.SessionID)
	if err != nil || !policy.Room || !policy.Worker || policy.RequestID != task.RequestID || len(policy.Tools) != 2 {
		t.Fatal("scoped original native runtime missing", err)
	}
	close(release)
	deadline := time.Now().Add(3 * time.Second)
	var final workspace.Task
	for time.Now().Before(deadline) {
		f.h.drainWorkspace(f.ctx)
		e, err := f.h.workspace.Lookup(f.ctx, f.owner, room.ID, first.Entity.ID)
		if err != nil {
			t.Fatal(err)
		}
		_ = json.Unmarshal(e.Body, &final)
		if final.Axes.Final == "sealed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if final.WorkItemID == "" || final.RunID == "" {
		t.Fatal("original WorkItem/Run projection missing", final)
	}
	if final.Axes.Final != "sealed" || final.Axes.Delivery != "delivered" || final.Axes.Human != "unreviewed" || final.CanonicalID == "" {
		t.Fatal("original final axes not separated", final)
	}
	journal, err := f.h.messageQueue.TaskJournal().FindTask(f.ctx, final.CanonicalID)
	if err != nil || journal.SessionID != task.SessionID || journal.RequestID != task.RequestID {
		t.Fatal("original TaskAPI journal missing", journal, err)
	}
	var metadata apiMetadata
	if json.Unmarshal(journal.Metadata, &metadata) != nil || metadata.Transport != "authenticated_workspace" || metadata.Caller.BindingKind != "workspace_grant" || metadata.WorkspaceRoomID != room.ID {
		t.Fatal("incorrect transport/caller provenance", metadata)
	}
	again := f.call(command)
	if again.Entity.ID != first.Entity.ID || calls.Load() != 1 {
		t.Fatal("lost-ACK replay produced another native execution", calls.Load())
	}
	if seal, err := f.h.messageQueue.TaskSeal(f.ctx, role.SessionID, task.RequestID, final.SealID); err != nil || seal.Hash != final.FinalHash {
		t.Fatal("room result not backed by original Task API seal", err)
	}
	if f.h.LineSessionAllowed(role.SessionID) {
		t.Fatal("room inherited Go-wide LINE")
	}
	var paired *Client
	f.h.mu.RLock()
	for client := range f.h.clients {
		if f.h.pairedTaskDevice(client) == "workspace-device" {
			paired = client
		}
	}
	f.h.mu.RUnlock()
	bound, err := f.h.taskHuman(paired)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := bound.Check(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.h.apiRecord(f.ctx, actual, final.CanonicalID); err != nil {
		t.Fatal("current room member cannot read original TaskAPI", err)
	}
	invite := f.call(workspace.Command{Action: "create_invite", RoomID: room.ID, Data: wireData(map[string]any{"role": "admin", "expires_at": time.Now().Add(time.Hour).UnixMilli()})})
	guest, _, err := f.h.workspace.AcceptInvite(f.ctx, invite.InviteCode, strings.Repeat("fixture-other-human-", 3), "other-device", "Other human")
	if err != nil {
		t.Fatal(err)
	}
	latest := f.call(workspace.Command{Action: "snapshot", RoomID: room.ID}).Snapshot.Room
	f.call(workspace.Command{Action: "transfer_owner", RoomID: room.ID, ExpectedRevision: &latest.Revision, Data: wireData(map[string]any{"principal_id": guest.ID})})
	f.call(workspace.Command{Action: "leave_room", RoomID: room.ID})
	if _, err = f.h.apiRecord(f.ctx, actual, final.CanonicalID); err == nil {
		t.Fatal("departed owner read room task through original TaskAPI")
	}
}

func TestWorkspaceGuestSocketHasNoLegacyFeedAndCannotForgeCaller(t *testing.T) {
	f := workspaceSetup(t)
	room := f.room("Shared channel")
	private := f.room("Other private room")
	invite := f.call(workspace.Command{Action: "create_invite", RoomID: room.ID, Data: wireData(map[string]any{"role": "editor", "expires_at": time.Now().Add(time.Minute).UnixMilli()})})
	credential := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x61}, 32))
	guest, _, err := websocket.Dial(f.ctx, "ws"+strings.TrimPrefix(f.server.URL, "http")+"/api/workspace/v1/guest", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer guest.CloseNow()
	f.send(guest, map[string]any{"type": "workspace_guest_hello", "credential": credential, "invite_code": invite.InviteCode, "device_id": "independent-guest-device", "name": "Second human"})
	ready := f.receive(guest, "workspace_guest_ready", "")
	if ready["principal_id"] == f.owner.ID {
		t.Fatal("guest used owner identity")
	}
	f.h.Emit(map[string]any{"type": "private_owner_probe", "private": "MUST_NOT_REACH_GUEST"})
	f.send(guest, map[string]any{"type": "ping", "request_id": "guest-heartbeat"})
	pong := f.receive(guest, "workspace_pong", "")
	if pong["private"] != nil {
		t.Fatal("legacy feed reached isolated socket")
	}
	call := func(action string, c workspace.Command) map[string]any {
		c.Authority = f.h.cfg.InstanceID
		f.send(guest, map[string]any{"type": "workspace_request", "request_id": action, "workspace": c})
		return f.receive(guest, "workspace_result", action)
	}
	v := call("private-room-read", workspace.Command{Action: "snapshot", RoomID: private.ID})
	if v["ok"] != false || !strings.Contains(fmt.Sprint(v["error"]), "room_permission_denied") {
		t.Fatal("guest private snapshot", v)
	}
	v = call("account-read", workspace.Command{Action: "tool_accounts", RoomID: room.ID})
	if v["ok"] != false {
		t.Fatal("guest inherited account metadata", v)
	}
	v = call("create-agent", workspace.Command{Action: "create_agent", RoomID: room.ID, MutationID: "guest-agent-mutation", Data: wireData(map[string]any{"name": "Forged", "backend": "codex", "model": "fixture-model"})})
	if v["ok"] != false {
		t.Fatal("guest self-granted original native account", v)
	}
	f.send(guest, map[string]any{"type": "hello", "request_id": "legacy-hello"})
	rejected := f.receive(guest, "workspace_result", "")
	if rejected["error"] != "isolated_workspace_transport_only" {
		t.Fatal("legacy owner protocol accepted", rejected)
	}
	f.h.mu.RLock()
	count := len(f.h.clients)
	f.h.mu.RUnlock()
	if count != 1 {
		t.Fatal("guest registered in owner Hub", count)
	}
}

func TestWorkspaceNativeReadToolsRejectUnpinnedSourceAndExpiredRoomToken(t *testing.T) {
	f := workspaceSetup(t)
	room := f.room("Sources")
	other := f.room("Unshared")
	secret := f.call(workspace.Command{Action: "save_document", RoomID: other.ID, Data: wireData(map[string]any{"title": "Private", "body": "PRIVATE SOURCE"})})
	agent := f.agent(room, "Reader")
	started := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	ex := f.h.exec.(*workspaceFixtureExecutor)
	ex.onSend = func(s *session.Session, request, content string) {
		close(started)
		<-release
		f.h.Emit(protocol.NewDone(s.ID, request))
	}
	task := f.call(workspace.Command{Action: "create_task", RoomID: room.ID, Data: wireData(workspace.TaskInput{Title: "Read", Instruction: "Read scoped inputs", AgentID: agent.ID})})
	var native workspace.Task
	_ = json.Unmarshal(task.Entity.Body, &native)
	<-started
	policy, err := f.h.workspaceRuntimePolicy(native.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	request := func(name string, args any) map[string]any {
		body := wireData(map[string]any{"jsonrpc": "2.0", "id": "call-1", "method": "tools/call", "params": map[string]any{"name": name, "arguments": args}})
		req, err := http.NewRequestWithContext(f.ctx, "POST", policy.MCPURL, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+policy.Token)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var data map[string]any
		_ = json.NewDecoder(response.Body).Decode(&data)
		return data
	}
	if got := request("room_read_source", map[string]any{"request_id": native.RequestID, "source_id": secret.Entity.ID}); got["error"] == nil {
		t.Fatal("unpinned private document readable", got)
	}
	if got := request("room_read_source", map[string]any{"request_id": "wrong-request", "source_id": secret.Entity.ID}); got["error"] == nil {
		t.Fatal("wrong round readable", got)
	}
	if got := request("send_email", map[string]any{}); got["error"] == nil {
		t.Fatal("MCP expanded its tool authority", got)
	}
	if got := request("room_get_context", map[string]any{}); got["result"] == nil {
		t.Fatal("current authorized context unavailable", got)
	}
	close(release)
}

func TestWorkspaceSecretsExcludedFromOptionalFrameLogs(t *testing.T) {
	// Logging is a startup-only setting. Exercise it in a fresh test process
	// instead of racing live actor callbacks by mutating a package global.
	if os.Getenv("BRIDGE_FRAMELOG_TEST_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestWorkspaceSecretsExcludedFromOptionalFrameLogs$")
		cmd.Env = append(os.Environ(), "BRIDGE_FRAMELOG_TEST_CHILD=1", "EG_FRAME_LOG=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("frame-log child: %v\n%s", err, out)
		}
		return
	}
	var out bytes.Buffer
	old := log.Writer()
	log.SetOutput(&out)
	defer log.SetOutput(old)
	logOutbound(workspaceWire{Type: "workspace_result", Authority: "fixture", Data: &workspace.Result{InviteCode: "PRIVATE-INVITE-BEARER", ToolData: "PRIVATE-ACCOUNT-DATA"}})
	logOutbound(taskapi.Response{Type: "task_api_response", Result: map[string]any{"text": "PRIVATE-ROOM-FINAL"}})
	logOutbound(protocol.NewTextChunk("s_cw_private", "r_original", "PRIVATE-NATIVE-TEXT"))
	if strings.Contains(out.String(), "PRIVATE-") || !strings.Contains(out.String(), "redacted") {
		t.Fatal("sensitive workspace payload logged", out.String())
	}
}

func TestWorkspaceCatalogUsesExistingLowercaseWireDefinitions(t *testing.T) {
	f := workspaceSetup(t)
	f.send(f.conn, map[string]any{"type": "workspace_request", "request_id": "fixture-catalog", "workspace": workspace.Command{Action: "agent_catalog", Authority: f.h.cfg.InstanceID}})
	frame := f.receive(f.conn, "workspace_result", "fixture-catalog")
	rows, ok := frame["backends"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatal("native catalog absent", frame)
	}
	definition, _ := rows[0].(map[string]any)
	models, _ := definition["models"].([]any)
	if definition["id"] != "codex" || definition["ID"] != nil || len(models) != 1 {
		t.Fatal("internal Go structs escaped wire adapter", definition)
	}
	model, _ := models[0].(map[string]any)
	if model["id"] != "fixture-model" || model["label"] != "Synthetic model" {
		t.Fatal("model wire fields not consumable", model)
	}
}

func TestWorkspaceAttachmentReadKeepsActionAndExactBytesOnWire(t *testing.T) {
	f := workspaceSetup(t)
	room := f.room("File channel")
	data := []byte(strings.Repeat("untrusted fixture file\n", 3000))
	file := f.call(workspace.Command{Action: "upload_attachment", RoomID: room.ID, Data: wireData(map[string]any{"name": "fixture.txt", "mime": "text/plain", "hash": workspaceHash(string(data)), "data": base64.StdEncoding.EncodeToString(data)})}).Entity
	all := []byte{}
	var offset int64
	for {
		r := f.call(workspace.Command{Action: "read_attachment", RoomID: room.ID, EntityID: file.ID, Offset: offset, Limit: 65536})
		if r.Action != "read_attachment" || r.NextOffset <= offset {
			t.Fatal("unparseable range envelope", r.Action, r.NextOffset, offset)
		}
		all = append(all, r.Bytes...)
		offset = r.NextOffset
		if !r.HasMore {
			break
		}
	}
	if !bytes.Equal(all, data) {
		t.Fatal("range read changed bytes")
	}
}

func TestWorkspaceNativeStreamAndLegacyCatalogRespectRoomMembership(t *testing.T) {
	f := workspaceSetup(t)
	room := f.room("Scoped native room")
	entity := f.agent(room, "Scoped worker")
	var agent workspace.Agent
	if err := json.Unmarshal(entity.Body, &agent); err != nil {
		t.Fatal(err)
	}
	var client *Client
	f.h.mu.RLock()
	for c := range f.h.clients {
		client = c
	}
	f.h.mu.RUnlock()
	if client == nil {
		t.Fatal("no actual paired client")
	}
	data, err := marshalEvent(protocol.NewTextChunk(agent.SessionID, "r_fixture_native", "private-native-body"), f.h.cfg.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if !f.h.workspaceNativeFrameAllowed(client, data) {
		t.Fatal("current member stream denied")
	}
	for _, summary := range f.h.sessionSummaries() {
		if summary.ID == agent.SessionID {
			t.Fatal("room worker appeared as ordinary legacy conversation")
		}
	}
	invite := f.call(workspace.Command{Action: "create_invite", RoomID: room.ID, Data: wireData(map[string]any{"role": "admin", "expires_at": time.Now().Add(time.Hour).UnixMilli()})})
	credential := strings.Repeat("fixture-guest-credential-", 3)
	guest, _, err := f.h.workspace.AcceptInvite(f.ctx, invite.InviteCode, credential, "fixture-guest-device", "Guest")
	if err != nil {
		t.Fatal(err)
	}
	current := f.call(workspace.Command{Action: "snapshot", RoomID: room.ID}).Snapshot.Room
	f.call(workspace.Command{Action: "transfer_owner", RoomID: room.ID, ExpectedRevision: &current.Revision, Data: wireData(map[string]any{"principal_id": guest.ID})})
	f.call(workspace.Command{Action: "leave_room", RoomID: room.ID})
	if f.h.workspaceNativeFrameAllowed(client, data) {
		t.Fatal("departed owner received room native content through scoped legacy wire")
	}
	var nested any
	if json.Unmarshal(data, &nested) != nil {
		t.Fatal("frame")
	}
	replay, _ := json.Marshal(map[string]any{"type": "offline_replay", "events": []any{map[string]any{"type": "wrapper", "event": nested}}})
	if f.h.workspaceNativeFrameAllowed(client, replay) {
		t.Fatal("nested legacy replay bypassed current room membership")
	}
}

func TestWorkspaceDisabledStillQuarantinesReservedNativeEvents(t *testing.T) {
	// A disabled capability must not downgrade old reserved sessions into
	// ordinary legacy output, even before its room DB is opened.
	h := &Hub{}
	private := protocol.NewTextChunk("s_cw_original", "r_original", "private room body")
	data, _ := json.Marshal(private)
	if h.workspaceNativeFrameAllowed(nil, data) || !h.workspaceRoomNativeEvent(private) {
		t.Fatal("disabled workspace exposed reserved native output")
	}
	ordinary := protocol.NewTextChunk("ordinary", "r_original", "ordinary body")
	data, _ = json.Marshal(ordinary)
	if !h.workspaceNativeFrameAllowed(nil, data) || h.workspaceRoomNativeEvent(ordinary) {
		t.Fatal("ordinary legacy output changed")
	}
}

func TestWorkspaceBackpressuredNativeFrameRechecksMembershipAtWrite(t *testing.T) {
	f := workspaceSetup(t)
	room := f.room("Backpressured room")
	entity := f.agent(room, "Worker")
	var agent workspace.Agent
	if err := json.Unmarshal(entity.Body, &agent); err != nil {
		t.Fatal(err)
	}
	invite := f.call(workspace.Command{Action: "create_invite", RoomID: room.ID, Data: wireData(map[string]any{"role": "admin", "expires_at": time.Now().Add(time.Hour).UnixMilli()})})
	guest, _, err := f.h.workspace.AcceptInvite(f.ctx, invite.InviteCode, strings.Repeat("backpressure-fixture-", 3), "guest-device", "Guest")
	if err != nil {
		t.Fatal(err)
	}
	current := f.call(workspace.Command{Action: "snapshot", RoomID: room.ID}).Snapshot.Room
	f.call(workspace.Command{Action: "transfer_owner", RoomID: room.ID, ExpectedRevision: &current.Revision, Data: wireData(map[string]any{"principal_id": guest.ID})})
	var client *Client
	f.h.mu.RLock()
	for c := range f.h.clients {
		client = c
	}
	f.h.mu.RUnlock()
	if client == nil {
		t.Fatal("actual client missing")
	}
	client.writeMu.Lock()
	locked := true
	defer func() {
		if locked {
			client.writeMu.Unlock()
		}
	}()
	// Admission-time ACL permits this frame. The socket cannot write yet.
	client.enqueueEventUnlogged(protocol.NewTextChunk(agent.SessionID, "r_fixture", "PRIVATE-BACKPRESSURED-NATIVE"))
	latest, _, err := f.h.workspace.Authorize(f.ctx, f.owner, room.ID, "read")
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.h.executeWorkspace(f.ctx, f.owner, workspace.Command{Authority: f.h.cfg.InstanceID, Action: "leave_room", RoomID: room.ID, MutationID: "fixture-leave-before-socket-write", ExpectedACL: &latest.ACLRevision})
	if err != nil {
		t.Fatal(err)
	}
	client.enqueueEventUnlogged(map[string]any{"type": "fixture_marker"})
	locked = false
	client.writeMu.Unlock()
	for {
		_, data, err := f.conn.Read(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("PRIVATE-BACKPRESSURED-NATIVE")) {
			t.Fatal("revoked queued native frame reached socket")
		}
		var header struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(data, &header)
		if header.Type == "fixture_marker" {
			break
		}
	}
}
