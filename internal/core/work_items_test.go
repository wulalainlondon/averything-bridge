package core

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/workitems"
)

func assertNoTypeWithin(t *testing.T, c *Client, typ string, duration time.Duration) {
	t.Helper()
	deadline := time.After(duration)
	for {
		select {
		case data := <-c.send:
			var event map[string]any
			if err := json.Unmarshal(data, &event); err != nil {
				t.Fatalf("bad event JSON: %v", err)
			}
			if event["type"] == typ {
				t.Fatalf("unexpected event type %q before client sync request", typ)
			}
		case <-deadline:
			return
		}
	}
}

func attachWorkService(t *testing.T, h *Hub, dir string) *workitems.Service {
	t.Helper()
	service, err := workitems.OpenService(dir, "i1")
	if err != nil {
		t.Fatal(err)
	}
	h.SetWorkItems(service)
	t.Cleanup(func() { _ = service.Close() })
	return service
}

func TestWorkStartRunUsesSessionActorAndProjectsSuccessToReview(t *testing.T) {
	h, exec := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	c.deviceID = "phone"
	route(h, c, `{"type":"new_session","session_id":"s1","name":"Run","backend":"codex"}`)
	_ = waitForType(t, c, "session_created")
	route(h, c, `{"type":"work_project_create","project_id":"p1","mutation_id":"mp","name":"P"}`)
	_ = waitForType(t, c, "work_mutation_ack")
	route(h, c, `{"type":"work_item_create","project_id":"p1","work_item_id":"wi1","mutation_id":"mi","title":"Deliver"}`)
	created := waitForType(t, c, "work_mutation_ack")
	version := uint64(created["entity_version"].(float64))
	route(h, c, `{"type":"work_item_move","work_item_id":"wi1","mutation_id":"mm","expected_version":`+formatUint(version)+`,"lifecycle":"ready"}`)
	moved := waitForType(t, c, "work_mutation_ack")
	version = uint64(moved["entity_version"].(float64))
	route(h, c, `{"type":"work_item_link_session","work_item_id":"wi1","session_id":"s1","mutation_id":"ml","expected_version":`+formatUint(version)+`,"role":"primary"}`)
	linked := waitForType(t, c, "work_mutation_ack")
	version = uint64(linked["entity_version"].(float64))
	exec.onSend = func(s *session.Session, reqID, content string) {
		if !strings.Contains(content, "Title: Deliver") || !strings.Contains(content, "[User instruction]\nimplement it") || !strings.Contains(content, "only the user can accept it") {
			t.Errorf("run content=%q", content)
		}
		h.Emit(protocol.NewDone(s.ID, reqID))
	}
	route(h, c, `{"type":"work_item_start_run","work_item_id":"wi1","session_id":"s1","request_id":"req1","run_id":"run1","run_kind":"implementation","mutation_id":"mr","expected_version":`+formatUint(version)+`,"content":"implement it"}`)
	ack := waitForType(t, c, "work_mutation_ack")
	if ack["run"].(map[string]any)["id"] != "run1" {
		t.Fatalf("run ack=%+v", ack)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		item, err := service.GetItem(context.Background(), "wi1")
		if err == nil && item.Lifecycle == workitems.LifecycleReview {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	item, _ := service.GetItem(context.Background(), "wi1")
	t.Fatalf("explicit successful run did not reach review: %+v", item)
}

func TestWorkReviewDecisionPersistsFeedbackAndDedupesQueuedRun(t *testing.T) {
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	c.deviceID = "note20"
	c.clientSurface = "android"
	route(h, c, `{"type":"new_session","session_id":"review-session","name":"Review","backend":"codex"}`)
	_ = waitForType(t, c, "session_created")
	ctx := context.Background()
	project, err := service.CreateProject(ctx, workitems.CreateProjectInput{ID: "review-project", Name: "Player review"})
	if err != nil {
		t.Fatal(err)
	}
	item, err := service.CreateItem(ctx, workitems.CreateItemInput{ID: "review-item", ProjectID: project.ID, Title: "Player report"})
	if err != nil {
		t.Fatal(err)
	}
	item, _ = service.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version, Lifecycle: workitems.LifecycleReady, Actor: workitems.Actor{Type: workitems.ActorUser}})
	_, item, _ = service.LinkSession(ctx, workitems.LinkSessionInput{ID: "review-link", WorkItemID: item.ID,
		SessionID: "review-session", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorUser}})
	item, _ = service.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version, Lifecycle: workitems.LifecycleActive, Actor: workitems.Actor{Type: workitems.ActorAgent}})
	item, _ = service.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version, Lifecycle: workitems.LifecycleReview, Actor: workitems.Actor{Type: workitems.ActorAgent}})

	frame := `{"type":"work_item_review_decision","work_item_id":"review-item","mutation_id":"review-mutation","expected_version":` + formatUint(item.Version) + `,"decision":"request_changes","body":"Include the missing screenshot.","comment_id":"review-comment","run_id":"review-run","request_id":"review-request"}`
	route(h, c, frame)
	ack := waitForType(t, c, "work_mutation_ack")
	if ack["comment"].(map[string]any)["body"] != "Include the missing screenshot." || ack["run"].(map[string]any)["status"] != "queued" {
		t.Fatalf("review decision ack=%+v", ack)
	}
	route(h, c, frame)
	second := waitForType(t, c, "work_mutation_ack")
	if second["mutation_id"] != "review-mutation" {
		t.Fatalf("dedup ack=%+v", second)
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil || len(snapshot.Runs) != 1 || len(snapshot.Comments) != 1 || snapshot.Items[0].Lifecycle != workitems.LifecycleActive {
		t.Fatalf("review snapshot=%+v err=%v", snapshot, err)
	}
}

func TestAutomaticWorkRunStaysDurableWhileSessionBusy(t *testing.T) {
	h, exec := newTestHub(t)
	sent := make(chan struct{}, 1)
	exec.onSend = func(_ *session.Session, _, _ string) { sent <- struct{}{} }
	service := attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	route(h, c, `{"type":"new_session","session_id":"s-busy","name":"Busy","backend":"codex"}`)
	_ = waitForType(t, c, "session_created")
	s, ok := h.registry.Get("s-busy")
	if !ok {
		t.Fatal("session missing")
	}
	s.Submit(func() {})
	deadline := time.Now().Add(time.Second)
	for s.State() != session.Streaming && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ctx := context.Background()
	project, err := service.CreateProject(ctx, workitems.CreateProjectInput{ID: "p-busy", Name: "P"})
	if err != nil {
		t.Fatal(err)
	}
	item, err := service.CreateItem(ctx, workitems.CreateItemInput{ID: "wi-busy", ProjectID: project.ID, Title: "Queued"})
	if err != nil {
		t.Fatal(err)
	}
	item, err = service.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version,
		Lifecycle: workitems.LifecycleReady, Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, item, err = service.LinkSession(ctx, workitems.LinkSessionInput{ID: "link-busy", WorkItemID: item.ID,
		SessionID: s.ID, ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	run, _, err := service.StartRun(ctx, workitems.StartRunInput{ID: "run-busy", WorkItemID: item.ID,
		SessionID: s.ID, RequestID: "req-busy", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorSystem}})
	if err != nil {
		t.Fatal(err)
	}
	claimed, _, claimedOK, err := service.ClaimNextRun(ctx, run.AvailableAt)
	if err != nil || !claimedOK {
		t.Fatalf("claim=%+v ok=%v err=%v", claimed, claimedOK, err)
	}
	if h.dispatchQueuedRun(ctx, claimed) {
		t.Fatal("busy session accepted automatic run")
	}
	if len(sent) != 0 || s.QueueLen() != 0 {
		t.Fatalf("automatic run entered in-memory queue: sends=%d queued=%d", len(sent), s.QueueLen())
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil || len(snapshot.Runs) != 1 || snapshot.Runs[0].Status != "deferred" || snapshot.Runs[0].QueueReason != "session_busy" {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	s.EndTurn()
}

func TestRelayReviewRunForcesReadOnlySandboxOnWritableCodexSession(t *testing.T) {
	h, exec := newTestHub(t)
	policy := make(chan string, 1)
	exec.onSendContext = func(ctx context.Context, _ *session.Session, _, _ string) {
		value, _ := backend.SandboxOverride(ctx)
		policy <- value
	}
	service := attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	route(h, c, `{"type":"new_session","session_id":"s-review","name":"Review","backend":"codex","sandbox":"danger-full-access"}`)
	_ = waitForType(t, c, "session_created")
	ctx := context.Background()
	project, err := service.CreateProject(ctx, workitems.CreateProjectInput{ID: "p-review", Name: "Review"})
	if err != nil {
		t.Fatal(err)
	}
	item, err := service.CreateItem(ctx, workitems.CreateItemInput{ID: "wi-review", ProjectID: project.ID, Title: "Player report"})
	if err != nil {
		t.Fatal(err)
	}
	item, err = service.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version,
		Lifecycle: workitems.LifecycleReady, Actor: workitems.Actor{Type: workitems.ActorSystem}})
	if err != nil {
		t.Fatal(err)
	}
	_, item, err = service.LinkSession(ctx, workitems.LinkSessionInput{ID: "link-review", WorkItemID: item.ID,
		SessionID: "s-review", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorSystem}})
	if err != nil {
		t.Fatal(err)
	}
	run, _, err := service.StartRun(ctx, workitems.StartRunInput{ID: "run-review", WorkItemID: item.ID,
		SessionID: "s-review", RequestID: "rjob_player_report", ExpectedVersion: item.Version,
		Actor: workitems.Actor{Type: workitems.ActorSystem}})
	if err != nil {
		t.Fatal(err)
	}
	claimed, _, ok, err := service.ClaimNextRun(ctx, run.AvailableAt)
	if err != nil || !ok {
		t.Fatalf("claim=%+v ok=%v err=%v", claimed, ok, err)
	}
	if !h.dispatchQueuedRun(ctx, claimed) {
		t.Fatal("review relay was not accepted by writable Codex Session")
	}
	select {
	case got := <-policy:
		if got != "read-only" {
			t.Fatalf("sandbox override=%q, want read-only", got)
		}
	case <-time.After(time.Second):
		t.Fatal("review run was not submitted")
	}
}

func TestRelayReviewRunStillRejectsWritableNonCodexSession(t *testing.T) {
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	route(h, c, `{"type":"new_session","session_id":"s-review-other","name":"Review","backend":"claude","sandbox":"danger-full-access"}`)
	_ = waitForType(t, c, "session_created")
	ctx := context.Background()
	project, _ := service.CreateProject(ctx, workitems.CreateProjectInput{ID: "p-review-other", Name: "Review"})
	item, _ := service.CreateItem(ctx, workitems.CreateItemInput{ID: "wi-review-other", ProjectID: project.ID, Title: "Player report"})
	item, _ = service.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version,
		Lifecycle: workitems.LifecycleReady, Actor: workitems.Actor{Type: workitems.ActorSystem}})
	_, item, _ = service.LinkSession(ctx, workitems.LinkSessionInput{ID: "link-review-other", WorkItemID: item.ID,
		SessionID: "s-review-other", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorSystem}})
	run, _, _ := service.StartRun(ctx, workitems.StartRunInput{ID: "run-review-other", WorkItemID: item.ID,
		SessionID: "s-review-other", RequestID: "rjob_player_report_other", ExpectedVersion: item.Version,
		Actor: workitems.Actor{Type: workitems.ActorSystem}})
	claimed, _, ok, err := service.ClaimNextRun(ctx, run.AvailableAt)
	if err != nil || !ok {
		t.Fatalf("claim=%+v ok=%v err=%v", claimed, ok, err)
	}
	if h.dispatchQueuedRun(ctx, claimed) {
		t.Fatal("review relay ran in a writable non-Codex Session")
	}
	snapshot, _ := service.Snapshot(ctx)
	if len(snapshot.Runs) != 1 || snapshot.Runs[0].QueueReason != "review_only_session_required" {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestWorkRequestOwnershipRemainsDurableAfterTerminalProjection(t *testing.T) {
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	ctx := context.Background()
	project, err := service.CreateProject(ctx, workitems.CreateProjectInput{ID: "p1", Name: "P"})
	if err != nil {
		t.Fatal(err)
	}
	item, err := service.CreateItem(ctx, workitems.CreateItemInput{ID: "wi1", ProjectID: project.ID, Title: "Deliver", Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	item, err = service.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, Lifecycle: workitems.LifecycleReady, ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, item, err = service.LinkSession(ctx, workitems.LinkSessionInput{ID: "link1", WorkItemID: item.ID, SessionID: "s1", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = service.StartRun(ctx, workitems.StartRunInput{ID: "run1", WorkItemID: item.ID, SessionID: "s1", RequestID: "req1", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorMobile}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.AdvanceRun(ctx, "s1", "req1", "succeeded", ""); err != nil {
		t.Fatal(err)
	}
	owned, err := service.OwnsRequest(ctx, "s1", "req1")
	if err != nil || !owned {
		t.Fatalf("terminal work request ownership: owned=%v err=%v", owned, err)
	}
	owned, err = service.OwnsRequest(ctx, "s1", "ordinary")
	if err != nil || owned {
		t.Fatalf("ordinary request ownership: owned=%v err=%v", owned, err)
	}
	if h.shouldNotifyTaskDone("s1", "req1") {
		t.Fatal("work request would also send legacy task_done notification")
	}
	if !h.shouldNotifyTaskDone("s1", "ordinary") {
		t.Fatal("ordinary request lost legacy task_done notification")
	}
}

func TestWorkHelloAdvertisesCapabilityAndReconcilesFromClientCursor(t *testing.T) {
	h, _ := newTestHub(t)
	attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	route(h, c, `{"type":"hello","device_id":"phone"}`)
	hello := waitForType(t, c, "hello_ack")
	capabilities, ok := hello["capabilities"].([]any)
	if !ok || len(capabilities) != 10 || capabilities[4] != "message_queue_v1" || capabilities[0] != "work_coordination_v1" || capabilities[1] != "work_items_v1" || capabilities[2] != "project_bootstrap_v1" || capabilities[3] != "work_review_feedback_v1" || capabilities[5] != "queue_message_detail_v1" || capabilities[6] != "session_config_revision_v1" || capabilities[7] != "file_attachments_v1" || capabilities[8] != "next_message_config_v1" || capabilities[9] != "bootstrap_snapshot_v1" {
		t.Fatalf("hello capabilities=%v", hello["capabilities"])
	}
	assertNoTypeWithin(t, c, "work_snapshot", 50*time.Millisecond)
	route(h, c, `{"type":"work_sync_request","revision":0}`)
	snapshot := waitForType(t, c, "work_snapshot")
	if _, ok := snapshot["projects"].([]any); !ok {
		t.Fatalf("work snapshot collections must be arrays: %+v", snapshot)
	}
}

func TestWorkSessionImportUsesRegistryIdentityAndBroadcastsWholeTransaction(t *testing.T) {
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	owner := newTestClient(h)
	owner.deviceID = "phone"
	owner.clientSurface = "android"
	observer := newTestClient(h)
	observer.deviceID = "desktop"
	route(h, owner, `{"type":"new_session","session_id":"s-import","name":"Verified session","cwd":"/verified/repo","backend":"codex","resume_claude_id":"thread-verified"}`)
	_ = waitForType(t, owner, "session_created")
	route(h, owner, `{"type":"work_session_import","session_id":"s-import","project_id":"p-import","work_item_id":"wi-import","mutation_id":"m-import","name":"Verified repo","workspace_path":"/untrusted/path","title":"Confirm imported outcome","outcome":"A durable draft","next_step":"Review scope","acceptance_criteria":"User accepts result"}`)
	ack := waitForType(t, owner, "work_mutation_ack")
	if ack["project"].(map[string]any)["workspace_path"] != "/verified/repo" {
		t.Fatalf("client workspace escaped registry authority: %+v", ack)
	}
	item := ack["item"].(map[string]any)
	if item["lifecycle"] != "inbox" || item["next_step"] != "Review scope" {
		t.Fatalf("imported draft=%+v", item)
	}
	if ack["link"].(map[string]any)["thread_id_snapshot"] != "thread-verified" {
		t.Fatalf("thread snapshot was not registry authoritative: %+v", ack)
	}
	delta := waitForType(t, observer, "work_delta_batch")
	if len(delta["changes"].([]any)) != 2 {
		t.Fatalf("observer missed atomic project/item changes: %+v", delta)
	}
	snapshot, err := service.Snapshot(context.Background())
	if err != nil || len(snapshot.Projects) != 1 || len(snapshot.Items) != 1 || len(snapshot.SessionLinks) != 1 {
		t.Fatalf("import snapshot=%+v err=%v", snapshot, err)
	}

	// A second semantic request for the same Session returns the original link
	// without another change or duplicate WorkItem.
	route(h, owner, `{"type":"work_session_import","session_id":"s-import","project_id":"p-other","work_item_id":"wi-other","mutation_id":"m-retry","name":"Other","title":"Duplicate"}`)
	retry := waitForType(t, owner, "work_mutation_ack")
	if retry["item"].(map[string]any)["id"] != "wi-import" {
		t.Fatalf("semantic retry did not return existing item: %+v", retry)
	}
}

func TestWorkMutationBroadcastAndPerDeviceRead(t *testing.T) {
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	a := newTestClient(h)
	a.deviceID = "device-a"
	b := newTestClient(h)
	b.deviceID = "device-b"

	route(h, a, `{"type":"work_project_create","project_id":"p1","mutation_id":"mp","name":"Bridge"}`)
	_ = waitForType(t, a, "work_mutation_ack")
	_ = waitForType(t, a, "work_delta_batch")
	_ = waitForType(t, b, "work_delta_batch")

	route(h, a, `{"type":"work_item_create","project_id":"p1","work_item_id":"wi1","mutation_id":"mi","title":"Native Work"}`)
	ack := waitForType(t, a, "work_mutation_ack")
	item := ack["item"].(map[string]any)
	revision := uint64(item["activity_revision"].(float64))
	_ = waitForType(t, a, "work_delta_batch")
	_ = waitForType(t, b, "work_delta_batch")

	// Lost-ACK retry returns the durable original response without a second
	// entity mutation or another broadcast.
	route(h, a, `{"type":"work_item_create","project_id":"p1","work_item_id":"wi1","mutation_id":"mi","title":"Native Work"}`)
	retry := waitForType(t, a, "work_mutation_ack")
	if retry["revision"] != ack["revision"] {
		t.Fatalf("retry response changed: first=%+v retry=%+v", ack, retry)
	}

	route(h, a, `{"type":"work_item_read","work_item_id":"wi1","revision":`+formatUint(revision)+`}`)
	read := waitForType(t, a, "work_read_ack")
	if read["item"].(map[string]any)["unread"] != float64(0) {
		t.Fatalf("read ACK=%+v", read)
	}
	as, err := service.SnapshotForDevice(context.Background(), "device-a")
	if err != nil {
		t.Fatal(err)
	}
	bs, err := service.SnapshotForDevice(context.Background(), "device-b")
	if err != nil {
		t.Fatal(err)
	}
	if as.Items[0].Unread != 0 || bs.Items[0].Unread != 1 {
		t.Fatalf("device read leaked: a=%d b=%d", as.Items[0].Unread, bs.Items[0].Unread)
	}
}

func TestWorkSnapshotSurvivesBridgeRestart(t *testing.T) {
	dir := t.TempDir()
	h1, _ := newTestHub(t)
	service1, err := workitems.OpenService(dir, "i1")
	if err != nil {
		t.Fatal(err)
	}
	h1.SetWorkItems(service1)
	c1 := newTestClient(h1)
	c1.deviceID = "desktop"
	route(h1, c1, `{"type":"work_project_create","project_id":"p1","mutation_id":"mp","name":"Persist"}`)
	_ = waitForType(t, c1, "work_mutation_ack")
	route(h1, c1, `{"type":"work_item_create","project_id":"p1","work_item_id":"wi1","mutation_id":"mi","title":"Restart proof"}`)
	_ = waitForType(t, c1, "work_mutation_ack")
	if err := service1.Close(); err != nil {
		t.Fatal(err)
	}

	h2, _ := newTestHub(t)
	service2, err := workitems.OpenService(dir, "i1")
	if err != nil {
		t.Fatal(err)
	}
	defer service2.Close()
	h2.SetWorkItems(service2)
	c2 := newTestClient(h2)
	c2.deviceID = "phone"
	h2.sendWorkSnapshot(c2, true)
	snapshot := waitForType(t, c2, "work_snapshot")
	items := snapshot["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["id"] != "wi1" {
		t.Fatalf("restart snapshot=%+v db=%s", snapshot, filepath.Join(dir, "everything_go_work_items.db"))
	}
}

func TestProjectBootstrapGeneratesReviewDraftAndRequiresHumanApproval(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("# Bridge\n\nManage local AI work reliably.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "package.json"), []byte(`{"name":"bridge","scripts":{"test":"vitest"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	c.deviceID = "phone"
	c.clientSurface = "android"
	newSession, _ := json.Marshal(map[string]any{"type": "new_session", "session_id": "s-bootstrap", "name": "Verify project import", "cwd": workspace, "backend": "codex"})
	route(h, c, string(newSession))
	_ = waitForType(t, c, "session_created")
	generate, _ := json.Marshal(map[string]any{
		"type": "work_project_bootstrap_generate", "mutation_id": "mb-generate", "name": "Bridge",
		"workspace_path": workspace, "session_ids": []string{"s-bootstrap"},
	})
	route(h, c, string(generate))
	ack := waitForType(t, c, "work_mutation_ack")
	draft := ack["bootstrap"].(map[string]any)
	if draft["status"] != "review" || !strings.Contains(draft["objective"].(string), "Manage local AI work") {
		t.Fatalf("bootstrap draft=%+v", draft)
	}
	suggestions := draft["suggestions"].([]any)
	if len(suggestions) != 1 || suggestions[0].(map[string]any)["session_id"] != "s-bootstrap" {
		t.Fatalf("suggestions=%+v", suggestions)
	}
	before, err := service.Snapshot(context.Background())
	if err != nil || len(before.Projects) != 0 || len(before.Items) != 0 {
		t.Fatalf("draft mutated formal work before approval: %+v err=%v", before, err)
	}
	approve, _ := json.Marshal(map[string]any{
		"type": "work_project_bootstrap_approve", "mutation_id": "mb-approve",
		"bootstrap_id": draft["id"], "expected_version": draft["version"], "name": "Bridge",
		"objective": draft["objective"], "current_state": draft["current_state"],
		"next_step": draft["next_step"], "acceptance_criteria": draft["acceptance_criteria"],
		"selected_suggestion_ids": []string{suggestions[0].(map[string]any)["id"].(string)},
	})
	route(h, c, string(approve))
	approved := waitForType(t, c, "work_mutation_ack")
	if approved["bootstrap"].(map[string]any)["status"] != "applied" || len(approved["items"].([]any)) != 1 || len(approved["links"].([]any)) != 1 {
		t.Fatalf("approval ACK=%+v", approved)
	}
	after, err := service.Snapshot(context.Background())
	if err != nil || len(after.Projects) != 1 || len(after.Items) != 1 || after.Bootstraps[0].Status != "applied" {
		t.Fatalf("approved snapshot=%+v err=%v", after, err)
	}
}

func TestProjectBootstrapRejectsClientChosenUnmappedWorkspace(t *testing.T) {
	h, _ := newTestHub(t)
	attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	c.deviceID = "phone"
	generate, _ := json.Marshal(map[string]any{
		"type": "work_project_bootstrap_generate", "mutation_id": "mb-denied", "name": "Unknown",
		"workspace_path": t.TempDir(),
	})
	route(h, c, string(generate))
	event := waitForType(t, c, "work_error")
	if !strings.Contains(event["message"].(string), "authorized") {
		t.Fatalf("unexpected error=%+v", event)
	}
}

func TestWorkAttachmentUsesCanonicalJournalAndReportsMissingSource(t *testing.T) {
	h, _ := newTestHub(t)
	attachWorkService(t, h, t.TempDir())
	c := newTestClient(h)
	c.deviceID = "phone"
	c.clientSurface = "android"
	route(h, c, `{"type":"work_project_create","project_id":"p1","mutation_id":"mp","name":"P"}`)
	_ = waitForType(t, c, "work_mutation_ack")
	route(h, c, `{"type":"work_item_create","project_id":"p1","work_item_id":"wi1","mutation_id":"mi","title":"Proof"}`)
	created := waitForType(t, c, "work_mutation_ack")
	version := uint64(created["entity_version"].(float64))

	path := filepath.Join(t.TempDir(), "proof.png")
	if err := os.WriteFile(path, []byte("png"), 0o600); err != nil {
		t.Fatal(err)
	}
	record, added := h.attachments.Add(protocol.Media{Type: "media", SessionID: "s1", RequestID: "r1", MediaType: "image", Path: path})
	if !added {
		t.Fatal("canonical attachment was not registered")
	}
	route(h, c, `{"type":"work_item_attachment_add","work_item_id":"wi1","work_attachment_id":"wa1","attachment_id":"`+record.AttachmentID+`","mutation_id":"ma","expected_version":`+formatUint(version)+`}`)
	ack := waitForType(t, c, "work_mutation_ack")
	attachment := ack["attachment"].(map[string]any)
	if attachment["status"] != "available" || attachment["url"] == "" {
		t.Fatalf("materialized attachment=%+v", attachment)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	route(h, c, `{"type":"work_sync_request","revision":0}`)
	snapshot := waitForType(t, c, "work_snapshot")
	attachments := snapshot["attachments"].([]any)
	if len(attachments) != 1 || attachments[0].(map[string]any)["status"] != "missing" {
		t.Fatalf("missing attachment snapshot=%+v", attachments)
	}
	activities := snapshot["activities"].([]any)
	if len(activities) == 0 || activities[0].(map[string]any)["actor"] != "mobile" {
		t.Fatalf("client surface was not preserved in activity: %+v", activities)
	}
}
