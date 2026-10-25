package workitems

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestContextPackIncludesProjectAndPreservesHumanAcceptanceContract(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_, err := s.CreateProject(ctx, CreateProjectInput{ID: "p1", Name: "Bridge", WorkspacePath: "/repo", Context: "Use the signed release pipeline."})
	if err != nil {
		t.Fatal(err)
	}
	item, err := s.CreateItem(ctx, CreateItemInput{ID: "i1", ProjectID: "p1", Title: "Ship it", Description: strings.Repeat("background ", 100), Outcome: "A verified release", AcceptanceCriteria: "Tests pass"})
	if err != nil {
		t.Fatal(err)
	}
	pack, err := s.BuildContextPack(ctx, item.ID, 700)
	if err != nil {
		t.Fatal(err)
	}
	if !pack.Truncated || !strings.Contains(pack.Prompt, "Use the signed release pipeline") || !strings.Contains(pack.Prompt, "Never mark this item done") {
		t.Fatalf("context contract or project context missing: truncated=%v prompt=%q", pack.Truncated, pack.Prompt)
	}
	pack.Prompt, pack.Truncated = RenderContextPrompt(pack, 0, pack.Truncated)
	if !strings.Contains(pack.Prompt, "Project context") || !strings.Contains(pack.Prompt, "Never mark this item done") {
		t.Fatalf("default render budget lost context: %q", pack.Prompt)
	}
}

func TestWorkflowDAGPersistsAndRejectsCycles(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.CreateProject(ctx, CreateProjectInput{ID: "p1", Name: "P"}); err != nil {
		t.Fatal(err)
	}
	definition := WorkflowDefinition{Nodes: []WorkflowNode{{ID: "a", Name: "Plan", Kind: "human"}, {ID: "b", Name: "Build", Kind: "agent"}}, Edges: []WorkflowEdge{{From: "a", To: "b"}}}
	wf, err := s.CreateWorkflow(ctx, CreateWorkflowInput{ID: "wf1", ProjectID: "p1", Name: "Delivery", Definition: definition})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateItem(ctx, CreateItemInput{ID: "i1", ProjectID: "p1", Title: "Use flow", WorkflowID: wf.ID, WorkflowNodeID: "a"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil || len(snapshot.Workflows) != 1 || snapshot.Items[0].WorkflowID != wf.ID {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	cycle := WorkflowDefinition{Nodes: definition.Nodes, Edges: []WorkflowEdge{{From: "a", To: "b"}, {From: "b", To: "a"}}}
	if err := ValidateWorkflow(cycle); err == nil {
		t.Fatal("cyclic workflow accepted")
	}
}

func TestPersistentRunQueueClaimsOnceDefersAndRecovers(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	item := seedItem(t, s, "p1", "i1")
	item = move(t, s, item, LifecycleReady, ActorUser)
	_, item, err := s.LinkSession(ctx, LinkSessionInput{ID: "link1", WorkItemID: item.ID, SessionID: "s1", ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	run, _, err := s.StartRun(ctx, StartRunInput{ID: "run1", WorkItemID: item.ID, SessionID: "s1", RequestID: "req1", Instruction: "continue", ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	claimed, _, ok, err := s.ClaimNextRun(ctx, run.AvailableAt)
	if err != nil || !ok || claimed.ID != run.ID || claimed.Attempt != 1 {
		t.Fatalf("claim=%+v ok=%v err=%v", claimed, ok, err)
	}
	if _, _, ok, err := s.ClaimNextRun(ctx, run.AvailableAt); err != nil || ok {
		t.Fatalf("duplicate claim ok=%v err=%v", ok, err)
	}
	if _, _, err := s.DeferRun(ctx, run.ID, run.AvailableAt+5000, "quota"); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := s.ClaimNextRun(ctx, run.AvailableAt+4999); ok {
		t.Fatal("deferred run claimed too early")
	}
	claimed, _, ok, err = s.ClaimNextRun(ctx, run.AvailableAt+5000)
	if err != nil || !ok || claimed.Attempt != 2 {
		t.Fatalf("deferred claim=%+v ok=%v err=%v", claimed, ok, err)
	}
	if err := s.MarkRunSubmitted(ctx, run.ID); err != nil {
		t.Fatalf("dispatching run receipt failed: %v", err)
	}
	if count, err := s.RecoverQueue(ctx); err != nil || count != 1 {
		t.Fatalf("recover count=%d err=%v", count, err)
	}
	claimed, _, ok, err = s.ClaimNextRun(ctx, run.AvailableAt+5000)
	if err != nil || !ok || claimed.QueueReason != "" {
		t.Fatalf("recovered claim=%+v ok=%v err=%v", claimed, ok, err)
	}
}

func TestPersistentRunQueueUsesSemanticPriorityOrder(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.CreateProject(ctx, CreateProjectInput{ID: "p1", Name: "P"}); err != nil {
		t.Fatal(err)
	}
	createRun := func(itemID, sessionID string, priority Priority) {
		item, err := s.CreateItem(ctx, CreateItemInput{ID: itemID, ProjectID: "p1", Title: itemID, Priority: priority})
		if err != nil {
			t.Fatal(err)
		}
		item = move(t, s, item, LifecycleReady, ActorUser)
		_, item, err = s.LinkSession(ctx, LinkSessionInput{ID: "link-" + itemID, WorkItemID: item.ID,
			SessionID: sessionID, ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.StartRun(ctx, StartRunInput{ID: "run-" + itemID, WorkItemID: item.ID,
			SessionID: sessionID, RequestID: "req-" + itemID, ExpectedVersion: item.Version,
			Actor: Actor{Type: ActorUser}}); err != nil {
			t.Fatal(err)
		}
	}
	createRun("low", "s-low", PriorityLow)
	createRun("urgent", "s-urgent", PriorityUrgent)
	createRun("high", "s-high", PriorityHigh)

	for _, want := range []string{"run-urgent", "run-high", "run-low"} {
		run, _, ok, err := s.ClaimNextRun(ctx, s.now().UnixMilli())
		if err != nil || !ok || run.ID != want {
			t.Fatalf("claim=%+v ok=%v err=%v want=%s", run, ok, err, want)
		}
		if _, _, err := s.DeferRun(ctx, run.ID, s.now().Add(time.Hour).UnixMilli(), "test_complete"); err != nil {
			t.Fatal(err)
		}
	}
}
