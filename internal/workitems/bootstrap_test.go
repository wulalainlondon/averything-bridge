package workitems

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func bootstrapInput() SaveBootstrapInput {
	return SaveBootstrapInput{
		ProjectName: "Bridge", WorkspacePath: "/work/bridge", Fingerprint: strings.Repeat("a", 64),
		Objective: "Keep local AI work reliable.", CurrentState: "Tests pass.", NextStep: "Review the release.",
		AcceptanceCriteria: "Owner accepts the signed release.", Constraints: []string{"Do not expose secrets."},
		Decisions: []string{"Go is the server runtime."}, SessionIDs: []string{"s1", "s2"},
		Sources: []BootstrapSource{{ID: "doc:README.md", Kind: "document", Label: "README.md", Fingerprint: "f1"}},
		Suggestions: []BootstrapSuggestion{
			{ID: "sg1", WorkItemID: "wi-bootstrap-1", SessionID: "s1", Title: "Verify offline replay", Description: "Review evidence", EvidenceRefs: []string{"session:s1"}},
			{ID: "sg2", WorkItemID: "wi-bootstrap-2", SessionID: "s2", Title: "Verify startup", EvidenceRefs: []string{"session:s2"}},
		},
	}
}

func TestBootstrapDraftPersistsAndApprovalIsAtomic(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, "authority")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	saved, err := store.SaveBootstrapDraft(ctx, bootstrapInput())
	if err != nil {
		t.Fatal(err)
	}
	if !saved.Changed || saved.Draft.Status != bootstrapStatusReview || saved.FirstRevision == 0 {
		t.Fatalf("saved=%+v", saved)
	}
	retry, err := store.SaveBootstrapDraft(ctx, bootstrapInput())
	if err != nil {
		t.Fatal(err)
	}
	if retry.Changed || retry.Draft.ID != saved.Draft.ID || retry.Draft.Version != saved.Draft.Version {
		t.Fatalf("same fingerprint was not idempotent: %+v", retry)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, "authority")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snap, err := store.Snapshot(ctx)
	if err != nil || len(snap.Bootstraps) != 1 || snap.Bootstraps[0].ID != saved.Draft.ID {
		t.Fatalf("restart snapshot=%+v err=%v", snap.Bootstraps, err)
	}
	approved, err := store.ApproveBootstrap(ctx, ApproveBootstrapInput{
		BootstrapID: saved.Draft.ID, ExpectedVersion: saved.Draft.Version,
		ProjectName: "Bridge", Objective: saved.Draft.Objective, CurrentState: saved.Draft.CurrentState,
		NextStep: saved.Draft.NextStep, AcceptanceCriteria: saved.Draft.AcceptanceCriteria,
		SelectedSuggestionIDs: []string{"sg1", "sg2"}, Actor: Actor{Type: ActorMobile, DeviceID: "phone"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if approved.Draft.Status != bootstrapStatusApplied || len(approved.Items) != 2 || len(approved.Links) != 2 {
		t.Fatalf("approved=%+v", approved)
	}
	if !strings.Contains(approved.Project.Context, bootstrapContextStart) || !strings.Contains(approved.Project.Context, "Keep local AI work reliable") {
		t.Fatalf("project context=%q", approved.Project.Context)
	}
	final, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Projects) != 1 || len(final.Items) != 2 || len(final.SessionLinks) != 2 || len(final.Bootstraps) != 1 {
		t.Fatalf("final snapshot=%+v", final)
	}
	again, err := store.ApproveBootstrap(ctx, ApproveBootstrapInput{BootstrapID: saved.Draft.ID, ExpectedVersion: saved.Draft.Version})
	if err != nil || !again.AlreadyApplied || len(again.Items) != 0 {
		t.Fatalf("second approval=%+v err=%v", again, err)
	}
}

func TestBootstrapApprovalRejectsStaleOrUnknownSelectionWithoutPartialProject(t *testing.T) {
	store, err := Open(t.TempDir(), "authority")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	saved, err := store.SaveBootstrapDraft(ctx, bootstrapInput())
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ApproveBootstrap(ctx, ApproveBootstrapInput{
		BootstrapID: saved.Draft.ID, ExpectedVersion: saved.Draft.Version,
		ProjectName: "Bridge", SelectedSuggestionIDs: []string{"unknown"}, Actor: Actor{Type: ActorMobile},
	})
	if err == nil {
		t.Fatal("unknown selection should fail")
	}
	snap, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Projects) != 0 || len(snap.Items) != 0 || snap.Bootstraps[0].Status != bootstrapStatusReview {
		t.Fatalf("failed approval partially mutated state: %+v", snap)
	}
	_, err = store.ApproveBootstrap(ctx, ApproveBootstrapInput{
		BootstrapID: saved.Draft.ID, ExpectedVersion: saved.Draft.Version + 1,
		ProjectName: "Bridge", Actor: Actor{Type: ActorMobile},
	})
	if err == nil || !strings.Contains(err.Error(), "version conflict") {
		t.Fatalf("stale approval err=%v", err)
	}
}

func TestBootstrapRefreshReplacesGeneratedContextWithoutDuplicatingIt(t *testing.T) {
	store, err := Open(t.TempDir(), "authority")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	project, err := store.CreateProject(ctx, CreateProjectInput{ID: "p1", Name: "Bridge", WorkspacePath: "/work/bridge", Context: "Human note."})
	if err != nil {
		t.Fatal(err)
	}
	in := bootstrapInput()
	in.ProjectID, in.ProjectVersion = project.ID, project.Version
	saved, err := store.SaveBootstrapDraft(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := store.ApproveBootstrap(ctx, ApproveBootstrapInput{
		BootstrapID: saved.Draft.ID, ExpectedVersion: saved.Draft.Version, ProjectName: "Bridge",
		Objective: "First objective", SelectedSuggestionIDs: nil, Actor: Actor{Type: ActorMobile},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(approved.Project.Context, bootstrapContextStart) != 1 || !strings.Contains(approved.Project.Context, "Human note.") {
		t.Fatalf("first context=%q", approved.Project.Context)
	}
	in.Fingerprint = strings.Repeat("b", 64)
	in.ProjectVersion = approved.Project.Version
	in.Objective = "Second objective"
	refreshed, err := store.SaveBootstrapDraft(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	approved, err = store.ApproveBootstrap(ctx, ApproveBootstrapInput{
		BootstrapID: refreshed.Draft.ID, ExpectedVersion: refreshed.Draft.Version, ProjectName: "Bridge",
		Objective: refreshed.Draft.Objective, Actor: Actor{Type: ActorMobile},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(approved.Project.Context, bootstrapContextStart) != 1 || !strings.Contains(approved.Project.Context, "Second objective") || strings.Contains(approved.Project.Context, "First objective") {
		t.Fatalf("refreshed context=%q", approved.Project.Context)
	}
}

func TestBootstrapDraftRegistryIsCapacityBounded(t *testing.T) {
	store, err := Open(t.TempDir(), "authority")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	for index := 0; index < 130; index++ {
		in := bootstrapInput()
		in.WorkspacePath = fmt.Sprintf("/work/project-%03d", index)
		in.Fingerprint = fmt.Sprintf("%064d", index)
		if _, err := store.SaveBootstrapDraft(ctx, in); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Bootstraps) != 128 {
		t.Fatalf("bootstrap capacity=%d", len(snap.Bootstraps))
	}
}
