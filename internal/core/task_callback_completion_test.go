package core

import (
	"context"
	"everything-go/internal/callbacks"
	"everything-go/internal/delegation"
	"everything-go/internal/workitems"
	"strings"
	"testing"
)

func TestCallbackRootWaitsForExactChildDeliveryAndRequiredQA(t *testing.T) {
	h, _, s, _ := callbackFixture(t)
	ctx := context.Background()
	r := callbacks.Record{Session: s.ID, Request: "root-request"}
	child := delegation.Record{ID: "owner-child", ParentSessionID: s.ID, OriginRequestID: r.Request, ChildSessionID: "child-session", ChildRequestID: "child-request", ParentRequestID: "child-return"}
	if e := h.delegations.Create(ctx, child); e != nil {
		t.Fatal(e)
	}
	if h.callbackRootReady(ctx, r) {
		t.Fatal("pending child completed root")
	}
	if _, e := h.delegations.Complete(ctx, child.ChildSessionID, child.ChildRequestID, "completed", "sealed child result", "", nil); e != nil {
		t.Fatal(e)
	}
	if h.callbackRootReady(ctx, r) {
		t.Fatal("undelivered child completed root")
	}
	if e := h.delegations.MarkDeliveryQueued(ctx, child.ID); e != nil {
		t.Fatal(e)
	}
	if _, e := h.delegations.MarkDelivered(ctx, s.ID, child.ParentRequestID); e != nil {
		t.Fatal(e)
	}
	if !h.callbackRootReady(ctx, r) {
		t.Fatal("delivered child not recognized")
	}
	if e := h.delegations.BindQA(ctx, child.ID, "independent-qa", func() error { return nil }); e != nil {
		t.Fatal(e)
	}
	if h.callbackRootReady(ctx, r) {
		t.Fatal("required QA skipped")
	}
	if e := h.delegations.RecordQA(ctx, child.ID, "qa-round", "PASS"); e != nil {
		t.Fatal(e)
	}
	if !h.callbackRootReady(ctx, r) {
		t.Fatal("exact required QA PASS ignored")
	}
}

func TestCallbackWorkReviewIsReportableWithoutAcceptingHumanWork(t *testing.T) {
	h, c, s, e := callbackFixture(t)
	ctx := context.Background()
	work := attachWorkService(t, h, t.TempDir())
	p, err := work.CreateProject(ctx, workitems.CreateProjectInput{ID: "callback-project", Name: "P"})
	if err != nil {
		t.Fatal(err)
	}
	item, err := work.CreateItem(ctx, workitems.CreateItemInput{ID: "callback-work", ProjectID: p.ID, Title: "待驗收的工作", Outcome: "尚未驗證的理想結果"})
	if err != nil {
		t.Fatal(err)
	}
	item, err = work.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version, Lifecycle: workitems.LifecycleReady, Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, item, err = work.LinkSession(ctx, workitems.LinkSessionInput{ID: "callback-work-link", WorkItemID: item.ID, SessionID: s.ID, ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	run, _, err := work.StartRun(ctx, workitems.StartRunInput{ID: "callback-work-run", WorkItemID: item.ID, SessionID: s.ID, RequestID: "callback-work-request", ExpectedVersion: item.Version, Actor: workitems.Actor{Type: workitems.ActorSystem}})
	if err != nil {
		t.Fatal(err)
	}
	got := callbackAction(t, h, c, callbackInput{Action: "set", Operation: "callback_work_arm", Session: s.ID, Request: run.RequestID, Enabled: true})
	if got["accepted"] != true {
		t.Fatal(got)
	}
	h.reconcileTaskCallbacks(ctx, s.ID, run.RequestID)
	rows, _ := h.callbacks.List(ctx)
	if rows[0].Report != nil {
		t.Fatal("queued work reported ready")
	}
	if _, err = work.AdvanceRun(ctx, s.ID, run.RequestID, "succeeded", ""); err != nil {
		t.Fatal(err)
	}
	h.reconcileTaskCallbacks(ctx, s.ID, run.RequestID)
	rows, _ = h.callbacks.List(ctx)
	r := rows[0]
	if r.Report == nil || r.Report.Status != "review_ready" || r.Report.SourceKind != "workitem" || strings.Contains(r.Report.Text, "尚未驗證的理想結果") {
		t.Fatal("review truth or desired-outcome boundary lost", r.Report)
	}
	item, err = work.GetItem(ctx, item.ID)
	if err != nil || item.Lifecycle != workitems.LifecycleReview {
		t.Fatal("callback accepted human work", item, err)
	}
	if _, err = work.DecideReview(ctx, workitems.ReviewDecisionInput{WorkItemID: item.ID, ExpectedVersion: item.Version, Decision: "accept", Actor: workitems.Actor{Type: workitems.ActorUser}}); err != nil {
		t.Fatal(err)
	}
	got = callbackAction(t, h, c, callbackInput{Action: "claim", Operation: "callback_work_stale_claim", Session: s.ID, Request: run.RequestID, Expected: r.Revision, Invitation: r.Invite.ID})
	if got["accepted"] != false || e.starts.Load() != 0 {
		t.Fatal("old review invited after human acceptance", got)
	}
}
