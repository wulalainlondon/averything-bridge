package sessiondispatch

import (
	"context"
	"testing"
)

func TestGrantRevisionAndDispatchIdempotency(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	g, e := s.Grant(ctx, "parent")
	if e != nil || g.Enabled || g.Instances == nil || g.Sessions == nil {
		t.Fatal(g, e)
	}
	g, e = s.SetGrant(ctx, "parent", Grant{Enabled: true, Local: true, Sessions: []string{"local:target"}}, 0)
	if e != nil || !g.Allows("local", "local", "target") || g.Allows("local", "local", "other") || !g.AllowsInstance("local", "local") {
		t.Fatal(g, e)
	}
	if _, e = s.SetGrant(ctx, "parent", g, 0); e == nil {
		t.Fatal("stale grant accepted")
	}
	r := Record{ID: "job", ParentID: "parent", OriginRequestID: "turn", ToolCallID: "call", RequestID: "request", IntentHash: "hash", State: "prepared"}
	first, e := s.Create(ctx, r)
	if e != nil || !first {
		t.Fatal(e)
	}
	again, e := s.Create(ctx, r)
	if e != nil || again {
		t.Fatal("duplicate inserted", e)
	}
	got, ok, e := s.ByOrigin(ctx, "parent", "turn", "call")
	if e != nil || !ok || got.ID != "job" {
		t.Fatal(got, e)
	}
}
func TestDispatchSurvivesRestartAndQueuedReturnRemainsPending(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, _ := Open(dir)
	r := Record{ID: "job", ParentID: "parent", OriginRequestID: "turn", ToolCallID: "call", RequestID: "request", IntentHash: "hash", State: "completed", DeliveryState: "queued", Result: "final only"}
	s.Create(ctx, r)
	s.Close()
	s, _ = Open(dir)
	defer s.Close()
	list, e := s.Pending(ctx)
	if e != nil || len(list) != 1 || list[0].Result != "final only" {
		t.Fatal(list, e)
	}
	list[0].DeliveryState = "delivered"
	s.Put(ctx, list[0])
	list, e = s.Pending(ctx)
	if e != nil || len(list) != 0 {
		t.Fatal(list, e)
	}
}
func TestFinalAnswerSignalCannotBeOverwrittenByAnOlderStatusPoll(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	ctx := context.Background()
	r := Record{ID: "job", ParentID: "parent", OriginRequestID: "turn", ToolCallID: "call", SessionID: "target", RequestID: "request", IntentHash: "hash", State: "queued"}
	s.Create(ctx, r)
	if e := s.SealResult(ctx, "target", "request", "exact final"); e != nil {
		t.Fatal(e)
	}
	r.State = "completed"
	if e := s.Put(ctx, r); e != nil {
		t.Fatal(e)
	}
	got, _, _ := s.Get(ctx, "job")
	if got.Result != "exact final" {
		t.Fatal(got)
	}
}
