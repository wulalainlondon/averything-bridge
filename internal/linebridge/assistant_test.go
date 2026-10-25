package linebridge

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestAssistantAliasesRemainCallerAndAccountBound(t *testing.T) {
	st, s, _, owner := fixture(t)
	ctx := context.Background()
	a, ta := accountFixture(t, s, owner, "A")
	b, tb := accountFixture(t, s, owner, "B")
	user := Caller{Subject: "session:voice"}
	if e := st.Grant(ctx, owner, a.ID, user.Subject, []string{"directory", "status"}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.SetTargetAlias(ctx, user, Request{Account: a.ID, Target: tb.Ref, Alias: "工作 Morrie"}); e == nil {
		t.Fatal("cross account alias")
	}
	if _, e := s.SetTargetAlias(ctx, user, Request{Account: a.ID, Target: ta.Ref, Alias: "工作 Morrie"}); e != nil {
		t.Fatal(e)
	}
	targets, e := s.Find(ctx, user, a.ID, "工作 MORRIE", 10)
	if e != nil || len(targets) != 1 || targets[0].Ref != ta.Ref {
		t.Fatal(targets, e)
	}
	if target, e := s.aliasTarget(ctx, owner, a.ID, "工作 Morrie"); e != nil || target != nil {
		t.Fatal("alias leaked to different caller")
	}
	if target, e := s.aliasTarget(ctx, user, b.ID, "工作 Morrie"); e != nil || target != nil {
		t.Fatal("alias leaked to different account")
	}
	other, e := st.SaveTarget(ctx, a.ID, "another", "Same Name", "user")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.SetTargetAlias(ctx, user, Request{Account: a.ID, Target: other.Ref, Alias: "工作 Morrie"}); SafeCode(e) != "line_alias_conflict" {
		t.Fatal(e)
	}
	if e = st.Grant(ctx, owner, a.ID, user.Subject, []string{"status"}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Find(ctx, user, a.ID, "工作 Morrie", 10); e == nil {
		t.Fatal("revoked alias remains usable")
	}
}

func TestAssistantOperationsPageFiltersCASAndNoReplay(t *testing.T) {
	st, s, _, c := fixture(t)
	ctx := context.Background()
	a, target := accountFixture(t, s, c, "A")
	due := time.Now().Add(time.Hour).UnixMilli()
	for i := 0; i < 105; i++ {
		if _, e := s.Send(ctx, c, a.ID, target.Ref, Content{Text: fmt.Sprintf("scheduled %d", i)}, fmt.Sprintf("intent-%03d", i), due, "Asia/Taipei", 60000); e != nil {
			t.Fatal(e)
		}
	}
	r := Request{Account: a.ID, State: "pending", Limit: 30}
	seen := map[string]bool{}
	var chosen OperationDetail
	var cursor string
	for {
		page, e := s.QueryOperations(ctx, c, r)
		if e != nil {
			t.Fatal(e)
		}
		for _, v := range page.Items {
			if seen[v.ID] {
				t.Fatal("duplicate")
			}
			seen[v.ID] = true
			chosen = v
			if v.Message.Text == "" || v.AllowedDelay != 60000 {
				t.Fatal("missing message/window")
			}
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
		r.Cursor = page.Next
	}
	if len(seen) != 105 {
		t.Fatal(len(seen))
	}
	r.Cursor = cursor
	r.State = "cancelled"
	if _, e := s.QueryOperations(ctx, c, r); e == nil {
		t.Fatal("cursor filter swap")
	}
	other := Caller{Subject: "session:other"}
	if e := st.Grant(ctx, c, a.ID, other.Subject, []string{"status"}); e != nil {
		t.Fatal(e)
	}
	r.State = "pending"
	if _, e := s.QueryOperations(ctx, other, r); e == nil {
		t.Fatal("cursor caller swap")
	}
	if _, e := s.OperationDetail(ctx, other, chosen.ID); e == nil {
		t.Fatal("content leaked")
	}
	changed, e := s.UpdateScheduledMessage(ctx, c, Request{Operation: chosen.ID, Revision: chosen.Revision, Content: Content{Text: "updated"}})
	if e != nil || changed.Message.Text != "updated" || changed.Revision != chosen.Revision+1 {
		t.Fatal(changed, e)
	}
	if _, e = s.UpdateScheduledMessage(ctx, c, Request{Operation: chosen.ID, Revision: chosen.Revision, Content: Content{Text: "stale"}}); e == nil {
		t.Fatal("stale edit")
	}
	if _, e = st.Transition(ctx, chosen.ID, "scheduled", "unknown", "unknown", "", ""); e != nil {
		t.Fatal(e)
	}
	if _, e = s.UpdateScheduledMessage(ctx, c, Request{Operation: chosen.ID, Revision: changed.Revision + 1, Content: Content{Text: "retry"}}); e == nil {
		t.Fatal("unknown modified")
	}
}

func TestResolveExplicitScheduleTimeRejectsDSTAmbiguity(t *testing.T) {
	if _, e := ResolveScheduleTime("2030-11-03 01:30:00", "America/New_York"); SafeCode(e) != "line_local_time_ambiguous" {
		t.Fatal(e)
	}
	if _, e := ResolveScheduleTime("2030-03-10 02:30:00", "America/New_York"); SafeCode(e) != "line_local_time_invalid" {
		t.Fatal(e)
	}
	if _, e := ResolveScheduleTime("2030-12-01 15:00:00", "Asia/Taipei"); e != nil {
		t.Fatal(e)
	}
	if _, e := ResolveScheduleTime("2030-12-01 15:00:00", "Local"); e == nil {
		t.Fatal("machine zone assumed")
	}
}

func TestHistoryCursorAndFiltersAreSourceBound(t *testing.T) {
	_, s, _, c := fixture(t)
	ctx := context.Background()
	a, target := accountFixture(t, s, c, "A")
	// Fixture driver implements paged upstream records without any network effects.
	r := Request{Account: a.ID, Target: target.Ref, Query: "fixture", Since: 1, Limit: 2}
	p, e := s.ReadPage(ctx, c, r)
	if e != nil || len(p.Messages) != 1 || p.Next == "" || p.CompleteHistory {
		t.Fatal(p, e)
	}
	r.Cursor = p.Next
	r.Query = "other"
	if _, e = s.ReadPage(ctx, c, r); e == nil {
		t.Fatal("filter replay")
	}
	r.Query = "fixture"
	r.Cursor = p.Next + "tampered"
	if _, e = s.ReadPage(ctx, c, r); e == nil {
		t.Fatal("tampered cursor")
	}
}

func TestScheduledEditWinsAgainstOldSchedulerSnapshot(t *testing.T) {
	st, s, _, c := fixture(t)
	ctx := context.Background()
	a, target := accountFixture(t, s, c, "A")
	o, e := s.Send(ctx, c, a.ID, target.Ref, Content{Text: "old"}, "intent-race", time.Now().Add(time.Hour).UnixMilli(), "Asia/Taipei", 0)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.UpdateScheduledMessage(ctx, c, Request{Operation: o.ID, Revision: o.Revision, Content: Content{Text: "new"}})
	if e != nil {
		t.Fatal(e)
	}
	if ok, e := st.transitionRevision(ctx, o, "queued", "not_dispatched", ""); e != nil || ok {
		t.Fatal("stale scheduler clobbered new payload", e)
	}
	current, e := s.OperationDetail(ctx, c, o.ID)
	if e != nil || current.Message.Text != "new" || current.State != "scheduled" {
		t.Fatal(current, e)
	}
}
