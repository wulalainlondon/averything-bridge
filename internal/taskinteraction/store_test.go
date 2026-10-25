package taskinteraction

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func fixtureRecord() Record {
	return Record{Binding: Binding{Authority: "a", Child: "child", ChildRequest: "request", Parent: "parent"}, OriginKey: "origin", IntentHash: "same", Question: "Which fixture?", Kind: "task_clarification"}
}
func TestDurableAnswerRaceAndOriginalReceipt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	r, fresh, e := s.Create(ctx, fixtureRecord())
	if e != nil || !fresh {
		t.Fatal(e)
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for _, actor := range []string{"parent_agent", "human"} {
		wg.Add(1)
		go func(actor string) {
			defer wg.Done()
			_, e := s.Answer(ctx, r.ID, actor, "once", "hash", 1, actor, actor, "fixture")
			if e == nil {
				winners.Add(1)
			}
		}(actor)
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatal("multiple answers", winners.Load())
	}
	answered, e := s.Get(ctx, r.ID)
	if e != nil || answered.Answer != "fixture" || answered.AckToken == "" {
		t.Fatal(answered, e)
	}
	raw, _ := json.Marshal(answered)
	if strings.Contains(string(raw), answered.AckToken) {
		t.Fatal("private proof leaked")
	}
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	restored, e := s.Get(ctx, r.ID)
	if e != nil || restored.AckToken != answered.AckToken || restored.State != "answer_reserved" {
		t.Fatal(restored, e)
	}
	if _, e = s.Acknowledge(ctx, r.ID, "wrong", "child"); e == nil {
		t.Fatal("foreign proof accepted")
	}
	acked, e := s.Acknowledge(ctx, r.ID, answered.AckToken, "original-child-call")
	if e != nil || acked.State != "resolved" {
		t.Fatal(acked, e)
	}
	again, e := s.Answer(ctx, r.ID, answered.Actor, "once", "hash", 1, answered.Actor, answered.Actor, "fixture")
	if e != nil || again.State != "resolved" {
		t.Fatal("original command not retained", again, e)
	}
	_ = filepath.Join(dir, "task_interactions.sqlite")
}
func TestQuestionIntentAndHumanOnly(t *testing.T) {
	ctx := context.Background()
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	proposal := fixtureRecord()
	proposal.HumanOnly = true
	r, _, e := s.Create(ctx, proposal)
	if e != nil {
		t.Fatal(e)
	}
	copy, fresh, e := s.Create(ctx, proposal)
	if e != nil || fresh || copy.ID != r.ID {
		t.Fatal("question duplicated")
	}
	proposal.IntentHash = "different"
	if _, _, e = s.Create(ctx, proposal); e == nil {
		t.Fatal("intent replaced")
	}
	if _, e = s.Answer(ctx, r.ID, "parent", "answer", "hash", 1, "parent_agent", "parent", "yes"); e == nil {
		t.Fatal("parent approved human decision")
	}
	if _, e = s.Answer(ctx, r.ID, "human", "answer", "hash", 2, "human", "human", "yes"); e == nil {
		t.Fatal("stale question answered")
	}
	if _, e = s.Answer(ctx, r.ID, "human", "answer", "hash", 1, "human", "human", "yes"); e != nil {
		t.Fatal(e)
	}
}

func TestPendingPagesDoNotStarveAndRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 300; i++ {
		r := fixtureRecord()
		r.OriginKey = fmt.Sprintf("origin-%d", i)
		r.Binding.ChildRequest = r.OriginKey
		r.Binding.ParentAuthority = r.Binding.Authority
		created, _, err := s.Create(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, created.ID)
	}
	first, err := s.Pending(ctx)
	if err != nil || len(first) != 256 || first[0].ID != ids[0] {
		t.Fatalf("first page %d %v", len(first), err)
	}
	second, err := s.Pending(ctx)
	if err != nil || len(second) != 44 || second[0].ID != ids[256] {
		t.Fatalf("starved second page %d %v", len(second), err)
	}
	next, err := s.Pending(ctx)
	if err != nil || len(next) != 256 || next[0].ID != ids[0] {
		t.Fatal("rotation failed", err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	restored, err := s.Pending(ctx)
	if err != nil || len(restored) != 256 || restored[0].ID != ids[0] {
		t.Fatal("restart lost durable pending", err)
	}
	// A cursor is an immutable record, not a random-ID sort or offset.
	page, cursor, err := s.ListPage(ctx, "parent", "", 100)
	if err != nil || len(page) != 100 || page[0].ID != ids[299] || cursor != ids[200] {
		t.Fatal("newest page order", err)
	}
	r := fixtureRecord()
	r.OriginKey = "arrived-between-pages"
	r.Binding.ChildRequest = r.OriginKey
	if _, _, err = s.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	page, cursor, err = s.ListPage(ctx, "parent", cursor, 100)
	if err != nil || len(page) != 100 || page[0].ID != ids[199] || cursor != ids[100] {
		t.Fatal("pagination moved after insert", err)
	}
}
func TestRemoteCommandReservationHasOneSenderAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fresh, e := s.ReserveRemoteOnce(ctx, "original-parent", "answer-once", "hash", "question")
			if e != nil {
				t.Error(e)
			}
			if fresh {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("sender count %d", winners.Load())
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if fresh, e := s.ReserveRemoteOnce(ctx, "original-parent", "answer-once", "hash", "question"); e != nil || fresh {
		t.Fatal("unknown replay after restart", e)
	}
	if _, e := s.ReserveRemoteOnce(ctx, "original-parent", "answer-once", "changed", "question"); e == nil {
		t.Fatal("changed intent accepted")
	}
	state, found, e := s.RemoteCommand(ctx, "original-parent", "answer-once")
	if e != nil || !found || state != "unknown" {
		t.Fatal("uncertain receipt lost", state, e)
	}
}
