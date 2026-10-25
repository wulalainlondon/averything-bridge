package search

import (
	"fmt"
	"testing"
	"time"
)

func TestPrivateNativeSessionsExcludedFromGlobalReads(t *testing.T) {
	idx := newTestIndex(t)
	ts := time.Now().UTC().Format(time.RFC3339)
	for _, sid := range []string{"claude:public", "claude:room-thread", "codex:rollout-initial"} {
		seed(t, idx, sid, "privateword 連線", ts)
	}
	if _, err := idx.db.Exec("UPDATE sessions SET cwd=? WHERE session_id=?", "/tmp/collaboration-workspace-v1/workdirs/room/session", "codex:rollout-initial"); err != nil {
		t.Fatal(err)
	}
	idx.ProtectNativeSession("room-thread")
	for i := 0; i < 1200; i++ {
		idx.ProtectNativeSession(fmt.Sprintf("unrelated-private-native-%04d", i))
	}
	for _, q := range []string{"privateword", "連線"} {
		got := idx.Search(q, Filters{}, 100, 0)
		if len(got.Hits) != 1 || got.Hits[0].SessionID != "claude:public" || got.Total != 1 {
			t.Fatalf("private search result %s: %+v", q, got)
		}
	}
	listed := idx.ListSessions("", 100, "", true, true)
	if len(listed.Items) != 1 || listed.Items[0].SessionID != "claude:public" {
		t.Fatalf("private listing: %+v", listed)
	}
	for _, sid := range []string{"claude:room-thread", "codex:rollout-initial"} {
		ctx := idx.GetContext(sid, sid+":"+ts, 2)
		if len(ctx.Messages) != 0 || ctx.SessionDisplayName != nil || ctx.Cwd != nil {
			t.Fatalf("private context: %+v", ctx)
		}
	}
	if len(idx.GetContext("claude:public", "claude:public:"+ts, 2).Messages) != 1 {
		t.Fatal("public context lost")
	}
}
