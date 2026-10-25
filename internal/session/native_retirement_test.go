package session

import "testing"

func TestRetireBoundBridgeCreatedNativeConversation(t *testing.T) {
	r := NewRegistry()
	id := "01a11651-7bbb-7e51-922f-e522a9f6b3ae"
	s := r.Create("s_native_deleted", "old", t.TempDir(), "codex", "", "", id)
	r.Create("s_unbound", "new", t.TempDir(), "codex", "", "", "")
	r.Create("foreign-id", "unowned", t.TempDir(), "codex", "", "", id)
	if n := r.PruneMissingNativeSessions(func(Snapshot) bool { return true }); n != 1 {
		t.Fatalf("removed=%d", n)
	}
	if _, ok := r.Get(s.ID); ok {
		t.Fatal("deleted bound original is still indexed")
	}
	for _, id := range []string{"s_unbound", "foreign-id"} {
		if _, ok := r.Get(id); !ok {
			t.Fatalf("unsafe retirement %s", id)
		}
	}
}

func TestRetirementKeepsBusyAndUnconfirmedNativeConversations(t *testing.T) {
	r := NewRegistry()
	id := "01a11651-7bbb-7e51-922f-e522a9f6b3ae"
	s := r.Create("s_busy", "busy", t.TempDir(), "codex", "", "", id)
	s.mu.Lock()
	s.state = Streaming
	s.mu.Unlock()
	r.Create("s_unconfirmed", "unconfirmed", t.TempDir(), "codex", "", "", id)
	if n := r.PruneMissingNativeSessions(func(s Snapshot) bool { return s.ID != "s_unconfirmed" }); n != 0 {
		t.Fatalf("unsafe removals=%d", n)
	}
}
