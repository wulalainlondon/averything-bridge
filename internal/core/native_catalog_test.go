package core

import (
	"everything-go/internal/messagequeue"
	"everything-go/internal/nativewatch"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeCatalogDeletionNeedsTwoScansAndProtectsDurableWork(t *testing.T) {
	h, _ := newTestHub(t)
	root := t.TempDir()
	opts := nativewatch.Options{ClaudeProjectsDir: root}
	ids := []string{"12345678-1234-1234-1234-123456789abc", "22345678-1234-1234-1234-123456789abc", "32345678-1234-1234-1234-123456789abc"}
	for _, id := range ids {
		h.registry.Create("jl_c_"+id[:12], "fixture", "/", "claude", "", "", id)
	}
	h.registry.Create("bridge-created", "retained", "/", "claude", "", "", ids[0])
	live := filepath.Join(root, ids[1]+".jsonl")
	if err := os.WriteFile(live, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	queuedID := "jl_c_" + ids[2][:12]
	if _, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: queuedID, RequestID: "r_protected", Content: "keep", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	previous, n := h.reconcileNativeCatalog(opts, nil)
	if n != 0 {
		t.Fatal("first absence removed row")
	}
	_, n = h.reconcileNativeCatalog(opts, previous)
	if n != 1 {
		t.Fatalf("removed %d want 1", n)
	}
	for _, id := range []string{"bridge-created", "jl_c_" + ids[1][:12], queuedID} {
		if _, ok := h.registry.Get(id); !ok {
			t.Fatalf("protected %s removed", id)
		}
	}
	if _, ok := h.registry.Get("jl_c_" + ids[0][:12]); ok {
		t.Fatal("deleted imported row retained")
	}
}

func TestBridgeCreatedNativeDeletionKeepsHistoricalAndQueuedBindings(t *testing.T) {
	h, _ := newTestHub(t)
	root := t.TempDir()
	opts := nativewatch.Options{CodexSessionsDir: root}
	missing := "12345678-1234-1234-1234-123456789abc"
	old := "22345678-1234-1234-1234-123456789abc"
	h.registry.Create("s_deleted", "deleted", "/", "codex", "", "", missing)
	historical := h.registry.Create("s_historical", "history", "/", "codex", "", "", missing)
	historical.AddHistoricalResumeID(old)
	if err := os.WriteFile(filepath.Join(root, "rollout-2026-01-01-"+old+".jsonl.zst"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	h.registry.Create("s_queued", "queued", "/", "codex", "", "", missing)
	if _, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: "s_queued", RequestID: "r_keep", Content: "keep", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	previous, n := h.reconcileNativeCatalog(opts, nil)
	if n != 0 {
		t.Fatal("retired on first scan")
	}
	_, n = h.reconcileNativeCatalog(opts, previous)
	if n != 1 {
		t.Fatalf("retired=%d want1", n)
	}
	for _, id := range []string{"s_historical", "s_queued"} {
		if _, ok := h.registry.Get(id); !ok {
			t.Fatalf("protected row %s lost", id)
		}
	}
}
