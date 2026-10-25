package nativewatch

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCatalogRequiresCompleteReadableOriginalSource(t *testing.T) {
	root := t.TempDir()
	id := "12345678-1234-1234-1234-123456789abc"
	path := filepath.Join(root, id+".jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	scan := func() TranscriptCatalog { return ScanTranscriptCatalog(map[string]string{BackendClaude: root}) }
	c := scan()
	if c.MissingSession(BackendClaude, id) || c.MissingPath(BackendClaude, path) {
		t.Fatal("live transcript removed")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c = scan()
	if !c.MissingSession(BackendClaude, id) || !c.MissingPath(BackendClaude, path) {
		t.Fatal("deletion not confirmed")
	}
	if c.MissingPath(BackendClaude, filepath.Join(t.TempDir(), "foreign.jsonl")) {
		t.Fatal("foreign path removed")
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "unknown-link")); err != nil {
		t.Fatal(err)
	}
	c = scan()
	if c.MissingSession(BackendClaude, id) || c.MissingPath(BackendClaude, path) {
		t.Fatal("incomplete tree treated as deleted")
	}
	missing := ScanTranscriptCatalog(map[string]string{BackendClaude: filepath.Join(root, "offline")})
	if missing.MissingSession(BackendClaude, id) {
		t.Fatal("offline source treated as deletion")
	}
}

func TestCatalogRetainsZstdArchivedTranscript(t *testing.T) {
	root := t.TempDir()
	id := "12345678-1234-1234-1234-123456789abc"
	path := filepath.Join(root, "rollout-2026-10-01T00-00-00-"+id+".jsonl")
	if err := os.WriteFile(path+".zst", []byte("compressed fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	c := ScanTranscriptCatalog(map[string]string{BackendCodex: root})
	if c.MissingSession(BackendCodex, id) || c.MissingPath(BackendCodex, path) {
		t.Fatal("compressed original treated as deleted")
	}
}
