package search

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"everything-go/internal/backend"
)

func TestPresentationProjectionRepairPreservesNativeIdentityAndOriginalFile(t *testing.T) {
	idx := newTestIndex(t)
	original := "資料正確 e\u0301😀\n保留原始值"
	effective := backend.PresentationContent(backend.WithVisualPresentation(context.Background(), "auto-v1"), original)
	path := filepath.Join(t.TempDir(), "original.jsonl")
	if err := os.WriteFile(path, []byte(effective), 0400); err != nil {
		t.Fatal(err)
	}
	before := sha256.Sum256([]byte(effective))
	for _, source := range []string{"codex", "claude"} {
		sid := source + ":presentation-test"
		if _, err := idx.db.Exec(`INSERT INTO sessions(session_id,source,source_path,project_dir,display_name,msg_count,backend) VALUES(?,?,?,?,?,2,?)`, sid, source, path, "/test", collapseWS(effective, 80), source); err != nil {
			t.Fatal(err)
		}
		for _, role := range []string{"user", "assistant"} {
			if _, err := idx.db.Exec(`INSERT INTO messages(session_id,msg_uuid,parent_uuid,role,ts,content) VALUES(?,?,?,?,?,?)`, sid, role+"-original", "parent-original", role, "2026-10-14T00:00:00Z", effective); err != nil {
				t.Fatal(err)
			}
		}
	}
	changed, err := idx.refreshPresentationProjection()
	if err != nil || changed != 2 {
		t.Fatalf("changed=%d err=%v", changed, err)
	}
	for _, source := range []string{"codex", "claude"} {
		var name, user, assistant, parent string
		var count int
		sid := source + ":presentation-test"
		idx.db.QueryRow("SELECT display_name,msg_count FROM sessions WHERE session_id=?", sid).Scan(&name, &count)
		idx.db.QueryRow("SELECT content,parent_uuid FROM messages WHERE session_id=? AND msg_uuid='user-original'", sid).Scan(&user, &parent)
		idx.db.QueryRow("SELECT content FROM messages WHERE session_id=? AND msg_uuid='assistant-original'", sid).Scan(&assistant)
		if name != collapseWS(original, 80) || user != original || assistant != effective || count != 2 || parent != "parent-original" {
			t.Fatalf("projection changed identity or original content for %s", source)
		}
	}
	var leaked int
	if err := idx.db.QueryRow(`SELECT COUNT(*) FROM messages_fts JOIN messages m ON m.rowid=messages_fts.rowid WHERE messages_fts MATCH '"Averything"' AND m.role='user'`).Scan(&leaked); err != nil || leaked != 0 {
		t.Fatalf("FTS prefix leak=%d err=%v", leaked, err)
	}
	if n, err := idx.refreshPresentationProjection(); err != nil || n != 0 {
		t.Fatalf("not idempotent: %d %v", n, err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || sha256.Sum256(raw) != before {
		t.Fatal("native transcript mutated")
	}
}

func TestOriginalPresentationContentExactBoundary(t *testing.T) {
	user := "原字 e\u0301😀\n第二行"
	for _, mode := range []string{"auto-v1", "text-v1"} {
		raw := backend.PresentationContent(backend.WithVisualPresentation(context.Background(), mode), user)
		if backend.OriginalPresentationContent(raw) != user {
			t.Fatalf("lost original for %s", mode)
		}
	}
	for _, raw := range []string{user, "[Averything presentation context: unknown]\napp\n[/Averything presentation context]\n\n" + user, "[Averything presentation context: text-v1]\nunclosed"} {
		if backend.OriginalPresentationContent(raw) != raw {
			t.Fatal("stripped unqualified input")
		}
	}
}
