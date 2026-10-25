package search

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDeletedTranscriptPrunesDerivedRowsAndFTSOnly(t *testing.T) {
	idx := newTestIndex(t)
	root := t.TempDir()
	idx.sources = []source{claudeSource{root: root}}
	path := filepath.Join(root, "12345678-1234-1234-1234-123456789abc.jsonl")
	if err := os.WriteFile(path, []byte("original evidence\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := idx.db.Exec(`INSERT INTO sessions(session_id,source,source_path,project_dir,msg_count) VALUES('claude:deleted','claude',?,'/',1)`, path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = idx.db.Exec(`INSERT INTO messages(session_id,msg_uuid,role,ts,content) VALUES('claude:deleted','m','user','','unique-deleted-evidence')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = idx.db.Exec(`INSERT INTO ingest_state(source_path,file_size,last_mtime,last_offset,last_ingest_at) VALUES(?,1,1,1,1)`, path)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := idx.pruneDeletedTranscripts(); err != nil || n != 0 {
		t.Fatalf("live removed: %d %v", n, err)
	}
	// Root offline must retain derived history.
	moved := root + "-offline"
	if err = os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	if n, err := idx.pruneDeletedTranscripts(); err != nil || n != 0 {
		t.Fatalf("offline removed: %d %v", n, err)
	}
	if err = os.Rename(moved, root); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if n, err := idx.pruneDeletedTranscripts(); err != nil || n != 1 {
		t.Fatalf("deleted remains: %d %v", n, err)
	}
	for _, table := range []string{"sessions", "messages", "ingest_state"} {
		var n int
		if err = idx.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s retained %d %v", table, n, err)
		}
	}
	var hits int
	if err = idx.db.QueryRow(`SELECT count(*) FROM messages_fts WHERE messages_fts MATCH '"unique"'`).Scan(&hits); err != nil || hits != 0 {
		t.Fatalf("FTS retained %d %v", hits, err)
	}
	if n, err := idx.pruneDeletedTranscripts(); err != nil || n != 0 {
		t.Fatalf("not idempotent: %d %v", n, err)
	}
}
