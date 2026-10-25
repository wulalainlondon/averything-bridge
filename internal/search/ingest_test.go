package search

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type countingSource struct {
	path      string
	metaReads int
	include   bool
}

func (s *countingSource) name() string  { return "codex" }
func (s *countingSource) enabled() bool { return true }
func (s *countingSource) owns(path string) bool {
	want, _ := filepath.EvalSymlinks(s.path)
	return path == want
}
func (s *countingSource) discover() []string { return []string{s.path} }
func (s *countingSource) iterMessages(path string, _ int64) ([]searchableMessage, int64) {
	info, _ := os.Stat(path)
	return nil, info.Size()
}

func TestIngestMetricsDistinguishChangedAndIdlePasses(t *testing.T) {
	idx := newTestIndex(t)
	path := filepath.Join(t.TempDir(), "rollout-metrics.jsonl")
	contents := []byte("{}\n{}\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	idx.sources = []source{&countingSource{path: path, include: true}}

	changed := idx.ingestAllMetrics()
	if changed.FilesSeen != 1 || changed.FilesChanged != 1 || changed.FilesQueued != 1 || changed.BytesRead != int64(len(contents)) {
		t.Fatalf("changed metrics=%+v", changed)
	}
	idle := idx.ingestAllMetrics()
	if idle.FilesSeen != 1 || idle.FilesChanged != 0 || idle.FilesQueued != 0 || idle.BytesRead != 0 {
		t.Fatalf("idle metrics=%+v", idle)
	}
}

func TestExplicitPathsOnlyTouchDirtyTranscript(t *testing.T) {
	idx := newTestIndex(t)
	dir := t.TempDir()
	dirty := filepath.Join(dir, "rollout-dirty.jsonl")
	other := filepath.Join(dir, "rollout-other.jsonl")
	for _, path := range []string{dirty, other} {
		if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	src := &countingSource{path: dirty, include: true}
	idx.sources = []source{src}

	metrics := idx.ingestPathsMetrics([]string{dirty, dirty, other, filepath.Join(dir, "missing.jsonl")})
	if metrics.Mode != "incremental" || metrics.FilesSeen != 1 || metrics.FilesChanged != 1 || metrics.FilesQueued != 1 {
		t.Fatalf("explicit metrics=%+v", metrics)
	}
	if src.metaReads != 1 {
		t.Fatalf("dirty metadata reads=%d want 1", src.metaReads)
	}

	idle := idx.ingestPathsMetrics([]string{dirty})
	if idle.FilesSeen != 1 || idle.FilesChanged != 0 || idle.FilesQueued != 0 || src.metaReads != 1 {
		t.Fatalf("idle explicit metrics=%+v reads=%d", idle, src.metaReads)
	}
}

func TestSourceOwnershipRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.jsonl")
	if err := os.WriteFile(outside, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "linked.jsonl")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	src := claudeSource{root: root}
	if src.owns(link) {
		t.Fatal("source accepted symlink escaping its root")
	}
}

func TestIdleRunDoesNotRepeatGlobalMaintenanceWrites(t *testing.T) {
	idx := newTestIndex(t)
	path := filepath.Join(t.TempDir(), "rollout-maintenance.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	idx.sources = []source{&countingSource{path: path, include: true}}
	idx.RunOnceMetrics()

	var before int64
	if err := idx.db.QueryRow("SELECT total_changes()").Scan(&before); err != nil {
		t.Fatal(err)
	}
	idle := idx.RunOnceMetrics()
	var after int64
	if err := idx.db.QueryRow("SELECT total_changes()").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("idle maintenance wrote rows: before=%d after=%d metrics=%+v", before, after, idle)
	}
	if idle.MaintenanceRows != 0 || idle.FilesChanged != 0 {
		t.Fatalf("idle metrics=%+v", idle)
	}
}

func TestMetadataOnlyTouchRefreshesCursorOnce(t *testing.T) {
	idx := newTestIndex(t)
	path := filepath.Join(t.TempDir(), "rollout-touch.jsonl")
	contents := []byte("{}\n")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	idx.sources = []source{&countingSource{path: path, include: true}}
	idx.ingestAllMetrics()

	touched := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(path, touched, touched); err != nil {
		t.Fatal(err)
	}
	metadataOnly := idx.ingestAllMetrics()
	if metadataOnly.FilesChanged != 1 || metadataOnly.FilesQueued != 1 || metadataOnly.BytesRead != 0 {
		t.Fatalf("metadata-only metrics=%+v", metadataOnly)
	}
	idle := idx.ingestAllMetrics()
	if idle.FilesChanged != 0 || idle.FilesQueued != 0 || idle.BytesRead != 0 {
		t.Fatalf("metadata-only touch repeated: %+v", idle)
	}
}
func (s *countingSource) headSignature(string) string { return "head" }
func (s *countingSource) sessionIDFor(string) string  { return "codex:test" }
func (s *countingSource) sessionMeta(string) sessionMeta {
	s.metaReads++
	return sessionMeta{Cwd: "/tmp", DisplayName: "test", FirstTS: "2026-01-01"}
}
func (s *countingSource) includeMeta(sessionMeta) bool { return s.include }

func TestDiscoverJobsSkipsMetadataForUnchangedFiles(t *testing.T) {
	idx := newTestIndex(t)
	path := filepath.Join(t.TempDir(), "rollout-test.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	mtime := float64(info.ModTime().UnixNano()) / 1e9
	if _, err := idx.db.Exec(
		`INSERT INTO ingest_state(source_path,file_size,last_mtime,last_offset,head_sha256,last_ingest_at,msg_extracted,errors)
		 VALUES(?,?,?,?,?,?,?,?)`, path, info.Size(), mtime, info.Size(), "head", mtime, 0, 0,
	); err != nil {
		t.Fatal(err)
	}
	src := &countingSource{path: path, include: true}
	idx.sources = []source{src}
	if jobs := idx.discoverJobs(); len(jobs) != 0 {
		t.Fatalf("unchanged file produced jobs: %+v", jobs)
	}
	if src.metaReads != 0 {
		t.Fatalf("unchanged file parsed metadata %d time(s)", src.metaReads)
	}

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.WriteString("{}\n")
	_ = f.Close()
	if jobs := idx.discoverJobs(); len(jobs) != 1 {
		t.Fatalf("changed file jobs=%d, want 1", len(jobs))
	}
	if src.metaReads != 1 {
		t.Fatalf("changed file metadata reads=%d, want 1", src.metaReads)
	}
}

func TestDiscoverJobsPersistsSkippedPolicyCursor(t *testing.T) {
	idx := newTestIndex(t)
	path := filepath.Join(t.TempDir(), "ignored.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	src := &countingSource{path: path, include: false}
	idx.sources = []source{src}
	if jobs := idx.discoverJobs(); len(jobs) != 0 || src.metaReads != 1 {
		t.Fatalf("first ignored discovery jobs=%d reads=%d", len(jobs), src.metaReads)
	}
	if jobs := idx.discoverJobs(); len(jobs) != 0 || src.metaReads != 1 {
		t.Fatalf("unchanged ignored file was reparsed jobs=%d reads=%d", len(jobs), src.metaReads)
	}
}

func TestLineReaderUntilStopsPhysicalRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "many.jsonl")
	if err := os.WriteFile(path, []byte("one\ntwo\nthree\nfour\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	offset := lineReaderUntil(path, 0, func(_ []byte, _ int, _ int64) bool {
		calls++
		return calls == 2
	})
	if calls != 2 || offset != int64(len("one\ntwo\n")) {
		t.Fatalf("early stop calls=%d offset=%d", calls, offset)
	}
}

func TestCodexIncrementalMessagesUseAbsoluteOffsetIdentity(t *testing.T) {
	idx := newTestIndex(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "rollout-2026-09-02T00-00-00-00000000-0000-0000-0000-000000000001.jsonl")
	first := `{"timestamp":"2026-09-02T00:00:01Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"first answer"}]}}` + "\n"
	second := `{"timestamp":"2026-09-02T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"second answer"}]}}` + "\n"
	// Keep the head-signature window stable when the second record is appended.
	if err := os.WriteFile(path, []byte(strings.Repeat(" ", 5000)+"\n"+first), 0o600); err != nil {
		t.Fatal(err)
	}
	src := codexSource{root: dir}
	job := ingestJob{src: src, path: path, meta: sessionMeta{FirstTS: "2026-09-02T00:00:01Z"}}
	if extracted, _, _, err := idx.ingestFile(job); err != nil || extracted != 1 {
		t.Fatalf("first ingest extracted=%d err=%v", extracted, err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(second); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if extracted, _, _, err := idx.ingestFile(job); err != nil || extracted != 1 {
		t.Fatalf("incremental ingest extracted=%d err=%v", extracted, err)
	}

	var count int
	var latest string
	if err := idx.db.QueryRow(`SELECT (SELECT COUNT(*) FROM messages WHERE session_id=?), content
		FROM messages WHERE session_id=? ORDER BY ts DESC LIMIT 1`, src.sessionIDFor(path), src.sessionIDFor(path)).Scan(&count, &latest); err != nil {
		t.Fatal(err)
	}
	if count != 2 || latest != "second answer" {
		t.Fatalf("indexed count=%d latest=%q, want 2 and second answer", count, latest)
	}
}

func TestIngestCountsIdentityConflictsSeparately(t *testing.T) {
	idx := newTestIndex(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "rollout-2026-09-02T00-00-00-00000000-0000-0000-0000-000000000002.jsonl")
	line := `{"timestamp":"2026-09-02T00:00:01Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"same"}]}}` + "\n"
	if err := os.WriteFile(file, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	job := ingestJob{src: codexSource{root: dir}, path: file, meta: sessionMeta{FirstTS: "2026-09-02T00:00:01Z"}}
	if inserted, conflicts, _, err := idx.ingestFile(job); err != nil || inserted != 1 || conflicts != 0 {
		t.Fatalf("first=(%d,%d,%v)", inserted, conflicts, err)
	}
	if _, err := idx.db.Exec(`UPDATE ingest_state SET last_offset=0 WHERE source_path=?`, file); err != nil {
		t.Fatal(err)
	}
	if inserted, conflicts, _, err := idx.ingestFile(job); err != nil || inserted != 0 || conflicts != 1 {
		t.Fatalf("second=(%d,%d,%v)", inserted, conflicts, err)
	}
}

func TestCodexIncrementalAcrossThreeIndexerProcessesAndPartialLine(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "search.db")
	sessionRoot := filepath.Join(dir, "sessions")
	sessionDir := filepath.Join(sessionRoot, "2026", "09", "02")
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	rollout := filepath.Join(sessionDir, "rollout-2026-09-02T00-00-00-00000000-0000-0000-0000-000000000003.jsonl")
	line := func(second int, text string, newline bool) string {
		value := fmt.Sprintf(`{"timestamp":"2026-09-02T00:00:%02dZ","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":%q}]}}`, second, text)
		if newline {
			value += "\n"
		}
		return value
	}
	if err := os.WriteFile(rollout, []byte(strings.Repeat(" ", 5000)+"\n"+line(1, "one", true)), 0o600); err != nil {
		t.Fatal(err)
	}
	run := func(wantAdded int) {
		idx, err := New(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		idx.sources = []source{codexSource{root: sessionRoot}}
		metrics := idx.RunOnceMetrics()
		if err := idx.Close(); err != nil {
			t.Fatal(err)
		}
		if metrics.MessagesAdded != wantAdded {
			t.Fatalf("added=%d want=%d conflicts=%d", metrics.MessagesAdded, wantAdded, metrics.MessageConflicts)
		}
	}
	run(1)
	f, err := os.OpenFile(rollout, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(line(2, "two", true) + line(3, "three", false)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	run(1) // incomplete third line is deliberately retained at the cursor
	f, err = os.OpenFile(rollout, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("\n"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	run(1)
	idx, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	var count int
	if err := idx.db.QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestCodexMessageIDMigrationRebuildsStaleRowsOnce(t *testing.T) {
	idx := newTestIndex(t)
	if _, err := idx.db.Exec(`INSERT INTO sessions(session_id,source,source_path,project_dir,cwd,display_name,first_ts,last_ts,msg_count,backend)
		VALUES('codex:old','codex','/tmp/old.jsonl','','','','2026-01-01','2026-01-01',1,'codex')`); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.db.Exec(`INSERT INTO messages(session_id,msg_uuid,role,ts,is_subagent,content)
		VALUES('codex:old','old:line:1','assistant','2026-01-01',0,'stale')`); err != nil {
		t.Fatal(err)
	}
	changed, err := idx.refreshCodexMessageIDVersion()
	if err != nil || changed != 2 {
		t.Fatalf("migration changed=%d err=%v", changed, err)
	}
	var count int
	if err := idx.db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE source='codex'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("Codex sessions after migration=%d err=%v", count, err)
	}
	changed, err = idx.refreshCodexMessageIDVersion()
	if err != nil || changed != 0 {
		t.Fatalf("second migration changed=%d err=%v", changed, err)
	}
}
