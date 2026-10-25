package nativewatch

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestUsePollingMode(t *testing.T) {
	t.Setenv("EVERYTHING_GO_NATIVEWATCH_MODE", "poll")
	if !usePolling() {
		t.Fatal("mode=poll must force polling")
	}
	t.Setenv("EVERYTHING_GO_NATIVEWATCH_MODE", "fsnotify")
	if usePolling() {
		t.Fatal("mode=fsnotify must force fsnotify")
	}
	// Default (unset) follows the platform: poll on darwin, fsnotify elsewhere.
	t.Setenv("EVERYTHING_GO_NATIVEWATCH_MODE", "")
	if got, want := usePolling(), runtime.GOOS == "darwin"; got != want {
		t.Fatalf("default usePolling=%v, want %v (GOOS=%s)", got, want, runtime.GOOS)
	}
}

func TestDefaultOptionsHonorClaudeProjectsOverride(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("EVERYTHING_GO_CLAUDE_PROJECTS_DIR", dir)
	if got := DefaultOptions().ClaudeProjectsDir; got != dir {
		t.Fatalf("ClaudeProjectsDir=%q want %q", got, dir)
	}
}

func TestWatchChangesDoesNotSignalInitialImport(t *testing.T) {
	t.Setenv("EVERYTHING_GO_NATIVEWATCH_MODE", "poll")
	root := t.TempDir()
	path := filepath.Join(root, "123e4567-e89b-12d3-a456-426614174000.jsonl")
	line := `{"type":"user","cwd":"/tmp/repo","message":{"content":[{"type":"text","text":"Initial"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := Options{ClaudeProjectsDir: root, PollInterval: 20 * time.Millisecond, Debounce: time.Millisecond, InitialLookback: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	imports := make(chan NativeSession, 2)
	changes := make(chan string, 2)
	go WatchChanges(ctx, opts, func(ns NativeSession) { imports <- ns }, func(path string) { changes <- path })
	select {
	case <-imports:
	case <-time.After(time.Second):
		t.Fatal("initial session was not imported")
	}
	select {
	case <-changes:
		t.Fatal("initial import was reported as a transcript change")
	case <-time.After(60 * time.Millisecond):
	}
	if f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0); err != nil {
		t.Fatal(err)
	} else {
		_, _ = f.WriteString(line)
		_ = f.Close()
	}
	select {
	case changedPath := <-changes:
		if changedPath != path {
			t.Fatalf("change path=%q want %q", changedPath, path)
		}
	case <-time.After(time.Second):
		t.Fatal("appended transcript did not signal a change")
	}
}

func TestParseClaudePath(t *testing.T) {
	root := t.TempDir()
	proj := filepath.Join(root, "-tmp-repo")
	if err := os.MkdirAll(proj, 0o755); err != nil {
		t.Fatal(err)
	}
	id := "12345678-1234-4234-9234-123456789abc"
	path := filepath.Join(proj, id+".jsonl")
	body := `{"type":"assistant","cwd":"/tmp/repo","message":{"content":[{"type":"text","text":"hi"}]}}` + "\n" +
		`{"type":"user","cwd":"/tmp/repo","message":{"content":[{"type":"text","text":"Fix the bridge watcher"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	ns, ok := ParsePath(path, Options{ClaudeProjectsDir: root})
	if !ok {
		t.Fatal("ParsePath returned false")
	}
	if ns.ID != "jl_c_12345678-123" || ns.ResumeID != id || ns.Backend != BackendClaude {
		t.Fatalf("bad identity: %+v", ns)
	}
	if ns.Cwd != "/tmp/repo" || ns.Name != "Fix the bridge watcher" || ns.LastUsed == 0 {
		t.Fatalf("bad metadata: %+v", ns)
	}
}

func TestParseCodexPath(t *testing.T) {
	root := t.TempDir()
	day := filepath.Join(root, "2026", "06", "07")
	if err := os.MkdirAll(day, 0o755); err != nil {
		t.Fatal(err)
	}
	id := "019e9ccf-affc-7d71-8370-ec247b2131c7"
	path := filepath.Join(day, "rollout-2026-06-07T10-11-12-"+id+".jsonl")
	body := `{"type":"session_meta","payload":{"cwd":"/repo/codex"}}` + "\n" +
		`{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"text","text":"Continue mobile handoff"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	appendTime := time.Date(2026, 9, 3, 9, 44, 59, 0, time.Local)
	if err := os.Chtimes(path, appendTime, appendTime); err != nil {
		t.Fatal(err)
	}

	ns, ok := ParsePath(path, Options{CodexSessionsDir: root})
	if !ok {
		t.Fatal("ParsePath returned false")
	}
	if ns.ID != "jl_x_019e9ccf-aff" || ns.ResumeID != id || ns.Backend != BackendCodex {
		t.Fatalf("bad identity: %+v", ns)
	}
	wantTS := appendTime.Unix()
	if ns.Cwd != "/repo/codex" || ns.Name != "Continue mobile handoff" || ns.LastUsed != wantTS {
		t.Fatalf("bad metadata: %+v", ns)
	}
}

func TestParseCodexPathAppliesIsolationPolicy(t *testing.T) {
	root := t.TempDir()
	day := filepath.Join(root, "2026", "07", "24")
	if err := os.MkdirAll(day, 0o755); err != nil {
		t.Fatal(err)
	}
	id := "12345678-1234-4234-9234-123456789abc"
	path := filepath.Join(day, "rollout-2026-07-24T01-02-03-"+id+".jsonl")
	body := `{"type":"session_meta","payload":{"cwd":"/Users/test/project"}}` + "\n" +
		`{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<recommended_plugins>\nnoise"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	opts := Options{
		CodexSessionsDir:        root,
		CodexIgnoreNamePrefixes: []string{"<recommended_plugins>"},
	}
	if _, ok := ParsePath(path, opts); ok {
		t.Fatal("expected evaluation session to be excluded")
	}
}

func TestParseCodexPathRejectsSubagent(t *testing.T) {
	root := t.TempDir()
	day := filepath.Join(root, "2026", "07", "20")
	if err := os.MkdirAll(day, 0o755); err != nil {
		t.Fatal(err)
	}
	id := "019f7ddf-17c7-7a53-a5c5-e6e81bd52d7b"
	path := filepath.Join(day, "rollout-2026-07-20T12-53-20-"+id+".jsonl")
	// Older Codex rollouts may identify workers only through source.subagent.
	body := `{"type":"session_meta","payload":{"id":"` + id + `","cwd":"/repo/codex","source":{"subagent":{"depth":1}}}}` + "\n" +
		`{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"text","text":"worker task"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	if ns, ok := ParsePath(path, Options{CodexSessionsDir: root}); ok {
		t.Fatalf("sub-agent rollout must not be imported: %+v", ns)
	}
}

func TestParseCodexPathKeepsUserFork(t *testing.T) {
	root := t.TempDir()
	day := filepath.Join(root, "2026", "07", "20")
	if err := os.MkdirAll(day, 0o755); err != nil {
		t.Fatal(err)
	}
	id := "019f7ddf-318d-7360-ac6b-617bebd1c195"
	path := filepath.Join(day, "rollout-2026-07-20T12-54-00-"+id+".jsonl")
	body := `{"type":"session_meta","payload":{"id":"` + id + `","cwd":"/repo/codex","forked_from_id":"019f541f-63b6-7e53-a4e2-dd36d875a7c2","thread_source":"user","source":"exec"}}` + "\n" +
		`{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"text","text":"user fork"}]}}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	if ns, ok := ParsePath(path, Options{CodexSessionsDir: root}); !ok || ns.Name != "user fork" {
		t.Fatalf("user-created fork must remain resumable: ok=%v session=%+v", ok, ns)
	}
}
