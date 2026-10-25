package core

import (
	"os"
	"path/filepath"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
)

func rootScopeFixture(t *testing.T) (*Hub, *Client, string, string) {
	t.Helper()
	h, _ := newTestHub(t)
	parent := t.TempDir()
	root := filepath.Join(parent, "Esteban")
	outside := filepath.Join(parent, "Other")
	for _, dir := range []string{root, outside} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "notes.md"), []byte("private notes"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	h.cfg.RootDir = root
	return h, newTestClient(h), realpath(root), realpath(outside)
}

func TestRestrictedBridgeBrowseStaysInsideRoot(t *testing.T) {
	h, c, root, outside := rootScopeFixture(t)
	for _, path := range []string{"", "~", root, filepath.Dir(root), outside, filepath.Join(root, "escape")} {
		h.sendDirListing(c, clientproto.Command{Path: path})
		for stage := 0; stage < 2; stage++ {
			event := waitForType(t, c, "dir_listing")
			if event["path"] != root {
				t.Fatalf("browse %q escaped root: %v", path, event)
			}
			entries := event["entries"].([]any)
			if len(entries) != 1 || entries[0].(map[string]any)["name"] != "notes.md" {
				t.Fatalf("browse exposed symlink outside root: %v", entries)
			}
		}
	}
}

func TestRestrictedBridgeRejectsOutsideFiles(t *testing.T) {
	h, c, root, outside := rootScopeFixture(t)
	for _, path := range []string{filepath.Join(outside, "notes.md"), filepath.Join(root, "escape", "notes.md")} {
		h.sendFileOpened(c, clientproto.Command{Path: path})
		event := waitForType(t, c, "file_opened")
		if event["error"] != "path is outside bridge root" || (event["content"] != nil && event["content"] != "") {
			t.Fatalf("outside file was exposed: %v", event)
		}
		h.saveFile(c, clientproto.Command{Path: path, Content: "changed"})
		event = waitForType(t, c, "file_saved")
		if event["error"] != "path is outside bridge root" {
			t.Fatalf("outside file save allowed: %v", event)
		}
	}
	data, err := os.ReadFile(filepath.Join(outside, "notes.md"))
	if err != nil || string(data) != "private notes" {
		t.Fatalf("outside file changed: %q, %v", data, err)
	}
	h.sendFileOpened(c, clientproto.Command{Path: "notes.md"})
	if event := waitForType(t, c, "file_opened"); event["content"] != "private notes" {
		t.Fatalf("inside relative file unavailable: %v", event)
	}
}

func TestRestrictedBridgeMarkdownScanAndSessionsStayInsideRoot(t *testing.T) {
	h, c, root, outside := rootScopeFixture(t)
	h.sendMarkdownFilesListing(c, clientproto.Command{Paths: []string{root, outside}})
	event := waitForType(t, c, "markdown_files_listing")
	files := event["files"].([]any)
	if len(files) != 1 || files[0].(map[string]any)["path"] != filepath.Join(root, "notes.md") {
		t.Fatalf("markdown scan escaped root: %v", event)
	}
	h.registry.Create("inside", "inside", root, "claude", "", "", "")
	h.registry.Create("outside", "outside", outside, "claude", "", "", "outside-resume")
	if summaries := h.sessionSummaries(); len(summaries) != 1 || summaries[0].ID != "inside" {
		t.Fatalf("outside session exposed: %v", summaries)
	}
	route(h, c, `{"type":"request_history","session_id":"outside"}`)
	if event := waitForType(t, c, "error"); event["message"] != "session is outside bridge root" {
		t.Fatalf("outside history request allowed: %v", event)
	}
	route(h, c, `{"type":"new_session","session_id":"resume-outside","resume_claude_id":"outside-resume","backend":"claude"}`)
	if event := waitForType(t, c, "error"); event["message"] != "session is outside bridge root" {
		t.Fatalf("outside existing session resumed: %v", event)
	}
	route(h, c, `{"type":"new_session","session_id":"default","backend":"claude"}`)
	created := waitForType(t, c, "session_created")
	if created["cwd"] != root {
		t.Fatalf("default session escaped root: %v", created)
	}
}

func TestUnrestrictedBridgePathsKeepExistingBehavior(t *testing.T) {
	h, _ := newTestHub(t)
	path := t.TempDir()
	got, err := h.scopedPath(path)
	if err != nil || got != path {
		t.Fatalf("unrestricted path changed: %q, %v", got, err)
	}
}

type scopedHistoryProvider struct {
	countingProvider
	entries []history.ResumableSession
}

func (p *scopedHistoryProvider) ResumableSessions(int) ([]history.ResumableSession, error) {
	return p.entries, nil
}

type scopedHistoryExecutor struct {
	histExec
	provider backend.HistoryProvider
}

func (e *scopedHistoryExecutor) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.provider}
}

func TestRestrictedBridgeResumableSessionsStayInsideRoot(t *testing.T) {
	h, _, root, outside := rootScopeFixture(t)
	p := &scopedHistoryProvider{entries: []history.ResumableSession{
		{ID: "inside", Cwd: root},
		{ID: "outside", Cwd: outside},
		{ID: "symlink", Cwd: filepath.Join(root, "escape")},
		{ID: "unknown"},
	}}
	h.SetExecutor(&scopedHistoryExecutor{provider: p})
	for attempt := 0; attempt < 2; attempt++ {
		entries := h.coalescedResumable(100)
		if len(entries) != 1 || entries[0].ID != "inside" {
			t.Fatalf("resumable sessions escaped root: %v", entries)
		}
	}
	p.entries = []history.ResumableSession{{ID: "outside", Cwd: outside}}
	if entries := h.coalescedResumable(99); entries == nil || len(entries) != 0 {
		t.Fatalf("empty scoped list must remain an array: %v", entries)
	}
}
