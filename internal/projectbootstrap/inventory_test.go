package projectbootstrap

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestBuildProducesBoundedEvidenceAndSuggestions(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "README.md"), "# Bridge\n\nManage local AI work from mobile.\n")
	writeTestFile(t, filepath.Join(root, "AGENTS.md"), "Never expose token=secret-value\nRun tests.\n")
	writeTestFile(t, filepath.Join(root, "go.mod"), "module example.com/bridge\n")
	writeTestFile(t, filepath.Join(root, ".env"), "TOKEN=must-not-appear\n")
	writeTestFile(t, filepath.Join(root, "private.md"), "must-not-be-read\n")

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout())
	defer cancel()
	result, err := Build(ctx, root, "Bridge", []SessionEvidence{{
		ID: "s1", Name: "Offline replay", Backend: "codex", LastActivity: time.Now().UnixMilli(),
		Messages: []SessionMessage{{Role: "assistant", Text: "Media replay is ready for review."}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result.Objective, "Manage local AI work") {
		t.Fatalf("objective=%q", result.Objective)
	}
	if len(result.Suggestions) != 1 || result.Suggestions[0].SessionID != "s1" {
		t.Fatalf("suggestions=%+v", result.Suggestions)
	}
	body := ""
	for _, source := range result.Sources {
		body += source.Excerpt
		if source.Path == ".env" || source.Path == "private.md" {
			t.Fatalf("unsafe source included: %+v", source)
		}
	}
	if strings.Contains(body, "secret-value") || strings.Contains(body, "must-not-appear") || strings.Contains(body, "must-not-be-read") {
		t.Fatalf("secret/unlisted content leaked: %q", body)
	}
	if !strings.Contains(body, "[redacted potentially sensitive line]") {
		t.Fatalf("expected redaction: %q", body)
	}
	if len(result.Fingerprint) != 64 {
		t.Fatalf("fingerprint=%q", result.Fingerprint)
	}
}

func TestBuildReadsGitWithoutIncludingUntrackedSecrets(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "README.md"), "# Test\n\nA test repository.\n")
	run := func(args ...string) {
		cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.com", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.com")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	run("init")
	run("add", "README.md")
	run("commit", "-m", "initial")
	writeTestFile(t, filepath.Join(root, "private-token.txt"), "untracked secret")

	result, err := Build(context.Background(), root, "Test", nil)
	if err != nil {
		t.Fatal(err)
	}
	var gitSource *Source
	for i := range result.Sources {
		if result.Sources[i].ID == "git:state" {
			gitSource = &result.Sources[i]
		}
	}
	if gitSource == nil {
		t.Fatal("git source missing")
	}
	if strings.Contains(gitSource.Excerpt, "private-token") {
		t.Fatalf("untracked filename leaked: %q", gitSource.Excerpt)
	}
}

func TestBuildRejectsRootAndDoesNotFollowDocumentSymlink(t *testing.T) {
	if _, err := Build(context.Background(), string(filepath.Separator), "root", nil); err == nil {
		t.Fatal("root workspace must be rejected")
	}
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "README.md")
	writeTestFile(t, outside, "outside secret")
	if err := os.Symlink(outside, filepath.Join(root, "README.md")); err != nil {
		t.Fatal(err)
	}
	result, err := Build(context.Background(), root, "Safe", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range result.Sources {
		if strings.Contains(source.Excerpt, "outside secret") {
			t.Fatal("symlink was followed")
		}
	}
}

func TestBuildCapsDocumentsSessionsAndEvidenceSize(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "README.md"), "# Large\n\n"+strings.Repeat("project context ", 10_000))
	for _, name := range []string{"AGENTS.md", "RTK.md", "ROADMAP.md", "TODO.md", "CHANGELOG.md", "CONTRIBUTING.md"} {
		writeTestFile(t, filepath.Join(root, name), strings.Repeat(name+" ", 10_000))
	}
	writeTestFile(t, filepath.Join(root, "docs", "architecture.md"), strings.Repeat("architecture ", 10_000))
	writeTestFile(t, filepath.Join(root, "docs", "roadmap-extra.md"), strings.Repeat("roadmap ", 10_000))
	var sessions []SessionEvidence
	for i := 0; i < 7; i++ {
		sessions = append(sessions, SessionEvidence{ID: string(rune('a' + i)), Name: "Session", LastActivity: int64(i), Messages: []SessionMessage{{Role: "assistant", Text: strings.Repeat("result ", 1000)}}})
	}
	result, err := Build(context.Background(), root, "Large", sessions)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Suggestions) != 5 {
		t.Fatalf("suggestions=%d", len(result.Suggestions))
	}
	if len(result.Sources) > maxDocuments+5 {
		t.Fatalf("sources exceeded cap: %d", len(result.Sources))
	}
	for _, source := range result.Sources {
		if len([]rune(source.Excerpt)) > maxEvidenceExcerpt+1 {
			t.Fatalf("source excerpt exceeded cap: %s=%d", source.ID, len([]rune(source.Excerpt)))
		}
	}
}
