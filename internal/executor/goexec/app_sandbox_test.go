package goexec

import (
	"context"
	"errors"
	"everything-go/internal/history"
	"everything-go/internal/session"
	"everything-go/internal/sourcepolicy"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type sandboxTestSink struct{}

func (sandboxTestSink) Emit(any) {}
func TestAppSandboxRequiresSignedHostBeforeCredentialAccess(t *testing.T) {
	called := false
	_, err := newCodexAppSandbox(sandboxTestSink{}, "does-not-exist", AppSandboxOptions{Tokens: func(context.Context) (ExternalChatGPTTokens, error) {
		called = true
		return ExternalChatGPTTokens{}, nil
	}}, func() error { return errors.New("not sandboxed") })
	if err == nil || called {
		t.Fatal("untrusted owner reached credential source")
	}
}
func TestAppSandboxCannotBeSelectedOnOrdinaryBackend(t *testing.T) {
	c := NewCodex(sandboxTestSink{}, "does-not-exist")
	s := session.NewRegistry().Create("test", "test", t.TempDir(), "codex", "", AppSandboxMode, "")
	if err := c.Send(context.Background(), s, "r_test", "test", nil, nil); err == nil || !strings.Contains(err.Error(), "owner_profile_required") {
		t.Fatalf("got %v", err)
	}
}
func TestAppSandboxWorkspaceAndPolicyAreFixed(t *testing.T) {
	workspace := t.TempDir()
	state := t.TempDir()
	c, err := newCodexAppSandbox(sandboxTestSink{}, "does-not-exist", AppSandboxOptions{Workspace: workspace, RuntimeDir: state, Tokens: func(context.Context) (ExternalChatGPTTokens, error) {
		return ExternalChatGPTTokens{}, errors.New("not requested")
	}}, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	s := session.NewRegistry().Create("test", "test", workspace, "codex", "", AppSandboxMode, "")
	if err := c.validateAppSandbox(s, ""); err != nil {
		t.Fatal(err)
	}
	if err := c.validateAppSandbox(s, "read-only"); err == nil {
		t.Fatal("accepted incompatible override")
	}
	other := session.NewRegistry().Create("other", "other", t.TempDir(), "codex", "", AppSandboxMode, "")
	if err := c.validateAppSandbox(other, ""); err == nil {
		t.Fatal("accepted foreign workspace")
	}
	if err := c.UpdateSessionSettings(context.Background(), other); err == nil {
		t.Fatal("accepted foreign workspace settings")
	}

	params := c.codexTurnParamsForSession("thread", nil, s.Snapshot(), "")
	policy := params["sandboxPolicy"].(map[string]any)
	if policy["type"] != "externalSandbox" || policy["networkAccess"] != "enabled" {
		t.Fatal(policy)
	}
	normal := NewCodex(sandboxTestSink{}, "none").codexTurnParamsForSession("thread", nil, session.Snapshot{Cwd: workspace, Sandbox: "workspace-write"}, "")["sandboxPolicy"].(map[string]any)
	if normal["type"] != "workspaceWrite" || normal["networkAccess"] != false {
		t.Fatal("changed normal backend policy")
	}
	if err := os.WriteFile(filepath.Join(state, "auth.json"), []byte("canary"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.validateAppSandbox(s, ""); err == nil {
		t.Fatal("accepted persistent credentials")
	}
}
func TestAppSandboxChildEnvironmentHasNoBrokerCredentials(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "fake-test-key")
	t.Setenv("CODEX_ACCESS_TOKEN", "fake-access")
	t.Setenv("BRIDGE_AUTH_PIPE", "fake-pipe")
	c := &Codex{appSandbox: &codexAppSandbox{runtimeDir: "/isolated/state"}}
	env := strings.Join(c.appSandboxEnvironment(), "\n")
	if strings.Contains(env, "fake-") || strings.Contains(env, "AUTH_PIPE") || !strings.Contains(env, "CODEX_HOME=/isolated/state") {
		t.Fatal("unexpected credential inheritance")
	}
}
func TestAppSandboxRefreshCannotChangeAccount(t *testing.T) {
	n := 0
	p := &codexAppSandbox{tokens: func(context.Context) (ExternalChatGPTTokens, error) {
		n++
		id := "first"
		if n > 1 {
			id = "second"
		}
		return ExternalChatGPTTokens{AccessToken: "fake", AccountID: id}, nil
	}}
	if _, err := p.credentials(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.credentials(context.Background()); err == nil {
		t.Fatal("accepted cross-account refresh")
	}
}
func TestHistoryDeniedIsNotEmptyEvenAfterCache(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses POSIX permission controls; real macOS sandbox case covers kernel denial")
	}
	root := t.TempDir()
	c := NewCodex(sandboxTestSink{}, "none")
	c.sessionsRoot = root
	if list, err := c.ResumableSessions(10); err != nil || len(list) != 0 {
		t.Fatal(list, err)
	}
	if err := os.Chmod(root, 0000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(root, 0700)
	if _, err := c.ResumableSessions(10); !errors.Is(err, history.ErrPermissionDenied) {
		t.Fatalf("expected permission error, got %v", err)
	}
	if _, err := c.LoadHistory("known", history.Opts{}); !errors.Is(err, history.ErrPermissionDenied) {
		t.Fatalf("expected permission error, got %v", err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	if list, err := c.ResumableSessions(10); err != nil || len(list) != 0 {
		t.Fatal("failed to recover after grant", err)
	}
}
func TestClaudeConfigDirSelectsSameHistoryRoot(t *testing.T) {
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	c := NewClaude(sandboxTestSink{}, "none")
	if c.projectsDir != sourcepolicy.ClaudeProjectsDir() {
		t.Fatalf("wrong source %s", c.projectsDir)
	}
}
