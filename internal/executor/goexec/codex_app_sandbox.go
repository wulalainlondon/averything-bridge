package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"everything-go/internal/executor"
	"everything-go/internal/session"
)

const AppSandboxMode = "app-sandbox"

// ExternalChatGPTTokens are supplied by the owning application's credential
// broker, over private IPC. Never put these values in flags, files or child env.
type ExternalChatGPTTokens struct {
	AccessToken string
	AccountID   string
	PlanType    string
}

type AppSandboxOptions struct {
	Workspace  string
	RuntimeDir string
	Tokens     func(context.Context) (ExternalChatGPTTokens, error)
}

type codexAppSandbox struct {
	workspace  string
	runtimeDir string
	tokens     func(context.Context) (ExternalChatGPTTokens, error)
	accountMu  sync.Mutex
	accountID  string
}

// NewCodexAppSandbox is a separate owner-only construction path. Ordinary
// backends, JSON session settings and daemon connections cannot enable it.
// "app-sandbox" has a different policy from workspace-write: macOS enforces
// file capabilities and allows the network-client capability of this process.
func NewCodexAppSandbox(sink executor.Sink, binary string, options AppSandboxOptions) (*Codex, error) {
	return newCodexAppSandbox(sink, binary, options, verifyInheritedAppSandbox)
}

func newCodexAppSandbox(sink executor.Sink, binary string, o AppSandboxOptions, verify func() error) (*Codex, error) {
	if err := verify(); err != nil {
		return nil, err
	}
	if o.Tokens == nil {
		return nil, errors.New("app_sandbox_credential_broker_required")
	}
	workspace, err := filepath.EvalSymlinks(o.Workspace)
	if err != nil || !filepath.IsAbs(workspace) {
		return nil, errors.New("app_sandbox_workspace_unavailable")
	}
	fi, err := os.Stat(workspace)
	if err != nil || !fi.IsDir() {
		return nil, errors.New("app_sandbox_workspace_unavailable")
	}
	runtimeDir, err := filepath.Abs(o.RuntimeDir)
	if err != nil || strings.TrimSpace(o.RuntimeDir) == "" {
		return nil, errors.New("app_sandbox_runtime_required")
	}
	if runtimeDir == workspace || strings.HasPrefix(runtimeDir, workspace+string(filepath.Separator)) {
		return nil, errors.New("app_sandbox_runtime_must_be_separate")
	}
	if err := os.MkdirAll(runtimeDir, 0700); err != nil {
		return nil, err
	}
	runtimeDir, err = filepath.EvalSymlinks(runtimeDir)
	if err != nil {
		return nil, err
	}
	if runtimeDir == workspace || strings.HasPrefix(runtimeDir, workspace+string(filepath.Separator)) {
		return nil, errors.New("app_sandbox_runtime_must_be_separate")
	}
	if _, err := os.Lstat(filepath.Join(runtimeDir, "auth.json")); !os.IsNotExist(err) {
		return nil, errors.New("app_sandbox_persistent_credentials_forbidden")
	}
	c := NewCodex(sink, binary)
	c.appServerMode = "stdio"
	c.appServerSocket = ""
	c.sessionsRoot = filepath.Join(runtimeDir, "sessions")
	c.indexPath = filepath.Join(runtimeDir, "session_index.jsonl")
	c.authPath = filepath.Join(runtimeDir, "auth.json")
	c.dataDir = filepath.Join(runtimeDir, "bridge")
	c.appSandbox = &codexAppSandbox{workspace: workspace, runtimeDir: runtimeDir, tokens: o.Tokens}
	return c, nil
}

func (c *Codex) validateAppSandbox(s *session.Session, override string) error {
	snap := s.Snapshot()
	if c.appSandbox == nil {
		if snap.Sandbox == AppSandboxMode || override == AppSandboxMode {
			return errors.New("app_sandbox_owner_profile_required")
		}
		return nil
	}
	if snap.Sandbox != AppSandboxMode || override != "" {
		return errors.New("app_sandbox_policy_mismatch")
	}
	actual, err := filepath.EvalSymlinks(snap.Cwd)
	if err != nil || actual != c.appSandbox.workspace {
		return errors.New("app_sandbox_workspace_mismatch")
	}
	if _, err := os.Lstat(c.authPath); !os.IsNotExist(err) {
		return errors.New("app_sandbox_persistent_credentials_forbidden")
	}
	return nil
}

func (c *Codex) appSandboxEnvironment() []string {
	out := []string{}
	for _, key := range []string{"PATH", "HOME", "USER", "LOGNAME", "TMPDIR", "LANG", "LC_ALL", "SHELL"} {
		if value, ok := os.LookupEnv(key); ok {
			out = append(out, key+"="+value)
		}
	}
	return append(out, "CODEX_HOME="+c.appSandbox.runtimeDir)
}

func (c *Codex) loginExternalChatGPT() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tokens, err := c.appSandbox.credentials(ctx)
	if err != nil || tokens.AccessToken == "" || tokens.AccountID == "" {
		return errors.New("app_sandbox_credentials_unavailable")
	}
	params := map[string]any{"type": "chatgptAuthTokens", "accessToken": tokens.AccessToken, "chatgptAccountId": tokens.AccountID}
	if tokens.PlanType != "" {
		params["chatgptPlanType"] = tokens.PlanType
	}
	if _, err := c.rpc.request("account/login/start", params, 15*time.Second); err != nil {
		return errors.New("app_sandbox_external_login_failed")
	}
	return nil
}

func (c *Codex) handleExternalAuthRefresh(id any, method string, raw json.RawMessage) bool {
	if c.appSandbox == nil || method != "account/chatgptAuthTokens/refresh" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tokens, err := c.appSandbox.credentials(ctx)
	if err != nil || tokens.AccessToken == "" || tokens.AccountID == "" {
		_ = c.rpc.write(map[string]any{"id": id, "error": map[string]any{"code": -32000, "message": "app_sandbox_credentials_unavailable"}})
		return true
	}
	result := map[string]any{"accessToken": tokens.AccessToken, "chatgptAccountId": tokens.AccountID}
	if tokens.PlanType != "" {
		result["chatgptPlanType"] = tokens.PlanType
	}
	_ = c.rpc.write(map[string]any{"id": id, "result": result})
	return true
}

func (p *codexAppSandbox) credentials(ctx context.Context) (ExternalChatGPTTokens, error) {
	t, err := p.tokens(ctx)
	if err != nil || t.AccessToken == "" || t.AccountID == "" {
		return ExternalChatGPTTokens{}, errors.New("app_sandbox_credentials_unavailable")
	}
	p.accountMu.Lock()
	defer p.accountMu.Unlock()
	if p.accountID != "" && p.accountID != t.AccountID {
		return ExternalChatGPTTokens{}, errors.New("app_sandbox_account_changed")
	}
	p.accountID = t.AccountID
	return t, nil
}

// CloseAppSandbox ends only this private owner-constructed server. It can never
// stop a normal/shared-daemon backend.
func (c *Codex) CloseAppSandbox() error {
	if c.appSandbox == nil {
		return errors.New("app_sandbox_owner_profile_required")
	}
	c.startMu.Lock()
	defer c.startMu.Unlock()
	return c.stopServerLocked()
}
