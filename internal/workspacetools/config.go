// Package workspacetools contains operator-configured account adapters. It has
// no wire registration, shell command profile, URL override or account-grant API.
package workspacetools

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"everything-go/internal/workspace"
)

type KeychainProfile struct {
	Service string `json:"service"`
	Account string `json:"account"`
}
type Profile struct {
	ID             string          `json:"id"`
	Kind           string          `json:"kind"`
	Provider       string          `json:"provider"`
	Revision       string          `json:"revision"`
	Subject        string          `json:"subject"`
	Keychain       KeychainProfile `json:"keychain"`
	AllowedActions []string        `json:"allowed_actions"`
	Calendars      []string        `json:"calendars,omitempty"`
}
type Configuration struct {
	Version  int       `json:"version"`
	Accounts []Profile `json:"accounts"`
}

func Load(path string) ([]workspace.ToolAdapter, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, errors.New("workspace_tool_configuration_unavailable")
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 65536 {
		return nil, errors.New("workspace_tool_configuration_requires_private_regular_file")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("workspace_tool_configuration_unavailable")
	}
	var cfg Configuration
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(&cfg) != nil || cfg.Version != 1 || len(cfg.Accounts) > 20 {
		return nil, errors.New("workspace_tool_configuration_invalid")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, errors.New("workspace_tool_configuration_invalid")
	}
	out := []workspace.ToolAdapter{}
	seen := map[string]bool{}
	for _, p := range cfg.Accounts {
		a, e := newGoogle(p)
		if e != nil {
			return nil, e
		}
		key := p.Kind + ":" + p.ID
		if seen[key] {
			return nil, errors.New("workspace_tool_account_ambiguous")
		}
		seen[key] = true
		originalHash := sha256.Sum256(b)
		a.configurationCurrent = func() error {
			current, e := os.Lstat(path)
			if e != nil || !current.Mode().IsRegular() || current.Mode().Perm()&0077 != 0 || current.Size() > 65536 {
				return errors.New("original_operator_account_configuration_changed")
			}
			raw, e := os.ReadFile(path)
			if e != nil || sha256.Sum256(raw) != originalHash {
				return errors.New("original_operator_account_configuration_changed")
			}
			return nil
		}
		out = append(out, a)
	}
	return out, nil
}
func newGoogle(p Profile) (*Google, error) {
	if p.Provider != "google" || p.ID == "" || len(p.ID) > 128 || p.Revision == "" || p.Subject == "" || strings.ContainsAny(p.ID+p.Revision+p.Subject, "\r\n\x00") || p.Keychain.Service == "" || p.Keychain.Account == "" || (p.Kind != "email" && p.Kind != "calendar") {
		return nil, errors.New("workspace_tool_profile_invalid")
	}
	allowed := map[string]bool{"health": true}
	for _, action := range p.AllowedActions {
		if (p.Kind == "email" && action != "list_messages" && action != "read_message" && action != "send") || (p.Kind == "calendar" && action != "list_events" && action != "read_event" && action != "create_event") {
			return nil, errors.New("workspace_tool_action_not_supported")
		}
		allowed[action] = true
	}
	if p.Kind == "calendar" && len(p.Calendars) == 0 {
		return nil, errors.New("workspace_tool_calendar_scope_required")
	}
	for _, id := range p.Calendars {
		if id == "" || len(id) > 1024 || strings.ContainsAny(id, "\r\n\x00") {
			return nil, errors.New("workspace_tool_calendar_scope_invalid")
		}
	}
	b, _ := json.Marshal(p)
	hash := sha256.Sum256(b)
	return &Google{profile: p, revision: p.Revision + ":" + hex.EncodeToString(hash[:]), allowed: allowed, http: &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, secret: readKeychain, now: time.Now}, nil
}
func readKeychain(ctx context.Context, p KeychainProfile) ([]byte, error) {
	if runtime.GOOS != "darwin" {
		return nil, errors.New("original_keychain_unavailable_on_this_host")
	}
	// These exact operator-supplied identifiers are the only permitted lookup.
	// Output stays in memory. Stderr and secret values are never returned/logged.
	c := exec.CommandContext(ctx, "/usr/bin/security", "find-generic-password", "-s", p.Service, "-a", p.Account, "-w")
	b, err := c.Output()
	if err != nil {
		return nil, errors.New("original_keychain_profile_unavailable")
	}
	if len(b) == 0 || len(b) > 65536 {
		return nil, errors.New("original_keychain_profile_invalid")
	}
	return bytes.TrimSpace(b), nil
}
