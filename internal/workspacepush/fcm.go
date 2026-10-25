// Package workspacepush provides a separate operator-configured FCM sender.
// It has no device registry, pairing/grant mutation or legacy notification feed.
package workspacepush

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"everything-go/internal/workspace"
	"golang.org/x/oauth2/google"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

type Configuration struct {
	Version         int    `json:"version"`
	ProjectID       string `json:"project_id"`
	Subject         string `json:"subject"`
	KeychainService string `json:"keychain_service"`
	KeychainAccount string `json:"keychain_account"`
}
type Sender struct {
	config  Configuration
	current func() bool
	secret  func(context.Context) ([]byte, error)
	http    *http.Client
}

func Load(path string) (*Sender, error) {
	raw, err := privateConfig(path)
	if err != nil {
		return nil, err
	}
	var c Configuration
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF || c.Version != 1 || c.ProjectID == "" || strings.ContainsAny(c.ProjectID, "/\\\r\n\x00") || c.Subject == "" || c.KeychainService == "" || c.KeychainAccount == "" {
		return nil, errors.New("workspace_push_configuration_invalid")
	}
	hash := sha256.Sum256(raw)
	s := &Sender{config: c, http: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	s.current = func() bool { b, e := privateConfig(path); return e == nil && sha256.Sum256(b) == hash }
	s.secret = func(ctx context.Context) ([]byte, error) {
		if runtime.GOOS != "darwin" {
			return nil, errors.New("original_keychain_unavailable_on_this_host")
		}
		b, err := exec.CommandContext(ctx, "/usr/bin/security", "find-generic-password", "-s", c.KeychainService, "-a", c.KeychainAccount, "-w").Output()
		if err != nil || len(b) == 0 || len(b) > 65536 {
			return nil, errors.New("original_push_keychain_profile_unavailable")
		}
		return bytes.TrimSpace(b), nil
	}
	return s, nil
}
func privateConfig(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 65536 {
		return nil, errors.New("workspace_push_private_configuration_unavailable")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("workspace_push_configuration_unavailable")
	}
	return b, nil
}
func (s *Sender) Send(ctx context.Context, token, platform, authority, room, notice string) workspace.PushReceipt {
	fail := func(reason string) workspace.PushReceipt {
		return workspace.PushReceipt{State: "failed", Reason: reason}
	}
	if !s.current() {
		return fail("original_push_configuration_changed")
	}
	raw, err := s.secret(ctx)
	if err != nil {
		return fail("original_push_keychain_profile_unavailable")
	}
	var sa struct {
		Type     string `json:"type"`
		Project  string `json:"project_id"`
		Subject  string `json:"client_email"`
		TokenURI string `json:"token_uri"`
	}
	if json.Unmarshal(raw, &sa) != nil || sa.Type != "service_account" || sa.Project != s.config.ProjectID || sa.Subject != s.config.Subject || sa.TokenURI != "https://oauth2.googleapis.com/token" {
		return fail("original_push_identity_mismatch")
	}
	jwt, err := google.JWTConfigFromJSON(raw, "https://www.googleapis.com/auth/firebase.messaging")
	if err != nil {
		return fail("original_push_credential_invalid")
	}
	access, err := jwt.TokenSource(ctx).Token()
	if err != nil {
		return fail("original_push_authentication_unavailable")
	}
	body := map[string]any{"message": map[string]any{"token": token, "notification": map[string]string{"title": "協作頻道", "body": "有新的頻道待辦"}, "data": map[string]string{"type": "workspace_attention", "workspace_push_version": "1", "authority_instance_id": authority, "room_id": room, "notification_id": notice}, "android": map[string]any{"ttl": "0s", "notification": map[string]any{"tag": notice, "channel_id": "workspace_updates"}}, "apns": map[string]any{"headers": map[string]string{"apns-expiration": "0", "apns-collapse-id": notice}, "payload": map[string]any{"aps": map[string]any{"sound": "default", "thread-id": "workspace-updates"}}}}}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://fcm.googleapis.com/v1/projects/"+s.config.ProjectID+"/messages:send", bytes.NewReader(b))
	if err != nil {
		return fail("push_prepare_failed")
	}
	req.Header.Set("Authorization", "Bearer "+access.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	res, err := s.http.Do(req)
	if err != nil {
		return workspace.PushReceipt{State: "unknown", Reason: "push_transport_unknown_no_replay"}
	}
	defer res.Body.Close()
	if res.StatusCode == 429 || res.StatusCode == 503 {
		return workspace.PushReceipt{State: "retryable", Reason: "push_service_rejected_retryable"}
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return fail("push_provider_rejected")
	}
	var receipt struct {
		Name string `json:"name"`
	}
	if json.NewDecoder(io.LimitReader(res.Body, 65536)).Decode(&receipt) != nil || !strings.HasPrefix(receipt.Name, "projects/"+s.config.ProjectID+"/messages/") {
		return workspace.PushReceipt{State: "unknown", Reason: "push_acknowledgement_unverified"}
	}
	return workspace.PushReceipt{State: "accepted", ProviderID: receipt.Name}
}
