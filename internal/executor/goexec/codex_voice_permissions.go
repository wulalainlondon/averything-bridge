package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"everything-go/internal/runtime"
	"everything-go/internal/session"
)

// Text turns, UI settings and native realtime backing turns share one policy
// mapping. This never raises an explicit restrictive session to full access.
func (c *Codex) sessionSandboxPolicy(snap session.Snapshot, override string) map[string]any {
	if c.appSandbox != nil && snap.Sandbox == AppSandboxMode && override == "" {
		return map[string]any{"type": "externalSandbox", "networkAccess": "enabled"}
	}
	return codexTurnSandboxPolicy(snap, override)
}

func (c *Codex) sessionPermissionSettings(thread string, snap session.Snapshot) map[string]any {
	p := map[string]any{
		"threadId": thread, "sandboxPolicy": c.sessionSandboxPolicy(snap, ""),
		"approvalPolicy": "never", "approvalsReviewer": "user",
	}
	if cwd := runtime.ExpandPath(snap.Cwd); filepath.IsAbs(cwd) {
		p["cwd"] = cwd
	}
	return p
}

func (c *Codex) syncVoiceSessionPermissions(ctx context.Context, s *session.Session, thread string) error {
	st := c.state(s.ID)
	st.ensureMu.Lock()
	defer st.ensureMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	snap, err := s.StableConfigurationSnapshot()
	if err != nil {
		return err
	}
	st.mu.Lock()
	loaded := st.threadID
	st.mu.Unlock()
	if thread == "" || thread != snap.ResumeID || loaded != thread {
		return errors.New("session_native_thread_mismatch")
	}
	if err = c.validateAppSandbox(s, ""); err != nil {
		return err
	}
	signal := make(chan json.RawMessage, 1)
	c.voiceMu.Lock()
	if c.voicePermissionSignals == nil {
		c.voicePermissionSignals = map[string]chan json.RawMessage{}
	}
	if c.voicePermissionSignals[thread] != nil {
		c.voiceMu.Unlock()
		return errors.New("voice_session_preparation_pending")
	}
	c.voicePermissionSignals[thread] = signal
	c.voiceMu.Unlock()
	defer func() {
		c.voiceMu.Lock()
		if c.voicePermissionSignals[thread] == signal {
			delete(c.voicePermissionSignals, thread)
		}
		c.voiceMu.Unlock()
	}()
	// realtime/start has no permission override. Its backing turns inherit the
	// native thread defaults, so synchronize them before starting the voice.
	if _, err = c.rpcCall("thread/settings/update", c.sessionPermissionSettings(thread, snap), 15*time.Second); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	after, err := s.StableConfigurationSnapshot()
	if err != nil {
		return err
	}
	if after.ResumeID != thread || after.ConfigRevision != snap.ConfigRevision || after.Sandbox != snap.Sandbox || after.Cwd != snap.Cwd || after.Backend != snap.Backend {
		return errors.New("voice_session_configuration_changed")
	}
	// The native settings notification works even before a voice-first thread
	// has a rollout. Older providers can be checked by metadata-only resume.
	var raw json.RawMessage
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case raw = <-signal:
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		raw, err = c.rpcCall("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true}, 15*time.Second)
		if err != nil {
			return err
		}
	}
	if !voicePermissionMetadataMatches(raw, c.sessionPermissionSettings(thread, snap)) {
		return errors.New("voice_session_permissions_not_applied")
	}
	after, err = s.StableConfigurationSnapshot()
	if err != nil {
		return err
	}
	if after.ResumeID != thread || after.ConfigRevision != snap.ConfigRevision || after.Sandbox != snap.Sandbox || after.Cwd != snap.Cwd || after.Backend != snap.Backend {
		return errors.New("voice_session_configuration_changed")
	}
	return nil
}

// Capture only the permission projection for a currently waiting original
// thread. Never keep transcripts, credentials, or settings from other threads.
func (c *Codex) observeVoicePermissionSettings(raw json.RawMessage) {
	var event struct {
		ThreadID string `json:"threadId"`
		Settings struct {
			Cwd      string          `json:"cwd"`
			Approval json.RawMessage `json:"approvalPolicy"`
			Reviewer json.RawMessage `json:"approvalsReviewer"`
			Sandbox  json.RawMessage `json:"sandboxPolicy"`
		} `json:"threadSettings"`
	}
	if len(raw) > 1<<20 || json.Unmarshal(raw, &event) != nil {
		return
	}
	c.voiceMu.Lock()
	defer c.voiceMu.Unlock()
	ch := c.voicePermissionSignals[event.ThreadID]
	if ch == nil {
		return
	}
	projection, err := json.Marshal(map[string]any{"thread": map[string]string{"id": event.ThreadID}, "cwd": event.Settings.Cwd, "approvalPolicy": event.Settings.Approval, "approvalsReviewer": event.Settings.Reviewer, "sandbox": event.Settings.Sandbox})
	if err != nil {
		return
	}
	select {
	case <-ch:
	default:
	}
	ch <- projection
}

func voicePermissionMetadataMatches(raw json.RawMessage, requested map[string]any) bool {
	var actual struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
		Cwd      string                     `json:"cwd"`
		Approval string                     `json:"approvalPolicy"`
		Reviewer string                     `json:"approvalsReviewer"`
		Sandbox  map[string]json.RawMessage `json:"sandbox"`
	}
	if json.Unmarshal(raw, &actual) != nil || actual.Thread.ID != requested["threadId"] || actual.Approval != requested["approvalPolicy"] || actual.Reviewer != requested["approvalsReviewer"] {
		return false
	}
	if cwd, ok := requested["cwd"].(string); ok && normalizedPermissionPath(actual.Cwd) != normalizedPermissionPath(cwd) {
		return false
	}
	encoded, _ := json.Marshal(requested["sandboxPolicy"])
	var expected map[string]json.RawMessage
	if json.Unmarshal(encoded, &expected) != nil {
		return false
	}
	var actualType, expectedType string
	if json.Unmarshal(actual.Sandbox["type"], &actualType) != nil || json.Unmarshal(expected["type"], &expectedType) != nil || actualType != expectedType {
		return false
	}
	switch expectedType {
	case "dangerFullAccess":
		return true
	case "externalSandbox":
		var a, e string
		return json.Unmarshal(actual.Sandbox["networkAccess"], &a) == nil && json.Unmarshal(expected["networkAccess"], &e) == nil && a == e
	case "readOnly", "workspaceWrite":
		var a, e bool
		if json.Unmarshal(actual.Sandbox["networkAccess"], &a) != nil || json.Unmarshal(expected["networkAccess"], &e) != nil || a != e {
			return false
		}
		if expectedType == "readOnly" {
			return true
		}
		var roots, wanted []string
		if json.Unmarshal(actual.Sandbox["writableRoots"], &roots) != nil || json.Unmarshal(expected["writableRoots"], &wanted) != nil {
			return false
		}
		for i := range roots {
			roots[i] = normalizedPermissionPath(roots[i])
		}
		for i := range wanted {
			wanted[i] = normalizedPermissionPath(wanted[i])
		}
		// Native workspaceWrite metadata omits redundant writableRoots equal
		// to cwd: that root is implicit. Compare equivalent effective root sets.
		roots = append(roots, normalizedPermissionPath(actual.Cwd))
		cwd, _ := requested["cwd"].(string)
		if cwd == "" {
			cwd = actual.Cwd
		}
		wanted = append(wanted, normalizedPermissionPath(cwd))
		unique := func(values []string) []string {
			set := map[string]bool{}
			for _, p := range values {
				if p != "" {
					set[p] = true
				}
			}
			out := []string{}
			for p := range set {
				out = append(out, p)
			}
			sort.Strings(out)
			return out
		}
		for _, key := range []string{"excludeSlashTmp", "excludeTmpdirEnvVar"} {
			var a, e bool
			if v, ok := actual.Sandbox[key]; ok && json.Unmarshal(v, &a) != nil {
				return false
			}
			if v, ok := expected[key]; ok && json.Unmarshal(v, &e) != nil {
				return false
			}
			if a != e {
				return false
			}
		}
		return reflect.DeepEqual(unique(roots), unique(wanted))
	}
	return false
}

func normalizedPermissionPath(path string) string {
	if path == "" {
		return ""
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return filepath.Clean(resolved)
	}
	return filepath.Clean(path)
}
