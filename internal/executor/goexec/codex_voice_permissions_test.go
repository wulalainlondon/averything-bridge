package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"reflect"
	"testing"
	"time"
)

func TestNativeSettingsUpdateCarriesExactSessionPermissions(t *testing.T) {
	for _, mode := range []string{"read-only", "workspace-write", "danger-full-access"} {
		t.Run(mode, func(t *testing.T) {
			c := NewCodex(&capSink{}, "codex")
			s := session.NewRegistry().Create("permission-session", "test", t.TempDir(), backend.Codex, "", mode, "original-thread")
			w := &toolTestWriter{c: c}
			w.reply = func(method string, raw json.RawMessage) (any, error) {
				var p map[string]any
				json.Unmarshal(raw, &p)
				want := c.codexTurnParamsForSession("original-thread", nil, s.Snapshot(), "")
				rawWant, _ := json.Marshal(want)
				json.Unmarshal(rawWant, &want)
				if method != "thread/settings/update" || p["threadId"] != "original-thread" || !reflect.DeepEqual(p["sandboxPolicy"], want["sandboxPolicy"]) || p["approvalPolicy"] != want["approvalPolicy"] || p["approvalsReviewer"] != want["approvalsReviewer"] {
					t.Errorf("settings lost session permissions: mode=%s params=%v", mode, p)
				}
				return map[string]any{}, nil
			}
			c.rpc.setWriter(w)
			if err := c.UpdateSessionSettings(context.Background(), s); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func voicePermissionFixture(t *testing.T) (*Codex, *session.Session, *toolTestWriter) {
	t.Helper()
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode, c.remoteReconnect = "daemon", false
	socket, _ := healthServer(t, func(string, string) (any, bool) { return map[string]any{}, true })
	c.appServerSocket = socket
	if err := c.startRemoteServerLocked(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	conn, done, cancel := c.remoteConn, c.remoteDone, c.remoteCancel
	t.Cleanup(func() {
		cancel()
		_ = conn.CloseNow()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("fixture reader did not stop")
		}
	})
	c.runtimeDiagnostics = map[string]any{"codex": map[string]any{"running_version": "0.162.1"}}
	s := session.NewRegistry().Create("voice-permission-session", "fixture", t.TempDir(), backend.Codex, "", "danger-full-access", "original-thread")
	w := &toolTestWriter{c: c}
	c.rpc.setWriter(w)
	return c, s, w
}

func TestVoiceStartupSynchronizesColdAndWarmSessionBeforeRealtime(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "warm"}[warm], func(t *testing.T) {
			c, s, w := voicePermissionFixture(t)
			if warm {
				c.state(s.ID).threadID = s.ResumeID()
			}
			w.reply = func(method string, raw json.RawMessage) (any, error) {
				var p map[string]any
				json.Unmarshal(raw, &p)
				if p["threadId"] != s.ResumeID() {
					t.Errorf("wrong thread: %v", p)
				}
				switch method {
				case "thread/resume":
					return map[string]any{"thread": map[string]any{"id": s.ResumeID()}, "sandbox": map[string]any{"type": "dangerFullAccess"}, "cwd": s.Cwd(), "approvalPolicy": "never", "approvalsReviewer": "user"}, nil
				case "thread/settings/update":
					if p["sandboxPolicy"].(map[string]any)["type"] != "dangerFullAccess" || p["model"] != nil {
						t.Errorf("voice permissions lost or unrelated model overridden: %v", p)
					}
				case "thread/realtime/start":
					c.dispatchVoice("thread/realtime/sdp", json.RawMessage(`{"threadId":"original-thread","realtimeSessionId":"voice-fixture","sdp":"v=0\r\nfixture"}`))
				default:
					t.Errorf("unexpected effect %s", method)
				}
				return map[string]any{}, nil
			}
			_, err := c.StartRealtimeVoice(context.Background(), s, backend.RealtimeVoiceStart{VoiceID: "voice-fixture", SDP: "v=0"})
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"thread/settings/update", "thread/resume", "thread/realtime/start"}
			if !warm {
				want = append([]string{"thread/resume"}, want...)
			}
			if !reflect.DeepEqual(w.methods, want) {
				t.Fatal(w.methods, want)
			}
			c.dispatchVoice("thread/realtime/closed", json.RawMessage(`{"threadId":"original-thread","realtimeSessionId":"voice-fixture"}`))
		})
	}
}

func TestVoiceFirstSettingsNotificationVerifiesWithoutRollout(t *testing.T) {
	for _, policy := range []string{"dangerFullAccess", "readOnly"} {
		t.Run(policy, func(t *testing.T) {
			c, s, w := voicePermissionFixture(t)
			c.state(s.ID).threadID = s.ResumeID()
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				if method != "thread/settings/update" {
					t.Errorf("fresh thread must not need rollout metadata: %s", method)
				}
				// Another thread's event must never satisfy this waiter.
				other, _ := json.Marshal(map[string]any{"method": "thread/settings/updated", "params": map[string]any{"threadId": "other", "threadSettings": map[string]any{"cwd": s.Cwd(), "sandboxPolicy": map[string]any{"type": "dangerFullAccess"}, "approvalPolicy": "never", "approvalsReviewer": "user"}}})
				c.dispatch(other)
				if len(c.voicePermissionSignals[s.ResumeID()]) != 0 {
					t.Fatal("cross-thread event accepted")
				}
				event, _ := json.Marshal(map[string]any{"method": "thread/settings/updated", "params": map[string]any{"threadId": s.ResumeID(), "threadSettings": map[string]any{"cwd": s.Cwd(), "sandboxPolicy": map[string]any{"type": policy, "networkAccess": false}, "approvalPolicy": "never", "approvalsReviewer": "user"}}})
				c.dispatch(event)
				return map[string]any{}, nil
			}
			err := c.syncVoiceSessionPermissions(context.Background(), s, s.ResumeID())
			if (err == nil) != (policy == "dangerFullAccess") {
				t.Fatalf("wrong notification result: %v", err)
			}
			if len(c.voicePermissionSignals) != 0 || !reflect.DeepEqual(w.methods, []string{"thread/settings/update"}) {
				t.Fatal("waiter leaked or unexpected native call", w.methods)
			}
		})
	}
}

func TestVoiceRefusesFailedSyncAndConcurrentSessionChanges(t *testing.T) {
	for _, cause := range []string{"rejected", "changed", "updating", "metadata"} {
		t.Run(cause, func(t *testing.T) {
			c, s, w := voicePermissionFixture(t)
			c.state(s.ID).threadID = s.ResumeID()
			var release func()
			if cause == "updating" {
				_, release, _ = s.ReserveConfiguration(nil)
				defer release()
			}
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				if cause == "metadata" {
					if method == "thread/settings/update" {
						return map[string]any{}, nil
					}
					if method == "thread/resume" {
						return map[string]any{"thread": map[string]any{"id": s.ResumeID()}, "sandbox": map[string]any{"type": "workspaceWrite", "networkAccess": false, "writableRoots": []string{}}, "cwd": s.Cwd(), "approvalPolicy": "never", "approvalsReviewer": "user"}, nil
					}
				}
				if method != "thread/settings/update" {
					t.Errorf("started voice or interrupted work after failed sync: %s", method)
				}
				if cause == "changed" {
					s.ApplyConfig("", "", "read-only")
					return map[string]any{}, nil
				}
				return nil, errors.New("permission sync rejected")
			}
			if _, err := c.StartRealtimeVoice(context.Background(), s, backend.RealtimeVoiceStart{VoiceID: "voice-fixture", SDP: "v=0"}); err == nil {
				t.Fatal("failed permission sync started voice")
			}
			if c.voiceCalls[s.ResumeID()] != nil || c.voicePreparing[s.ResumeID()] {
				t.Fatal("failed preparation retained voice ownership")
			}
			if cause == "updating" && len(w.methods) != 0 {
				t.Fatal("crossed uncommitted config update")
			}
		})
	}
}

func TestVoicePermissionReadbackRejectsWrongScopeAndAcceptsImplicitCwd(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	snap := session.Snapshot{Cwd: t.TempDir(), Sandbox: "workspace-write"}
	want := c.sessionPermissionSettings("thread", snap)
	base := map[string]any{"thread": map[string]any{"id": "thread"}, "cwd": snap.Cwd, "approvalPolicy": "never", "approvalsReviewer": "user", "sandbox": map[string]any{"type": "workspaceWrite", "networkAccess": false, "writableRoots": []string{}}}
	raw, _ := json.Marshal(base)
	if !voicePermissionMetadataMatches(raw, want) {
		t.Fatal("implicit cwd not recognized")
	}
	for _, key := range []string{"thread", "network", "roots", "cwd", "approval"} {
		var bad map[string]any
		json.Unmarshal(raw, &bad)
		switch key {
		case "thread":
			bad["thread"] = map[string]any{"id": "other"}
		case "network":
			bad["sandbox"].(map[string]any)["networkAccess"] = true
		case "roots":
			bad["sandbox"].(map[string]any)["writableRoots"] = []string{t.TempDir()}
		case "cwd":
			bad["cwd"] = t.TempDir()
		case "approval":
			bad["approvalPolicy"] = "on-request"
		}
		b, _ := json.Marshal(bad)
		if voicePermissionMetadataMatches(b, want) {
			t.Fatal("accepted wrong scope", key)
		}
	}
}

func TestVoiceDuplicateDoesNotTouchNativePermissions(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	s := session.NewRegistry().Create("s", "s", t.TempDir(), backend.Codex, "", "read-only", "thread")
	c.voiceCalls = map[string]*codexVoiceCall{"thread": {voiceID: "existing", sessionID: s.ID}}
	w := &toolTestWriter{c: c, reply: func(method string, _ json.RawMessage) (any, error) {
		t.Errorf("duplicate mutated runtime: %s", method)
		return nil, nil
	}}
	c.rpc.setWriter(w)
	if _, err := c.StartRealtimeVoice(context.Background(), s, backend.RealtimeVoiceStart{VoiceID: "duplicate", SDP: "v=0"}); err == nil {
		t.Fatal("duplicate accepted")
	}
	if len(w.methods) != 0 {
		t.Fatal(w.methods)
	}
}

func TestVoicePermissionSyncRejectsMismatchedNativeIdentity(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	s := session.NewRegistry().Create("s", "s", t.TempDir(), backend.Codex, "", "read-only", "correct")
	c.state(s.ID).threadID = "stale"
	w := &toolTestWriter{c: c, reply: func(method string, _ json.RawMessage) (any, error) {
		t.Errorf("mutated wrong thread: %s", method)
		return nil, nil
	}}
	c.rpc.setWriter(w)
	if err := c.syncVoiceSessionPermissions(context.Background(), s, "correct"); err == nil {
		t.Fatal("wrong identity accepted")
	}
	if err := c.UpdateSessionSettings(context.Background(), s); err == nil {
		t.Fatal("settings targeted stale thread")
	}
	if len(w.methods) != 0 {
		t.Fatal(w.methods)
	}
}
