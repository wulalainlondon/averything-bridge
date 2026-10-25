package core

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/workitems"
)

// Opt-in, isolated HTTP/WS UI harness. The synthetic executor never starts a
// provider, accesses a real account or borrows any production pairing identity.
func TestWorkspaceBrowserFixture(t *testing.T) {
	path := os.Getenv("WORKSPACE_BROWSER_FIXTURE_METADATA")
	if path == "" {
		t.Skip("browser harness not requested")
	}
	f := workspaceSetupDuration(t, 45*time.Minute)
	f.h.pairing.OpenEnrollment(time.Second)
	if err := f.h.pairing.Claim("fixture-native-workspace-device-token", "native-ux-fixture-device"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.h.work.CreateProject(f.ctx, workitems.CreateProjectInput{Name: "Synthetic original project"}); err != nil {
		t.Fatal(err)
	}
	ex := f.h.exec.(*workspaceFixtureExecutor)
	ex.onSend = func(s *session.Session, request, content string) {
		if s.ResumeID() == "" {
			s.SetResumeID("synthetic-native-" + s.ID)
			f.h.Emit(protocol.NewSessionUUID(s.ID, s.ResumeID()))
		}
		turn := "synthetic-turn-" + request
		f.h.Emit(backend.NativeTaskAccepted{SessionID: s.ID, RequestID: request, ThreadID: s.ResumeID(), TurnID: turn})
		p, err := f.h.workspace.TaskForRequest(context.Background(), s.ID, request)
		if err != nil {
			return
		}
		text := "SYNTHETIC ENGINEERING FIXTURE ONLY. Draft with an explicit limitation and source https://example.com/source ."
		if p.Task.Step == "research" {
			f.h.Emit(protocol.NewToolStart(s.ID, request, "web-"+request, "WebSearch", "synthetic current-source query"))
			f.h.Emit(protocol.NewToolResult(s.ID, request, "web-"+request, "SYNTHETIC: https://example.com/source (no real web lookup or factual verification)."))
			text = "SYNTHETIC RESEARCH ONLY. Source https://example.com/source was fixture data, not fetched."
		}
		if p.Task.Step == "review" {
			verdict := "PASS"
			findings := "Synthetic independent-session mechanics only; not real model QA."
			if p.Task.Draft == 1 {
				verdict = "FAIL"
				findings = "Synthetic first review: include the fixture limitation."
			}
			b, _ := json.Marshal(map[string]string{"owner_round_id": p.Task.OwnerRoundID, "verdict": verdict, "findings": findings})
			text = string(b)
		}
		ex.proof.add(s, request, turn, text)
		f.h.Emit(protocol.NewDone(s.ID, request))
	}
	meta := map[string]any{"schema": "synthetic-workspace-browser-fixture.v1", "url": "ws" + strings.TrimPrefix(f.server.URL, "http"), "authority": f.h.cfg.InstanceID, "device_id": "workspace-device", "token": "fixture-workspace-device-token", "real_model": false, "real_accounts": false}
	data, _ := json.Marshal(meta)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path); _ = os.Remove(path + ".stop") })
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-f.ctx.Done():
			t.Fatal("browser fixture timed out")
		case <-ticker.C:
			if _, err := os.Stat(path + ".stop"); err == nil {
				return
			}
		}
	}
}
