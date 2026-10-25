package goexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/session"
)

func TestColdRecoveryWritesBoundedRedactedCheckpointAndCommitsManifest(t *testing.T) {
	root := t.TempDir()
	dataDir := t.TempDir()
	resumeID := "019fb03e-0666-7ce3-8f99-8f19c79540e2"
	rollout := filepath.Join(root, "rollout-2026-08-17T00-00-00-"+resumeID+".jsonl")
	rows := []string{
		`{"timestamp":"2026-08-17T00:00:00Z","type":"session_meta","payload":{"id":"019fb03e-0666-7ce3-8f99-8f19c79540e2","cwd":"/work"}}`,
		`{"timestamp":"2026-08-17T00:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"fix with api_key=super-secret-value"}]}}`,
		`{"timestamp":"2026-08-17T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"work remains"}]}}`,
	}
	if err := os.WriteFile(rollout, []byte(strings.Join(rows, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	c.dataDir = dataDir
	c.checkpointMaxBytes = 16 * 1024
	snap := session.Snapshot{ID: "jl_x_old", Cwd: "/work", Backend: "codex", ResumeID: resumeID}

	recovery, err := c.prepareColdRecovery(snap, rollout, fileSize(rollout), "cold_resume_hard_limit")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(recovery.Handoff, "super-secret-value") || !strings.Contains(recovery.Handoff, "[REDACTED]") {
		t.Fatalf("checkpoint secret redaction failed: %s", recovery.Handoff)
	}
	if err := c.commitColdRecovery(recovery, "01a01044-6c39-7d02-a5c0-164b2289ecdc"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(recovery.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest generationManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Pending != nil || manifest.ActiveGeneration != 2 || manifest.ActiveResumeID != "01a01044-6c39-7d02-a5c0-164b2289ecdc" {
		t.Fatalf("manifest did not commit atomically: %+v", manifest)
	}
}

func TestEnsureThreadSingleflightSendsOneResumeRPC(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	reg := session.NewRegistry()
	s := reg.Create("logical", "session", t.TempDir(), "codex", "", "", "thread-old")
	st := c.state(s.ID)
	writer := &rpcCaptureWriter{writes: make(chan []byte, 4)}
	c.rpc.setWriter(writer)

	methods := make(chan string, 4)
	go func() {
		request := <-writer.writes
		var frame struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
			Params struct {
				ExcludeTurns bool `json:"excludeTurns"`
			} `json:"params"`
		}
		_ = json.Unmarshal(request, &frame)
		if !frame.Params.ExcludeTurns {
			methods <- "missing excludeTurns"
		} else {
			methods <- frame.Method
		}
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":"thread-old"}}}`, frame.ID)))
	}()

	errs := make(chan error, 2)
	go func() { errs <- c.ensureThread(s, st) }()
	go func() { errs <- c.ensureThread(s, st) }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if method := <-methods; method != "thread/resume" {
		t.Fatalf("first RPC method=%q", method)
	}
	select {
	case request := <-writer.writes:
		t.Fatalf("singleflight emitted a second RPC: %s", request)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestEnsureThreadStdioResumeKeepsLegacyParams(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "stdio"
	reg := session.NewRegistry()
	s := reg.Create("logical", "session", t.TempDir(), "codex", "", "", "thread-old")
	writer := &rpcCaptureWriter{writes: make(chan []byte, 1)}
	c.rpc.setWriter(writer)

	checked := make(chan error, 1)
	go func() {
		request := <-writer.writes
		var frame struct {
			ID     int            `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if err := json.Unmarshal(request, &frame); err != nil {
			checked <- err
			return
		}
		if frame.Method != "thread/resume" {
			checked <- fmt.Errorf("method=%q", frame.Method)
			return
		}
		if _, ok := frame.Params["excludeTurns"]; ok {
			checked <- errors.New("stdio compatibility request included excludeTurns")
			return
		}
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":"thread-old"}}}`, frame.ID)))
		checked <- nil
	}()

	if err := c.ensureThread(s, c.state(s.ID)); err != nil {
		t.Fatal(err)
	}
	if err := <-checked; err != nil {
		t.Fatal(err)
	}
}

func TestEnsureThreadLargeRolloutSkipsDirectResume(t *testing.T) {
	root := t.TempDir()
	resumeID := "019fb03e-0666-7ce3-8f99-8f19c79540e2"
	newID := "01a01044-6c39-7d02-a5c0-164b2289ecdc"
	rollout := filepath.Join(root, "rollout-2026-08-17T00-00-00-"+resumeID+".jsonl")
	rows := []string{
		`{"timestamp":"2026-08-17T00:00:00Z","type":"session_meta","payload":{"id":"019fb03e-0666-7ce3-8f99-8f19c79540e2","cwd":"/work"}}`,
		`{"timestamp":"2026-08-17T00:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}}`,
		`{"timestamp":"2026-08-17T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"checkpoint state"}]}}`,
	}
	if err := os.WriteFile(rollout, []byte(strings.Join(rows, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "stdio"
	c.sessionsRoot = root
	c.dataDir = t.TempDir()
	c.rolloverEnabled = true
	c.coldResumeMaxBytes = 1
	reg := session.NewRegistry()
	s := reg.Create("jl_x_old", "long", t.TempDir(), "codex", "", "", resumeID)
	st := c.state(s.ID)
	writer := &rpcCaptureWriter{writes: make(chan []byte, 2)}
	c.rpc.setWriter(writer)
	method := make(chan string, 1)
	go func() {
		request := <-writer.writes
		var frame struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
		}
		_ = json.Unmarshal(request, &frame)
		method <- frame.Method
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":%q}}}`, frame.ID, newID)))
	}()

	if err := c.ensureThread(s, st); err != nil {
		t.Fatal(err)
	}
	if got := <-method; got != "thread/start" {
		t.Fatalf("large rollout attempted %q, want thread/start without resume", got)
	}
	if s.ResumeID() != resumeID || len(s.Snapshot().HistoricalResumeIDs) != 0 {
		t.Fatalf("generation mapping advanced before first turn: %+v", s.Snapshot())
	}
	if st.pendingHandoff == "" || st.pendingRecovery == nil {
		t.Fatal("recovery handoff was not staged for the first user turn")
	}
	newRollout := filepath.Join(root, "rollout-2026-08-18T00-00-00-"+newID+".jsonl")
	if err := os.WriteFile(newRollout, []byte("materialized first turn\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c.finalizePendingRecovery(s, st, newID)
	if s.ResumeID() != newID || strings.Join(s.Snapshot().HistoricalResumeIDs, ",") != resumeID {
		t.Fatalf("generation mapping not advanced after first turn: %+v", s.Snapshot())
	}
}

func TestEnsureThreadDaemonLargeRolloutKeepsCanonicalThread(t *testing.T) {
	root := t.TempDir()
	resumeID := "019fb03e-0666-7ce3-8f99-8f19c79540e2"
	rollout := filepath.Join(root, "rollout-2026-08-17T00-00-00-"+resumeID+".jsonl")
	if err := os.WriteFile(rollout, []byte("large enough for the configured threshold\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.sessionsRoot = root
	c.rolloverEnabled = true
	c.coldResumeMaxBytes = 1
	reg := session.NewRegistry()
	s := reg.Create("logical", "large daemon session", t.TempDir(), "codex", "", "", resumeID)
	writer := &rpcCaptureWriter{writes: make(chan []byte, 1)}
	c.rpc.setWriter(writer)

	method := make(chan string, 1)
	go func() {
		request := <-writer.writes
		var frame struct {
			ID     int            `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.Unmarshal(request, &frame)
		if frame.Params["excludeTurns"] != true {
			method <- "thread/resume without excludeTurns"
		} else {
			method <- frame.Method
		}
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":%q}}}`, frame.ID, resumeID)))
	}()

	if err := c.ensureThread(s, c.state(s.ID)); err != nil {
		t.Fatal(err)
	}
	if got := <-method; got != "thread/resume" {
		t.Fatalf("large daemon rollout used %q, want metadata-only thread/resume", got)
	}
	if s.ResumeID() != resumeID {
		t.Fatalf("daemon changed canonical thread to %q", s.ResumeID())
	}
}

func TestNoRolloutFoundIsStaleThreadError(t *testing.T) {
	err := errors.New(`{"code":-32600,"message":"no rollout found for thread id missing"}`)
	if !isStaleThreadError(err) {
		t.Fatal("Codex no-rollout response must trigger safe local recovery")
	}
}

func TestLatestAvailableRecoverySourceFallsBackToHistoricalGeneration(t *testing.T) {
	root := t.TempDir()
	oldID := "019fb760-170f-7371-9bd6-eb10e23254ef"
	path := filepath.Join(root, "rollout-2026-07-31T00-00-00-"+oldID+".jsonl")
	if err := os.WriteFile(path, []byte("old generation\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	snap := session.Snapshot{ResumeID: "01a0292b-ecba-7e12-925a-e2349c57da9e", HistoricalResumeIDs: []string{oldID}}
	source, gotPath, gotBytes, ok := c.latestAvailableRecoverySource(snap)
	if !ok || source.ResumeID != oldID || gotPath != path || gotBytes <= 0 {
		t.Fatalf("historical fallback = (%+v, %q, %d, %v)", source, gotPath, gotBytes, ok)
	}
}

func TestEnsureThreadRecoversMissingActiveGenerationFromHistoricalRollout(t *testing.T) {
	root := t.TempDir()
	oldID := "019fb760-170f-7371-9bd6-eb10e23254ef"
	missingID := "01a0292b-ecba-7e12-925a-e2349c57da9e"
	newID := "01a02a64-0b4b-7681-af53-4ff558c8cedb"
	rollout := filepath.Join(root, "rollout-2026-07-31T00-00-00-"+oldID+".jsonl")
	rows := []string{
		fmt.Sprintf(`{"timestamp":"2026-07-31T00:00:00Z","type":"session_meta","payload":{"id":%q,"cwd":"/work"}}`, oldID),
		`{"timestamp":"2026-07-31T00:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"keep this context"}]}}`,
		`{"timestamp":"2026-07-31T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"saved answer"}]}}`,
	}
	if err := os.WriteFile(rollout, []byte(strings.Join(rows, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	c.dataDir = t.TempDir()
	reg := session.NewRegistry()
	s := reg.Create("s1", "broken generation", t.TempDir(), "codex", "", "", missingID)
	s.AddHistoricalResumeID(oldID)
	st := c.state(s.ID)
	writer := &rpcCaptureWriter{writes: make(chan []byte, 2)}
	c.rpc.setWriter(writer)
	methods := make(chan string, 2)
	go func() {
		for i := 0; i < 2; i++ {
			request := <-writer.writes
			var frame struct {
				ID     int    `json:"id"`
				Method string `json:"method"`
			}
			_ = json.Unmarshal(request, &frame)
			methods <- frame.Method
			if i == 0 {
				c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"error":{"code":-32600,"message":"no rollout found for thread id %s"}}`, frame.ID, missingID)))
			} else {
				c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":%q}}}`, frame.ID, newID)))
			}
		}
	}()

	if err := c.ensureThread(s, st); err != nil {
		t.Fatal(err)
	}
	if first, second := <-methods, <-methods; first != "thread/resume" || second != "thread/start" {
		t.Fatalf("RPC sequence = %q, %q", first, second)
	}
	if st.pendingRecovery == nil || st.pendingRecovery.OldResumeID != oldID || st.pendingHandoff == "" {
		t.Fatalf("historical recovery not staged: %+v", st.pendingRecovery)
	}
	if s.ResumeID() != missingID {
		t.Fatalf("missing generation committed before a materialized turn: %s", s.ResumeID())
	}
}

func TestStripCodexBridgeHandoffKeepsOnlyCurrentUserRequest(t *testing.T) {
	text := `<bridge_session_handoff schema="1">{"recent_messages":[]}</bridge_session_handoff>

<current_user_request>
continue fixing
</current_user_request>`
	if got := stripCodexBridgeHandoff(text); got != "continue fixing" {
		t.Fatalf("stripCodexBridgeHandoff=%q", got)
	}
}
