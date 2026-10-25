package core

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	artifact "everything-go/contracts/task-api/artifacts-v3"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/history"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type extensionExec struct {
	claudeProof *extensionClaudeProof
	*capacityAppendExecutor
	final string
}

func (f *extensionExec) FinalAnswerForSession(*session.Session, string) (string, bool, error) {
	return f.final, true, nil
}

func extensionFormal(t *testing.T) (*Hub, *session.Session, backend.TaskCaller, taskapi.IntentRecord, delegation.Record) {
	return extensionFormalBackend(t, backend.Codex)
}
func extensionFormalBackend(t *testing.T, childBackend string) (*Hub, *session.Session, backend.TaskCaller, taskapi.IntentRecord, delegation.Record) {
	t.Helper()
	h, p, _ := continuityFixture(t)
	if childBackend == backend.Claude {
		p.ApplyConfig(backend.Claude, "sonnet", "workspace-write")
	}
	f := &extensionExec{capacityAppendExecutor: &capacityAppendExecutor{&capacityScopeCapabilityFixture{&fakeExec{sink: h}}}, final: "中文 🧪 café e\u0301 𠮷"}
	started := make(chan string, 4)
	f.onSend = func(s *session.Session, req, content string) {
		if s.ResumeID() == "" {
			s.SetResumeID("synthetic-retained-thread")
		}
		if childBackend == backend.Codex {
			emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-turn-" + req})
		} else {
			f.claudeProof = &extensionClaudeProof{thread: s.ResumeID(), text: f.final, id: "synthetic-claude-anchor"}
			h.Emit(backend.NativeProviderEvidence{SessionID: s.ID, RequestID: req, Backend: backend.Claude, ConversationID: s.ResumeID(), Token: "synthetic-message-uuid", TokenKind: "message_uuid", MessageID: "synthetic-message-id", Text: f.final, Status: "succeeded"})
		}
		bound, found, err := h.delegations.ByChildRequest(context.Background(), s.ID, req)
		if err != nil || !found {
			t.Error("fixture original record missing")
			return
		}
		bound.Result = f.final
		if childBackend == backend.Codex {
			correctionSeal(t, h, bound, false)
		} else {
			digest := sha256.Sum256([]byte(f.final))
			hash := hex.EncodeToString(digest[:])
			anchor, _ := json.Marshal(map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "request_id": req, "source_message_id": "synthetic-claude-anchor", "execution": map[string]any{"backend": "claude", "provider_session_id": s.ResumeID(), "provider_token": "synthetic-message-uuid", "token_kind": "message_uuid"}})
			if _, err := h.messageQueue.SealTask(context.Background(), messagequeue.TaskSeal{ID: "seal_" + hash, SessionID: s.ID, RequestID: req, Text: f.final, Hash: hash, Anchor: anchor}); err != nil {
				t.Error(err)
			}
		}
		started <- req
		h.Emit(protocol.NewDone(s.ID, req))
	}
	h.SetExecutor(f)
	c := backend.TaskCaller{Session: p, RequestID: p.ActiveQueuedID(), ThreadID: p.ResumeID(), TurnID: "synthetic-source-turn", CallID: "synthetic-create", ProviderVersion: "0.162.1", Validate: func() bool { return true }}
	if childBackend == backend.Claude {
		c.ProviderVersion = "2.1.296"
		c.ProcessGeneration = "synthetic-original-claude-source"
		c.TurnID = ""
	}
	input := map[string]any{"goal": "synthetic extension original store", "instruction": "read only", "new_worker": map[string]any{"name": "owned bounded", "profile": map[string]any{"backend": childBackend, "model": map[string]string{backend.Codex: "gpt-6.1-sol", backend.Claude: "sonnet"}[childBackend], "effort": "high"}}, "scope": map[string]any{"workspace_roots": []string{p.Cwd()}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "workspace": map[string]any{"cwd": p.Cwd(), "input_artifact_ids": []any{}}, "acceptance": []any{}, "dependencies": []any{}}
	raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-extension-create", "operation": "create_dispatch", "idempotency_key": "synthetic-extension-create", "input": input})
	response := h.ExecuteTask(context.Background(), c, raw)
	if !response.OK {
		t.Fatal(response.Error)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fixture input missing")
	}
	intent, err := h.delegations.TaskJournal().Task(context.Background(), "session:"+p.ID, 0, response.Result.(map[string]any)["task_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	r := continuityWait(t, h, intent.NativeID, "terminal")
	if childBackend == backend.Codex {
		correctionSeal(t, h, r, false)
	}
	return h, p, c, intent, r
}

func TestExtensionNativeOwnerFormalAppendAndRecovery(t *testing.T) {
	h, p, c, intent, r := extensionFormal(t)
	child, _ := h.registry.Get(r.ChildSessionID)
	scopeBefore, _ := h.TaskWorkerScope(child)
	in := continuityInput(r, p, h, "ASCII retained original followup")
	invoke := func(key string, in backend.DelegationContinuation) taskapi.Response {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": key, "operation": "append", "idempotency_key": key, "input": map[string]any{"continuity": map[string]any{"action": "continue", "arguments": in}}})
		c.CallID = key
		return h.ExecuteTask(context.Background(), c, raw)
	}
	response := invoke("synthetic-original-owner-command", in)
	if !response.OK {
		t.Fatal(response.Error)
	}
	data := response.Result.(map[string]any)["continuity"].(map[string]any)["data"].(map[string]any)
	newRound := data["delegation_id"].(string)
	if data["logical_task_id"] != r.ID || data["child_session_id"] != child.ID {
		t.Fatal("new task/worker created", data)
	}
	if again := invoke("synthetic-original-owner-command", in); !again.OK {
		t.Fatal("original receipt recovery failed", again.Error)
	}
	latest, err := h.delegations.LatestRound(context.Background(), r.ID)
	if err != nil || latest.ID != newRound || latest.Version != 2 {
		t.Fatal("duplicate continuation", latest, err)
	}
	newRecord := capacityRecordFor(t, h, newRound)
	if newRecord.ToolCallID != "synthetic-original-owner-command" {
		t.Fatal("real native caller replaced", newRecord.ToolCallID)
	}
	scopeAfter, _ := h.TaskWorkerScope(child)
	a, _ := json.Marshal(scopeBefore)
	b, _ := json.Marshal(scopeAfter)
	if string(a) != string(b) || child.ResumeID() != in.ExpectedThreadID {
		t.Fatal("scope/thread changed")
	}
	// Recover a reserved post-acceptance command via its recorded effect task.
	bound, _ := taskapi.BindCaller(context.Background(), apiVerifier{func() (taskapi.VerifiedContext, error) {
		return taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + p.ID, SourceSessionID: p.ID, SourceResumeID: p.ResumeID(), SourceConfigKnown: true, SourceConfigRevision: p.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool", SourceRequestID: p.ActiveQueuedID(), ToolCallID: "synthetic-recover", InvocationGeneration: "synthetic-recover", Transport: "native_dynamic_tool"}, nil
	}}, taskapi.Invocation{})
	bc, _ := bound.Check(context.Background())
	saved, found, err := h.delegations.ContinuityCommand(context.Background(), p.ID, "synthetic-original-owner-command")
	if err != nil || !found || saved.EffectTaskID != intent.TaskID {
		t.Fatal("effect locator missing", saved, err)
	}
	if _, err := h.recoverFormalOwner(context.Background(), bc, p, saved); err != nil {
		t.Fatal(err)
	}
	bad := in
	bad.ExpectedPreviousID = newRound
	bad.ExpectedConfigRevision++
	if denied := invoke("synthetic-wrong-target-cfg", bad); denied.OK {
		t.Fatal("target cfg bypass")
	}
	bad = in
	if denied := invoke("synthetic-stale-round", bad); denied.OK {
		t.Fatal("predecessor bypass")
	}
	c.Validate = func() bool { return false }
	bad.ExpectedPreviousID = newRound
	if denied := invoke("synthetic-revoked-source", bad); denied.OK || denied.Error.Code != "caller_unbound" {
		t.Fatal("revocation bypass", denied)
	}
}

func TestExtensionImmutableFinalBytesVersionAndMembership(t *testing.T) {
	h, _, c, intent, r := extensionFormal(t)
	seal := correctionSeal(t, h, r, false)
	ctx := context.Background()
	offset := int64(0)
	joined := []byte{}
	call := func(version, task, artifactID, hash string, offset int64) taskapi.Response {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": version, "correlation_id": fmt.Sprint("synthetic-bytes-", offset), "operation": "read_artifact_bytes", "input": map[string]any{"task_id": task, "seal_id": seal.ID, "artifact_id": artifactID, "expected_hash": hash, "offset": offset, "limit": 3}})
		return h.ExecuteTask(ctx, c, raw)
	}
	for {
		response := call(artifact.Version, intent.TaskID, "result.txt", seal.Hash, offset)
		if !response.OK {
			t.Fatal(response.Error)
		}
		raw, _ := json.Marshal(response)
		if err := artifact.Validate(raw, "Response"); err != nil {
			t.Fatal(err)
		}
		b := response.Result.(map[string]any)
		chunk, err := base64.StdEncoding.DecodeString(b["data_base64"].(string))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(chunk)
		if hex.EncodeToString(digest[:]) != b["chunk_hash"] {
			t.Fatal("chunk corrupt")
		}
		joined = append(joined, chunk...)
		offset = b["next_offset"].(int64)
		if !b["has_more"].(bool) {
			break
		}
	}
	if string(joined) != seal.Text {
		t.Fatal("Unicode binary byte range corrupt")
	}
	for _, test := range []struct{ version, task, id, hash string }{{contract.Version, intent.TaskID, "result.txt", seal.Hash}, {"1.0.0-rc.2", intent.TaskID, "result.txt", seal.Hash}, {artifact.Version, intent.TaskID, "/arbitrary/file", seal.Hash}, {artifact.Version, apiID("foreign", "delegation", r.ID), "result.txt", seal.Hash}, {artifact.Version, intent.TaskID, "result.txt", fmt.Sprintf("%064d", 0)}} {
		if result := call(test.version, test.task, test.id, test.hash, 0); result.OK {
			t.Fatal("foreign/version/path/hash accepted", test)
		}
	}
	wrongSealRaw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": artifact.Version, "correlation_id": "synthetic-wrong-seal", "operation": "read_artifact_bytes", "input": map[string]any{"task_id": intent.TaskID, "seal_id": "foreign-seal", "artifact_id": "result.txt", "expected_hash": seal.Hash, "offset": 0, "limit": 3}})
	if result := h.ExecuteTask(ctx, c, wrongSealRaw); result.OK {
		t.Fatal("wrong immutable seal accepted")
	}
	child, _ := h.registry.Get(r.ChildSessionID)
	source := c
	c.Session = child
	c.ThreadID = child.ResumeID()
	c.RequestID = child.ActiveQueuedID()
	if result := call(artifact.Version, intent.TaskID, "result.txt", seal.Hash, 0); result.OK {
		t.Fatal("worker became artifact source")
	}
	c = source
	c.Validate = func() bool { return false }
	if result := call(artifact.Version, intent.TaskID, "result.txt", seal.Hash, 0); result.OK {
		t.Fatal("revoked source read bytes")
	}
}

func TestExtensionNativeStructuredCaptureFiltersAndNormalization(t *testing.T) {
	h, p, c, intent, r := extensionFormal(t)
	seal := correctionSeal(t, h, r, false)
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.SetNativeResponseCapture(&NativeResponseCapture{Directory: dir, SourceSession: p.ID, SourceThread: p.ResumeID(), SourceRevision: p.SettingsSnapshot().ConfigRevision, Tasks: map[string]string{intent.TaskID: seal.Hash}})
	op := "read_result"
	text := seal.Text
	result := map[string]any{"task_id": intent.TaskID, "seal_id": seal.ID, "final_hash": seal.Hash, "offset": 0, "next_offset": len([]rune(text)), "has_more": false, "terminal_status": "succeeded", "text": text, "provider_token": "must-not-capture", "private_reasoning": "must-not-capture"}
	response := taskapi.Response{Type: "task_api_response", Version: contract.Version, CorrelationID: "synthetic-owned-read", Operation: &op, OK: true, Result: result}
	raw, _ := json.Marshal(taskapi.Request{Type: "task_api_request", Version: contract.Version, CorrelationID: response.CorrelationID, Operation: op, Input: json.RawMessage(`{}`)})
	if err := h.CaptureTaskResponse(context.Background(), c, raw, response); err != nil {
		t.Fatal(err)
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 1 {
		t.Fatal("capture missing")
	}
	data, _ := os.ReadFile(filepath.Join(dir, files[0].Name()))
	var captured map[string]any
	json.Unmarshal(data, &captured)
	if captured["public_result"].(map[string]any)["text"] != text || captured["native_call_id"] != c.CallID || captured["raw_wire"] != false {
		t.Fatal("response normalized/misattributed")
	}
	if _, found := captured["public_result"].(map[string]any)["provider_token"]; found {
		t.Fatal("token captured")
	}
	result["task_id"] = "foreign"
	response.CorrelationID = "foreign-read"
	c.CallID = "foreign-call"
	h.CaptureTaskResponse(context.Background(), c, raw, response)
	result["task_id"] = intent.TaskID
	result["final_hash"] = "wrong"
	h.CaptureTaskResponse(context.Background(), c, raw, response)
	c.Validate = func() bool { return false }
	result["final_hash"] = seal.Hash
	h.CaptureTaskResponse(context.Background(), c, raw, response)
	files, _ = os.ReadDir(dir)
	if len(files) != 1 {
		t.Fatal("foreign/hash/revocation captured")
	}
}

// Synthetic authoritative provider oracle for isolated handler/store tests only.
type extensionClaudeProof struct{ thread, text, id string }

func (p *extensionClaudeProof) LoadHistory(string, history.Opts) (*history.Result, error) {
	return &history.Result{Kind: "snapshot", Messages: []map[string]any{{"source_message_id": p.id, "history_read_result_verified": true, "content": p.text}}}, nil
}
func (p *extensionClaudeProof) ResumableSessions(int) ([]history.ResumableSession, error) {
	return nil, nil
}
func (p *extensionClaudeProof) NativeTaskFinalAnchor(thread, message, text string) (string, bool) {
	return p.id, thread == p.thread && message == "synthetic-message-id" && text == p.text
}
func (f *extensionExec) ProviderFor(s *session.Session) (backend.HistoryProvider, bool) {
	if s.Backend() == backend.Claude && f.claudeProof != nil {
		return f.claudeProof, true
	}
	return f.fakeExec.ProviderFor(s)
}
func TestExtensionClaudeOriginalOwnerSameTaskThread(t *testing.T) {
	h, p, c, intent, r := extensionFormalBackend(t, backend.Claude)
	if _, _, known := h.messageQueue.NativeExecution(r.ChildSessionID, r.ChildRequestID); known {
		t.Fatal("Claude tuple fabricated")
	}
	in := continuityInput(r, p, h, "ASCII original task followup")
	raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-claude-continuation", "operation": "append", "idempotency_key": "synthetic-claude-continuation", "input": map[string]any{"continuity": map[string]any{"action": "continue", "arguments": in}}})
	c.CallID = "synthetic-claude-original-call"
	response := h.ExecuteTask(context.Background(), c, raw)
	if !response.OK {
		t.Fatal(response.Error)
	}
	data := response.Result.(map[string]any)["continuity"].(map[string]any)["data"].(map[string]any)
	if data["logical_task_id"] != r.ID || data["child_session_id"] != r.ChildSessionID {
		t.Fatal("Claude task replaced")
	}
	command, _, _ := h.delegations.ContinuityCommand(context.Background(), p.ID, "synthetic-claude-continuation")
	if command.EffectTaskID != intent.TaskID {
		t.Fatal("canonical task changed")
	}
	child, _ := h.registry.Get(r.ChildSessionID)
	if child.ResumeID() != in.ExpectedThreadID || child.Backend() != backend.Claude {
		t.Fatal("native context changed")
	}
}
