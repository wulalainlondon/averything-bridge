package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/executor"
	"everything-go/internal/executor/goexec"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInteractionLiveOriginalNativeParentChild(t *testing.T) {
	path := os.Getenv("BRIDGE_INTERACTION_LIVE_INPUT")
	if path == "" {
		t.Skip("explicit single-use isolated native probe required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var in struct{ SessionID, RequestID, Workspace, Report, Key, Backend, Model, CodexBin, ClaudeBin, Mode string }
	if json.Unmarshal(raw, &in) != nil || in.SessionID == "" || in.RequestID == "" || in.Workspace == "" || in.Report == "" || in.Key == "" {
		t.Fatal("persisted probe context required")
	}
	if _, err = os.Stat(filepath.Join(in.Workspace, "message_queue.sqlite")); !os.IsNotExist(err) {
		t.Fatal("original probe already admitted or unavailable; inspect receipt, never replay")
	}
	reg := session.NewRegistry()
	reg.AttachStore(session.NewStore(filepath.Join(in.Workspace, "sessions.json")))
	h := NewHub(reg, Config{InstanceID: "interaction-native-probe", RootDir: in.Workspace, DataDir: in.Workspace}, governance.NewPairing(filepath.Join(in.Workspace, "pairing.json")), 0)
	observed := &interactionProbeEvents{hub: h, finals: map[string]string{}}
	sink := executor.NewTerminalSink(observed)
	codex := goexec.NewCodex(sink, in.CodexBin)
	codex.SetDataDir(in.Workspace)
	capture := &interactionProbeProvider{Hub: h, path: in.Report + ".calls.jsonl", legacyParent: in.Mode == "legacy-after-final", parentID: in.SessionID}
	codex.SetTaskAPIProvider(capture)
	claude := goexec.NewClaude(sink, in.ClaudeBin)
	claude.SetTaskAPIProvider(capture)
	mux := executor.NewReliableMux(map[string]executor.Executor{backend.Codex: codex, backend.Claude: claude}, codex, sink)
	if err = codex.ConnectExistingDaemon(); err != nil {
		t.Fatal("existing daemon unavailable; not starting another daemon", err)
	}
	diagnostics := codex.RuntimeDiagnostics()
	native, _ := diagnostics["codex"].(map[string]any)
	version, _ := native["running_version"].(string)
	if !taskapi.SupportedNativeProviderVersion("codex", version) {
		t.Fatalf("qualified native runtime missing: %v", diagnostics)
	}
	h.SetExecutor(mux)
	parent := reg.Create(in.SessionID, "Isolated parent interaction probe", in.Workspace, in.Backend, in.Model, "read-only", "")
	parent.SetEffort("high")
	if tools, e := h.TaskTools(parent); e != nil || len(tools) < 2 {
		t.Fatalf("probe tool registration unavailable: %v", e)
	}
	if err = reg.PersistDurably(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	defer func() {
		for _, s := range reg.List() {
			_ = mux.Close(context.Background(), s)
		}
	}()
	inputs := filepath.Join(in.Workspace, "inputs")
	if err = os.MkdirAll(inputs, 0700); err != nil {
		t.Fatal(err)
	}
	childPrompt := `Isolated native parent/child clarification probe. Use only bridge_tasks task_interactions. First call with {"type":"task_api_request","api_version":"1.0.0-rc.5","operation":"interactions","correlation_id":"child-ask","idempotency_key":"child-ask-original","input":{"action":"ask_parent","question":"Which synthetic fixture should this probe use?","kind":"task_clarification","blocking":true,"wait_ms":30000}}. Keep the returned interaction_id. If pending, call action wait for that exact ID with wait_ms=30000, at most twice. Once the answer is received, require answer_actor=parent_agent and human_authorization=false, call action ack with the exact returned ack_token and same interaction_id, then final exactly INTERACTION_NATIVE_CHILD_CONFIRMED. No file, shell, network or other work. If still pending after two waits, final WAITING_ORIGINAL_ANSWER; an exact Bridge answer continuation may resume only this probe.`
	if in.Mode == "legacy-after-final" {
		childPrompt = strings.Replace(childPrompt, `"blocking":true,"wait_ms":30000`, `"blocking":false,"wait_ms":0`, 1)
		childPrompt = strings.Replace(childPrompt, "Keep the returned interaction_id. If pending, call action wait for that exact ID with wait_ms=30000, at most twice.", "Keep the returned interaction_id. Immediately final WAITING_ORIGINAL_ANSWER without waiting, even if an answer is already present. When the server's exact answer continuation arrives, get only that question and follow the acknowledgment procedure below.", 1)
	}
	frame := map[string]any{"type": "task_api_request", "api_version": "1.0.0-rc.1", "correlation_id": "probe-create", "operation": "create_dispatch", "idempotency_key": in.Key, "input": map[string]any{"goal": "Isolated native question/answer receipt probe", "instruction": childPrompt, "scope": map[string]any{"workspace_roots": []string{inputs}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "acceptance": []any{}, "dependencies": []any{}, "workspace": map[string]any{"cwd": in.Workspace, "input_artifact_ids": []any{}}, "new_worker": map[string]any{"name": "Isolated clarification worker", "profile": map[string]string{"backend": in.Backend, "model": in.Model, "effort": "high"}}}}
	data, _ := json.Marshal(frame)
	prompt := "Isolated Bridge interaction probe, authorized only for this synthetic fixture. Call registered bridge_tasks task_create_dispatch exactly once with these complete arguments: " + string(data) + ". Once accepted, final PARENT_PROBE_WAITING. Do not poll or use other tools now. Later a Bridge notification for this child's question will arrive in this same conversation. Read that exact interaction via task_interactions get (or its supplied original task_get recipe), answer fixture-alpha using task_interactions answer with expected_revision from the question and a single retained idempotency_key. This is agent clarification, never human authorization. Then final PARENT_ANSWER_ACCEPTED. Do not create a second task, use shell/network/files, or affect any other session. On final child return acknowledge without any tool."
	if in.Mode == "legacy-after-final" {
		prompt += " This parent deliberately has only the existing task_get/task_append catalog. Use ONLY the supplied task_get ti5: question ID and task_append exact answer recipe; do not call task_interactions from the parent."
	}
	h.StartDelegationScheduler(ctx)
	client := &Client{hub: h, deviceID: "isolated-probe", send: make(chan []byte, 256), quit: make(chan struct{}), ctx: ctx}
	if !h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: parent.ID, RequestID: in.RequestID, MessagePurpose: "instruction", Content: prompt}) {
		t.Fatal("isolated probe refused before admission")
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	passed := false
	reason := "timeout"
	evidence := map[string]any{"fixture": false, "isolated_probe": true, "deployment": false, "backend": in.Backend, "parent_session": in.SessionID, "parent_request": in.RequestID}
	for !passed {
		select {
		case <-ctx.Done():
			goto finish
		case <-ticker.C:
			rows, e := h.interactions.List(ctx, parent.ID, 100)
			if e != nil {
				reason = e.Error()
				goto finish
			}
			if len(rows) > 0 {
				evidence["questions"] = rows
			}
			for _, q := range rows {
				if q.State == "resolved" && q.Actor == "parent_agent" && q.Answer == "fixture-alpha" && q.NativeCall != "" && q.AcknowledgedBy != "" {
					child, ok := reg.Get(q.Binding.Child)
					if !ok {
						continue
					}
					finalRequest := q.ProducerRequest
					if in.Mode == "legacy-after-final" {
						finalRequest = "tianswer_" + q.ID
					}
					final, found, e := mux.FinalAnswerForSession(child, finalRequest)
					if in.Backend == backend.Claude {
						final, found = observed.final(child.ID, q.ProducerRequest)
						e = nil
					}
					if e == nil && found && strings.TrimSpace(final) == "INTERACTION_NATIVE_CHILD_CONFIRMED" {
						record, ok, err := h.delegations.ByChildRequest(ctx, q.Binding.Child, q.Binding.ChildRequest)
						if err != nil || !ok {
							continue
						}
						evidence["delivery_state"] = record.DeliveryState
						evidence["parent_return_request"] = record.ParentRequestID
						if record.DeliveryState != "delivered" {
							continue
						}
						returned, ready, err := mux.FinalAnswerForSession(parent, record.ParentRequestID)
						if in.Backend == backend.Claude {
							returned, ready = observed.final(parent.ID, record.ParentRequestID)
							err = nil
						}
						if err != nil || !ready || strings.TrimSpace(returned) == "" {
							continue
						}
						evidence["parent_return_final"] = returned
						evidence["mode"] = in.Mode
						if in.Mode == "legacy-after-final" {
							originalFinal, originalFound, err := mux.FinalAnswerForSession(child, q.ProducerRequest)
							if err != nil || !originalFound || strings.TrimSpace(originalFinal) != "WAITING_ORIGINAL_ANSWER" {
								continue
							}
							evidence["original_child_final"] = originalFinal
							evidence["answer_continuation_request"] = finalRequest
							evidence["answer_continuation_final"] = final
						}
						passed = true
						reason = "native_question_answer_ack_child_final_and_parent_delivery"
						break
					}
				}
			}
			entry, found, _ := h.messageQueue.Get(parent.ID, in.RequestID)
			capture.mu.Lock()
			refusal := capture.refusal
			capture.mu.Unlock()
			if found && entry.State == "completed" && refusal != "" {
				reason = refusal
				goto finish
			}
			if found && (entry.State == "failed" || entry.State == "uncertain") {
				reason = string(entry.State)
				goto finish
			}
		}
	}
finish:
	evidence["passed"] = passed
	evidence["reason"] = reason
	evidence["parent_thread"] = parent.ResumeID()
	final, found, finalErr := mux.FinalAnswerForSession(parent, in.RequestID)
	evidence["parent_final"] = final
	evidence["parent_final_found"] = found
	if finalErr != nil {
		evidence["parent_final_error"] = finalErr.Error()
	}
	sessions := []map[string]any{}
	for _, s := range reg.List() {
		sessions = append(sessions, map[string]any{"session_id": s.ID, "thread_id": s.ResumeID(), "active_request": s.ActiveQueuedID()})
	}
	evidence["sessions"] = sessions
	observed.mu.Lock()
	evidence["native_errors"] = observed.errors
	observed.mu.Unlock()
	if entry, found, e := h.messageQueue.Get(parent.ID, in.RequestID); e == nil && found {
		evidence["parent_receipt_state"] = entry.State
		evidence["parent_receipt_message"] = entry.Message
	}
	output, _ := json.MarshalIndent(evidence, "", "  ")
	if e := os.WriteFile(in.Report, output, 0600); e != nil {
		t.Fatal(e)
	}
	if !passed {
		t.Fatalf("native interaction probe did not pass: %s; original workspace retained", reason)
	}
}

type interactionProbeProvider struct {
	*Hub
	mu           sync.Mutex
	path         string
	refusal      string
	legacyParent bool
	parentID     string
}

func (p *interactionProbeProvider) TaskTools(s *session.Session) ([]map[string]any, error) {
	tools, err := p.Hub.TaskTools(s)
	if !p.legacyParent || s.ID != p.parentID {
		return tools, err
	}
	out := []map[string]any{}
	for _, tool := range tools {
		if tool["name"] != "task_interactions" {
			out = append(out, tool)
		}
	}
	return out, err
}

func (p *interactionProbeProvider) ExecuteTask(ctx context.Context, c backend.TaskCaller, raw []byte) taskapi.Response {
	registered, exists := p.registry.Get(c.Session.ID)
	before := map[string]any{"provider": c.ProviderVersion, "session": c.Session.ID, "request": c.RequestID, "active_request": c.Session.ActiveQueuedID(), "thread": c.ThreadID, "current_thread": c.Session.ResumeID(), "turn": c.TurnID, "call": c.CallID, "valid": c.Validate != nil && c.Validate(), "registered": exists && registered == c.Session, "may_write": p.controls.MobileMayWrite(c.Session.ID)}
	result := p.Hub.ExecuteTask(ctx, c, raw)
	before["response"] = result
	before["request_payload"] = json.RawMessage(raw)
	b, _ := json.Marshal(before)
	p.mu.Lock()
	defer p.mu.Unlock()
	if !result.OK && result.Error != nil && result.Error.Acceptance == "known_none" {
		p.refusal = result.Error.Code
	}
	f, e := os.OpenFile(p.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e == nil {
		_, _ = f.Write(append(b, '\n'))
		_ = f.Sync()
		_ = f.Close()
	}
	return result
}

type interactionProbeEvents struct {
	hub    *Hub
	mu     sync.Mutex
	finals map[string]string
	errors []map[string]string
}

func (s *interactionProbeEvents) final(session, request string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	text, ok := s.finals[session+":"+request]
	return text, ok
}
func (s *interactionProbeEvents) Emit(event any) {
	s.mu.Lock()
	if f, ok := event.(backend.CompletedAnswer); ok {
		s.finals[f.SessionID+":"+f.RequestID] = f.Text
	}
	if e, ok := event.(protocol.Error); ok {
		s.errors = append(s.errors, map[string]string{"session": e.SessionID, "request": e.RequestID, "code": e.Code, "message": e.Message})
	}
	s.mu.Unlock()
	s.hub.Emit(event)
}
