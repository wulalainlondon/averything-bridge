package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/executor"
	"everything-go/internal/executor/goexec"
	"everything-go/internal/governance"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTaskAPILiveNativeInvocation(t *testing.T) {
	inputPath := os.Getenv("BRIDGE_TASK_API_LIVE_INPUT")
	if inputPath == "" {
		t.Skip("explicit isolated fresh persisted IDs required")
	}
	raw, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	var input struct{ Backend, Model, Effort, SessionID, RequestID, Workspace, Report, WorkerBackend, ProbeKey string }
	if json.Unmarshal(raw, &input) != nil || input.RequestID == "" || input.SessionID == "" || input.Workspace == "" {
		t.Fatal("invalid persisted probe context")
	}
	// A persisted probe is single-use. Never reopen an uncertain/accepted DB.
	if _, err := os.Stat(filepath.Join(input.Workspace, "message_queue.sqlite")); err == nil || !os.IsNotExist(err) {
		t.Fatal("probe workspace already admitted or unavailable; inspect original receipt, no replay")
	}
	reg := session.NewRegistry()
	reg.AttachStore(session.NewStore(filepath.Join(input.Workspace, "sessions.json")))
	h := NewHub(reg, Config{InstanceID: "task-api-live", RootDir: input.Workspace, DataDir: input.Workspace}, governance.NewPairing(filepath.Join(input.Workspace, "pairing.json")), 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	defer h.dispatches.Close()
	observed := &taskLiveEvidenceSink{hub: h, init: map[string]map[string]any{}}
	sink := executor.NewTerminalSink(observed)
	codex := goexec.NewCodex(sink, "codex")
	codex.SetDataDir(input.Workspace)
	codex.SetTaskAPIProvider(h)
	claude := goexec.NewClaude(sink, "claude")
	claude.SetTaskAPIProvider(h)
	mux := executor.NewReliableMux(map[string]executor.Executor{backend.Codex: codex, backend.Claude: claude}, codex, sink)
	if err := codex.ConnectExistingDaemon(); err != nil {
		t.Fatal("existing daemon unavailable; no daemon start attempted")
	}
	h.SetExecutor(mux)
	source := reg.Create(input.SessionID, "Isolated Task API native probe", input.Workspace, input.Backend, input.Model, "read-only", "")
	source.SetEffort(input.Effort)
	if err = reg.PersistDurably(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	defer func() {
		for _, s := range reg.List() {
			mux.Close(context.Background(), s)
		}
	}()
	prompt := `This is an isolated Bridge Task API invocation probe. Call bridge_tasks task_capabilities exactly once. Its complete arguments must be {"type":"task_api_request","api_version":"1.0.0-rc.1","correlation_id":"native-capability-probe","operation":"capabilities","input":{}}. Use the registered task_capabilities tool, not shell or prose. After it returns, reply exactly TASK_API_NATIVE_OK. Do not use any other tool, change files, create workers, or affect another session.`
	if input.WorkerBackend != "" {
		if input.ProbeKey == "" {
			t.Fatal("persistent worker probe key required")
		}
		inputs := filepath.Join(input.Workspace, "inputs")
		if err = os.MkdirAll(inputs, 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(inputs, "probe.txt"), []byte("ISOLATED_TASK_API_INPUT_OK"), 0600); err != nil {
			t.Fatal(err)
		}
		workerModel := "gpt-6.1-sol"
		if input.WorkerBackend == "claude" {
			workerModel = "sonnet"
		}
		frame := map[string]any{"type": "task_api_request", "api_version": "1.0.0-rc.1", "correlation_id": "isolated-worker-create", "operation": "create_dispatch", "idempotency_key": input.ProbeKey, "input": map[string]any{"goal": "Verify isolated bounded Task API worker", "instruction": `This is an isolated bounded worker probe. Use task_read_input exactly once with {"type":"task_api_request","api_version":"1.0.0-rc.1","correlation_id":"bounded-read","operation":"read_input","input":{"relative_path":"inputs/probe.txt"}}. Reply exactly the returned file text. Use no other tool.`, "scope": map[string]any{"workspace_roots": []string{inputs}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "acceptance": []any{}, "dependencies": []any{}, "workspace": map[string]any{"cwd": input.Workspace, "input_artifact_ids": []any{}}, "new_worker": map[string]any{"name": "Isolated bounded native worker", "profile": map[string]string{"backend": input.WorkerBackend, "model": workerModel, "effort": "high"}}}}
		encoded, _ := json.Marshal(frame)
		prompt = "This is an isolated Bridge Task API worker probe. Call registered bridge_tasks task_create_dispatch exactly once with these complete arguments: " + string(encoded) + ". No shell, files, other session, or other tool. After its receipt reply exactly TASK_API_WORKER_QUEUED. A later Bridge return is this probe's worker result; acknowledge it without tools."
		h.StartDelegationScheduler(ctx)
	}
	client := &Client{hub: h, deviceID: "isolated-probe", send: make(chan []byte, 128), quit: make(chan struct{}), ctx: ctx}
	if !h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: source.ID, RequestID: input.RequestID, MessagePurpose: "instruction", Content: prompt}) {
		t.Fatal("fresh probe admission refused")
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	terminal := "unknown"
	invoked := false
	workerPassed, deliveryPassed := false, false
	var workerEvidence map[string]any
	var workerRecord taskapi.IntentRecord
	for {
		select {
		case <-ctx.Done():
			terminal = "timeout"
			goto complete
		case <-ticker.C:
			changes, _, e := h.messageQueue.TaskJournal().Changes(ctx, 0, 1000)
			if e != nil {
				t.Fatal(e)
			}
			for _, change := range changes {
				if change.SessionID == source.ID && change.RequestID == input.RequestID && change.Kind == "native_tool_invoked" {
					invoked = true
				}
			}
			entry, found, e := h.messageQueue.Get(source.ID, input.RequestID)
			if e != nil {
				t.Fatal(e)
			}
			if input.WorkerBackend != "" && found && entry.State == "completed" {
				records, _ := h.delegations.TaskJournal().List(ctx, "session:"+source.ID, 0)
				if len(records) == 0 && !invoked {
					terminal = "pre_execution_refusal"
					goto complete
				}
				if len(records) == 1 {
					workerRecord = records[0]
					workerEvidence, _ = h.apiTask(ctx, workerRecord)
					if workerEvidence != nil {
						axes := workerEvidence["axes"].(map[string]string)
						encoded, _ := json.Marshal(workerEvidence)
						compiler, _ := contract.New()
						valid := compiler.Validate(encoded, "Task") == nil
						workerPassed = axes["final"] == "sealed" && valid
						deliveryPassed = axes["delivery"] == "delivered"
					}
				}
				if !workerPassed || !deliveryPassed {
					continue
				}
			}
			if found && (string(entry.State) == "completed" || string(entry.State) == "failed" || string(entry.State) == "uncertain") {
				terminal = string(entry.State)
				goto complete
			}
		}
	}
complete:
	evidence := map[string]any{"request_id": input.RequestID, "session_id": source.ID, "backend": input.Backend, "model": input.Model, "native_conversation_id": source.ResumeID(), "actual_bound_tool_invocation": invoked, "terminal": terminal, "fixture": false, "deployment": false, "device": false}
	if input.WorkerBackend != "" {
		evidence["worker_backend"] = input.WorkerBackend
		evidence["worker_task"] = workerEvidence
		evidence["worker_exact_final_sealed"] = workerPassed
		evidence["source_native_delivery_sealed"] = deliveryPassed
		changes, _, _ := h.messageQueue.TaskJournal().Changes(context.Background(), 0, 1000)
		readInvoked := false
		for _, change := range changes {
			if change.SessionID == workerRecord.SessionID && change.RequestID == workerRecord.RequestID && change.Kind == "native_tool_invoked" {
				readInvoked = true
			}
		}
		evidence["worker_actual_bound_input_tool"] = readInvoked
		workerPassed = workerPassed && readInvoked
	}
	if final, found, e := mux.FinalAnswerForSession(source, input.RequestID); input.Backend == backend.Codex && e == nil && found {
		evidence["source_matching_native_final"] = final
	}
	changes, _, _ := h.messageQueue.TaskJournal().Changes(context.Background(), 0, 1000)
	refusals := []string{}
	for _, change := range changes {
		if change.RequestID == input.RequestID && strings.HasPrefix(change.Kind, "native_tool_refused:") {
			refusals = append(refusals, change.Kind)
		}
	}
	evidence["bound_tool_refusals"] = refusals
	observed.mu.Lock()
	evidence["actual_provider_init_catalogs"] = observed.init
	observed.mu.Unlock()
	if proof, found, _ := h.messageQueue.ProviderEvidence(source.ID, input.RequestID); found {
		evidence["native_provider_evidence"] = proof
	}
	if turn := h.messageQueue.NativeAcceptance(source.ID, input.RequestID, source.ResumeID()); turn != "" {
		evidence["native_turn_id"] = turn
	}
	data, _ := json.MarshalIndent(evidence, "", "  ")
	if input.Report != "" {
		_ = os.WriteFile(input.Report, data, 0600)
	}
	if !invoked || terminal != "completed" || input.WorkerBackend != "" && (!workerPassed || !deliveryPassed) {
		t.Fatalf("native invocation gate not passed: backend=%s invoked=%v terminal=%s (safe evidence report saved)", input.Backend, invoked, terminal)
	}
}

// Public init metadata only, no argv, environment, URL, header or credential.
type taskLiveEvidenceSink struct {
	hub  *Hub
	mu   sync.Mutex
	init map[string]map[string]any
}

func (s *taskLiveEvidenceSink) Emit(event any) {
	if value, ok := event.(backend.SessionInitInfo); ok {
		s.mu.Lock()
		s.init[value.SessionID] = map[string]any{"model": value.Model, "permission_mode": value.PermissionMode, "tools": value.Tools, "mcp_servers": value.MCPServers}
		s.mu.Unlock()
	}
	s.hub.Emit(event)
}
