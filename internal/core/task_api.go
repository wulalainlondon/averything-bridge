package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	artifactcontract "everything-go/contracts/task-api/artifacts-v4"
	interactioncontract "everything-go/contracts/task-api/interactions-v5"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/coordination"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
)

type apiInput struct {
	ExpectedPreviousRoundID    string `json:"expected_previous_round_id"`
	ReturnToParent             bool   `json:"return_to_parent"`
	IncludeRelationCoverage    bool   `json:"include_relation_coverage"`
	TaskID                     string `json:"task_id"`
	SessionID                  string `json:"session_id"`
	Goal, Instruction, Content string
	Existing                   *taskapi.TargetIdentity `json:"existing_target"`
	New                        *struct {
		Name    string
		Profile struct{ Backend, Model, Effort string }
	} `json:"new_worker"`
	Scope struct {
		Roots            []string `json:"workspace_roots"`
		Operations       []string `json:"allowed_operations"`
		Sandbox, Network string
		MaxChildren      int `json:"max_children"`
	}
	Workspace struct {
		Cwd    string
		Inputs []string `json:"input_artifact_ids"`
	}
	Mode           string
	Revision       uint64           `json:"expected_revision"`
	Original       *taskapi.Locator `json:"original_receipt"`
	CommandReceipt string           `json:"command_receipt_id"`
	Path           string
	Views          []string
	Cursor         string
	Limit          int
	SealID         string `json:"seal_id"`
	Offset         int
	Dependencies   []json.RawMessage
	ApprovedPM     json.RawMessage `json:"approved_pm_task_ref"`
}
type apiMetadata struct {
	WorkspaceRoomID string
	WorkspaceTaskID string
	CanonicalRoots  []string
	CanonicalCwd    string
	RootIdentities  map[string]string
	Request         taskapi.Request
	Caller          taskapi.VerifiedContext
	SourceThread    string
	SourceRevision  uint64
	Transport       string
	Intake          *intakeMetadata
}
type apiSeal struct{ Backend, SessionID, RequestID, ThreadID, TurnID, MessageID, Text, Hash, Status string }

func (h *Hub) initializeTaskAPI() {
	if h.messageQueue == nil {
		return
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return
	}
	h.taskCursor, _ = taskapi.NewCursorCodec(key, nil)
	h.taskSeals = map[string]apiSeal{}
	h.taskService, _ = taskapi.NewService(h, h)
}
func apiID(authority, path, native string) string {
	raw, _ := json.Marshal([]string{authority, path, native})
	return "ta1:" + base64.RawURLEncoding.EncodeToString(raw)
}
func apiRef(id string) ([]string, error) {
	raw, e := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(id, "ta1:"))
	if e != nil || !strings.HasPrefix(id, "ta1:") {
		return nil, taskapi.Failure("not_found", "known_none", "correct_input")
	}
	var a []string
	e = json.Unmarshal(raw, &a)
	if e != nil || len(a) != 3 {
		return nil, taskapi.Failure("not_found", "known_none", "correct_input")
	}
	return a, nil
}
func (h *Hub) apiJournals() map[string]*taskapi.Journal {
	m := map[string]*taskapi.Journal{"ordinary": h.messageQueue.TaskJournal()}
	if h.dispatches != nil {
		m["controller"] = h.dispatches.TaskJournal()
	}
	if h.delegations != nil {
		m["delegation"] = h.delegations.TaskJournal()
	}
	if h.work != nil {
		m["pm_v1"] = h.work.TaskJournal()
		m["pm_v2"] = h.work.TaskJournal()
	}
	return m
}

type apiVerifier struct {
	verify func() (taskapi.VerifiedContext, error)
}

func (v apiVerifier) Verify(context.Context, taskapi.Invocation) (taskapi.VerifiedContext, error) {
	return v.verify()
}
func (h *Hub) taskHuman(c *Client) (taskapi.BoundCaller, error) {
	return taskapi.BindCaller(c.ctx, apiVerifier{func() (taskapi.VerifiedContext, error) {
		device := h.pairedTaskDevice(c)
		if device == "" || !c.live() {
			return taskapi.VerifiedContext{}, taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		}
		proof := c.readIdentity.Load()
		digest := sha256.Sum256([]byte(proof.token))
		generation := binary.BigEndian.Uint64(digest[:8]) & ((1 << 53) - 1)
		return taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:" + device, NamespaceGeneration: generation, InvocationGeneration: c.clientID, Transport: "authenticated_ws", BindingKind: "paired_human"}, nil
	}}, taskapi.Invocation{})
}
func (h *Hub) TaskTools(s *session.Session) ([]map[string]any, error) {
	if h.taskService == nil || s == nil || !h.controllerInScope(s) || s.Snapshot().Hidden {
		return nil, errors.New("task API unavailable")
	}
	if p, e := h.PMConfiguration(s.ID); e != nil || p != nil {
		return nil, errors.New("original PM toolset retained")
	}
	scope, err := h.TaskWorkerScope(s)
	if err != nil {
		return nil, err
	}
	defs, err := contract.ToolInputs()
	if err != nil {
		return nil, err
	}
	extra, err := artifactcontract.CompatibleToolInputs()
	if err != nil {
		return nil, err
	}
	for name, input := range extra {
		defs[name] = input
	}

	legacyWorker := false
	if h.delegations != nil && scope == nil {
		legacyWorker, err = h.delegations.HasChild(context.Background(), s.ID)
		if err != nil {
			return nil, err
		}
	}
	tools := []map[string]any{}
	if h.interactions != nil {
		tools = append(tools, map[string]any{"type": "function", "name": "task_interactions", "description": "Original-task questions. ask_parent requests clarification; list/get/wait read exact questions; answer/escalate only for authorized original parent or human; ack confirms the child consumed the exact answer. Use api_version 1.0.0-rc.5 and operation interactions. Agent answers are never human authorization.", "inputSchema": interactioncontract.ToolInput()})
	}
	for name, schema := range defs {
		if legacyWorker && name != "task_capabilities" {
			continue
		}
		if scope != nil && name != "task_read_input" && name != "task_capabilities" {
			continue
		}
		tools = append(tools, map[string]any{"type": "function", "name": name, "description": "Authorized Bridge Task API. Native consumption, final, QA and delivery are independent evidence axes. Never replay unknown acceptance.", "inputSchema": schema})
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i]["name"].(string) < tools[j]["name"].(string) })
	return tools, nil
}
func (h *Hub) ExecuteTask(ctx context.Context, c backend.TaskCaller, raw []byte) taskapi.Response {
	verifier := apiVerifier{func() (taskapi.VerifiedContext, error) {
		s, ok := h.registry.Get(c.Session.ID)
		if !ok || s != c.Session || c.Validate == nil || !c.Validate() || s.ActiveQueuedID() != c.RequestID || s.ResumeID() != c.ThreadID || s.State() == session.Closed || s.Snapshot().Hidden || !h.controls.MobileMayWrite(s.ID) {
			return taskapi.VerifiedContext{}, taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		}
		if s.Backend() == backend.Codex && (!taskapi.SupportedNativeProviderVersion("codex", c.ProviderVersion) || c.ProcessGeneration != "" || c.TurnID == "") || s.Backend() == backend.Claude && (!taskapi.SupportedNativeProviderVersion("claude", c.ProviderVersion) || c.ProcessGeneration == "" || c.TurnID != "") {
			return taskapi.VerifiedContext{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		snapshot := s.Snapshot()
		if entry, found, e := h.messageQueue.Get(s.ID, c.RequestID); e == nil && found {
			var admission queuedPayload
			if json.Unmarshal(entry.Payload, &admission) == nil && admission.Configuration != nil {
				snapshot.ConfigRevision = admission.Configuration.Revision
			}
		}
		return taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + s.ID, NamespaceGeneration: 0, InvocationGeneration: c.ProcessGeneration + c.TurnID + c.CallID, NativeTurnID: c.TurnID, ToolCallID: c.CallID, ProcessGeneration: c.ProcessGeneration, SourceSessionID: s.ID, SourceRequestID: c.RequestID, SourceResumeID: c.ThreadID, SourceConfigRevision: snapshot.ConfigRevision, SourceConfigKnown: true, Transport: func() string {
			if c.ProcessGeneration != "" {
				return "authenticated_mcp"
			}
			return "native_dynamic_tool"
		}(), BindingKind: func() string {
			if c.ProcessGeneration != "" {
				return "mcp_process_binding"
			}
			return "native_tool"
		}()}, nil
	}}
	if c.Session == nil {
		verifier.verify = func() (taskapi.VerifiedContext, error) {
			return taskapi.VerifiedContext{}, taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		}
	}
	caller, bindErr := taskapi.BindCaller(ctx, verifier, taskapi.Invocation{})
	response, continuityHandled := h.executeInteractions(ctx, caller, raw)
	if !continuityHandled {
		response, continuityHandled = h.executeArtifactBytes(ctx, caller, raw)
	}
	if !continuityHandled {
		response, continuityHandled = h.executeNativeContinuity(ctx, caller, c.Session, raw)
	}
	if !continuityHandled {
		response = h.taskService.Execute(ctx, caller, raw)
	}
	if bindErr == nil {
		kind := "native_tool_invoked"
		if !response.OK {
			kind = "native_tool_refused"
			if response.Error != nil {
				kind += ":" + response.Error.Code
			}
		}
		_ = h.messageQueue.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error { return taskapi.ChangeTx(tx, c.Session.ID, c.RequestID, kind) })
	}
	return response
}
func (h *Hub) handleTaskAPI(ctx context.Context, c *Client, raw []byte) {
	if h.taskService == nil {
		return
	}
	caller, _ := h.taskHuman(c)
	response, handled := h.executeInteractions(ctx, caller, raw)
	if !handled {
		response, handled = h.executeArtifactBytes(ctx, caller, raw)
	}
	if !handled {
		response = h.taskService.Execute(ctx, caller, raw)
	}
	c.enqueueEvent(response)
}
func (h *Hub) Authorize(ctx context.Context, c taskapi.VerifiedContext, r taskapi.Request) (taskapi.Locator, error) {
	if c.SourceSessionID != "" && (h.workspaceReservedSession(c.SourceSessionID) || h.workspaceManagedRequest(c.SourceSessionID, c.SourceRequestID)) {
		return taskapi.Locator{}, taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	var in apiInput
	if json.Unmarshal(r.Input, &in) != nil {
		return taskapi.Locator{}, taskapi.Failure("invalid_argument", "known_none", "correct_input")
	}
	l := taskapi.Locator{Authority: h.cfg.InstanceID, Path: "ordinary", Operation: r.Operation, Key: r.IdempotencyKey}
	if c.SourceSessionID != "" {
		if source, ok := h.registry.Get(c.SourceSessionID); ok {
			worker, e := h.TaskWorkerScope(source)
			legacyWorker := false
			if h.delegations != nil && worker == nil {
				legacyWorker, e = h.delegations.HasChild(ctx, source.ID)
			}
			if e != nil {
				return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
			}
			if (legacyWorker && r.Operation != "capabilities") || (worker != nil && r.Operation != "read_input" && r.Operation != "capabilities") {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
		}
	}
	if c.Authority != h.cfg.InstanceID {
		return l, taskapi.Failure("wrong_authority", "known_none", "refresh_identity")
	}
	if r.Operation == "intake" {
		return h.authorizeIntake(ctx, c, r, l)
	}
	if r.Operation == "legacy_source" {
		return h.authorizeLegacySource(ctx, c, r, l)
	}
	if r.Operation == "get" && in.Original != nil {
		l = *in.Original
		if l.Authority != h.cfg.InstanceID {
			return l, taskapi.Failure("wrong_authority", "known_none", "refresh_identity")
		}
		if h.apiJournals()[l.Path] == nil {
			return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		return l, nil
	}
	if in.TaskID != "" {
		ref, err := apiRef(in.TaskID)
		if err != nil {
			return l, err
		}
		if ref[0] != h.cfg.InstanceID {
			return l, taskapi.Failure("wrong_authority", "known_none", "refresh_identity")
		}
		l.Path = ref[1]
		if r.Operation == "append" && l.Path != "delegation" && (in.ReturnToParent || in.ExpectedPreviousRoundID != "") {
			return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		l.TaskID = in.TaskID
		record, err := h.apiRecord(ctx, c, in.TaskID)
		var legacy legacySourceMetadata
		json.Unmarshal(record.Metadata, &legacy)
		if legacy.LegacyLinkID != "" && (r.Operation == "append" || r.Operation == "cancel") {
			return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		if err == nil && (r.Operation == "append" || r.Operation == "cancel") && (record.ScopeID != c.StableScopeID || record.Generation != c.NamespaceGeneration) {
			return l, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		if err != nil {
			return l, err
		}
		if r.Operation == "cancel" && l.Path == "delegation" {
			selected, e := h.delegations.TaskJournal().Receipt(ctx, c.StableScopeID, c.NamespaceGeneration, in.CommandReceipt)
			if e != nil {
				return l, e
			}
			if selected.TaskID != record.TaskID {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			if original, found, e := h.delegations.ByChildRequest(ctx, selected.SessionID, selected.RequestID); e != nil {
				return l, e
			} else if found {
				ns := taskapi.Namespace{Authority: c.Authority, StableScopeID: c.StableScopeID, Generation: c.NamespaceGeneration, Path: l.Path, Operation: r.Operation, TaskID: l.TaskID}
				if previous, e := h.delegations.TaskJournal().Lookup(ctx, ns, r.IdempotencyKey); e == nil {
					digest, _ := taskapi.IntentHash(r)
					if digest != previous.Hash {
						return l, taskapi.Failure("idempotency_conflict", "known_receipt", "lookup_original")
					}
					return l, nil
				}
				if original.State == "queued" {
					return l, h.authorizeQueuedDelegationCancel(ctx, c, r, selected, original, in.Revision)
				}
			}
		}
		target, ok := h.registry.Get(record.SessionID)
		if !ok || !h.controllerInScope(target) || target.Snapshot().Hidden {
			return l, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		if r.Operation == "append" || r.Operation == "cancel" {
			ns := taskapi.Namespace{Authority: c.Authority, StableScopeID: c.StableScopeID, Generation: c.NamespaceGeneration, Path: l.Path, Operation: r.Operation, TaskID: l.TaskID}
			if previous, e := h.apiJournals()[l.Path].Lookup(ctx, ns, r.IdempotencyKey); e == nil {
				digest, _ := taskapi.IntentHash(r)
				if previous.Hash != digest {
					return l, taskapi.Failure("idempotency_conflict", "known_receipt", "lookup_original")
				}
				return l, nil
			}

			if !apiOwnedAdmissionEnabled && r.Operation == "append" {
				return l, taskapi.Failure("busy", "known_none", "read_capabilities")
			}
			if !h.controls.MobileMayWrite(target.ID) {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			task, err := h.apiTask(ctx, record)
			if err != nil {
				return l, err
			}
			if task["revision"].(uint64) != in.Revision {
				return l, taskapi.Failure("stale_revision", "known_none", "refresh_identity")
			}
			if in.Existing == nil {
				var wrapper struct{ Target *taskapi.TargetIdentity }
				json.Unmarshal(r.Input, &wrapper)
				in.Existing = wrapper.Target
			}
			if in.Existing == nil || in.Existing.InstanceID != h.cfg.InstanceID || in.Existing.SessionID != target.ID || in.Existing.ResumeID != target.ResumeID() || in.Existing.ConfigRevision != target.SettingsSnapshot().ConfigRevision {
				return l, taskapi.Failure("stale_revision", "known_none", "refresh_identity")
			}
			if r.Operation == "append" {
				var originalMeta apiMetadata
				var originalInput apiInput
				if json.Unmarshal(record.Metadata, &originalMeta) != nil || json.Unmarshal(originalMeta.Request.Input, &originalInput) != nil {
					return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
				}
				if err := h.enforceExistingScope(target, originalInput); err != nil {
					return l, err
				}
				if l.Path == "delegation" && in.ExpectedPreviousRoundID != "" {
					if root, found, e := h.delegations.Round(ctx, record.NativeID); e != nil {
						return l, e
					} else if !found {
						return l, taskapi.Failure("not_found", "known_none", "lookup_original")
					} else {
						latest, e := h.delegations.LatestRound(ctx, root.LogicalID)
						if e != nil {
							return l, e
						}
						if latest.ID != in.ExpectedPreviousRoundID {
							return l, taskapi.Failure("stale_revision", "known_none", "refresh_identity")
						}
					}
				}
			}
			if in.Mode == "steer" {
				return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
			}
			if in.Mode == "active" {
				return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
			}
			if in.Mode == "steer" {
				source, ok := h.registry.Get(c.SourceSessionID)
				if !ok {
					return l, taskapi.Failure("permission", "known_none", "request_scope_change")
				}
				g, e := h.dispatches.Grant(ctx, source.ID)
				if e != nil || !g.Steer {
					return l, taskapi.Failure("permission", "known_none", "request_scope_change")
				}
			}
		}
		return l, nil
	}
	if r.Operation == "create_dispatch" {
		if in.New != nil {
			l.Path = "delegation"
		} else if c.SourceSessionID != "" {
			l.Path = "controller"
		}
		if len(in.ApprovedPM) > 0 {
			var ref struct{ Path string }
			json.Unmarshal(in.ApprovedPM, &ref)
			l.Path = ref.Path
		}
		if j := h.apiJournals()[l.Path]; j != nil {
			ns := taskapi.Namespace{Authority: c.Authority, StableScopeID: c.StableScopeID, Generation: c.NamespaceGeneration, Path: l.Path, Operation: r.Operation}
			if previous, e := j.Lookup(ctx, ns, r.IdempotencyKey); e == nil {
				if e = h.checkAPIRecord(ctx, c, previous); e != nil {
					return l, e
				}
				digest, _ := taskapi.IntentHash(r)
				if digest != previous.Hash {
					return l, taskapi.Failure("idempotency_conflict", "known_receipt", "lookup_original")
				}
				return l, nil
			} else if failure, ok := e.(*taskapi.APIError); ok && failure.Code == "key_expired" {
				return l, e
			}
		}
		if !apiOwnedAdmissionEnabled {
			return l, taskapi.Failure("busy", "known_none", "read_capabilities")
		}
		if len(in.ApprovedPM) > 0 {
			var ref struct {
				Authority string `json:"authority_instance_id"`
				Path      string
				NativeID  string `json:"native_record_id"`
			}
			if json.Unmarshal(in.ApprovedPM, &ref) != nil || ref.Authority != h.cfg.InstanceID || h.work == nil {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			state, err := h.work.Collaboration(ctx)
			if err != nil {
				return l, err
			}
			task, ok := state.Tasks[ref.NativeID]
			if !ok {
				return l, taskapi.Failure("not_found", "known_none", "correct_input")
			}
			project := state.Projects[task.ProjectID]
			target, ok := h.registry.Get(task.SessionID)
			if !ok {
				return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
			}
			if e := h.enforceExistingScope(target, in); e != nil {
				return l, e
			}
			if c.SourceSessionID != "" && c.SourceSessionID != project.PMSessionID {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			if task.Instruction != in.Instruction || task.Sandbox != in.Scope.Sandbox || in.Workspace.Cwd != project.Cwd {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			if project.EngineVersion == 2 && ref.Path != "pm_v2" || project.EngineVersion != 2 && ref.Path != "pm_v1" {
				return l, taskapi.Failure("wrong_authority", "known_none", "correct_input")
			}
			if project.EngineVersion == 2 {
				l.Path = "pm_v2"
			} else {
				l.Path = "pm_v1"
			}
			return l, nil
		}
		if len(in.Dependencies) > 0 {
			return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		if in.New != nil {
			if len(in.Workspace.Inputs) > 0 {
				return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
			}
			l.Path = "delegation"
			parent, ok := h.registry.Get(c.SourceSessionID)
			if !ok {
				return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
			}
			if strings.HasPrefix(parent.ID, "s_dg_") || strings.HasPrefix(parent.ID, "pm_") {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			if p, e := h.PMConfiguration(parent.ID); e != nil || p != nil {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			root := realpath(parent.Snapshot().Cwd)
			if !scopePathWithin(root, in.Workspace.Cwd) || in.Scope.Sandbox != "read-only" || in.Scope.Network != "deny" || in.Scope.MaxChildren != 0 || len(in.Scope.Operations) != 1 || in.Scope.Operations[0] != "read" {
				return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
			}
			for _, p := range in.Scope.Roots {
				if !scopePathWithin(root, p) {
					return l, taskapi.Failure("permission", "known_none", "request_scope_change")
				}
			}
			if in.New.Profile.Backend != "codex" && in.New.Profile.Backend != "claude" {
				return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
			}
			supported := false
			if source, ok := h.exec.(backend.TaskCapabilitySource); ok {
				for _, provider := range source.TaskAPICapabilities() {
					if provider.Backend != in.New.Profile.Backend || provider.Lifecycle == "unsupported" {
						continue
					}
					encoded, _ := json.Marshal(provider.Models)
					var models []struct {
						Model   string
						Efforts []string
					}
					if json.Unmarshal(encoded, &models) != nil {
						continue
					}
					for _, model := range models {
						if model.Model == in.New.Profile.Model {
							for _, effort := range model.Efforts {
								if effort == in.New.Profile.Effort {
									supported = true
								}
							}
						}
					}
				}
			}

			if !supported {
				return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
			}
			return l, nil
		}
		t := in.Existing
		if t == nil {
			return l, taskapi.Failure("invalid_argument", "known_none", "correct_input")
		}
		if t.InstanceID != h.cfg.InstanceID {
			return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		target, ok := h.registry.Get(t.SessionID)
		if !ok || !h.controllerInScope(target) || target.Snapshot().Hidden || target.State() == session.Closed || !h.controls.MobileMayWrite(target.ID) {
			return l, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		if p, e := h.PMConfiguration(target.ID); e != nil || p != nil || strings.HasPrefix(target.ID, "s_dg_") {
			return l, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		if target.Backend() != backend.Codex && target.Backend() != backend.Claude {
			return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		if c.SourceSessionID != "" {
			l.Path = "controller"
		}
		ns := taskapi.Namespace{Authority: c.Authority, StableScopeID: c.StableScopeID, Generation: c.NamespaceGeneration, Path: l.Path, Operation: r.Operation}
		if previous, e := h.apiJournals()[l.Path].Lookup(ctx, ns, r.IdempotencyKey); e == nil {
			if e = h.checkAPIRecord(ctx, c, previous); e != nil {
				return l, e
			}
			digest, _ := taskapi.IntentHash(r)
			if previous.Hash != digest {
				return l, taskapi.Failure("idempotency_conflict", "known_receipt", "lookup_original")
			}
			return l, nil
		}
		if err := h.enforceExistingScope(target, in); err != nil {
			return l, err
		}
		if target.ResumeID() != t.ResumeID || target.SettingsSnapshot().ConfigRevision != t.ConfigRevision {
			return l, taskapi.Failure("stale_revision", "known_none", "refresh_identity")
		}
		if c.SourceSessionID != "" {
			l.Path = "controller"
			source, ok := h.registry.Get(c.SourceSessionID)
			if !ok || source.ID == target.ID {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			if p, e := h.PMConfiguration(source.ID); e != nil || p != nil {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			g, e := h.dispatches.Grant(ctx, source.ID)
			if e != nil || !g.Allows(h.cfg.InstanceID, h.cfg.InstanceID, target.ID) {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
		}
		return l, nil
	}
	if in.SessionID != "" {
		target, ok := h.registry.Get(in.SessionID)
		if !ok || !h.controllerInScope(target) || target.Snapshot().Hidden {
			return l, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
	}
	return l, nil
}
func (h *Hub) apiRecord(ctx context.Context, c taskapi.VerifiedContext, id string) (taskapi.IntentRecord, error) {
	ref, e := apiRef(id)
	if e != nil {
		return taskapi.IntentRecord{}, e
	}
	j := h.apiJournals()[ref[1]]
	if j == nil {
		return taskapi.IntentRecord{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	if ref[1] == "ordinary" {
		links, e := h.messageQueue.LegacySourceLinks(ctx)
		if e != nil {
			return taskapi.IntentRecord{}, e
		}
		for _, link := range links {
			record := h.legacyProjectionRecord(link)
			if record.TaskID == id && h.legacyReadAllowed(c, link) {
				return record, nil
			}
		}
	}
	record, err := j.Task(ctx, c.StableScopeID, c.NamespaceGeneration, id)
	if err != nil && c.BindingKind == "paired_human" {
		record, err = j.FindTask(ctx, id)
	}
	if err != nil {
		return record, err
	}
	var legacy legacySourceMetadata
	json.Unmarshal(record.Metadata, &legacy)
	if legacy.LegacyLinkID != "" {
		return taskapi.IntentRecord{}, taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	if err = h.checkAPIRecord(ctx, c, record); err != nil {
		return record, err
	}
	return record, nil
}
func (h *Hub) Mutate(ctx context.Context, c taskapi.AuthorizedCommand) (any, error) {
	h.taskMu.Lock()
	defer h.taskMu.Unlock()
	if c.Revalidate == nil {
		return nil, taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
	}
	if err := c.Revalidate(ctx); err != nil {
		return nil, err
	}
	if _, err := h.Authorize(ctx, c.Caller, c.Request); err != nil {
		return nil, err
	}
	if c.Request.Operation == "intake" {
		return h.mutateIntake(ctx, c)
	}
	if c.Request.Operation == "legacy_source" {
		return h.mutateLegacySource(ctx, c)
	}
	if previous, err := h.apiJournals()[c.Locator.Path].Lookup(ctx, c.Namespace, c.Request.IdempotencyKey); err == nil {
		failure, _, e := h.apiJournals()[c.Locator.Path].Outcome(ctx, previous.Key)
		if e != nil {
			return nil, e
		}
		if failure != nil {
			return nil, failure
		}
	}
	var in apiInput
	json.Unmarshal(c.Request.Input, &in)
	canonicalRoots := []string{}
	rootIdentities := map[string]string{}
	canonicalCwd := ""
	if c.Request.Operation == "create_dispatch" {
		parentPath := ""
		if in.New != nil {
			parent, ok := h.registry.Get(c.Caller.SourceSessionID)
			if !ok {
				return nil, taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
			}
			parentPath = parent.Snapshot().Cwd
		} else if in.Existing != nil {
			target, ok := h.registry.Get(in.Existing.SessionID)
			if !ok {
				return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			parentPath = target.Snapshot().Cwd
		}
		var captureErr error
		canonicalRoots, rootIdentities, canonicalCwd, captureErr = captureTaskWorkspace(h.cfg.RootDir, parentPath, in.Workspace.Cwd, in.Scope.Roots)
		if captureErr != nil {
			return nil, captureErr
		}
	}

	metadata, _ := json.Marshal(apiMetadata{Request: c.Request, Caller: c.Caller, SourceThread: c.Caller.SourceResumeID, SourceRevision: c.Caller.SourceConfigRevision, Transport: c.Caller.Transport, CanonicalRoots: canonicalRoots, CanonicalCwd: canonicalCwd, RootIdentities: rootIdentities})
	id := randomID()
	record := taskapi.IntentRecord{ReceiptID: "tr_" + id, NativeID: id, Metadata: metadata}
	if c.Request.Operation == "create_dispatch" {
		switch c.Locator.Path {
		case "pm_v1", "pm_v2":
			var ref struct {
				NativeID string `json:"native_record_id"`
			}
			json.Unmarshal(in.ApprovedPM, &ref)
			state, err := h.work.Collaboration(ctx)
			if err != nil {
				return nil, err
			}
			task := state.Tasks[ref.NativeID]
			project := state.Projects[task.ProjectID]
			record.NativeID = task.ID
			record.TaskID = apiID(h.cfg.InstanceID, c.Locator.Path, task.ID)
			record.SessionID = task.SessionID
			principal := coordination.Principal{ID: c.Caller.StableScopeID, SessionID: c.Caller.SourceSessionID, Human: c.Caller.BindingKind == "paired_human"}
			cmd := coordination.Command{Action: "dispatch", ProjectID: project.ID, TaskID: task.ID, MutationID: c.Request.IdempotencyKey, ExpectedRevision: state.Revision}
			collab := coordination.CollaborationCommand{Command: cmd}
			if state.Collaboration != nil {
				collab.ExpectedEntityRevision = state.Collaboration.Tasks[task.ID].Revision
			}
			stored, created, _, err := h.work.DispatchAPI(ctx, c, record, principal, collab, cmd)
			if err != nil {
				return nil, err
			}
			if created {
				h.reconcilePM()
			}
			return h.apiReceipt(ctx, c, stored)

		case "ordinary":
			record.SessionID = in.Existing.SessionID
			record.RequestID = "r_" + id
			record.NativeID = record.SessionID + "/" + record.RequestID
			record.TaskID = apiID(h.cfg.InstanceID, "ordinary", record.NativeID)
			payload := queuedPayload{Content: in.Instruction, MessagePurpose: "instruction", OwnerDevice: strings.TrimPrefix(c.Caller.StableScopeID, "device:"), ExpectedTarget: &dispatchTargetExpectation{in.Existing.ResumeID, in.Existing.ConfigRevision}}
			target, _ := h.registry.Get(record.SessionID)
			config := session.ConfigurationFrom(target.SettingsSnapshot())
			payload.Configuration = &config
			raw, _ := json.Marshal(payload)
			entry, created, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: record.SessionID, RequestID: record.RequestID, Content: truncateGraphemes(in.Instruction, 4000), Payload: raw, API: &messagequeue.APIAdmission{Command: c, Record: record}})
			if err != nil {
				return nil, err
			}
			stored, err := h.messageQueue.TaskJournal().Lookup(ctx, c.Namespace, c.Request.IdempotencyKey)
			if err != nil {
				return nil, err
			}
			if created {
				h.submitAPINamed(target, entry.RequestID)
			}
			return h.apiReceipt(ctx, c, stored)
		case "controller":
			record.SessionID = in.Existing.SessionID
			record.RequestID = "scjob_scd_" + id
			record.NativeID = "scd_" + id
			record.TaskID = apiID(h.cfg.InstanceID, "controller", record.NativeID)
			r := sessiondispatch.Record{ID: record.NativeID, ParentID: c.Caller.SourceSessionID, ParentThreadID: c.Caller.SourceResumeID, OriginRequestID: c.Caller.SourceRequestID, ToolCallID: c.Caller.ToolCallID, TurnID: c.Caller.NativeTurnID, InstanceID: h.cfg.InstanceID, SessionID: record.SessionID, ThreadID: in.Existing.ResumeID, ConfigRevision: in.Existing.ConfigRevision, RequestID: record.RequestID, Content: in.Instruction, Mode: "queue", IntentHash: c.IntentHash}
			stored, created, err := h.dispatches.CreateAPI(ctx, c, record, r)
			if err != nil {
				return nil, err
			}
			if created {
				h.submitLocalSessionDispatch(ctx, r)
			}
			return h.apiReceipt(ctx, c, stored)
		case "delegation":
			record.NativeID = "dg_" + id
			record.SessionID = "s_dg_" + id
			record.RequestID = "dgtask_" + record.NativeID
			record.TaskID = apiID(h.cfg.InstanceID, "delegation", record.NativeID)
			r := delegation.Record{ID: record.NativeID, ParentSessionID: c.Caller.SourceSessionID, OriginRequestID: c.Caller.SourceRequestID, ToolCallID: c.Caller.ToolCallID, IntentHash: c.IntentHash, ChildSessionID: record.SessionID, ChildRequestID: record.RequestID, ChildName: in.New.Name, Cwd: canonicalCwd, Instruction: in.Instruction, Model: in.New.Profile.Model, Effort: in.New.Profile.Effort, Sandbox: "read-only", ParentRequestID: "dgreturn_" + record.NativeID, SourceThread: c.Caller.SourceResumeID, SourceRevision: c.Caller.SourceConfigRevision, WorkspaceIdentity: rootIdentities[canonicalCwd], Authority: h.cfg.InstanceID}
			// Preserve the original policy and caller inside the existing store
			// transaction. Do not recursively Authorize there: it queries this
			// same store and would deadlock its single SQLite connection.
			admission := c
			admission.Revalidate = func(checkContext context.Context) error {
				if c.Revalidate == nil {
					return taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
				}
				if err := c.Revalidate(checkContext); err != nil {
					return err
				}
				parent, ok := h.registry.Get(c.Caller.SourceSessionID)
				if !ok || !h.controllerInScope(parent) || parent.Snapshot().Hidden || parent.State() == session.Closed || parent.ResumeID() != c.Caller.SourceResumeID || (c.Caller.SourceConfigKnown && parent.SettingsSnapshot().ConfigRevision != c.Caller.SourceConfigRevision) {
					return taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
				}
				return nil
			}
			stored, created, err := h.delegations.CreateAPI(ctx, admission, record, r, in.New.Profile.Backend)
			if err != nil {
				return nil, err
			}
			if created {
				// Provisioning is guarded by the same durable reservation/FIFO path.
				// A post-ACK retryable error must never masquerade as known-no-effect.
				_ = h.provisionDelegation(ctx, r)
				h.wakeDelegations()
			}
			return h.apiReceipt(ctx, c, stored)
		}
	}
	original, err := h.apiRecord(ctx, c.Caller, in.TaskID)
	if err != nil {
		return nil, err
	}
	j := h.apiJournals()[c.Locator.Path]
	record.TaskID = original.TaskID
	record.NativeID = original.NativeID
	record.SessionID = original.SessionID
	record.RequestID = "r_" + id
	if c.Request.Operation == "append" {
		target, _ := h.registry.Get(record.SessionID)
		configuration := session.ConfigurationFrom(target.SettingsSnapshot())
		payload, _ := json.Marshal(queuedPayload{Content: in.Content, MessagePurpose: "instruction", Configuration: &configuration, ExpectedTarget: &dispatchTargetExpectation{target.ResumeID(), target.SettingsSnapshot().ConfigRevision}})
		var stored taskapi.IntentRecord
		created := false
		if c.Locator.Path == "delegation" {
			return h.appendCapacityDelegation(ctx, c, record, original, in, target)
		}
		if c.Locator.Path == "ordinary" {
			_, created, err = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: record.SessionID, RequestID: record.RequestID, Content: truncateGraphemes(in.Content, 4000), Payload: payload, API: &messagequeue.APIAdmission{Command: c, Record: record}})
			if err == nil {
				stored, err = j.Lookup(ctx, c.Namespace, c.Request.IdempotencyKey)
			}
		} else {
			err = j.Transaction(ctx, func(tx *sql.Tx) error { var e error; stored, created, e = j.ClaimTx(tx, c, record); return e })
		}
		if err != nil {
			return nil, err
		}
		if created {
			if c.Locator.Path != "ordinary" {
				_, _, err = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: stored.SessionID, RequestID: stored.RequestID, Content: truncateGraphemes(in.Content, 4000), Payload: payload})
			}
			if err != nil {
				return nil, err
			}
			h.submitAPINamed(target, stored.RequestID)
			if in.Mode == "steer" {
				return nil, taskapi.Failure("unsupported", "known_receipt", "lookup_original")
			}
		}
		return h.apiReceipt(ctx, c, stored)
	}
	if c.Request.Operation == "cancel" {
		selected, e := j.Receipt(ctx, c.Caller.StableScopeID, c.Caller.NamespaceGeneration, in.CommandReceipt)
		if e != nil {
			return nil, e
		}
		if selected.TaskID != original.TaskID {
			return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		original = selected
		record.RequestID = original.RequestID
		if original.Path == "delegation" {
			if d, found, e := h.delegations.ByChildRequest(ctx, original.SessionID, original.RequestID); e != nil {
				return nil, e
			} else if found && d.State == "queued" {
				admission := c
				admission.Revalidate = func(check context.Context) error {
					if e := c.Revalidate(check); e != nil {
						return e
					}
					return h.checkQueuedCancellationSource(c.Caller, c.Request, d)
				}
				h.messageQueueMu.Lock()
				messageRevision, revisionErr := h.delegationMessageRevision(ctx, original.TaskID)
				if revisionErr != nil {
					h.messageQueueMu.Unlock()
					return nil, revisionErr
				}
				stored, e := h.delegations.CancelQueuedAPI(ctx, admission, record, original, in.Revision, messageRevision)
				h.messageQueueMu.Unlock()
				if e != nil {
					return nil, e
				}
				h.wakeDelegations()
				return h.apiReceipt(ctx, c, stored)
			}
		}

		var stored taskapi.IntentRecord
		created := false
		err = j.Transaction(ctx, func(tx *sql.Tx) error { var e error; stored, created, e = j.ClaimTx(tx, c, record); return e })
		if err != nil {
			return nil, err
		}
		if created {
			h.messageQueueMu.Lock()
			entry, found, e := h.messageQueue.Get(original.SessionID, original.RequestID)
			if e != nil || !found || entry.State != messagequeue.Queued {
				h.messageQueueMu.Unlock()
				failure := taskapi.Failure("busy", "known_receipt", "lookup_original")
				failure.ReceiptID = stored.ReceiptID
				if e := j.SaveOutcome(ctx, stored.Key, failure); e != nil {
					return nil, e
				}
				return nil, failure
			}
			target, ok := h.registry.Get(original.SessionID)
			if !ok {
				h.messageQueueMu.Unlock()
				return nil, taskapi.Failure("permission", "known_receipt", "request_scope_change")
			}
			snapshot, loadErr := h.messageQueue.Snapshot(original.SessionID)
			if loadErr != nil {
				h.messageQueueMu.Unlock()
				return nil, loadErr
			}
			outcomeKey := ""
			if original.Path == "ordinary" {
				outcomeKey = stored.Key
			}
			canCommit := func() bool {
				if c.Revalidate(ctx) != nil {
					return false
				}
				current, found := h.registry.Get(target.ID)
				if !found || current != target || !h.controllerInScope(current) || current.Snapshot().Hidden || current.State() == session.Closed || !h.controls.MobileMayWrite(current.ID) {
					return false
				}
				identity := in.Existing
				if identity == nil {
					var wrapper struct{ Target *taskapi.TargetIdentity }
					json.Unmarshal(c.Request.Input, &wrapper)
					identity = wrapper.Target
				}
				if identity == nil || identity.InstanceID != h.cfg.InstanceID || identity.SessionID != target.ID || identity.ResumeID != target.ResumeID() || identity.ConfigRevision != target.SettingsSnapshot().ConfigRevision {
					return false
				}
				if policy, e := h.PMConfiguration(target.ID); e != nil || policy != nil {
					return false
				}
				if c.Caller.SourceSessionID != "" {
					source, found := h.registry.Get(c.Caller.SourceSessionID)
					if !found || source.ResumeID() != c.Caller.SourceResumeID || source.SettingsSnapshot().ConfigRevision != c.Caller.SourceConfigRevision || !h.controllerInScope(source) || source.Snapshot().Hidden {
						return false
					}
					if original.Path == "controller" {
						grant, e := h.dispatches.Grant(ctx, source.ID)
						if e != nil || !grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, target.ID) {
							return false
						}
					}
				}
				return true
			}
			err = h.cancelWaitingInputWithOutcome(entry, &snapshot.Revision, canCommit, outcomeKey)
			h.messageQueueMu.Unlock()
			if err != nil {
				failure := taskapi.Failure("busy", "known_receipt", "lookup_original")
				failure.ReceiptID = stored.ReceiptID
				if e := j.SaveOutcome(ctx, stored.Key, failure); e != nil {
					return nil, e
				}
				return nil, failure
			}
			h.publishMessageQueue(original.SessionID)
			if err = j.SaveOutcome(ctx, stored.Key, nil); err != nil {
				return nil, err
			}
		}
		return h.apiReceipt(ctx, c, stored)
	}
	return nil, taskapi.Failure("unsupported", "known_none", "read_capabilities")
}
func (h *Hub) submitAPINamed(s *session.Session, id string) {
	if !h.submitQueuedMessage(s, id) {
		h.messageQueue.Transition(s.ID, id, []messagequeue.State{messagequeue.Queued}, messagequeue.Failed, "Actor unavailable", "", "")
	}
	h.publishMessageQueue(s.ID)
}
func (h *Hub) apiReceipt(ctx context.Context, c taskapi.AuthorizedCommand, r taskapi.IntentRecord) (any, error) {
	task, err := h.apiTask(ctx, r)
	if failure, done, e := h.apiJournals()[r.Path].Outcome(ctx, r.Key); e != nil {
		return nil, e
	} else if failure != nil {
		return nil, failure
	} else if !done && c.Request.Operation == "cancel" {
		return nil, taskapi.Failure("unknown_acceptance", "known_receipt", "lookup_original")
	}
	if err != nil {
		return nil, err
	}
	var metadata apiMetadata
	json.Unmarshal(r.Metadata, &metadata)
	return map[string]any{"receipt_id": r.ReceiptID, "task_id": r.TaskID, "task_ref": task["task_ref"], "idempotency_key": c.Request.IdempotencyKey, "operation": c.Request.Operation, "intent_hash": r.Hash, "axes": task["axes"], "task_revision": task["revision"], "effect_request_ids": apiEffectRequests(c.Request.Operation, r.RequestID), "transport": metadata.Transport, "created_at_ms": r.CreatedAt, "reused": false, "locator": c.Locator}, nil
}

// TaskWorkerScope is a formal child-profile lookup, never a name/prefix guess.
func (h *Hub) TaskWorkerScope(s *session.Session) (*taskapi.ChildScope, error) {
	if h.delegations == nil {
		return nil, nil
	}
	_, found, err := h.delegations.APIBackend(context.Background(), s.ID)
	if err != nil || !found {
		return nil, err
	}
	raw, found, err := h.delegations.APIChildMetadata(context.Background(), s.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("formal child scope metadata missing")
	}
	var meta apiMetadata
	if json.Unmarshal(raw, &meta) != nil {
		return nil, errors.New("child scope unavailable")
	}
	var input apiInput
	if json.Unmarshal(meta.Request.Input, &input) != nil {
		return nil, errors.New("child scope unavailable")
	}
	root, found, err := h.delegations.OriginalRootForChild(context.Background(), s.ID)
	if err != nil || !found {
		return nil, errors.New("original bounded profile unavailable")
	}
	provider, _, err := h.delegations.APIBackend(context.Background(), s.ID)
	if err != nil {
		return nil, err
	}
	_, revision, _, err := h.delegations.ExecutionBinding(context.Background(), root.ID)
	if err != nil {
		return nil, err
	}
	if err := validateDelegationChildProfile(root, s, s.ResumeID(), revision, provider, true); err != nil {
		return nil, err
	}
	for path, want := range meta.RootIdentities {
		if got, err := taskRootIdentity(path); err != nil || got != want {
			return nil, errors.New("bounded root identity changed")
		}
	}
	return &taskapi.ChildScope{Roots: meta.CanonicalRoots, Operations: input.Scope.Operations, Sandbox: input.Scope.Sandbox, Network: input.Scope.Network, MaxChildren: input.Scope.MaxChildren}, nil
}

// Silence unused OS helpers until the bounded read-input transport is added.
var _ = os.ErrNotExist
var _ = filepath.IsAbs
var _ = hex.EncodeToString
var _ = time.Second

func scopePathWithin(root, p string) bool {
	a, e := filepath.EvalSymlinks(root)
	if e != nil {
		return false
	}
	b, e := filepath.EvalSymlinks(p)
	if e != nil {
		return false
	}
	rel, e := filepath.Rel(a, b)
	return e == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

var _ = fmt.Sprintf

func (h *Hub) checkAPIRecord(ctx context.Context, c taskapi.VerifiedContext, r taskapi.IntentRecord) error {
	target, ok := h.registry.Get(r.SessionID)
	preparingChild := false
	if !ok && r.Path == "delegation" {
		if original, found, e := h.delegations.ByID(ctx, r.NativeID); e == nil && found && original.ChildSessionID == r.SessionID && original.ChildRequestID == r.RequestID {
			preparingChild = true
		}
	}
	if !preparingChild && (!ok || target.State() == session.Closed || target.Snapshot().Hidden || !h.controllerInScope(target)) {
		return taskapi.Failure("permission", "known_none", "request_scope_change")
	}

	var meta apiMetadata
	if json.Unmarshal(r.Metadata, &meta) != nil {
		return taskapi.Failure("key_expired", "known_receipt", "lookup_original")
	}
	if meta.WorkspaceRoomID != "" {
		if !h.workspaceTaskReader(ctx, c, meta.WorkspaceRoomID) {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		_, task, err := h.workspace.WorkProjectionState(ctx, meta.WorkspaceRoomID, meta.WorkspaceTaskID)
		if err != nil || task.SessionID != r.SessionID || task.RequestID != r.RequestID {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		return nil
	}
	if preparingChild {
		source, found := h.registry.Get(meta.Caller.SourceSessionID)
		if !found || source.Snapshot().Hidden || source.State() == session.Closed || !h.controllerInScope(source) {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
	}
	if r.ScopeID != c.StableScopeID || r.Generation != c.NamespaceGeneration {
		// Existing paired-human task-mode permission can read a formally bound
		// visible source/child relation, but does not gain mutation authority.
		if c.BindingKind != "paired_human" || meta.Caller.SourceSessionID == "" || (meta.Caller.BindingKind != "native_tool" && meta.Caller.BindingKind != "mcp_process_binding") {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		source, ok := h.registry.Get(meta.Caller.SourceSessionID)
		if !ok || source.Snapshot().Hidden || !h.controllerInScope(source) || source.State() == session.Closed {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
	}

	if r.Path == "controller" {
		source, ok := h.registry.Get(meta.Caller.SourceSessionID)
		if !ok || !h.controls.MobileMayWrite(source.ID) {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		grant, err := h.dispatches.Grant(ctx, source.ID)
		if err != nil || !grant.Enabled || !grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, r.SessionID) {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
	}
	if r.Path == "pm_v1" || r.Path == "pm_v2" {
		state, err := h.work.Collaboration(ctx)
		if err != nil {
			return err
		}
		task, ok := state.Tasks[r.NativeID]
		if !ok {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		project := state.Projects[task.ProjectID]
		if c.BindingKind != "paired_human" && c.SourceSessionID != project.PMSessionID && c.SourceSessionID != task.SessionID {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
	}
	return nil
}

func (h *Hub) enforceExistingScope(target *session.Session, in apiInput) error {
	enforcer, ok := h.exec.(backend.ExistingTaskScopeEnforcer)
	if !ok {
		return taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	if len(in.Workspace.Inputs) > 0 {
		return taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	return enforcer.ValidateExistingTaskScope(target, taskapi.ChildScope{Roots: in.Scope.Roots, Operations: in.Scope.Operations, Sandbox: in.Scope.Sandbox, Network: in.Scope.Network, MaxChildren: in.Scope.MaxChildren}, in.Workspace.Cwd)
}

// Persist only an opaque directory identity. Reflection keeps the source
// portable; unsupported filesystems/platforms reject before native admission.
func taskRootInfoIdentity(info os.FileInfo) (string, error) {
	if info == nil || !info.IsDir() {
		return "", errors.New("root directory unavailable")
	}
	value := reflect.ValueOf(info.Sys())
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return "", errors.New("root identity unsupported")
	}
	dev, ino := value.FieldByName("Dev"), value.FieldByName("Ino")
	if !dev.IsValid() || !ino.IsValid() {
		return "", errors.New("stable directory identity unsupported")
	}
	var device, inode uint64
	if dev.CanUint() {
		device = dev.Uint()
	} else if dev.CanInt() {
		device = uint64(dev.Int())
	} else {
		return "", errors.New("root identity unsupported")
	}
	if ino.CanUint() {
		inode = ino.Uint()
	} else if ino.CanInt() {
		inode = uint64(ino.Int())
	} else {
		return "", errors.New("root identity unsupported")
	}
	raw, _ := json.Marshal([]uint64{device, inode})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}
func taskRootIdentity(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	return taskRootInfoIdentity(info)
}

func apiEffectRequests(operation, request string) []string {
	if operation == "legacy_source" {
		return []string{}
	}
	return []string{request}
}

func (h *Hub) existingTargetCreationAvailable() bool {
	if mode, ok := h.exec.(interface{ ExistingTaskScopeMode() string }); ok && mode.ExistingTaskScopeMode() == "retained_bounded_only" {
		return false
	}
	_, ok := h.exec.(backend.ExistingTaskScopeEnforcer)
	return ok
}
