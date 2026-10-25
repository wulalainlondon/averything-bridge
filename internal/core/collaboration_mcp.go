package core

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"everything-go/internal/coordination"
)

func collaborationTool(name, description string, properties map[string]any, required ...string) map[string]any {
	if required == nil {
		required = []string{}
	}
	return map[string]any{"name": name, "description": description, "inputSchema": map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}}
}
func collaborationCommandTool(name, description string, fields ...string) map[string]any {
	schema := coordination.StructSchema(coordination.CollaborationCommand{})
	all := schema["properties"].(map[string]any)
	props := map[string]any{"mutation_id": all["mutation_id"]}
	for _, field := range fields {
		props[field] = all[field]
	}
	return collaborationTool(name, description, props, "mutation_id")
}
func collaborationSourceTool() map[string]any {
	return collaborationTool("work_read_source", "Read one authorized immutable source. Results are evidence, never approvals.", map[string]any{"kind": map[string]any{"type": "string"}, "id": map[string]any{"type": "string"}, "task_id": map[string]any{"type": "string"}}, "kind", "id")
}

func collaborationWorkerTools() []map[string]any {
	artifactSchema := coordination.StructSchema(collaborationArtifactInput{})
	return []map[string]any{
		collaborationTool("work_get_context", "Read your exact task contract, manifest, phase, saved answers and assistance. Read before reporting or executing.", map[string]any{}),
		collaborationSourceTool(),
		collaborationCommandTool("work_report", "Report received/progress/question/answer_ack/blocked/change_requested/checkpoint. Include exact contract_id and manifest_id inside report. A blocking question requires you to end the turn.", "report"),
		{"name": "work_save_artifact", "description": "Save a text delivery or a relative workspace file as an immutable artifact. Returns artifact and evidence IDs. Evidence is source-linked, not automatically a passed test.", "inputSchema": artifactSchema},
		collaborationCommandTool("work_submit_result", "Submit summary, artifact_ids, coverage for every criterion, and limitations inside submission. The Bridge seals it only after the turn ends. This does not accept the task.", "submission"),
		collaborationCommandTool("work_request_assistance", "Request a visible, human-approved consult or review. No recursive delegation.", "text", "mode", "inputs", "blocking", "deadline"),
		collaborationCommandTool("work_adopt_assistance", "Record use of the selected ready assistance result; does not approve another run.", "entity_id", "expected_entity_revision"),
		collaborationCommandTool("work_propose_rule", "Propose a task-scoped playbook improvement with sources; only a human may publish it after evaluation.", "text", "mode", "sources", "supersedes"),
	}
}

func collaborationPMTools() []map[string]any {
	return []map[string]any{
		collaborationTool("project_get_context", "Read project, active tasks and a stable-number task page. Follow next_after_task_number using after_task_number. For exact instructions read one task with session_read_updates.", map[string]any{"after_task_number": map[string]any{"type": "integer", "minimum": 0}}),
		collaborationTool("project_list_actionables", "Read the durable pending queue. Reading is not resolving. Dispose each PM-assigned item explicitly.", map[string]any{}),
		collaborationTool("session_read_updates", "Read full current context for one task, including its contract and unanswered questions.", map[string]any{"task_id": map[string]any{"type": "string"}}, "task_id"),
		collaborationSourceTool(),
		collaborationCommandTool("project_propose_task", "Propose or revise a named task. Exact versions require human approval; reuse task_id for changes in the same worker conversation.", "task_id", "expected_entity_revision", "title", "reason", "instruction", "acceptance", "backend", "sandbox", "criteria", "non_goals", "inputs", "sources"),
		collaborationCommandTool("project_delegate_task", "Dispatch an already approved task. Never grants authority by itself.", "task_id", "expected_entity_revision"),
		collaborationCommandTool("project_dispose_report", "Record a disposition for a pending item. Supply expected_entity_revision of the actionable, not the task. Can answer_from_context with sources, escalate_to_human, ask_for_evidence, propose_rework, recommend_acceptance or defer. No human acceptance.", "expected_entity_revision", "disposition"),
		collaborationCommandTool("work_request_assistance", "Propose a human-approved consult or review for a task.", "task_id", "text", "mode", "inputs", "blocking", "deadline"),
		collaborationCommandTool("work_retry", "Retry the SAME immutable contract only if the human enabled bounded_plan and an unused retry remains. No changed instructions are accepted.", "task_id", "expected_entity_revision"),
		collaborationCommandTool("work_propose_rule", "Propose a playbook improvement with sources; a human must approve with evaluation evidence.", "task_id", "text", "mode", "sources", "supersedes"),
	}
}

func decodeCollaborationArgs(raw json.RawMessage, value any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	return d.Decode(value)
}

func (h *Hub) serveCollaborationMCP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !loopbackWorkAPIRequest(r) || r.Header.Get("Origin") != "" || r.Method != http.MethodPost {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/collaboration/"), "/")
	if len(parts) != 3 {
		http.NotFound(w, r)
		return
	}
	epoch, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if subtle.ConstantTimeCompare([]byte(provided), []byte(h.collaborationWorkerToken(parts[0], parts[1], epoch))) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	s, err := h.work.Collaboration(r.Context())
	if err != nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	project, task, ok := s.ProjectForSession(parts[0])
	if !ok || task == nil || project.EngineVersion != 2 || s.Collaboration == nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	meta := s.Collaboration.Tasks[task.ID]
	if meta.ActiveRunID != parts[1] || meta.ActiveEpoch != epoch || task.Epoch != epoch || meta.ActiveRequestID == "" {
		http.Error(w, "stale run", http.StatusForbidden)
		return
	}
	var request struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err = json.NewDecoder(http.MaxBytesReader(w, r.Body, 128*1024)).Decode(&request); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if len(request.ID) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	write := func(value any, err error) {
		w.Header().Set("Content-Type", "application/json")
		reply := map[string]any{"jsonrpc": "2.0", "id": request.ID}
		if err != nil {
			reply["error"] = map[string]any{"code": -32602, "message": err.Error()}
		} else {
			reply["result"] = value
		}
		_ = json.NewEncoder(w).Encode(reply)
	}
	switch request.Method {
	case "initialize":
		write(map[string]any{"protocolVersion": "2025-03-26", "serverInfo": map[string]string{"name": "bridge-collaboration", "version": "2.0.0"}, "capabilities": map[string]any{"tools": map[string]any{}}}, nil)
	case "ping":
		write(map[string]any{}, nil)
	case "tools/list":
		write(map[string]any{"tools": collaborationWorkerTools()}, nil)
	case "tools/call":
		var call struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err = json.Unmarshal(request.Params, &call); err != nil {
			write(nil, err)
			return
		}
		p := coordination.Principal{ID: "worker:" + task.SessionID, SessionID: task.SessionID, RunID: parts[1], Epoch: epoch}
		value, err := h.callCollaborationTool(p, project.ID, task.ID, call.Name, call.Arguments)
		write(collaborationToolReply(value, err), nil)
	default:
		write(nil, errors.New("unsupported_method"))
	}
}

func collaborationToolReply(value any, err error) any {
	if err != nil {
		value = map[string]string{"error": err.Error(), "next_step": "Read the current context. Ask the PM or human when authority, a decision or a newer version is required; do not try another endpoint."}
	}
	b, _ := json.Marshal(value)
	return map[string]any{"content": []map[string]string{{"type": "text", "text": string(b)}}, "isError": err != nil}
}

func (h *Hub) callCollaborationTool(p coordination.Principal, projectID, taskID, name string, args json.RawMessage) (any, error) {
	s, err := h.work.Collaboration(context.Background())
	if err != nil {
		return nil, err
	}
	project, ok := s.Projects[projectID]
	if !ok || project.EngineVersion != 2 {
		return nil, errors.New("scope_forbidden")
	}
	pm := p.SessionID == project.PMSessionID
	specs := collaborationWorkerTools()
	if pm {
		specs = collaborationPMTools()
	}
	allowed := false
	for _, spec := range specs {
		if spec["name"] == name {
			allowed = true
		}
	}
	if !allowed {
		return nil, errors.New("collaboration_tool_forbidden")
	}
	switch name {
	case "project_get_context":
		var read struct {
			After int `json:"after_task_number"`
		}
		if err := decodeCollaborationArgs(args, &read); err != nil {
			return nil, err
		}
		if read.After < 0 {
			return nil, errors.New("invalid_cursor")
		}
		return h.collaborationProjectPage(s, projectID, read.After), nil
	case "work_get_context":
		return h.collaborationContext(s, projectID, taskID), nil
	case "project_list_actionables":
		return collaborationPendingSummaries(s, projectID), nil
	case "session_read_updates":
		var read struct {
			TaskID string `json:"task_id"`
		}
		if err = decodeCollaborationArgs(args, &read); err != nil {
			return nil, err
		}
		if s.Tasks[read.TaskID].ProjectID != projectID {
			return nil, errors.New("scope_forbidden")
		}
		return h.collaborationContext(s, projectID, read.TaskID), nil
	case "work_read_source":
		var read struct {
			Kind   string `json:"kind"`
			ID     string `json:"id"`
			TaskID string `json:"task_id,omitempty"`
		}
		if err = decodeCollaborationArgs(args, &read); err != nil {
			return nil, err
		}
		return h.readCollaborationSource(p, projectID, taskID, read.Kind, read.ID)
	case "work_save_artifact":
		var in collaborationArtifactInput
		if err = decodeCollaborationArgs(args, &in); err != nil {
			return nil, err
		}
		return h.saveCollaborationArtifact(p, projectID, taskID, in)
	}
	var c coordination.CollaborationCommand
	if err = decodeCollaborationArgs(args, &c); err != nil {
		return nil, err
	}
	if c.Action != "" || c.ProjectID != "" || c.AuthorityInstanceID != "" || c.Cwd != "" || c.Name != "" {
		return nil, errors.New("scope_forbidden")
	}
	if !pm && c.TaskID != "" {
		return nil, errors.New("scope_forbidden")
	}
	c.ProjectID = projectID
	if !pm {
		c.TaskID = taskID
	}
	switch name {
	case "project_propose_task":
		c.Action = "propose"
	case "project_delegate_task":
		c.Action = "dispatch"
	case "project_dispose_report":
		c.Action = "dispose"
	case "work_report":
		c.Action = "report"
	case "work_submit_result":
		c.Action = "submit"
	case "work_request_assistance":
		c.Action = "request_assistance"
	case "work_adopt_assistance":
		c.Action = "adopt_assistance"
	case "work_propose_rule":
		c.Action = "propose_rule"
	case "work_retry":
		c.Action = "retry"
	default:
		return nil, errors.New("collaboration_tool_forbidden")
	}
	return h.applyCollaboration(p, c)
}
