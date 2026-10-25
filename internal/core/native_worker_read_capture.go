package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"os"
	"path/filepath"
)

// Version 3 records only capability/input-read receipts of the already-selected
// formal workers. It grants no tools, input access or delegation authority.
// Input contents and requested paths are hashed, never copied into the sink.
func (h *Hub) captureWorkerRead(ctx context.Context, p *NativeResponseCapture, c backend.TaskCaller, raw json.RawMessage, response taskapi.Response) error {
	if p.Version != 3 || !p.WorkerReadProofs || c.Session == nil || c.Validate == nil || !c.Validate() || ctx.Err() != nil || c.CallID == "" || c.RequestID == "" || c.Session.ActiveQueuedID() != c.RequestID || c.ThreadID == "" || c.Session.ResumeID() != c.ThreadID {
		return nil
	}
	registered, exists := h.registry.Get(c.Session.ID)
	if !exists || registered != c.Session || c.Session.State() == session.Closed || c.Session.Snapshot().Hidden || !h.controllerInScope(c.Session) {
		return nil
	}
	parent, exists := h.registry.Get(p.SourceSession)
	if !exists || parent.State() == session.Closed || parent.Snapshot().Hidden || parent.ResumeID() != p.SourceThread || parent.SettingsSnapshot().ConfigRevision != p.SourceRevision {
		return nil
	}
	var request taskapi.Request
	if json.Unmarshal(raw, &request) != nil || request.Version != contract.Version || response.Version != contract.Version || request.CorrelationID != response.CorrelationID || response.Operation == nil || *response.Operation != request.Operation || (request.Operation != "capabilities" && request.Operation != "read_input") {
		return nil
	}
	scope, err := h.TaskWorkerScope(c.Session)
	if err != nil || scope == nil {
		return nil
	}
	record, found, err := h.delegations.ByChildRequest(ctx, c.Session.ID, c.RequestID)
	if err != nil || !found || record.ParentSessionID != p.SourceSession {
		return nil
	}
	round, found, err := h.delegations.Round(ctx, record.ID)
	if err != nil || !found {
		return nil
	}
	taskID := apiID(h.cfg.InstanceID, "delegation", round.LogicalID)
	if _, selected := p.Tasks[taskID]; !selected {
		return nil
	}
	hash := func(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
	public := map[string]any{"ok": response.OK, "task_id": taskID, "owner_request_id": record.ChildRequestID, "owner_round_id": record.ID}
	if !response.OK {
		if response.Error == nil {
			return nil
		}
		public["error_code"] = response.Error.Code
		public["acceptance"] = response.Error.Acceptance
	} else {
		encoded, err := json.Marshal(response.Result)
		if err != nil {
			return err
		}
		var body map[string]any
		if json.Unmarshal(encoded, &body) != nil {
			return nil
		}
		if request.Operation == "capabilities" {
			for _, key := range []string{"api_versions", "schema_hash", "allowed_operations"} {
				if value, ok := body[key]; ok {
					public[key] = value
				}
			}
		} else {
			var input struct {
				Relative string `json:"relative_path"`
			}
			if json.Unmarshal(request.Input, &input) != nil || input.Relative == "" {
				return nil
			}
			text, ok := body["text"].(string)
			if !ok || len(text) > 64000 || body["truncated"] != false || body["relative_path"] != input.Relative || body["hash"] != hash([]byte(text)) {
				return nil
			}
			public["input_sha256"] = body["hash"]
			public["input_bytes"] = len([]byte(text))
		}
	}
	if request.Operation == "read_input" {
		var input struct {
			Relative string `json:"relative_path"`
		}
		if json.Unmarshal(request.Input, &input) != nil || input.Relative == "" || len(input.Relative) > 4096 {
			return nil
		}
		public["requested_path_sha256"] = hash([]byte(input.Relative))
	}
	// Recheck both source and original worker after the metadata reads.
	if !c.Validate() || ctx.Err() != nil || !h.captureConfigCurrent(p) || c.Session.ActiveQueuedID() != c.RequestID || c.Session.ResumeID() != c.ThreadID || parent.ResumeID() != p.SourceThread || parent.SettingsSnapshot().ConfigRevision != p.SourceRevision {
		return nil
	}
	if current, err := h.TaskWorkerScope(c.Session); err != nil || current == nil {
		return nil
	}
	upstream, err := json.Marshal(response)
	if err != nil {
		return err
	}
	envelope := map[string]any{"evidence_kind": "original_native_worker_read_public_projection", "raw_wire": false, "authority_instance_id": h.cfg.InstanceID, "capture_config_sha256": p.ConfigHash, "parent_session_id": p.SourceSession, "parent_native_thread_id": p.SourceThread, "parent_config_revision": p.SourceRevision, "source_session_id": c.Session.ID, "source_native_thread_id": c.ThreadID, "source_config_revision": c.Session.SettingsSnapshot().ConfigRevision, "source_request_id": c.RequestID, "native_call_id": c.CallID, "correlation_id": request.CorrelationID, "operation": request.Operation, "api_version": response.Version, "upstream_structured_response_sha256": hash(upstream), "public_result": public}
	data, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	if !captureOwned(p.Directory, os.ModeDir) {
		return os.ErrPermission
	}
	canonical, err := filepath.EvalSymlinks(p.Directory)
	if err != nil || canonical != filepath.Clean(p.Directory) {
		return os.ErrPermission
	}
	identity := hash([]byte(c.RequestID + "\x00" + c.CallID + "\x00" + request.CorrelationID))
	// The final directory/alias checks can perform filesystem I/O. Revalidate
	// the original lease/config immediately before creating the public receipt.
	if !c.Validate() || ctx.Err() != nil || !h.captureConfigCurrent(p) || c.Session.ActiveQueuedID() != c.RequestID || c.Session.ResumeID() != c.ThreadID || parent.ResumeID() != p.SourceThread || parent.SettingsSnapshot().ConfigRevision != p.SourceRevision {
		return nil
	}
	file, err := os.OpenFile(filepath.Join(p.Directory, identity+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(append(data, '\n'))
	return err
}
