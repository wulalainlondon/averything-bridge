package core

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"strings"

	"everything-go/internal/messagequeue"
	"everything-go/internal/taskapi"
	"everything-go/internal/workspace"
)

// workspaceTaskAdmission records the authenticated ROOM transport in the same
// transaction as original queue admission. Its namespace describes the actual
// persisted room principal/grant; it never impersonates a native tool, device
// transport, controller or delegation caller.
func (h *Hub) workspaceTaskAdmission(ctx context.Context, p workspace.PreparedTask) *messagequeue.APIAdmission {
	scope := "workspace:" + p.Room.ID + ":" + p.Grant.PrincipalID
	caller := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: scope, NamespaceGeneration: p.Grant.ACLRevision, Transport: "authenticated_workspace", BindingKind: "workspace_grant"}
	input, _ := json.Marshal(map[string]any{"goal": p.Task.Title, "workspace_room_id": p.Room.ID, "workspace_task_id": p.Entity.ID})
	request := taskapi.Request{Type: "task_api_request", Version: "1.0.0-rc.1", Operation: "create_dispatch", IdempotencyKey: p.Task.RequestID, Input: input}
	metadata, _ := json.Marshal(apiMetadata{Request: request, Caller: caller, Transport: "authenticated_workspace", WorkspaceRoomID: p.Room.ID, WorkspaceTaskID: p.Entity.ID})
	hash := sha256.Sum256([]byte(p.Task.RequestID + "\x00" + p.Task.Manifest.Hash + "\x00" + p.Task.Role.RoleHash))
	native := p.Task.SessionID + "/" + p.Task.RequestID
	command := taskapi.AuthorizedCommand{Caller: caller, Request: request, Locator: taskapi.Locator{Authority: h.cfg.InstanceID, Path: "ordinary"}, Namespace: taskapi.Namespace{Authority: h.cfg.InstanceID, StableScopeID: scope, Generation: p.Grant.ACLRevision, Path: "ordinary", Operation: request.Operation}, IntentHash: hex.EncodeToString(hash[:])}
	return &messagequeue.APIAdmission{Command: command, Record: taskapi.IntentRecord{ReceiptID: "tr_cw_" + strings.TrimPrefix(p.Task.RequestID, "r_"), TaskID: apiID(h.cfg.InstanceID, "ordinary", native), NativeID: native, SessionID: p.Task.SessionID, RequestID: p.Task.RequestID, Metadata: metadata}, CanCommit: func() bool { _, _, err := h.validateWorkspaceNative(ctx, p.Room.ID, p.Entity.ID); return err == nil }}
}

// Only an actual currently paired human may use the ordinary TaskAPI to read
// a room task, and current membership is checked independently of its journal
// namespace. Guests continue to use the isolated room gateway.
func (h *Hub) workspacePairedTaskActor(c taskapi.VerifiedContext) (workspace.Actor, bool) {
	if h.workspace == nil || h.pairing == nil || c.BindingKind != "paired_human" || c.Transport != "authenticated_ws" || c.Authority != h.cfg.InstanceID {
		return workspace.Actor{}, false
	}
	for _, binding := range h.pairing.DeviceBindings() {
		if "device:"+binding.DeviceID != c.StableScopeID {
			continue
		}
		digest := sha256.Sum256([]byte(binding.Token))
		if binary.BigEndian.Uint64(digest[:8])&((1<<53)-1) != c.NamespaceGeneration {
			return workspace.Actor{}, false
		}
		return h.workspace.Owner(binding.DeviceID, hex.EncodeToString(digest[:])), true
	}
	return workspace.Actor{}, false
}
func (h *Hub) workspaceTaskReader(ctx context.Context, c taskapi.VerifiedContext, roomID string) bool {
	a, ok := h.workspacePairedTaskActor(c)
	if !ok {
		return false
	}
	_, _, err := h.workspace.Authorize(ctx, a, roomID, "read")
	return err == nil
}
