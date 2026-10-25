package core

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"time"
)

type legacySourceInput struct {
	Action     string                            `json:"action"`
	Source     messagequeue.LegacySourceIdentity `json:"source"`
	Target     messagequeue.LegacySourceIdentity `json:"target"`
	RequestID  string                            `json:"request_id"`
	Hash       string                            `json:"receipt_hash"`
	Revision   uint64                            `json:"expected_queue_revision"`
	RelationID string                            `json:"relation_id"`
	Cursor     string                            `json:"cursor"`
	SessionID  string                            `json:"session_id"`
}
type legacySourceMetadata struct {
	LegacyLinkID string
	Request      taskapi.Request
	Transport    string
}

func (h *Hub) legacyActorActive(link messagequeue.LegacySourceLink) bool {
	for _, device := range h.pairing.DeviceBindings() {
		if "device:"+device.DeviceID != link.Actor {
			continue
		}
		digest := sha256.Sum256([]byte(device.Token))
		if binary.BigEndian.Uint64(digest[:8])&((1<<53)-1) == link.Generation {
			return true
		}
	}
	return false
}
func (h *Hub) legacyIdentityAllowed(identity messagequeue.LegacySourceIdentity) bool {
	s, found := h.registry.Get(identity.SessionID)
	return identity.InstanceID == h.cfg.InstanceID && found && h.controllerInScope(s) && !s.Snapshot().Hidden && s.State() != session.Closed && s.ResumeID() == identity.ThreadID && s.SettingsSnapshot().ConfigRevision == identity.Revision
}
func (h *Hub) legacyLinkVisible(link messagequeue.LegacySourceLink) bool {
	if link.Revoked || !h.legacyActorActive(link) || !h.legacyIdentityAllowed(link.Source) || !h.legacyIdentityAllowed(link.Target) {
		return false
	}
	for _, id := range []string{link.Source.SessionID, link.Target.SessionID} {
		if policy, err := h.PMConfiguration(id); err != nil || policy != nil {
			return false
		}
	}
	return true
}
func (h *Hub) authorizeLegacySource(ctx context.Context, caller taskapi.VerifiedContext, r taskapi.Request, l taskapi.Locator) (taskapi.Locator, error) {
	if caller.BindingKind != "paired_human" || caller.Transport != "authenticated_ws" {
		return l, taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	var input legacySourceInput
	json.Unmarshal(r.Input, &input)
	if input.Action == "confirm" || input.Action == "revoke" {
		ns := taskapi.Namespace{Authority: caller.Authority, StableScopeID: caller.StableScopeID, Generation: caller.NamespaceGeneration, Path: "ordinary", Operation: "legacy_source"}
		if previous, err := h.messageQueue.TaskJournal().Lookup(ctx, ns, r.IdempotencyKey); err == nil {
			hash, _ := taskapi.IntentHash(r)
			if previous.Hash != hash {
				return l, taskapi.Failure("idempotency_conflict", "known_receipt", "lookup_original")
			}
			return l, nil
		}
	}
	switch input.Action {
	case "list":
		return l, nil
	case "inspect":
		target, found := h.registry.Get(input.SessionID)
		if !found || !h.controllerInScope(target) || target.Snapshot().Hidden || target.State() == session.Closed || target.ResumeID() == "" {
			return l, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		return l, nil
	case "preview", "confirm":
		if input.Source.SessionID == input.Target.SessionID || !h.legacyIdentityAllowed(input.Source) || !h.legacyIdentityAllowed(input.Target) {
			return l, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		entry, found, err := h.messageQueue.Get(input.Target.SessionID, input.RequestID)
		if err != nil {
			return l, err
		}
		if !found {
			return l, taskapi.Failure("not_found", "known_none", "lookup_original")
		}
		formal, err := h.messageQueue.HasFormalTaskSource(ctx, input.Target.SessionID, input.RequestID)
		if err != nil {
			return l, err
		}
		if h.dispatches != nil {
			known, e := h.dispatches.HasTargetReceipt(ctx, h.cfg.InstanceID, input.Target.SessionID, input.RequestID)
			if e != nil {
				return l, e
			}
			formal = formal || known
		}
		if h.delegations != nil {
			known, e := h.delegations.HasChildReceipt(ctx, input.Target.SessionID, input.RequestID)
			if e != nil {
				return l, e
			}
			formal = formal || known
		}
		if formal {
			return l, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		admission := h.taskAdmission(entry)
		if admission.Origin != nil {
			return l, taskapi.Failure("permission", "known_none", "request_scope_change")
		} // Existing server/native origin remains authoritative.
		if input.Action == "confirm" {
			if admission.OwnerDevice != "" && "device:"+admission.OwnerDevice != caller.StableScopeID {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			if !h.controls.MobileMayWrite(input.Source.SessionID) || !h.controls.MobileMayWrite(input.Target.SessionID) {
				return l, taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			for _, id := range []string{input.Source.SessionID, input.Target.SessionID} {
				if policy, e := h.PMConfiguration(id); e != nil || policy != nil {
					return l, taskapi.Failure("unsupported", "known_none", "read_capabilities")
				}
			}
			if input.Hash != entry.PayloadHash {
				return l, taskapi.Failure("stale_revision", "known_none", "refresh_identity")
			}
		}
		return l, nil
	case "revoke":
		link, err := h.messageQueue.LegacySourceLink(ctx, input.RelationID)
		if err != nil {
			return l, err
		}
		if link.Actor != caller.StableScopeID || link.Generation != caller.NamespaceGeneration {
			return l, taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		return l, nil
	}
	return l, taskapi.Failure("invalid_argument", "known_none", "correct_input")
}
func (h *Hub) readLegacySource(ctx context.Context, c taskapi.AuthorizedCommand) (any, error) {
	var input legacySourceInput
	json.Unmarshal(c.Request.Input, &input)
	before, err := h.apiWatermarks(ctx)
	if err != nil {
		return nil, err
	}
	links, err := h.messageQueue.LegacySourceLinks(ctx)
	if err != nil {
		return nil, err
	}
	visible := []messagequeue.LegacySourceLink{}
	for _, link := range links {
		if h.legacyReadAllowed(c.Caller, link) && (input.SessionID == "" || link.Source.SessionID == input.SessionID || link.Target.SessionID == input.SessionID) {
			visible = append(visible, link)
		}
	}
	scope, err := h.apiReadScope(ctx, c.Caller, apiInput{SessionID: input.SessionID})
	if err != nil {
		return nil, err
	}
	scope.FilterHash, err = taskapi.FilterHash(map[string]any{"base": scope.FilterHash, "kind": "legacy-source-list", "membership": visible})
	if err != nil {
		return nil, err
	}
	marks, err := h.apiWatermarks(ctx)
	if err != nil {
		return nil, err
	}
	if !equalWatermarks(before, marks) {
		return nil, taskapi.Failure("cursor_expired", "known_none", "refresh_snapshot")
	}
	watermark := uint64(0)
	for _, m := range marks {
		if m.Store == "ordinary" {
			watermark = m.Sequence
		}
	}
	offset := uint64(0)
	if input.Cursor != "" {
		previous, e := h.taskCursor.EventWatermarks(input.Cursor, scope)
		if e != nil {
			return nil, e
		}
		for _, m := range previous {
			if m.Store == "ordinary" && m.Sequence != watermark {
				return nil, taskapi.Failure("cursor_expired", "known_none", "refresh_identity")
			}
			if m.Store == "legacy-offset" {
				offset = m.Sequence
			}
		}
	}
	if offset > uint64(len(visible)) {
		return nil, taskapi.Failure("cursor_expired", "known_none", "refresh_identity")
	}
	end := offset + 100
	if end > uint64(len(visible)) {
		end = uint64(len(visible))
	}
	result := map[string]any{"links": visible[offset:end], "has_more": end < uint64(len(visible))}
	if end < uint64(len(visible)) {
		cursor, e := h.taskCursor.EventsFromWatermarks(scope, []taskapi.Watermark{{Store: "ordinary", Sequence: watermark}, {Store: "legacy-offset", Sequence: end}}, time.Now().Add(5*time.Minute))
		if e != nil {
			return nil, e
		}
		result["next_cursor"] = cursor
	}
	if input.Action == "inspect" {
		target, _ := h.registry.Get(input.SessionID)
		result["identities"] = []messagequeue.LegacySourceIdentity{{InstanceID: h.cfg.InstanceID, SessionID: target.ID, ThreadID: target.ResumeID(), Revision: target.SettingsSnapshot().ConfigRevision}}
		snapshot, err := h.messageQueue.Snapshot(target.ID)
		if err != nil {
			return nil, err
		}
		receipts := []any{}
		for _, entry := range snapshot.Items {
			if len(receipts) == 100 {
				break
			}
			receipts = append(receipts, map[string]any{"request_id": entry.RequestID, "content": truncateGraphemes(entry.Content, 4000), "state": string(entry.State)})
		}
		result["receipts"] = receipts
	}
	if input.Action == "preview" {
		snap, err := h.messageQueue.Snapshot(input.Target.SessionID)
		if err != nil {
			return nil, err
		}
		entry, found, err := h.messageQueue.Get(input.Target.SessionID, input.RequestID)
		if err != nil || !found {
			return nil, taskapi.Failure("not_found", "known_none", "lookup_original")
		}
		result["preview"] = map[string]any{"source": input.Source, "target": input.Target, "request_id": input.RequestID, "receipt_hash": entry.PayloadHash, "queue_revision": snap.Revision, "content": truncateGraphemes(entry.Content, 4000), "admission": string(entry.State), "native_origin_status": "unknown"}
	}
	if c.Revalidate == nil || c.Revalidate(ctx) != nil {
		return nil, taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
	}
	if _, err = h.authorizeLegacySource(ctx, c.Caller, c.Request, c.Locator); err != nil {
		return nil, err
	}
	after, err := h.apiWatermarks(ctx)
	if err != nil {
		return nil, err
	}
	if !equalWatermarks(before, after) {
		return nil, taskapi.Failure("cursor_expired", "known_none", "refresh_snapshot")
	}
	for _, link := range visible[offset:end] {
		current, err := h.messageQueue.LegacySourceLink(ctx, link.ID)
		if err != nil || !h.legacyReadAllowed(c.Caller, current) {
			return nil, taskapi.Failure("cursor_expired", "known_none", "refresh_snapshot")
		}
	}
	return result, nil
}
func (h *Hub) mutateLegacySource(ctx context.Context, c taskapi.AuthorizedCommand) (any, error) {
	var input legacySourceInput
	json.Unmarshal(c.Request.Input, &input)
	j := h.messageQueue.TaskJournal()
	if previous, err := j.Lookup(ctx, c.Namespace, c.Request.IdempotencyKey); err == nil {
		return h.apiReceipt(ctx, c, previous)
	}
	id := "hl_" + randomID()
	link := messagequeue.LegacySourceLink{ID: id, Source: input.Source, Target: input.Target, RequestID: input.RequestID, Hash: input.Hash, Actor: c.Caller.StableScopeID, Generation: c.Caller.NamespaceGeneration, Kind: "human_linked", NativeOrigin: "unknown"}
	if input.Action == "revoke" {
		var err error
		link, err = h.messageQueue.LegacySourceLink(ctx, input.RelationID)
		if err != nil {
			return nil, err
		}
	}
	raw, _ := json.Marshal(legacySourceMetadata{link.ID, c.Request, c.Caller.Transport})
	record := taskapi.IntentRecord{ReceiptID: "tr_" + randomID(), TaskID: apiID(h.cfg.InstanceID, "ordinary", link.Target.SessionID+"/"+link.RequestID), NativeID: link.Target.SessionID + "/" + link.RequestID, SessionID: link.Target.SessionID, RequestID: link.RequestID, Metadata: raw}
	h.messageQueueMu.Lock()
	if c.Revalidate == nil || c.Revalidate(ctx) != nil {
		h.messageQueueMu.Unlock()
		return nil, taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
	}
	if _, err := h.authorizeLegacySource(ctx, c.Caller, c.Request, c.Locator); err != nil {
		h.messageQueueMu.Unlock()
		return nil, err
	}
	var stored taskapi.IntentRecord
	var err error
	if input.Action == "confirm" {
		stored, err = h.messageQueue.ConfirmLegacySource(ctx, c, record, link, input.Revision)
	} else {
		stored, err = h.messageQueue.RevokeLegacySource(ctx, c, record, link)
	}
	h.messageQueueMu.Unlock()
	if err != nil {
		return nil, err
	}
	return h.apiReceipt(ctx, c, stored)
}
func (h *Hub) projectLegacyAPITask(ctx context.Context, record taskapi.IntentRecord, id string) (map[string]any, error) {
	link, err := h.messageQueue.LegacySourceLink(ctx, id)
	if err != nil {
		return nil, err
	}
	entry, found, err := h.messageQueue.Get(link.Target.SessionID, link.RequestID)
	if err != nil || !found {
		return nil, taskapi.Failure("not_found", "known_none", "lookup_original")
	}
	revision, err := h.messageQueue.TaskRevision(ctx, entry.SessionID, entry.RequestID)
	if err != nil {
		return nil, err
	}
	axes := map[string]string{"admission": "unknown", "execution": "unknown", "final": "unavailable", "artifact": "none", "qa": "not_requested", "delivery": "not_requested", "cancellation": "none"}
	if link.Revoked || !h.legacyActorActive(link) {
		axes["cancellation"] = "unknown"
	}
	assertion := map[string]any{"kind": "human_linked", "record_id": link.ID, "actor_scope_id": link.Actor, "original_receipt_hash": link.Hash, "native_origin_status": "unknown"}
	source := map[string]any{"identity": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": link.Source.SessionID}, "binding_kind": "legacy_unknown", "provenance": "legacy", "human_assertion": assertion}
	views := []string{"results"}
	if h.legacyLinkVisible(link) {
		views = append(views, "source_children")
	}
	return map[string]any{"task_id": record.TaskID, "task_ref": map[string]string{"authority_instance_id": h.cfg.InstanceID, "path": "ordinary", "native_record_id": record.NativeID}, "revision": revision, "definition_revision": uint64(1), "source": source, "target": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": entry.SessionID, "request_id": entry.RequestID}, "axes": axes, "artifact_refs": []any{}, "qa": map[string]any{"status": "not_requested", "evidence_refs": []any{}}, "evidence_refs": []any{map[string]any{"kind": "legacy", "authority_instance_id": h.cfg.InstanceID, "record_id": link.ID, "verified": false}}, "receipt_ids": legacyReceiptIDs(record.ReceiptID), "child_task_ids": []string{}, "provenance": "legacy", "views": views, "capabilities": []string{"get"}}, nil
}
func (h *Hub) legacySessionChildren(ctx context.Context, source string, reader taskapi.VerifiedContext) []protocol.SessionTask {
	links, err := h.messageQueue.LegacySourceLinks(ctx)
	if err != nil {
		return nil
	}
	out := []protocol.SessionTask{}
	for _, link := range links {
		if link.Source.SessionID != source || !h.legacyReadAllowed(reader, link) {
			continue
		}
		entry, found, err := h.messageQueue.Get(link.Target.SessionID, link.RequestID)
		if err != nil || !found || entry.PayloadHash != link.Hash {
			continue
		}
		target, _ := h.registry.Get(link.Target.SessionID)
		out = append(out, protocol.SessionTask{RequestID: entry.RequestID, ExecutionRequestID: entry.RequestID, MessagePurpose: "legacy_unknown", ReceiptFound: true, State: "legacy_unknown", Content: entry.Content, CreatedAt: entry.CreatedAt, UpdatedAt: entry.UpdatedAt, Files: []string{}, Transport: "human_linked", InstanceID: h.cfg.InstanceID, SessionID: entry.SessionID, SessionName: target.SettingsSnapshot().Name, ThreadID: link.Target.ThreadID, ConfigRevision: link.Target.Revision, CanOpenTarget: true, HumanSourceLink: map[string]string{"kind": "human_linked", "relation_id": link.ID, "native_origin_status": "unknown"}})
	}
	return out
}

func (h *Hub) legacyHistoryReadAllowed(c taskapi.VerifiedContext, link messagequeue.LegacySourceLink) bool {
	// Uses the existing authenticated paired-human read policy, not a link grant.
	// Native/model callers never acquire human annotation access through this edge.
	return c.Authority == h.cfg.InstanceID && c.BindingKind == "paired_human" && c.Transport == "authenticated_ws" && c.StableScopeID == link.Actor && c.NamespaceGeneration == link.Generation && h.legacyIdentityAllowed(link.Source) && h.legacyIdentityAllowed(link.Target)
}
func (h *Hub) legacyReadAllowed(c taskapi.VerifiedContext, link messagequeue.LegacySourceLink) bool {
	return h.legacyHistoryReadAllowed(c, link) && h.legacyLinkVisible(link)
}
func (h *Hub) legacyProjectionRecord(link messagequeue.LegacySourceLink) taskapi.IntentRecord {
	raw, _ := json.Marshal(legacySourceMetadata{LegacyLinkID: link.ID})
	return taskapi.IntentRecord{TaskID: apiID(h.cfg.InstanceID, "ordinary", link.Target.SessionID+"/"+link.RequestID), NativeID: link.Target.SessionID + "/" + link.RequestID, SessionID: link.Target.SessionID, RequestID: link.RequestID, Path: "ordinary", ScopeID: link.Actor, Generation: link.Generation, Metadata: raw}
}

func legacyReceiptIDs(id string) []string {
	if id == "" {
		return []string{}
	}
	return []string{id}
}

func equalWatermarks(a, b []taskapi.Watermark) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
