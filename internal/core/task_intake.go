package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

type intakeTarget struct {
	Instance string  `json:"instance_id"`
	Session  string  `json:"session_id"`
	Thread   *string `json:"expected_native_thread_id"`
	Revision uint64  `json:"expected_config_revision"`
}
type intakeImage struct {
	Data      string `json:"data"`
	MediaType string `json:"media_type"`
}
type intakeFile struct {
	AttachmentID string `json:"attachment_id"`
	Name         string `json:"name"`
	Content      string `json:"content"`
	MediaType    string `json:"media_type"`
	SizeBytes    int64  `json:"size_bytes"`
}
type intakeInput struct {
	Action    string        `json:"action"`
	SessionID string        `json:"session_id"`
	RequestID string        `json:"submission_request_id"`
	Target    intakeTarget  `json:"target"`
	Goal      string        `json:"goal"`
	Images    []intakeImage `json:"images"`
	Files     []intakeFile  `json:"files"`
}

// Admitted user source evidence, not a fabricated native TaskOrigin or a new
// caller principal. Unknown native IDs remain absent until real consumption.
type intakeMetadata struct {
	Source        map[string]any
	Goal          string
	Configuration session.Configuration
}

func (h *Hub) intakeSession(id string) (*session.Session, error) {
	s, ok := h.registry.Get(id)
	if !ok || !h.controllerInScope(s) || s.Snapshot().Hidden || s.State() == session.Closed {
		return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	return s, nil
}
func (h *Hub) intakeWritePolicy(s *session.Session) error {
	if readOnly, e := h.IsReadOnlyCollaborationWorker(s.ID); e != nil || readOnly {
		return taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	if h.work != nil {
		state, e := h.work.Collaboration(context.Background())
		if e != nil {
			return taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		if _, task, managed := state.ProjectForSession(s.ID); managed {
			if task != nil && task.Owner != "human" {
				return taskapi.Failure("permission", "known_none", "request_scope_change")
			}
			return taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
	}

	if !h.controls.MobileMayWrite(s.ID) {
		return taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	if p, e := h.PMConfiguration(s.ID); e != nil || p != nil {
		return taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	if scope, e := h.TaskWorkerScope(s); e != nil || scope != nil {
		return taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	if s.Backend() != backend.Codex && s.Backend() != backend.Claude {
		return taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	if s.SettingsSnapshot().Model == "" {
		return taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	return nil
}
func (h *Hub) authorizeIntake(ctx context.Context, c taskapi.VerifiedContext, r taskapi.Request, l taskapi.Locator) (taskapi.Locator, error) {
	if c.BindingKind != "paired_human" || c.Transport != "authenticated_ws" || c.SourceSessionID != "" || !strings.HasPrefix(c.StableScopeID, "device:") {
		return l, taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	var in intakeInput
	if json.Unmarshal(r.Input, &in) != nil {
		return l, taskapi.Failure("invalid_argument", "known_none", "correct_input")
	}
	id := in.SessionID
	if in.Action == "submit" {
		id = in.Target.Session
	}
	s, e := h.intakeSession(id)
	if e != nil {
		return l, e
	}
	if in.Action == "inspect" {
		return l, nil
	}
	if in.Action != "submit" || in.RequestID != r.IdempotencyKey || strings.TrimSpace(in.Goal) == "" {
		return l, taskapi.Failure("invalid_argument", "known_none", "correct_input")
	}
	if in.Target.Instance != h.cfg.InstanceID {
		return l, taskapi.Failure("wrong_authority", "known_none", "refresh_identity")
	}
	ns := taskapi.Namespace{Authority: c.Authority, StableScopeID: c.StableScopeID, Generation: c.NamespaceGeneration, Path: "ordinary", Operation: "intake"}
	if prior, e := h.messageQueue.TaskJournal().Lookup(ctx, ns, r.IdempotencyKey); e == nil {
		if e = h.checkAPIRecord(ctx, c, prior); e != nil {
			return l, e
		}
		digest, _ := taskapi.IntentHash(r)
		if digest != prior.Hash {
			return l, taskapi.Failure("idempotency_conflict", "known_receipt", "lookup_original")
		}
		return l, nil // Original receipt readback, not another effect or config activation.
	} else if failure, ok := e.(*taskapi.APIError); ok && failure.Code == "key_expired" {
		return l, e
	}
	if !apiOwnedAdmissionEnabled {
		return l, taskapi.Failure("busy", "known_none", "read_capabilities")
	}
	if e = h.intakeWritePolicy(s); e != nil {
		return l, e
	}
	thread := ""
	if in.Target.Thread != nil {
		thread = *in.Target.Thread
	}
	if s.ResumeID() != thread || s.SettingsSnapshot().ConfigRevision != in.Target.Revision {
		return l, taskapi.Failure("stale_revision", "known_none", "refresh_identity")
	}
	// Never relabel an existing ordinary/admission request or another device's row.
	if _, found, e := h.messageQueue.Get(s.ID, in.RequestID); e != nil {
		return l, e
	} else if found {
		return l, taskapi.Failure("idempotency_conflict", "known_receipt", "lookup_original")
	}
	return l, nil
}
func (h *Hub) readIntake(ctx context.Context, c taskapi.AuthorizedCommand) (any, error) {
	var in intakeInput
	json.Unmarshal(c.Request.Input, &in)
	s, e := h.intakeSession(in.SessionID)
	if e != nil {
		return nil, e
	}
	v := s.SettingsSnapshot()
	frozenThread := s.ResumeID()
	status, reason := "available", ""
	if err := h.intakeWritePolicy(s); err != nil {
		status = "unsupported"
		if a, ok := err.(*taskapi.APIError); ok {
			status = a.Code
			reason = a.Code
		}
	}
	var thread any
	if s.ResumeID() != "" {
		thread = s.ResumeID()
	}
	var grant any
	if h.dispatches != nil {
		if g, err := h.dispatches.Grant(ctx, s.ID); err == nil {
			grant = g.Enabled
		}
	}
	current, found := h.registry.Get(s.ID)
	if c.Revalidate(ctx) != nil || !found || current != s || current.State() == session.Closed || current.Snapshot().Hidden || !h.controllerInScope(current) || current.ResumeID() != frozenThread || !reflect.DeepEqual(current.SettingsSnapshot(), v) {
		return nil, taskapi.Failure("stale_revision", "known_none", "refresh_identity")
	}
	if err := h.intakeWritePolicy(current); err != nil && status == "available" {
		return nil, taskapi.Failure("stale_revision", "known_none", "refresh_identity")
	}
	return map[string]any{"status": status, "reason": reason, "session_id": s.ID, "target": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "expected_native_thread_id": thread, "expected_config_revision": v.ConfigRevision}, "current_configuration": map[string]string{"backend": string(v.Backend), "model": v.Model, "effort": v.Effort, "sandbox": v.Sandbox, "cwd": v.Cwd}, "configuration_known": v.Model != "", "delegation_grant_enabled": grant, "scope_behavior": "preserve_current_session", "server_verified_binding": "paired_human", "max_frame_bytes": 262144, "max_goal_chars": 32000}, nil
}
func (h *Hub) mutateIntake(ctx context.Context, c taskapi.AuthorizedCommand) (any, error) {
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	if e := c.Revalidate(ctx); e != nil {
		return nil, e
	}
	if _, e := h.Authorize(ctx, c.Caller, c.Request); e != nil {
		return nil, e
	}
	if previous, e := h.messageQueue.TaskJournal().Lookup(ctx, c.Namespace, c.Request.IdempotencyKey); e == nil {
		return h.apiReceipt(ctx, c, previous)
	}
	var in intakeInput
	json.Unmarshal(c.Request.Input, &in)
	s, e := h.intakeSession(in.Target.Session)
	if e != nil {
		return nil, e
	}
	images := make([]backend.ImageAttachment, 0, len(in.Images))
	for _, i := range in.Images {
		if _, e := base64.StdEncoding.DecodeString(i.Data); e != nil {
			return nil, taskapi.Failure("invalid_argument", "known_none", "correct_input")
		}
		images = append(images, backend.ImageAttachment{Data: i.Data, MediaType: i.MediaType})
	}
	files := make([]backend.FileAttachment, 0, len(in.Files))
	for _, f := range in.Files {
		files = append(files, backend.FileAttachment{AttachmentID: f.AttachmentID, Name: f.Name, Content: f.Content, MediaType: f.MediaType, SizeBytes: f.SizeBytes})
	}
	content, resolved, e := h.resolveUploadedVideos(s.ID, in.Goal, files)
	if e != nil {
		return nil, taskapi.Failure("invalid_argument", "known_none", "correct_input")
	}
	config := session.ConfigurationFrom(s.SettingsSnapshot())
	thread := ""
	if in.Target.Thread != nil {
		thread = *in.Target.Thread
	}
	source := map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "request_id": in.RequestID, "config_revision": in.Target.Revision}
	if thread != "" {
		source["native_thread_id"] = thread
	}
	metadata, _ := json.Marshal(apiMetadata{Request: c.Request, Caller: c.Caller, Transport: c.Caller.Transport, Intake: &intakeMetadata{Source: source, Goal: in.Goal, Configuration: config}})
	record := taskapi.IntentRecord{ReceiptID: "tr_" + randomID(), NativeID: s.ID + "/" + in.RequestID, SessionID: s.ID, RequestID: in.RequestID, TaskID: apiID(h.cfg.InstanceID, "ordinary", s.ID+"/"+in.RequestID), Metadata: metadata}
	payload, _ := json.Marshal(queuedPayload{FormalIntake: true, Content: content, Images: images, Files: resolved, MessagePurpose: "instruction", OwnerDevice: strings.TrimPrefix(c.Caller.StableScopeID, "device:"), Configuration: &config, ExpectedTarget: &dispatchTargetExpectation{ThreadID: thread, Revision: in.Target.Revision}})
	names := []string{}
	for _, f := range files {
		names = append(names, f.Name)
	}
	entry, created, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: in.RequestID, Content: truncateGraphemes(in.Goal, 4000), ImageCount: len(images), FileNames: names, Payload: payload, PayloadHash: c.IntentHash, API: &messagequeue.APIAdmission{Command: c, Record: record, CanCommit: func() bool {
		current, found := h.registry.Get(s.ID)
		return c.Revalidate(ctx) == nil && found && current == s && !current.Snapshot().Hidden && current.State() != session.Closed && h.controllerInScope(current) && h.intakeWritePolicy(current) == nil && current.ResumeID() == thread && current.SettingsSnapshot().ConfigRevision == in.Target.Revision
	}}})
	if errors.Is(e, messagequeue.ErrConflict) {
		return nil, taskapi.Failure("idempotency_conflict", "known_receipt", "lookup_original")
	}
	if e != nil {
		return nil, e
	}
	stored, e := h.messageQueue.TaskJournal().Lookup(ctx, c.Namespace, c.Request.IdempotencyKey)
	if e != nil {
		return nil, e
	}
	if created {
		h.submitAPINamed(s, entry.RequestID)
	}
	return h.apiReceipt(ctx, c, stored)
}
