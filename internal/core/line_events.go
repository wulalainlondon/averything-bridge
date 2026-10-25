package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"everything-go/internal/eventinbox"
	"everything-go/internal/linebridge"
	"strings"
)

const lineSourcePrefix = "line_personal."

type lineEventSink struct {
	hub   *Hub
	store *linebridge.Store
}

func (h *Hub) LineEventSink(store *linebridge.Store) linebridge.EventSink {
	return &lineEventSink{hub: h, store: store}
}
func (s *lineEventSink) Cursor(ctx context.Context, account, stream string) (string, error) {
	if s.hub.events == nil {
		return "", linebridge.Fail("line_event_store_unavailable")
	}
	data, err := s.hub.events.SourceCursor(ctx, lineSourcePrefix+account, stream)
	if err != nil || len(data) == 0 {
		return "", err
	}
	var value string
	err = s.store.OpenEnvelope(account, data, &value)
	return value, err
}
func (s *lineEventSink) Commit(ctx context.Context, account, stream, previous string, events []linebridge.RuntimeEvent, next string) error {
	if s.hub.events == nil {
		return linebridge.Fail("line_event_store_unavailable")
	}
	source := lineSourcePrefix + account
	current, err := s.hub.events.SourceCursor(ctx, source, stream)
	if err != nil {
		return err
	}
	var expected string
	if len(current) > 0 {
		if err = s.store.OpenEnvelope(account, current, &expected); err != nil {
			return err
		}
	}
	if expected != previous {
		return linebridge.Fail("line_cursor_conflict")
	}
	if previous == next && len(events) == 0 {
		return nil
	}
	cursor, err := s.store.SealEnvelope(account, next)
	if err != nil {
		return err
	}
	input := []eventinbox.Input{}
	for _, event := range events {
		value, err := s.store.SealEnvelope(account, event)
		if err != nil {
			return err
		}
		kind := "message.received"
		severity := "info"
		if event.Kind == "sync_gap" || event.Kind == "decrypt_failed" {
			kind = event.Kind
			severity = "warning"
		}
		metadata, _ := json.Marshal(map[string]string{"account_ref": account, "target_ref": event.Target})
		input = append(input, eventinbox.Input{Source: source, EventKey: event.Key, Kind: kind, Severity: severity, Title: "LINE 訊息", Body: "lineenc:v1:" + base64.RawStdEncoding.EncodeToString(value), Data: metadata})
	}
	committed, err := s.hub.events.CommitSourceBatch(ctx, source, stream, current, cursor, input)
	if err != nil {
		return err
	}
	for _, event := range committed {
		s.hub.publishLineEvent(event)
	}
	return nil
}
func (h *Hub) projectLineEvent(ctx context.Context, device string, event eventinbox.Event) (eventinbox.Event, bool) {
	if !strings.HasPrefix(event.Source, lineSourcePrefix) {
		return event, true
	}
	account := strings.TrimPrefix(event.Source, lineSourcePrefix)
	if !h.LineSubjectAllowed("device:" + device) {
		return event, false
	}
	if h.lineService == nil {
		return event, false
	}
	store := h.lineService.Store()
	caller := linebridge.Caller{Subject: "device:" + device}
	if !store.Allowed(ctx, caller, account, "events") || !store.Allowed(ctx, caller, account, "messages.read") {
		return event, false
	}
	if !strings.HasPrefix(event.Body, "lineenc:v1:") {
		return event, false
	}
	value, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(event.Body, "lineenc:v1:"))
	if err != nil {
		return event, false
	}
	var payload linebridge.RuntimeEvent
	if store.OpenEnvelope(account, value, &payload) != nil {
		return event, false
	}
	if a, err := store.Account(ctx, account); err == nil {
		event.Title = a.Label + " · LINE"
	}
	if target, err := store.Target(ctx, account, payload.Target); err == nil {
		event.Title += " · " + target.Name
	}
	event.Body = ""
	if payload.Message != nil {
		event.Body = payload.Message.Text
	}
	if payload.Code != "" {
		event.Body = payload.Code
	}
	data, _ := json.Marshal(map[string]any{"account_ref": account, "target_ref": payload.Target, "line_event": payload})
	event.Data = data
	return event, true
}
func (h *Hub) filterLineSnapshot(ctx context.Context, device string, snapshot eventinbox.Snapshot) eventinbox.Snapshot {
	items := make([]eventinbox.View, 0, len(snapshot.Items))
	for _, view := range snapshot.Items {
		event, ok := h.projectLineEvent(ctx, device, view.Event)
		if ok {
			view.Event = event
			items = append(items, view)
		}
	}
	snapshot.Items = items
	return snapshot
}
func (h *Hub) authorizeLineEvent(ctx context.Context, device, id string) bool {
	event, err := h.events.Get(ctx, id)
	if err != nil {
		return false
	}
	_, ok := h.projectLineEvent(ctx, device, event)
	return ok
}
func (h *Hub) publishLineEvent(event eventinbox.Event) {
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		if projected, ok := h.projectLineEvent(context.Background(), c.deviceID, event); ok {
			c.enqueueEventUnlogged(h.client.ExternalEventUpsert(eventinbox.View{Event: projected}))
		}
	}
	if h.fcm != nil {
		go h.fcm.NotifyScopedExternalEvent(h.cfg.InstanceID, event.ID, event.Source, event.Kind, func(device string) bool { _, ok := h.projectLineEvent(context.Background(), device, event); return ok })
	}
	// LINE events remain inert until a user explicitly enables an automation route.
}

func (h *Hub) externalSnapshot(ctx context.Context, device string) (eventinbox.Snapshot, error) {
	return h.externalSnapshotMode(ctx, device, false)
}
func (h *Hub) externalSnapshotMode(ctx context.Context, device string, archived bool) (eventinbox.Snapshot, error) {
	sources := []string{}
	if h.lineService != nil {
		store := h.lineService.Store()
		c := linebridge.Caller{Subject: "device:" + device}
		accounts, err := store.Accounts(ctx, c)
		if err != nil {
			return eventinbox.Snapshot{}, err
		}
		for _, account := range accounts {
			if store.Allowed(ctx, c, account.ID, "events") && store.Allowed(ctx, c, account.ID, "messages.read") {
				sources = append(sources, lineSourcePrefix+account.ID)
			}
		}
	}
	var snapshot eventinbox.Snapshot
	var err error
	if archived {
		snapshot, err = h.events.SnapshotForSourcesIncludingArchived(ctx, device, 0, lineSourcePrefix, sources)
	} else {
		snapshot, err = h.events.SnapshotForSources(ctx, device, 0, lineSourcePrefix, sources)
	}
	if err != nil {
		return snapshot, err
	}
	return h.filterLineSnapshot(ctx, device, snapshot), nil
}
