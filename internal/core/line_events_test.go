package core

import (
	"bytes"
	"context"
	"encoding/json"
	"everything-go/internal/linebridge"
	"strings"
	"testing"
)

func TestLineEventsEncryptedAndDeviceScoped(t *testing.T) {
	h, _ := newTestHub(t)
	inbox := attachEventStore(t, h)
	h.pairing.OpenEnrollment(0)
	if err := h.pairing.Claim("fixture-events-owner", "owner"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	store, err := linebridge.OpenStore(t.TempDir(), "bridge-test", bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := linebridge.NewService(store, nil, linebridge.Config{})
	h.SetLineService(s)
	defer s.Close()
	a, err := store.CreateAccount(ctx, "personal", "device:owner", "/fixture/line", "/fixture/helper")
	if err != nil {
		t.Fatal(err)
	}
	sink := h.LineEventSink(store)
	event := linebridge.RuntimeEvent{Kind: "RECEIVE_MESSAGE", Key: "fixture-key", Message: &linebridge.Message{ID: "lm_fixture", Text: "private event text"}}
	if err = sink.Commit(ctx, a.ID, "talk", "", []linebridge.RuntimeEvent{event}, "private cursor"); err != nil {
		t.Fatal(err)
	}
	stored, _ := inbox.Snapshot(ctx, "owner", 0)
	if len(stored.Items) != 1 {
		t.Fatal("missing durable event")
	}
	encoded, _ := json.Marshal(stored)
	if strings.Contains(string(encoded), "private event text") {
		t.Fatal("plaintext stored in inbox")
	}
	owner := h.filterLineSnapshot(ctx, "owner", stored)
	if len(owner.Items) != 1 || owner.Items[0].Body != "private event text" {
		t.Fatal("owner projection unavailable")
	}
	other := h.filterLineSnapshot(ctx, "other", stored)
	if len(other.Items) != 0 {
		t.Fatal("account data leaked to ungranted device")
	}
	if h.authorizeLineEvent(ctx, "other", stored.Items[0].ID) {
		t.Fatal("ungranted mark-read authorized")
	}
	if err = sink.Commit(ctx, a.ID, "talk", "", nil, "stale"); err == nil {
		t.Fatal("stale checkpoint writer accepted")
	}
	cursor, err := sink.Cursor(ctx, a.ID, "talk")
	if err != nil || cursor != "private cursor" {
		t.Fatal("checkpoint changed", err)
	}
	if err = store.Grant(ctx, linebridge.Caller{Subject: "device:owner"}, a.ID, "device:owner", nil); err != nil {
		t.Fatal(err)
	}
	if len(h.filterLineSnapshot(ctx, "owner", stored).Items) != 0 {
		t.Fatal("revoked grant still exposes event")
	}
	if h.offline.Len() != 0 {
		t.Fatal("LINE content copied into global offline journal")
	}
}
func TestGenericIngressCannotForgeLineSource(t *testing.T) {
	if validateExternalEvent(externalEventRequest{Source: "line_personal.ln_forged", Kind: "message", Title: "LINE", Severity: "info"}, "key") == nil {
		t.Fatal("reserved LINE event source accepted")
	}
}
