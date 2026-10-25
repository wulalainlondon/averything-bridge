package core

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"testing"
)

func TestVisualPreferenceDurableQueueRetainsOriginalAndHashes(t *testing.T) {
	h, fe := newTestHub(t)
	c := sharedReadClient(t, h, "visual-owner")
	h.registry.Create("s1", "visual", t.TempDir(), backend.Codex, "", "", "")
	fe.onSend = func(s *session.Session, id, content string) {
		if content != "Original é" {
			t.Errorf("queue rewrote original: %q", content)
		}
		h.Emit(protocol.NewDone(s.ID, id))
	}
	route(h, c, `{"type":"message","session_id":"s1","request_id":"visual-one","message_purpose":"instruction","content":"Original é","visual_presentation":"auto-v1"}`)
	waitForType(t, c, "message_ack")
	entry, found, err := h.messageQueue.Get("s1", "visual-one")
	if err != nil || !found {
		t.Fatal(err)
	}
	var p queuedPayload
	if err = json.Unmarshal(entry.Payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.Content != "Original é" || p.VisualPresentation != "auto-v1" || len(p.VisualUserHash) != 64 || len(p.VisualEffectiveHash) != 64 || p.VisualUserHash == p.VisualEffectiveHash {
		t.Fatalf("invalid persisted visual metadata: mode %q hashes %d/%d", p.VisualPresentation, len(p.VisualUserHash), len(p.VisualEffectiveHash))
	}
}
func TestInvalidVisualPreferenceCannotEnterQueue(t *testing.T) {
	h, _ := newTestHub(t)
	c := sharedReadClient(t, h, "visual-refusal")
	h.registry.Create("s1", "visual", t.TempDir(), backend.Codex, "", "", "")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"visual-bad","content":"Original","visual_presentation":"execute-js"}`)
	waitForType(t, c, "error")
	_, found, _ := h.messageQueue.Get("s1", "visual-bad")
	if found {
		t.Fatal("invalid mode persisted")
	}
}
