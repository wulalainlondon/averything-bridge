package core

import (
	"encoding/json"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
)

func TestFloatingQueueDetailIsExplicitTextOnlyAndSessionBound(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	h.registry.Create("s1", "QA", t.TempDir(), backend.Codex, "", "read-only", "")
	h.registry.Create("other", "Other QA", t.TempDir(), backend.Codex, "", "read-only", "")
	content := strings.Repeat("完整排隊內容 ", 1200)
	payload, _ := json.Marshal(queuedPayload{Content: content, Files: []backend.FileAttachment{{Name: "qa.txt", Content: "attachment bytes must not be exposed"}}})
	if _, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: "s1", RequestID: "detail-qa", Content: "preview", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	route(h, c, `{"type":"request_message_queue","session_id":"s1"}`)
	regular := waitForType(t, c, "message_queue_snapshot")
	if regular["detail"] != nil {
		t.Fatal("routine snapshot included full content")
	}
	route(h, c, `{"type":"request_message_queue","session_id":"s1","request_id":"detail-qa"}`)
	detail := waitForType(t, c, "message_queue_snapshot")
	value, ok := detail["detail"].(map[string]any)
	if !ok || value["full_content"] != content || detail["detail_request_id"] != "detail-qa" {
		t.Fatal("full queue text missing")
	}
	encoded, _ := json.Marshal(detail)
	if strings.Contains(string(encoded), "attachment bytes must not be exposed") {
		t.Fatal("attachment payload exposed")
	}
	route(h, c, `{"type":"request_message_queue","session_id":"other","request_id":"detail-qa"}`)
	if denied := waitForType(t, c, "error"); denied["code"] != "queue_not_found" {
		t.Fatal(denied)
	}
	entry, found, err := h.messageQueue.Get("s1", "detail-qa")
	if err != nil || !found || entry.State != messagequeue.Queued {
		t.Fatal("read altered queue state")
	}
}
