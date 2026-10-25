package goexec

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// Recovery only for an explicitly selected temporary collaboration test thread.
// Never restarts the daemon or interrupts an ordinary project conversation.
func TestCollaborationCleanupAbandonedLiveProbe(t *testing.T) {
	threadID, turnID := os.Getenv("BRIDGE_COLLAB_CLEANUP_THREAD"), os.Getenv("BRIDGE_COLLAB_CLEANUP_TURN")
	if threadID == "" || turnID == "" {
		t.Skip("explicit isolated live-probe cleanup")
	}
	c := NewCodex(&capSink{}, "codex")
	if err := c.ConnectExistingDaemon(); err != nil {
		t.Fatal(err)
	}
	defer c.remoteCancel()
	raw, err := c.rpcCall("thread/read", map[string]any{"threadId": threadID, "includeTurns": false}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Thread struct {
			Cwd    string          `json:"cwd"`
			Status json.RawMessage `json:"status"`
		} `json:"thread"`
	}
	if err = json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(response.Thread.Cwd, "TestCollaborationV2LiveCodexClosedLoop") {
		t.Fatal("refusing to interrupt a non-test conversation")
	}
	if _, err = c.rpcCall("turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, 15*time.Second); err != nil {
		if strings.Contains(err.Error(),"no active turn"){t.Log("ISOLATED_TEST_TURN_ALREADY_STOPPED",threadID);return}
		t.Fatal(err)
	}
	t.Log("ISOLATED_TEST_TURN_INTERRUPT_SENT", threadID)
}
