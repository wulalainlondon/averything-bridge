package core

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/executor"
	"everything-go/internal/executor/goexec"
	"everything-go/internal/governance"
	"everything-go/internal/history"
	"everything-go/internal/session"
)

// Opt-in end-to-end acceptance against the installed shared Codex app-server.
// All Sessions and files are isolated; no production Bridge is restarted.
func TestLiveDelegationParentChildReturn(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_RUN_DELEGATION_LIVE") != "1" {
		t.Skip("set EVERYTHING_GO_RUN_DELEGATION_LIVE=1")
	}
	dir := t.TempDir()
	reg := session.NewRegistry()
	reg.AttachStore(session.NewStore(filepath.Join(dir, "sessions.json")))
	h := NewHub(reg, Config{InstanceID: "delegation-live", InstanceName: "Delegation QA", DataDir: dir, RootDir: dir},
		governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	defer h.messageQueue.Close()
	defer h.delegations.Close()
	sink := executor.NewTerminalSink(h)
	codex := goexec.NewCodex(sink, "codex")
	codex.SetDataDir(dir)
	codex.SetDelegationProvider(h)
	mux := executor.NewReliableMux(map[string]executor.Executor{backend.Codex: codex}, codex, sink)
	h.SetExecutor(mux)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	h.StartDelegationScheduler(ctx)
	parent := reg.Create("delegation-live-parent", "Delegation QA parent", dir, backend.Codex, "gpt-6-luna", "read-only", "")
	parent.SetEffort("low")
	if err := reg.PersistDurably(); err != nil {
		t.Fatal(err)
	}
	client := &Client{hub: h, deviceID: "delegation-qa", send: make(chan []byte, 64), quit: make(chan struct{}), ctx: ctx}
	h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: parent.ID, RequestID: "live-parent-task",
		Content: `Use the bridge_sessions.delegate_session tool exactly once to create a fresh child conversation in the current directory. Name it "Delegation QA child". Its standalone instruction is: "Reply exactly CHILD_DELEGATION_OK. Do not call tools." After the tool returns, say only that the child is working. When Bridge sends the child result back to this conversation, respond with PARENT_DELEGATION_OK.`})
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("delegation live acceptance timed out")
		case <-ticker.C:
			items, err := h.delegations.ListByParent(ctx, parent.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) == 0 {
				continue
			}
			r := items[0]
			if r.DeliveryState == "failed" {
				t.Fatalf("parent delivery failed: %+v", r)
			}
			if r.DeliveryState != "delivered" {
				continue
			}
			if r.TerminalStatus != "completed" || !strings.Contains(r.Result, "CHILD_DELEGATION_OK") {
				t.Fatalf("child result not captured: %+v", r)
			}
			answer, found, err := codex.FinalAnswerForRequest(parent, r.ParentRequestID)
			if err != nil || !found || !strings.Contains(answer, "PARENT_DELEGATION_OK") {
				t.Fatalf("parent did not process child result: answer=%q found=%v err=%v", answer, found, err)
			}
			provider, ok := mux.ProviderFor(parent)
			if !ok {
				t.Fatal("missing parent history provider")
			}
			loaded, err := provider.LoadHistory(parent.ResumeID(), history.Opts{Limit: 200})
			if err != nil {
				t.Fatal(err)
			}
			provenance := false
			for _, message := range h.annotateDelegationHistory(parent.ID, loaded.Messages) {
				if message["request_id"] == r.ParentRequestID && message["source"] == "codex" && message["origin"] == "bridge_delegation_result" && message["source_session_id"] == r.ChildSessionID {
					provenance = true
				}
			}
			if !provenance {
				t.Fatal("parent history did not preserve Bridge delegation provenance")
			}
			return
		}
	}
}
