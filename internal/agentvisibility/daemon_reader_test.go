package agentvisibility

import (
	"context"
	"encoding/json"
	"github.com/coder/websocket"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Synthetic native schema oracle, real Unix/WS framing; not provider QA.
func TestDaemonMetadataAttachmentBoundedReadAndRetiredSocket(t *testing.T) {
	dir, e := os.MkdirTemp("", "metadata-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "s")
	l, e := net.Listen("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	requests := make(chan string, 8)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		c, e := websocket.Accept(w, q, nil)
		if e != nil {
			return
		}
		defer c.CloseNow()
		for {
			_, raw, e := c.Read(q.Context())
			if e != nil {
				return
			}
			var req struct {
				ID     int            `json:"id"`
				Method string         `json:"method"`
				Params map[string]any `json:"params"`
			}
			if json.Unmarshal(raw, &req) != nil {
				return
			}
			requests <- req.Method
			if req.Method == "initialized" {
				continue
			}
			var result any = map[string]any{}
			if req.Method == "thread/read" {
				if req.Params["includeTurns"] != false || req.Params["threadId"] != "fixture-root" {
					return
				}
				result = map[string]any{"thread": map[string]any{"id": "fixture-root", "sessionId": "fixture-tree", "status": map[string]any{"type": "active"}, "turns": []any{}, "preview": "PRIVATE_FIXTURE_MUST_NOT_RETAIN"}}
			}
			b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
			if c.Write(q.Context(), websocket.MessageText, b) != nil {
				return
			}
		}
	})}
	defer server.Close()
	go server.Serve(l)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, e := OpenExistingDaemonMetadata(ctx, path, Scope{Authority: "fixture-authority", SessionID: "fixture-session", RootActorID: "fixture-root"}, 7)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if _, e = r.ReadThread(ctx, "UNOWNED"); e == nil {
		t.Fatal("unselected thread read")
	}
	p, e := (SelectedDaemonReader{RPC: r, Now: time.Now}).Probe(ctx, r.Scope())
	if e != nil || p.Root.ID != "fixture-root" || p.Root.Status != "active" || p.Coverage != "partial_selected_native_root" {
		t.Fatal("bounded read failed", e)
	}
	b, _ := json.Marshal(p)
	if string(b) == "" || len(b) > 3000 {
		t.Fatal("unexpected retained data")
	}
	if e = os.Remove(path); e != nil {
		t.Fatal(e)
	}
	if r.Scope().Valid() {
		t.Fatal("retired socket scope remained valid")
	}
	for _, wanted := range []string{"initialize", "initialized", "thread/read"} {
		if got := <-requests; got != wanted {
			t.Fatalf("method %s wanted %s", got, wanted)
		}
	}
	select {
	case method := <-requests:
		t.Fatalf("unexpected provider method %s", method)
	default:
	}
}

func TestMissingSelectedDaemonReaderIsUnsupported(t *testing.T) {
	reader := SelectedDaemonReader{Now: time.Now}
	if reader.CurrentScope().Valid() {
		t.Fatal("missing original callback acquired scope")
	}
	if _, e := reader.Probe(context.Background(), Scope{}); e != ErrUnsupported {
		t.Fatal("missing original callback did not refuse")
	}
}
