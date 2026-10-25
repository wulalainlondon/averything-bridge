package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// This is a MOCK transport test of the real provider method, not a live probe.
func TestVisibilityOriginalRPCReadOnlyMetadataPath(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.appServerSocket = filepath.Join(t.TempDir(), "missing.sock")
	c.RegisterExistingDaemonObservation()
	s := session.NewRegistry().Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("root")
	st := c.state(s.ID)
	st.threadID = "root"
	st.agents["qa"] = &codexAgent{id: "qa", parentID: "root", description: "PRIVATE_PROMPT", output: "PRIVATE_OUTPUT"}
	w := &rpcCaptureWriter{writes: make(chan []byte, 2)}
	c.rpc.setWriter(w)
	go func() {
		for i := 0; i < 2; i++ {
			var frame struct {
				ID     int
				Method string
				Params map[string]any
			}
			if err := json.Unmarshal(<-w.writes, &frame); err != nil {
				t.Error(err)
				return
			}
			if frame.Method != "thread/read" || frame.Params["includeTurns"] != false || len(frame.Params) != 2 {
				t.Errorf("not read-only metadata: %+v", frame)
			}
			id := frame.Params["threadId"].(string)
			parent := "null"
			if id == "qa" {
				parent = `"root"`
			}
			c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":%q,"parentThreadId":%s,"sessionId":"native-tree","status":{"type":"active"},"source":"appServer","turns":[],"preview":"PRIVATE_PREVIEW","path":"PRIVATE_PATH"}}}`, frame.ID, id, parent)))
		}
	}()
	out := c.ObserveInternalAgents(context.Background(), s, "authority")
	if out.Availability != "live" || len(out.Children) != 1 || out.Children[0].Source != "native_probe" || out.Children[0].Association != "unmatched" || out.Children[0].State != "active" {
		t.Fatalf("path not observed: %+v", out)
	}
	raw, _ := json.Marshal(out)
	if strings.Contains(string(raw), "PRIVATE_") {
		t.Fatal("private payload retained")
	}
	if st.turnActive {
		t.Fatal("read starts task")
	}
}

func TestVisibilityMissingWriterUnsupportedNoLaunch(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.appServerSocket = filepath.Join(t.TempDir(), "missing.sock")
	c.RegisterExistingDaemonObservation()
	s := session.NewRegistry().Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("root")
	if out := c.ObserveInternalAgents(context.Background(), s, "authority"); out.Availability != "unsupported" {
		t.Fatalf("absent source promoted: %+v", out)
	}
}

func TestVisibilityRestartRejectsLateOldRead(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.appServerSocket = filepath.Join(t.TempDir(), "missing.sock")
	c.RegisterExistingDaemonObservation()
	s := session.NewRegistry().Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("root")
	c.state(s.ID).threadID = "root"
	w := &rpcCaptureWriter{writes: make(chan []byte, 1)}
	c.rpc.setWriter(w)
	go func() {
		var f struct{ ID int }
		_ = json.Unmarshal(<-w.writes, &f)
		c.rpc.setWriter(nil)
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":"root","sessionId":"native-tree","status":{"type":"active"},"turns":[]}}}`, f.ID)))
	}()
	if out := c.ObserveInternalAgents(context.Background(), s, "authority"); out.Availability != "unknownstale" || out.ObservedAt != "" {
		t.Fatalf("late old source promoted: %+v", out)
	}
}

func TestVisibilityStdioAndUnregisteredBootRefuseWithoutRPCWrite(t *testing.T) {
	for _, mode := range []string{"stdio", "daemon"} {
		t.Run(mode, func(t *testing.T) {
			c := NewCodex(&capSink{}, "forbidden-model-binary")
			c.appServerMode = mode
			c.appServerSocket = filepath.Join(t.TempDir(), "missing.sock")
			s := session.NewRegistry().Create("s1", "S", t.TempDir(), backend.Codex, "", "", "root")
			w := &rpcCaptureWriter{writes: make(chan []byte, 1)}
			c.rpc.setWriter(w)
			if mode == "stdio" {
				c.RegisterExistingDaemonObservation()
			}
			out := c.ObserveInternalAgents(context.Background(), s, "authority")
			if out.Availability != "unsupported" || out.ObservedAt != "" || c.proc != nil || c.state(s.ID).turnActive {
				t.Fatal("unsupported mode became active")
			}
			select {
			case raw := <-w.writes:
				t.Fatal("unsupported mode sent RPC", string(raw))
			default:
			}
		})
	}
}
