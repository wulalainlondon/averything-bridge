package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type continuityProviderFixture struct {
	delegationFixtureProvider
	calls chan string
}

func (p continuityProviderFixture) ContinueDelegation(s *session.Session, r, c string, in backend.DelegationContinuation) (any, error) {
	p.calls <- fmt.Sprint(s.ID, "/", r, "/", c)
	return map[string]string{"acceptance": "known_receipt"}, nil
}
func (p continuityProviderFixture) DelegationRounds(*session.Session, string, uint64) (any, error) {
	return nil, nil
}
func (p continuityProviderFixture) BindDelegationQA(*session.Session, string, string, string, string) (any, error) {
	return nil, nil
}
func (p continuityProviderFixture) RecordDelegationReview(*session.Session, string, string, string, string) (any, error) {
	return nil, nil
}
func (p continuityProviderFixture) PreserveDelegationContext(*session.Session) (bool, error) {
	return true, nil
}
func TestCodexContinuityCatalogAndBoundTransport(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	provider := continuityProviderFixture{calls: make(chan string, 4)}
	c.SetDelegationProvider(provider)
	s := session.NewRegistry().Create("parent", "Parent", t.TempDir(), backend.Codex, "", "workspace-write", "native-thread")
	c.threadToSession["native-thread"] = s
	params := map[string]any{}
	c.applyDelegationThreadTools(s, params)
	raw, _ := json.Marshal(params)
	for _, tool := range []string{"continue_delegation", "get_delegation_rounds", "bind_delegation_qa", "record_delegation_review"} {
		if !strings.Contains(string(raw), tool) {
			t.Fatal("missing tool", tool)
		}
	}
	st := c.state(s.ID)
	st.mu.Lock()
	st.reqID, st.currentTurnID, st.turnActive = "r_parent", "native-turn", true
	st.mu.Unlock()
	writer := &rpcCaptureWriter{writes: make(chan []byte, 16)}
	c.rpc.setWriter(writer)
	for _, tc := range []struct {
		name, turn string
		extra      bool
		want       bool
	}{{"foreign-turn", "foreign", false, false}, {"unknown-field", "native-turn", true, false}, {"original-parent", "native-turn", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			args := map[string]any{"delegation_id": "dg_original", "expected_previous_round_id": "dg_original", "instance_id": "i1", "expected_thread_id": "child-thread", "expected_config_revision": 0, "content": "fix", "mode": "queue"}
			if tc.extra {
				args["sandbox"] = "danger-full-access"
			}
			raw, _ := json.Marshal(map[string]any{"threadId": "native-thread", "namespace": "bridge_sessions", "tool": "continue_delegation", "turnId": tc.turn, "callId": "call-one", "arguments": args})
			c.handleServerRequest(1, "item/tool/call", raw)
			deadline := time.After(time.Second)
			for {
				select {
				case response := <-writer.writes:
					if !strings.Contains(string(response), "contentItems") {
						continue
					}
					if strings.Contains(string(response), `"success":true`) != tc.want {
						t.Fatal(string(response))
					}
					return
				case <-deadline:
					t.Fatal("tool response missing")
				}
			}
		})
	}
	select {
	case got := <-provider.calls:
		if got != "parent/r_parent/call-one" {
			t.Fatal("forged caller", got)
		}
	default:
		t.Fatal("continuation not routed")
	}
	select {
	case got := <-provider.calls:
		t.Fatal("unauthorized extra call", got)
	default:
	}
	worker := session.NewRegistry().Create("s_dg_worker", "Worker", t.TempDir(), backend.Codex, "", "read-only", "worker-thread")
	params = map[string]any{}
	c.applyDelegationThreadTools(worker, params)
	if params["dynamicTools"] != nil {
		t.Fatal("worker got parent management tools")
	}
}
func TestCodexContinuityRefusesAutomaticContextRollover(t *testing.T) {
	root := t.TempDir()
	id := "019fb03e-0666-7ce3-8f99-8f19c79540e2"
	if e := os.WriteFile(filepath.Join(root, "rollout-2026-08-17T00-00-00-"+id+".jsonl"), []byte("synthetic large rollout\n"), 0600); e != nil {
		t.Fatal(e)
	}
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	c.dataDir = t.TempDir()
	c.appServerMode = "stdio"
	c.rolloverEnabled = true
	c.coldResumeMaxBytes = 1
	c.SetDelegationProvider(continuityProviderFixture{})
	s := session.NewRegistry().Create("s_dg_original", "Worker", t.TempDir(), backend.Codex, "", "read-only", id)
	writer := &rpcCaptureWriter{writes: make(chan []byte, 4)}
	c.rpc.setWriter(writer)
	if e := c.ensureThread(s, c.state(s.ID)); e == nil || !strings.Contains(e.Error(), "successor_required") {
		t.Fatal("automatic rollover accepted", e)
	}
	if s.ResumeID() != id || len(s.Snapshot().HistoricalResumeIDs) != 0 {
		t.Fatal("context reset")
	}
	select {
	case rpc := <-writer.writes:
		t.Fatal("unexpected runtime RPC", string(rpc))
	default:
	}
}
func TestCodexContinuityStaleResumeRefusesNewThread(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.sessionsRoot = t.TempDir()
	c.dataDir = t.TempDir()
	c.SetDelegationProvider(continuityProviderFixture{})
	s := session.NewRegistry().Create("s_dg_original", "Worker", t.TempDir(), backend.Codex, "", "read-only", "original-thread")
	writer := &rpcCaptureWriter{writes: make(chan []byte, 4)}
	c.rpc.setWriter(writer)
	go func() {
		raw := <-writer.writes
		var frame struct {
			ID     int
			Method string
		}
		json.Unmarshal(raw, &frame)
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"error":{"code":-32000,"message":"thread not found"}}`, frame.ID)))
	}()
	if e := c.ensureThread(s, c.state(s.ID)); e == nil || !strings.Contains(e.Error(), "successor_required") {
		t.Fatal(e)
	}
	if s.ResumeID() != "original-thread" {
		t.Fatal("thread replaced")
	}
	select {
	case rpc := <-writer.writes:
		t.Fatal("silently started successor", string(rpc))
	default:
	}
}

func TestCodexContinuityUnavailableWorkspaceHasNoHomeFallback(t *testing.T) {
	for _, loaded := range []bool{false, true} {
		t.Run(fmt.Sprint(loaded), func(t *testing.T) {
			c := NewCodex(&capSink{}, "codex")
			c.sessionsRoot = t.TempDir()
			c.dataDir = t.TempDir()
			c.SetDelegationProvider(continuityProviderFixture{})
			s := session.NewRegistry().Create("s_dg_original", "Worker", filepath.Join(t.TempDir(), "missing"), backend.Codex, "", "workspace-write", "original-thread")
			st := c.state(s.ID)
			if loaded {
				st.threadID = "original-thread"
			}
			writer := &rpcCaptureWriter{writes: make(chan []byte, 4)}
			c.rpc.setWriter(writer)
			if e := c.ensureThread(s, st); e == nil || e.Error() != "delegation_original_workspace_unavailable" {
				t.Fatal("home fallback allowed", e)
			}
			if s.ResumeID() != "original-thread" {
				t.Fatal("thread changed")
			}
			select {
			case raw := <-writer.writes:
				t.Fatal("runtime fallback RPC", string(raw))
			default:
			}
		})
	}
}
