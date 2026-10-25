package goexec

import (
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/linecontrol"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"sync"
)

type lineCommandProof struct {
	caller      backend.SessionControlCaller
	fingerprint string
}
type lineOriginState struct {
	mu      sync.Mutex
	proofs  map[string]lineCommandProof
	blocked bool
}

func (c *Codex) observeLineNative(s *session.Session, method string, raw json.RawMessage) {
	if s == nil || method != "item/started" || len(raw) > 256<<10 {
		return
	}
	var p struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
		Item     struct {
			ID, Type, Source string
			CommandActions   []struct{ Type, Command string }
		} `json:"item"`
	}
	if json.Unmarshal(raw, &p) != nil || p.ThreadID != s.ResumeID() || p.Item.ID == "" || p.Item.Type != "commandExecution" || (p.Item.Source != "agent" && p.Item.Source != "unifiedExecStartup") || len(p.Item.CommandActions) != 1 || p.Item.CommandActions[0].Type != "unknown" {
		return
	}
	argv, ok := simpleMediaCommand(p.Item.CommandActions[0].Command)
	home, e := os.UserHomeDir()
	if !ok || e != nil || len(argv) < 2 || argv[0] != filepath.Join(home, ".local", "bin", "bridge-line") || !unambiguousPublicJSON(raw) {
		return
	}
	q, e := linecontrol.ParseInvocation(argv[1:])
	if e != nil {
		return
	}
	caller, e := c.sessionControlCaller(s, p.TurnID, p.Item.ID)
	if e != nil {
		return
	}
	key := p.ThreadID + "|" + q.ID
	proof := lineCommandProof{caller: caller, fingerprint: q.Fingerprint()}
	state := &c.lineOrigins
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.blocked {
		return
	}
	if state.proofs == nil {
		state.proofs = map[string]lineCommandProof{}
	}
	if old, ok := state.proofs[key]; ok {
		if old.caller.TurnID != caller.TurnID || old.caller.ToolCallID != caller.ToolCallID || old.fingerprint != proof.fingerprint {
			state.proofs[key] = lineCommandProof{}
		}
		return
	}
	if len(state.proofs) >= 4096 {
		for k, old := range state.proofs {
			if old.caller.Parent == nil {
				delete(state.proofs, k)
				continue
			}
			if _, e := c.sessionControlCaller(old.caller.Parent, old.caller.TurnID, old.caller.ToolCallID); e != nil {
				delete(state.proofs, k)
			}
		}
		if len(state.proofs) >= 4096 {
			return
		}
	}
	state.proofs[key] = proof
}
func (c *Codex) ResolveLineCaller(thread, operation, fingerprint string) (backend.SessionControlCaller, error) {
	state := &c.lineOrigins
	state.mu.Lock()
	proof, ok := state.proofs[thread+"|"+operation]
	blocked := state.blocked
	state.mu.Unlock()
	if blocked || !ok || proof.caller.Parent == nil || fingerprint != proof.fingerprint {
		return backend.SessionControlCaller{}, errors.New("line_origin_unavailable")
	}
	caller, e := c.sessionControlCaller(proof.caller.Parent, proof.caller.TurnID, proof.caller.ToolCallID)
	if e != nil || caller.RequestID != proof.caller.RequestID || caller.VoiceID != proof.caller.VoiceID {
		return backend.SessionControlCaller{}, errors.New("line_origin_retired")
	}
	return caller, nil
}
