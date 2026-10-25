package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/agentvisibility"
	"everything-go/internal/session"
	"sort"
	"strconv"
	"time"
)

// ObserveInternalAgents uses only an already-running provider RPC writer. No
// start, resume, restart, paid generation, list/discovery or history read occurs.
// The caller is the original Hub read handler, not a model-supplied scope/body.
func (c *Codex) ObserveInternalAgents(ctx context.Context, s *session.Session, authority string) agentvisibility.Snapshot {
	// Stdio and hidden/non-Codex sessions are not part of this metadata mode.
	// Never initialize a model transport merely to discover its metadata.
	c.startMu.Lock()
	mode := c.appServerMode
	c.startMu.Unlock()
	if mode != "daemon" || s.Snapshot().Hidden || s.Snapshot().Backend != "codex" {
		return agentvisibility.Unsupported(agentvisibility.Scope{Authority: authority, SessionID: s.ID, RootActorID: s.ResumeID()})
	}
	rpc := codexVisibilityRPC{c: c, s: s, authority: authority}
	scope := rpc.Scope()
	if c.rpc == nil || !c.rpc.hasWriter() {
		if out, registered := c.observeRegisteredDaemon(ctx, s, authority); registered {
			return out
		}
	}
	if !scope.Valid() || c.rpc == nil {
		return agentvisibility.Unsupported(scope)
	}
	st := c.state(s.ID)
	if !c.rpc.hasWriter() {
		return st.visibility.Offline(scope, time.Now())
	}
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// Canonical resolver deliberately absent until the original owner adopts the
	// read-only port. Native ancestry must not be promoted to a fake task dispatch.
	return st.visibility.Read(bounded, scope, agentvisibility.CodexReader{RPC: rpc, Now: time.Now}, nil, time.Now, time.Minute)
}

type codexVisibilityRPC struct {
	c         *Codex
	s         *session.Session
	authority string
}

func (r codexVisibilityRPC) Scope() agentvisibility.Scope {
	r.c.mu.Lock()
	boot := r.c.metadataObserverBoot
	r.c.mu.Unlock()
	if r.c.rpc == nil || boot == "" {
		return agentvisibility.Scope{Authority: r.authority, SessionID: r.s.ID, RootActorID: r.s.ResumeID()}
	}
	return agentvisibility.Scope{Authority: r.authority, Epoch: boot + ":" + strconv.FormatUint(r.c.rpc.transportEpoch.Load(), 10) + ":" + strconv.FormatUint(r.s.Snapshot().ConfigRevision, 10), SessionID: r.s.ID, RootActorID: r.s.ResumeID()}
}
func (r codexVisibilityRPC) ChildIDs() []string {
	r.c.mu.Lock()
	st := r.c.states[r.s.ID]
	r.c.mu.Unlock()
	if st == nil {
		return nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.threadID != r.s.ResumeID() {
		return nil
	}
	ids := make([]string, 0, len(st.agents))
	for id := range st.agents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
func (r codexVisibilityRPC) ReadThread(ctx context.Context, id string) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !r.c.rpc.hasWriter() {
		return nil, agentvisibility.ErrUnsupported
	}
	epoch := r.c.rpc.transportEpoch.Load()
	timeout := time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	// This metadata-only call deliberately bypasses rpcCallAtEpoch's thread/turn
	// health admission and recovery scheduler. A missing metadata reply cannot
	// establish a shared transport failure. Keep the original writer, epoch check
	// and unique correlation; cancellation/timeout only retires this wait.
	// Physical disconnects still belong to the original shared transport owner.
	raw, err := r.c.rpc.requestAtEpochContext(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": false}, timeout, &epoch)
	var timedOut *rpcTimeoutError
	if errors.As(err, &timedOut) {
		return nil, context.DeadlineExceeded
	}
	return raw, err
}
