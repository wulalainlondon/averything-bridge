package goexec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"everything-go/internal/agentvisibility"
	"everything-go/internal/session"
	"path/filepath"
	"time"
)

// RegisterExistingDaemonObservation installs the original provider's read-only
// factory at boot. It neither connects nor starts a provider or model turn.
func (c *Codex) RegisterExistingDaemonObservation() {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return
	}
	c.mu.Lock()
	c.metadataObserverBoot = hex.EncodeToString(nonce[:])
	c.mu.Unlock()
}

type bootMetadataRPC struct {
	*agentvisibility.DaemonMetadataRPC
	boot string
}

func (r bootMetadataRPC) Scope() agentvisibility.Scope {
	s := r.DaemonMetadataRPC.Scope()
	if s.Valid() {
		s.Epoch = r.boot + ":" + s.Epoch
	}
	return s
}

type bootMetadataReader struct{ agentvisibility.CodexReader }

func (r bootMetadataReader) Probe(ctx context.Context, s agentvisibility.Scope) (agentvisibility.Probe, error) {
	p, err := r.CodexReader.Probe(ctx, s)
	p.Coverage = "partial_selected_native_root"
	return p, err
}
func (c *Codex) observeRegisteredDaemon(ctx context.Context, s *session.Session, authority string) (agentvisibility.Snapshot, bool) {
	c.mu.Lock()
	boot := c.metadataObserverBoot
	c.mu.Unlock()
	scope := agentvisibility.Scope{Authority: authority, SessionID: s.ID, RootActorID: s.ResumeID()}
	if boot == "" {
		return agentvisibility.Unsupported(scope), false
	}
	before := s.Snapshot()
	if scope.Authority == "" || scope.RootActorID == "" || before.Hidden || before.Backend != "codex" {
		return agentvisibility.Unsupported(scope), true
	}
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// Paths come from the provider configuration, never a client JSON path/scope.
	c.startMu.Lock()
	mode := c.appServerMode
	socket := c.daemonSocketPath(filepath.Dir(c.sessionsRoot))
	c.startMu.Unlock()
	if mode != "daemon" {
		return agentvisibility.Unsupported(scope), true
	}
	attached, err := agentvisibility.OpenExistingDaemonMetadata(bounded, socket, scope, before.ConfigRevision)
	if err != nil {
		return agentvisibility.Unsupported(scope), true
	}
	defer attached.Close()
	rpc := bootMetadataRPC{DaemonMetadataRPC: attached, boot: boot}
	reader := bootMetadataReader{agentvisibility.CodexReader{RPC: rpc, Now: time.Now}}
	out := agentvisibility.Observe(bounded, rpc.Scope(), reader, nil, time.Now, time.Minute)
	c.mu.Lock()
	current := c.metadataObserverBoot
	c.mu.Unlock()
	after := s.Snapshot()
	if current != boot || before.ConfigRevision != after.ConfigRevision || before.ResumeID != after.ResumeID || before.Backend != after.Backend {
		out = agentvisibility.Unsupported(scope)
		out.Availability = "unknownstale"
		out.ProbeError = "scope_changed"
	}
	return out, true
}
