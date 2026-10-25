package governance

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"

	"everything-go/internal/protocol"
)

// PermissionManager gates high-risk operations (kill_process, shell_input)
// behind a human approval round-trip, mirroring bridge/permission_manager.py.
// Request broadcasts a permission_request, blocks until the user answers via
// permission_response (→ Resolve) or the TTL expires (→ deny), then returns the
// decision. Modes: "off" (always allow), "warn" (auto-allow + notify),
// "enforce" (real prompt; the default, matching Python).
//
// IMPORTANT: Request blocks. It must NOT be called on the connection read loop
// (the answer arrives on that same loop), so callers run it in a goroutine.
type PermissionManager struct {
	emit func(any)
	mode string
	ttl  time.Duration

	mu        sync.Mutex
	waiters   map[string]chan bool
	details   map[string]permissionDetails
	requester map[string]string // request_id -> requester device_id (binds the answer)
}

type permissionDetails struct {
	sessionID, action string
	expires           time.Time
}

// NewPermissionManager builds a manager. emit broadcasts events to all clients.
// mode defaults to "enforce" when empty.
func NewPermissionManager(emit func(any), mode string) *PermissionManager {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "enforce"
	}
	return &PermissionManager{
		emit: emit, mode: mode, ttl: 60 * time.Second,
		waiters: map[string]chan bool{}, requester: map[string]string{}, details: map[string]permissionDetails{},
	}
}

func (m *PermissionManager) Mode() string { return m.mode }

// Request asks the user to approve an action and blocks until decided/expired.
func (m *PermissionManager) Request(deviceID, action, title, justification, preview, risk, sessionID string) bool {
	switch m.mode {
	case "off":
		return true
	case "warn":
		m.emit(protocol.NewPermissionResult("", sessionID, action, "warn_auto_approved", "permission mode=warn"))
		return true
	}

	if len(preview) > 500 {
		preview = preview[:500]
	}
	rid := "perm_" + permRandHex(12)
	ch := make(chan bool, 1)
	deadline := time.Now().Add(m.ttl)
	expires := deadline.UnixMilli()

	m.mu.Lock()
	m.waiters[rid] = ch
	m.requester[rid] = deviceID
	m.details[rid] = permissionDetails{sessionID, action, deadline}
	m.mu.Unlock()

	m.emit(protocol.NewPermissionRequest(rid, sessionID, action, title, justification, preview, risk, expires))

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case approved := <-ch:
		return approved
	case <-timer.C:
		// If Resolve won first, read its decision; otherwise timeout wins the same CAS.
		m.finish(rid, "expired", "", true)
		return <-ch
	}
}

// Resolve returns command errors only to the responder. It never emits a false
// terminal result for a gate that is still waiting for its authenticated owner.
func (m *PermissionManager) Resolve(requestID, decision, responderDeviceID string) error {
	if decision != "approve" && decision != "deny" {
		return errors.New("invalid permission decision")
	}
	return m.finish(requestID, decision, responderDeviceID, false)
}

func (m *PermissionManager) finish(requestID, decision, responderDeviceID string, timeout bool) error {
	m.mu.Lock()
	ch := m.waiters[requestID]
	if ch == nil {
		m.mu.Unlock()
		return errors.New("permission no longer pending")
	}
	if !timeout && m.requester[requestID] != "" && responderDeviceID != m.requester[requestID] {
		m.mu.Unlock()
		return errors.New("permission response device mismatch")
	}
	detail := m.details[requestID]
	if !detail.expires.IsZero() && !time.Now().Before(detail.expires) {
		decision = "expired"
	}
	delete(m.waiters, requestID)
	delete(m.requester, requestID)
	delete(m.details, requestID)
	m.mu.Unlock()
	// Publish the actual single winner before allowing the protected operation to
	// continue. A timeout losing the CAS waits on this same channel.
	m.emit(protocol.NewPermissionResult(requestID, detail.sessionID, detail.action, decision, ""))
	ch <- decision == "approve"
	return nil
}

func permRandHex(n int) string {
	b := make([]byte, (n+1)/2)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)[:n]
}
