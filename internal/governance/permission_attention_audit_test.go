package governance

import (
	"everything-go/internal/protocol"
	"sync"
	"testing"
	"time"
)

func TestAuditWrongResponderMustNotBroadcastOriginalPermissionAsDenied(t *testing.T) {
	var mu sync.Mutex
	var terminal []protocol.PermissionResult
	ready := make(chan string, 1)
	m := NewPermissionManager(func(e any) {
		switch v := e.(type) {
		case protocol.PermissionRequest:
			ready <- v.RequestID
		case protocol.PermissionResult:
			mu.Lock()
			terminal = append(terminal, v)
			mu.Unlock()
		}
	}, "enforce")
	done := make(chan bool, 1)
	go func() { done <- m.Request("owner-device", "fixture.none", "Synthetic", "No effect", "", "low", "s1") }()
	var id string
	select {
	case id = <-ready:
	case <-time.After(time.Second):
		t.Fatal("request setup")
	}
	m.Resolve(id, "approve", "other-device")
	mu.Lock()
	wrong := append([]protocol.PermissionResult(nil), terminal...)
	mu.Unlock()
	// Cleanup also proves the original gate is still waiting and can still be approved by its owner.
	m.Resolve(id, "approve", "owner-device")
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("original gate was incorrectly denied")
		}
	case <-time.After(time.Second):
		t.Fatal("owner resolution blocked")
	}
	if len(wrong) != 0 {
		t.Errorf("wrong-device command failure emitted %d global terminal permission result(s) for still-pending original request: decision=%s", len(wrong), wrong[0].Decision)
	}
}
func TestAuditDuplicatePermissionResultMustMatchAcceptedGateDecision(t *testing.T) {
	var results []protocol.PermissionResult
	m := NewPermissionManager(func(e any) {
		if v, ok := e.(protocol.PermissionResult); ok {
			results = append(results, v)
		}
	}, "enforce")
	// Deterministic valid pending state before the Request goroutine consumes its buffered decision.
	ch := make(chan bool, 1)
	m.waiters["synthetic"] = ch
	m.requester["synthetic"] = "owner-device"
	m.Resolve("synthetic", "approve", "owner-device")
	m.Resolve("synthetic", "deny", "owner-device")
	actual := <-ch
	if !actual {
		t.Fatal("first accepted decision must still be approve")
	}
	if len(results) != 1 {
		t.Errorf("accepted one gate decision but broadcast %d terminal results; last says %s while gate approved", len(results), results[len(results)-1].Decision)
	}
}

func TestPermissionDecisionTimeoutRaceHasOneMatchingTerminal(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := &capEmit{}
		ready := make(chan string, 1)
		m := NewPermissionManager(func(event any) {
			c.emit(event)
			if request, ok := event.(protocol.PermissionRequest); ok {
				ready <- request.RequestID
			}
		}, "enforce")
		m.ttl = 3 * time.Millisecond
		done := make(chan bool, 1)
		go func() { done <- m.Request("owner", "fixture.none", "t", "j", "", "low", "scope") }()
		id := <-ready
		var wg sync.WaitGroup
		for _, decision := range []string{"approve", "deny"} {
			wg.Add(1)
			go func(d string) {
				defer wg.Done()
				time.Sleep(time.Duration(i%4) * time.Millisecond)
				m.Resolve(id, d, "owner")
			}(decision)
		}
		accepted := <-done
		wg.Wait()
		c.mu.Lock()
		count := 0
		for _, event := range c.ev {
			if r, ok := event.(protocol.PermissionResult); ok {
				count++
				if (r.Decision == "approve") != accepted || r.SessionID != "scope" || r.Action != "fixture.none" {
					t.Errorf("gate/result mismatch: %+v gate=%v", r, accepted)
				}
			}
		}
		c.mu.Unlock()
		if count != 1 {
			t.Fatalf("terminal count %d", count)
		}
	}
}
func TestPermissionInvalidAndEmptyResponderKeepOriginalGate(t *testing.T) {
	c := &capEmit{}
	m := NewPermissionManager(c.emit, "enforce")
	done := make(chan bool, 1)
	go func() { done <- m.Request("owner", "fixture.none", "t", "j", "", "low", "s") }()
	id := c.waitRequestID(t)
	if m.Resolve(id, "approve", "") == nil {
		t.Fatal("missing authenticated identity accepted")
	}
	if m.Resolve(id, "invalid", "owner") == nil {
		t.Fatal("invalid decision accepted")
	}
	if c.count(func(e any) bool { _, ok := e.(protocol.PermissionResult); return ok }) != 0 {
		t.Fatal("command error broadcast as terminal")
	}
	if err := m.Resolve(id, "deny", "owner"); err != nil {
		t.Fatal(err)
	}
	if <-done {
		t.Fatal("denied gate allowed")
	}
	if m.Resolve(id, "approve", "owner") == nil {
		t.Fatal("already decided must not be accepted again")
	}
}
