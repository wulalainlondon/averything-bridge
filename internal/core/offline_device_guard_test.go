package core

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coder/websocket"
)

func TestOfflineDeviceGuardDoesNotReplaceActiveUser(t *testing.T) {
	h, _ := newTestHub(t)
	user := newDeviceClient(h, "paired", 4)
	h.registerLatest(user)
	qa := newDeviceClient(h, "paired", 4)
	qa.offlineDeviceGuard = true
	if h.reserveOfflineDevice(qa) {
		t.Fatal("guard accepted an active identity")
	}
	h.registerLatest(qa)
	if !h.isCurrent(user) || !user.live() {
		t.Fatal("guarded QA evicted the active user")
	}
}

func readGuardEvent(t *testing.T, conn *websocket.Conn, ctx context.Context, kind string) map[string]any {
	t.Helper()
	for {
		event := readEvent(t, ctx, conn)
		if event["type"] == kind {
			return event
		}
	}
}

func TestOfflineDeviceGuardHandshakeRejectsUnauthorizedOrUnboundIdentities(t *testing.T) {
	for _, tc := range []struct {
		name    string
		paired  bool
		payload string
	}{
		{"unauthorized", true, `{"type":"hello","device_id":"qa","auth_token":"wrong","require_offline_device":true}`},
		{"wrong_device", true, `{"type":"hello","device_id":"other","auth_token":"fixture-token","require_offline_device":true}`},
		{"probe", true, `{"type":"hello","device_id":"qa","auth_token":"fixture-token","connection_probe":true,"require_offline_device":true}`},
		{"enrollment", false, `{"type":"hello","device_id":"qa","auth_token":"unpaired-fixture","require_offline_device":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHub(t)
			if tc.paired {
				if err := h.pairing.Claim("fixture-token", "qa"); err != nil {
					t.Fatal(err)
				}
			}
			conn, ctx, cleanup := dialWS(t, h)
			defer cleanup()
			if err := conn.Write(ctx, websocket.MessageText, []byte(tc.payload)); err != nil {
				t.Fatal(err)
			}
			event := readEvent(t, ctx, conn)
			if event["type"] != "error" || h.clientCount() != 0 {
				t.Fatal("guarded unauthorized/enrollment/probe handshake registered a client")
			}
		})
	}
}

func TestOfflineDeviceGuardRealHandshakeRejectsActiveIdentityPromptly(t *testing.T) {
	h, _ := newTestHub(t)
	if err := h.pairing.Claim("fixture-token", "qa"); err != nil {
		t.Fatal(err)
	}
	user, uctx, uclose := dialWS(t, h)
	defer uclose()
	if err := user.Write(uctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"qa","auth_token":"fixture-token"}`)); err != nil {
		t.Fatal(err)
	}
	readGuardEvent(t, user, uctx, "hello_ack")
	qa, ctx, closeQA := dialWS(t, h)
	defer closeQA()
	if err := qa.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"qa","auth_token":"fixture-token","require_offline_device":true}`)); err != nil {
		t.Fatal(err)
	}
	event := readEvent(t, ctx, qa)
	if event["code"] != "device_identity_in_use" {
		t.Fatal("active identity was not rejected before bootstrap")
	}
	if err := user.Write(uctx, websocket.MessageText, []byte(`{"type":"ping"}`)); err != nil {
		t.Fatal(err)
	}
	readGuardEvent(t, user, uctx, "pong")
}

func TestOfflineDeviceGuardNormalTakeoverCannotBeReclaimedOrRemovedByOldQA(t *testing.T) {
	h, _ := newTestHub(t)
	if err := h.pairing.Claim("fixture-token", "qa"); err != nil {
		t.Fatal(err)
	}
	qaConn, qctx, qclose := dialWS(t, h)
	defer qclose()
	if err := qaConn.Write(qctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"qa","auth_token":"fixture-token","require_offline_device":true}`)); err != nil {
		t.Fatal(err)
	}
	readGuardEvent(t, qaConn, qctx, "hello_ack")
	h.latestMu.Lock()
	old := h.latestByDevice["qa"]
	h.latestMu.Unlock()
	user, ctx, closeUser := dialWS(t, h)
	defer closeUser()
	if err := user.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"qa","auth_token":"fixture-token"}`)); err != nil {
		t.Fatal(err)
	}
	readGuardEvent(t, user, ctx, "hello_ack")
	h.latestMu.Lock()
	current := h.latestByDevice["qa"]
	h.latestMu.Unlock()
	if old == current {
		t.Fatal("normal user could not reconnect")
	}
	h.registerLatest(old)
	route(h, old, `{"type":"hello","device_id":"qa","auth_token":"fixture-token"}`)
	h.removeClient(old)
	if !h.isCurrent(current) || !current.live() {
		t.Fatal("old guarded QA reclaimed/deleted the normal user's slot")
	}
	if err := user.Write(ctx, websocket.MessageText, []byte(`{"type":"ping"}`)); err != nil {
		t.Fatal(err)
	}
	readGuardEvent(t, user, ctx, "pong")
}

func TestOfflineDeviceGuardRepeatedHelloCannotChangeIdentity(t *testing.T) {
	h, _ := newTestHub(t)
	if err := h.pairing.Claim("fixture-token", "qa"); err != nil {
		t.Fatal(err)
	}
	conn, ctx, cleanup := dialWS(t, h)
	defer cleanup()
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","device_id":"qa","auth_token":"fixture-token","require_offline_device":true}`)); err != nil {
		t.Fatal(err)
	}
	readGuardEvent(t, conn, ctx, "hello_ack")
	for _, payload := range []map[string]any{
		{"type": "hello", "device_id": "other", "auth_token": "fixture-token"},
		{"type": "hello", "device_id": "qa", "auth_token": "another-token"},
		{"type": "hello", "device_id": "qa", "connection_probe": true},
	} {
		b, _ := json.Marshal(payload)
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			t.Fatal(err)
		}
		if e := readGuardEvent(t, conn, ctx, "error"); e["code"] != "offline_device_guard_identity_changed" {
			t.Fatal("guarded identity changed")
		}
	}
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"ping"}`)); err != nil {
		t.Fatal(err)
	}
	readGuardEvent(t, conn, ctx, "pong")
}

func TestOfflineDeviceGuardConcurrentReservationHasOneWinner(t *testing.T) {
	h, _ := newTestHub(t)
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := newDeviceClient(h, "paired", 4)
			c.offlineDeviceGuard = true
			if h.reserveOfflineDevice(c) {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := accepted.Load(); got != 1 {
		t.Fatalf("concurrent QA connections accepted: %d", got)
	}
}
