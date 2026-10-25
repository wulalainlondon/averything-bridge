package core

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestConnectionProbeDoesNotReplaceOrSyncActiveClient(t *testing.T) {
	h, _ := newTestHub(t)
	active := newDeviceClient(h, "probe-device", 1024)
	probe := newDeviceClient(h, "probe-device", 1024)
	probe.inventoryProbe = true
	h.addClient(active)
	h.registerLatest(active)
	route(h, probe, `{"type":"hello","device_id":"probe-device","connection_probe":true,"protocol_version":3}`)
	_ = waitForType(t, probe, "hello_ack")
	if !h.isCurrent(active) {
		t.Fatal("probe replaced the active device lease")
	}
	select {
	case <-active.quit:
		t.Fatal("probe shut down active transport")
	default:
	}
	if len(probe.send) != 0 {
		t.Fatal("probe received bootstrap or replay data")
	}
	route(h, probe, `{"type":"ping"}`)
	_ = waitForType(t, probe, "pong")
	route(h, probe, `{"type":"request_sessions_list"}`)
	_ = waitForType(t, probe, "error")
	route(h, probe, `{"type":"message","session_id":"unknown","request_id":"probe-write","content":"must not run"}`)
	_ = waitForType(t, probe, "error")
	// A second hello cannot promote an authenticated probe into a writer.
	route(h, probe, `{"type":"hello","device_id":"probe-device"}`)
	_ = waitForType(t, probe, "hello_ack")
	if !h.isCurrent(active) || len(probe.send) != 0 {
		t.Fatal("probe escaped its connection scope")
	}
	h.removeClient(probe)
	if !h.isCurrent(active) {
		t.Fatal("probe teardown removed active device lease")
	}
}

func TestPingWaitsForBoundedDataWrite(t *testing.T) {
	h, _ := newTestHub(t)
	pc := &pingConn{}
	pc.failPing.Store(true)
	c := newDeviceClient(h, "slow-link", 8)
	c.conn = pc
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.writeMu.Lock()
	go c.pingLoopEvery(ctx, 5*time.Millisecond, 5*time.Millisecond)
	time.Sleep(35 * time.Millisecond)
	if atomic.LoadInt64(&pc.pingCalls) != 0 {
		c.writeMu.Unlock()
		t.Fatal("ping raced active data write")
	}
	select {
	case <-c.quit:
		c.writeMu.Unlock()
		t.Fatal("slow data write was treated as a dead peer")
	default:
	}
	c.writeMu.Unlock()
	select {
	case <-c.quit:
	case <-time.After(time.Second):
		t.Fatal("genuinely dead peer was not dropped after write finished")
	}
}

type captureTransport struct {
	nopConn
	frames chan []byte
}

func (w captureTransport) Write(_ context.Context, data []byte) error {
	w.frames <- append([]byte(nil), data...)
	return nil
}

func TestPongBypassesSnapshotBacklog(t *testing.T) {
	h, _ := newTestHub(t)
	w := captureTransport{frames: make(chan []byte, 4)}
	c := newDeviceClient(h, "priority", 4)
	c.conn = w
	c.urgent = make(chan []byte, 2)
	c.send <- []byte(`{"type":"sessions_list"}`)
	c.enqueuePong()
	go c.writePump(context.Background())
	defer c.shutdown()
	select {
	case data := <-w.frames:
		var event map[string]any
		_ = json.Unmarshal(data, &event)
		if event["type"] != "pong" {
			t.Fatalf("heartbeat waited behind bulk data: %s", data)
		}
	case <-time.After(time.Second):
		t.Fatal("no heartbeat reply")
	}
}

type countedSocket struct {
	net.Conn
	received *atomic.Int64
}

func (c countedSocket) Read(p []byte) (int, error) {
	n, e := c.Conn.Read(p)
	c.received.Add(int64(n))
	return n, e
}

func TestBootstrapNegotiatesCompressionAndReducesWireBytes(t *testing.T) {
	t.Setenv("BRIDGE_AUTH_TOKEN", "transport-test-token")
	h, _ := newTestHub(t)
	for i := 0; i < 200; i++ {
		h.registry.Create(fmt.Sprintf("session-%03d", i), strings.Repeat("snapshot test conversation ", 4), "/qa/repository", "codex", "", "", "")
	}
	server := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	defer server.Close()
	var received atomic.Int64
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return countedSocket{conn, &received}, nil
	}}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport}, CompressionMode: websocket.CompressionNoContextTakeover,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(4 << 20)
	if !strings.Contains(response.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate") {
		t.Fatal("compression not negotiated")
	}
	if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","auth_token":"transport-test-token","device_id":"wire-test","protocol_version":3}`)); err != nil {
		t.Fatal(err)
	}
	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var event struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(raw, &event)
		if event.Type == "sessions_list" {
			if received.Load() >= int64(len(raw))/2 {
				t.Fatalf("bulk snapshot did not shrink: wire=%d decoded=%d", received.Load(), len(raw))
			}
			t.Logf("bootstrap wire=%d decoded session snapshot=%d", received.Load(), len(raw))
			break
		}
	}
}

func TestAppleBootstrapDoesNotNegotiateCompression(t *testing.T) {
	t.Setenv("BRIDGE_AUTH_TOKEN", "transport-test-token")
	h, _ := newTestHub(t)
	h.registry.Create("apple-session", strings.Repeat("bootstrap snapshot ", 100), "/qa", "codex", "", "", "")
	server := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	defer server.Close()
	for _, ua := range []string{
		"BridgeDesktopNative/28 CFNetwork/3860 Darwin/25.6.0",
		"Mozilla/5.0 (iPad; CPU OS 26_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/26.0 Safari/605.1.15",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 26_0 like Mac OS X) AppleWebKit/605.1.15 CriOS/140.0 Mobile/15E148 Safari/604.1",
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &websocket.DialOptions{
			HTTPHeader: http.Header{"User-Agent": []string{ua}}, CompressionMode: websocket.CompressionNoContextTakeover,
		})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if response.Header.Get("Sec-WebSocket-Extensions") != "" {
			t.Fatal("Apple client still negotiated compression")
		}
		if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","auth_token":"transport-test-token","device_id":"apple-test","protocol_version":3}`)); err != nil {
			t.Fatal(err)
		}
		for {
			_, raw, err := conn.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var event struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(raw, &event)
			if event.Type == "sessions_list" {
				break
			}
		}
		conn.CloseNow()
		cancel()
	}
	for _, ua := range []string{
		"Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/140.0 Mobile Safari/537.36",
		"Mozilla/5.0 (Macintosh) AppleWebKit/537.36 Chrome/140.0 Safari/537.36",
		"Go-http-client/1.1",
	} {
		if webSocketCompressionMode(ua) != websocket.CompressionNoContextTakeover {
			t.Fatal("compatible transport lost compression")
		}
	}
}

type delayedControlPing struct {
	nopConn
	entered, release chan struct{}
	writes           chan []byte
	once             sync.Once
}

func (p *delayedControlPing) ConcurrentControlPing() {}
func (p *delayedControlPing) Ping(ctx context.Context) error {
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *delayedControlPing) Write(_ context.Context, b []byte) error {
	p.writes <- append([]byte(nil), b...)
	return nil
}
func TestPendingProtocolPingDoesNotBlockApplicationPong(t *testing.T) {
	h, _ := newTestHub(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &delayedControlPing{entered: make(chan struct{}), release: make(chan struct{}), writes: make(chan []byte, 4)}
	defer close(p.release)
	c := newDeviceClient(h, "pending-probe", 4)
	c.conn = p
	c.urgent = make(chan []byte, 2)
	defer c.shutdown()
	go c.pingLoopEvery(ctx, time.Millisecond, time.Second)
	<-p.entered
	go c.writePump(ctx)
	c.enqueuePong()
	select {
	case b := <-p.writes:
		var event map[string]any
		_ = json.Unmarshal(b, &event)
		if event["type"] != "pong" {
			t.Fatal("wrong event")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("protocol ping blocked application heartbeat response")
	}
}
