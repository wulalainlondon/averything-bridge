package core

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// The peer is healthy and its real websocket reader automatically answers
// control pings. A slow application handler must not stop our protocol reader.
func TestSlowDispatchDoesNotStarveRealWebSocketPong(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	busy, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	serverConn := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		serverConn <- conn
		_ = readAndDispatchFrames(ctx, wsConn{c: conn}, func(ctx context.Context, _ []byte) error {
			once.Do(func() { close(busy) })
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}))
	var peer *websocket.Conn
	t.Cleanup(func() {
		cancel()
		close(release)
		if peer != nil {
			peer.CloseNow()
		}
		server.Close()
	})
	var err error
	peer, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			if _, _, err := peer.Read(ctx); err != nil {
				return
			}
		}
	}()
	conn := <-serverConn
	if err := peer.Write(ctx, websocket.MessageText, []byte(`{"type":"slow-command"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-busy:
	case <-ctx.Done():
		t.Fatal("dispatch not entered")
	}
	// A real mobile bootstrap can send a burst of runtime ACKs while a read request is busy.
	for i := 0; i < 64; i++ {
		if err := peer.Write(ctx, websocket.MessageText, []byte(`{"type":"runtime-ack"}`)); err != nil {
			t.Fatal(err)
		}
	}
	probe, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	if err := conn.Ping(probe); err != nil {
		t.Fatalf("healthy peer falsely declared unresponsive while handler was busy: %v", err)
	}
}

type frameInboxConn struct {
	nopConn
	input      chan []byte
	readExited chan struct{}
	once       sync.Once
}

func (f *frameInboxConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case data, ok := <-f.input:
		if !ok {
			return nil, io.EOF
		}
		return data, nil
	case <-ctx.Done():
		f.once.Do(func() { close(f.readExited) })
		return nil, ctx.Err()
	}
}

func TestDispatchStaysOrderedAndCancelsJoinedReader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &frameInboxConn{input: make(chan []byte, 8), readExited: make(chan struct{})}
	seen := make(chan string, 8)
	done := make(chan error, 1)
	go func() {
		done <- readAndDispatchFrames(ctx, f, func(_ context.Context, b []byte) error { seen <- string(b); return nil })
	}()
	for _, s := range []string{"first", "second", "third"} {
		f.input <- []byte(s)
	}
	for _, want := range []string{"first", "second", "third"} {
		select {
		case got := <-seen:
			if got != want {
				t.Fatalf("dispatch order %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("missing dispatch")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader was not joined")
	}
	select {
	case <-f.readExited:
	default:
		t.Fatal("read goroutine survived return")
	}
}

func TestDispatchBacklogIsBoundedAndPreservesCause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f := &frameInboxConn{input: make(chan []byte, 64), readExited: make(chan struct{})}
	busy := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- readAndDispatchFrames(ctx, f, func(ctx context.Context, _ []byte) error { close(busy); <-ctx.Done(); return ctx.Err() })
	}()
	f.input <- []byte("blocking command")
	<-busy
	for i := 0; i < inboundDispatchFrames+1; i++ {
		f.input <- []byte("queued")
	}
	select {
	case err := <-done:
		if !errors.Is(err, errInboundBacklog) {
			t.Fatalf("backpressure cause lost: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unbounded reader backlog")
	}
}

func TestDispatchByteBudgetIsBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f := &frameInboxConn{input: make(chan []byte, 8), readExited: make(chan struct{})}
	busy := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- readAndDispatchFrames(ctx, f, func(ctx context.Context, _ []byte) error { close(busy); <-ctx.Done(); return ctx.Err() })
	}()
	f.input <- []byte("blocking command")
	<-busy
	f.input <- make([]byte, inboundDispatchBytes/2+1)
	f.input <- make([]byte, inboundDispatchBytes/2+1)
	select {
	case err := <-done:
		if !errors.Is(err, errInboundBacklog) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unbounded reader bytes")
	}
}

// Existing clients use application pong as a barrier after registration.
// Reading wire-level controls concurrently must not acknowledge that app ping
// before earlier commands have actually completed.
func TestApplicationPingRemainsAnOrderingBarrier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &frameInboxConn{input: make(chan []byte, 8), readExited: make(chan struct{})}
	busy, release, pong := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- readAndDispatchFrames(ctx, f, func(ctx context.Context, b []byte) error {
			if string(b) == "registration" {
				close(busy)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if string(b) == `{"type":"ping"}` {
				pong <- struct{}{}
			}
			return nil
		})
	}()
	f.input <- []byte("registration")
	<-busy
	f.input <- []byte(`{"type":"ping"}`)
	select {
	case <-pong:
		t.Fatal("pong overtook registration")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-pong:
	case <-time.After(time.Second):
		t.Fatal("pong missing after registration")
	}
	cancel()
	<-done
}
