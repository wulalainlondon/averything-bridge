package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type progressPingFixture struct {
	last    atomic.Int64
	answer  chan struct{}
	stopped chan struct{}
}

func (p *progressPingFixture) LastInboundProgress() time.Time {
	n := p.last.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}
func (p *progressPingFixture) Ping(ctx context.Context) error {
	defer close(p.stopped)
	select {
	case <-p.answer:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func newProgressPing() *progressPingFixture {
	return &progressPingFixture{answer: make(chan struct{}), stopped: make(chan struct{})}
}

func TestInboundProgressReaderPreservesEveryByte(t *testing.T) {
	want := bytes.Repeat([]byte("clear-photo-data"), 100000)
	var progress atomic.Int64
	got, err := io.ReadAll(inboundProgressReader{Reader: bytes.NewReader(want), progress: &progress})
	if err != nil || !bytes.Equal(want, got) || progress.Load() == 0 {
		t.Fatal("stream bytes/progress mismatch", err)
	}
}
func TestInboundProgressKeepsSlowPhotoAliveUntilPong(t *testing.T) {
	p := newProgressPing()
	done := make(chan error, 1)
	go func() { done <- pingWithInboundProgress(context.Background(), p, 80*time.Millisecond, time.Second) }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(240 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			p.last.Store(time.Now().UnixNano())
		case err := <-done:
			t.Fatal("active photo disconnected", err)
		case <-deadline:
			close(p.answer)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			return
		}
	}
}
func TestInboundProgressStillExpiresStalledPeer(t *testing.T) {
	p := newProgressPing()
	p.last.Store(time.Now().UnixNano())
	start := time.Now()
	err := pingWithInboundProgress(context.Background(), p, 30*time.Millisecond, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 500*time.Millisecond {
		t.Fatal("stalled peer did not expire", err)
	}
	select {
	case <-p.stopped:
	default:
		t.Fatal("probe not joined")
	}
}
func TestInboundProgressCannotKeepPeerForever(t *testing.T) {
	p := newProgressPing()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				p.last.Store(time.Now().UnixNano())
			}
		}
	}()
	start := time.Now()
	err := pingWithInboundProgress(context.Background(), p, 50*time.Millisecond, 160*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 600*time.Millisecond {
		t.Fatal("missing absolute deadline", err)
	}
}
func TestInboundProgressCancellationJoinsProbe(t *testing.T) {
	p := newProgressPing()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pingWithInboundProgress(ctx, p, time.Second, time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-p.stopped:
	default:
		t.Fatal("probe survived cancellation")
	}
}
