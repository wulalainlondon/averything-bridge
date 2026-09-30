package core

import (
	"context"
	"io"
	"sync/atomic"
	"time"
)

// A non-fragmented photo JSON frame can hold a client's pong behind megabytes
// of actively arriving bytes. Observe those bytes without changing message
// boundaries, the existing read limit, or application delivery semantics.
type inboundProgressReader struct {
	io.Reader
	progress *atomic.Int64
}

func (r inboundProgressReader) Read(p []byte) (int, error) {
	if len(p) > 32*1024 {
		p = p[:32*1024]
	}
	n, err := r.Reader.Read(p)
	if n > 0 && r.progress != nil {
		r.progress.Store(time.Now().UnixNano())
	}
	return n, err
}

type inboundProgress interface{ LastInboundProgress() time.Time }

// Retain the usual idle deadline, extending it only for actual inbound bytes.
// An absolute budget still evicts a peer that continuously dribbles data but
// never answers its ping. Cancel and join Ping before the caller releases its
// write lock, so the old probe cannot later interfere with a data write.
func pingWithInboundProgress(ctx context.Context, p pinger, idle, maximum time.Duration) error {
	progress, ok := p.(inboundProgress)
	if !ok {
		probe, cancel := context.WithTimeout(ctx, idle)
		defer cancel()
		return p.Ping(probe)
	}
	probe, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- p.Ping(probe) }()
	timer, hard := time.NewTimer(idle), time.NewTimer(maximum)
	defer timer.Stop()
	defer hard.Stop()
	stop := func(err error) error { cancel(); <-result; return err }
	for {
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			return stop(ctx.Err())
		case <-hard.C:
			return stop(context.DeadlineExceeded)
		case <-timer.C:
			last := progress.LastInboundProgress()
			remaining := idle - time.Since(last)
			if last.IsZero() || remaining <= 0 {
				return stop(context.DeadlineExceeded)
			}
			if remaining > idle {
				remaining = idle
			}
			timer.Reset(remaining)
		}
	}
}
