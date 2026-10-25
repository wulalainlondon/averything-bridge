package core

import (
	"context"
	"errors"
	"sync/atomic"
)

const inboundDispatchFrames = 1024
const inboundDispatchBytes = 32 << 20

var errInboundBacklog = errors.New("inbound dispatch backlog exceeded")

// A single socket reader must continue consuming control frames while the
// ordered application dispatcher is busy. Bounds apply to queued data; the
// existing websocket per-frame read limit and protocol authorization are intact.
func readAndDispatchFrames(ctx context.Context, conn wireConn, dispatch func(context.Context, []byte) error) error {
	readCtx, cancel := context.WithCancel(ctx)
	frames := make(chan []byte, inboundDispatchFrames)
	done := make(chan struct{})
	var queued atomic.Int64
	var readErr error // published by close(done)
	go func() {
		defer close(done)
		defer cancel() // interrupt an in-flight cancellable application operation
		for {
			data, err := conn.Read(readCtx)
			if err != nil {
				readErr = err
				return
			}
			if queued.Add(int64(len(data))) > inboundDispatchBytes {
				queued.Add(-int64(len(data)))
				readErr = errInboundBacklog
				return
			}
			select {
			case frames <- data:
			case <-readCtx.Done():
				readErr = readCtx.Err()
				return
			default:
				queued.Add(-int64(len(data)))
				readErr = errInboundBacklog
				return
			}
		}
	}()
	defer func() { cancel(); <-done }()
	for {
		select {
		case <-readCtx.Done():
			<-done
			return readErr
		case data := <-frames:
			queued.Add(-int64(len(data)))
			if readCtx.Err() != nil {
				<-done
				return readErr
			}
			if err := dispatch(readCtx, data); err != nil {
				if readCtx.Err() != nil {
					<-done
					return readErr
				}
				return err
			}
		}
	}
}
