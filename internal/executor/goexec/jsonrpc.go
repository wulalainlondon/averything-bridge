package goexec

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// rpcPlumber implements JSON-RPC 2.0 over newline-delimited stdio, matching
// bridge/backends/jsonrpc.py. The codex app-server omits the "jsonrpc" field, so
// we do too: requests are {id, method, params}, responses {id, result|error},
// notifications {method, params}.
type rpcPlumber struct {
	name string

	wmu            sync.Mutex
	w              io.Writer
	transportEpoch atomic.Uint64

	mu      sync.Mutex
	nextID  int
	pending map[int]chan rpcReply
}

type rpcReply struct {
	result json.RawMessage
	err    error
}

// rpcTimeoutError means the response is unknown, not that the server cancelled
// the operation. In particular, callers must not replay mutations on this error.
type rpcResponseError struct{ raw string }

func (e *rpcResponseError) Error() string { return e.raw }

type rpcTimeoutError struct {
	Name, Method string
	Timeout      time.Duration
}

func (e *rpcTimeoutError) Error() string {
	return fmt.Sprintf("[%s] %q timed out after %s", e.Name, e.Method, e.Timeout)
}

func newRPCPlumber(name string) *rpcPlumber {
	return &rpcPlumber{name: name, nextID: 1, pending: make(map[int]chan rpcReply)}
}

func (p *rpcPlumber) setWriter(w io.Writer) {
	p.wmu.Lock()
	p.w = w
	p.transportEpoch.Add(1)
	p.wmu.Unlock()
}

func (p *rpcPlumber) hasWriter() bool {
	p.wmu.Lock()
	defer p.wmu.Unlock()
	return p.w != nil
}

func (p *rpcPlumber) write(obj any) error {
	return p.writeAtEpoch(obj, nil)
}

// The expected generation is checked under the same lock as the write. A
// reconnected transport must not reuse a read admission from an older peer.
func (p *rpcPlumber) writeAtEpoch(obj any, expected *uint64) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	p.wmu.Lock()
	defer p.wmu.Unlock()
	if expected != nil && p.transportEpoch.Load() != *expected {
		return &codexAcceptanceError{Reason: "native_source_changed"}
	}
	if p.w == nil {
		return fmt.Errorf("[%s] no writer", p.name)
	}
	_, err = p.w.Write(append(data, '\n'))
	return err
}

// request sends a method call and blocks until the matching response arrives or
// the timeout elapses.
func (p *rpcPlumber) request(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	return p.requestAtEpoch(method, params, timeout, nil)
}

func (p *rpcPlumber) requestAtEpoch(method string, params any, timeout time.Duration, expected *uint64) (json.RawMessage, error) {
	return p.requestAtEpochContext(context.Background(), method, params, timeout, expected)
}

// Cancellation retires only this response correlation. It neither cancels the
// server operation nor closes/replaces the shared writer. Writes retain the
// original transport's locking and I/O bounds; ctx bounds the response wait.
func (p *rpcPlumber) requestAtEpochContext(ctx context.Context, method string, params any, timeout time.Duration, expected *uint64) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	id := p.nextID
	p.nextID++
	ch := make(chan rpcReply, 1)
	p.pending[id] = ch
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
	}()

	req := map[string]any{"id": id, "method": method}
	if params != nil {
		req["params"] = params
	}
	if err := p.writeAtEpoch(req, expected); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case reply := <-ch:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return reply.result, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, &rpcTimeoutError{Name: p.name, Method: method, Timeout: timeout}
	}
}

func (p *rpcPlumber) notify(method string, params any) error {
	msg := map[string]any{"method": method}
	if params != nil {
		msg["params"] = params
	}
	return p.write(msg)
}

// dispatchResponse routes a response/error frame to its pending request.
// Returns true if the frame was consumed (i.e. it was a response, not a
// notification or server request).
func (p *rpcPlumber) dispatchResponse(raw json.RawMessage) bool {
	var probe struct {
		ID     *int            `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if json.Unmarshal(raw, &probe) != nil || probe.ID == nil {
		return false
	}
	if probe.Result == nil && probe.Error == nil {
		return false // server request (id + method) — not a response
	}
	p.mu.Lock()
	ch := p.pending[*probe.ID]
	delete(p.pending, *probe.ID)
	p.mu.Unlock()
	if ch == nil {
		return true
	}
	if probe.Error != nil {
		ch <- rpcReply{err: &rpcResponseError{raw: string(probe.Error)}}
	} else {
		ch <- rpcReply{result: probe.Result}
	}
	return true
}

func (p *rpcPlumber) failAll(err error) {
	p.transportEpoch.Add(1)
	p.mu.Lock()
	for id, ch := range p.pending {
		ch <- rpcReply{err: err}
		delete(p.pending, id)
	}
	p.mu.Unlock()
}

func (p *rpcPlumber) pendingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending)
}
