package goexec

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

// rpcPlumber implements JSON-RPC 2.0 over newline-delimited stdio, matching
// bridge/backends/jsonrpc.py. The codex app-server omits the "jsonrpc" field, so
// we do too: requests are {id, method, params}, responses {id, result|error},
// notifications {method, params}.
type rpcPlumber struct {
	name string

	wmu sync.Mutex
	w   io.Writer

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
	p.wmu.Unlock()
}

func (p *rpcPlumber) hasWriter() bool {
	p.wmu.Lock()
	defer p.wmu.Unlock()
	return p.w != nil
}

func (p *rpcPlumber) write(obj any) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	p.wmu.Lock()
	defer p.wmu.Unlock()
	if p.w == nil {
		return fmt.Errorf("[%s] no writer", p.name)
	}
	_, err = p.w.Write(append(data, '\n'))
	return err
}

// request sends a method call and blocks until the matching response arrives or
// the timeout elapses.
func (p *rpcPlumber) request(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	p.mu.Lock()
	id := p.nextID
	p.nextID++
	ch := make(chan rpcReply, 1)
	p.pending[id] = ch
	p.mu.Unlock()

	req := map[string]any{"id": id, "method": method}
	if params != nil {
		req["params"] = params
	}
	if err := p.write(req); err != nil {
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
		return nil, err
	}

	select {
	case reply := <-ch:
		return reply.result, reply.err
	case <-time.After(timeout):
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
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
