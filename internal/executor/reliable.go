package executor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type turnKey struct {
	sessionID string
	reqID     string
}

// TerminalSink wraps the real outbound sink and tracks terminal turn events.
// Reliable executors report confirmed terminals, send failures and panics.
// Elapsed time alone is not a terminal: it cannot settle an active native turn.
type TerminalSink struct {
	delegate Sink

	mu       sync.Mutex
	inflight map[turnKey]chan struct{}
	bySess   map[string]map[turnKey]bool
}

func NewTerminalSink(delegate Sink) *TerminalSink {
	return &TerminalSink{delegate: delegate, inflight: make(map[turnKey]chan struct{}), bySess: make(map[string]map[turnKey]bool)}
}

// Deprecated: the duration is ignored. Keep source compatibility for embedders,
// but never manufacture an error or free a live turn on a wall-clock deadline.
func NewTerminalSinkWithTimeout(delegate Sink, _ time.Duration) *TerminalSink {
	return NewTerminalSink(delegate)
}

func (s *TerminalSink) Emit(event any) {
	s.delegate.Emit(event)
	s.observeTerminal(event)
}

func (s *TerminalSink) Begin(sessionID, reqID string) turnKey {
	k := turnKey{sessionID: sessionID, reqID: reqID}
	done := make(chan struct{})
	s.mu.Lock()
	s.inflight[k] = done
	if s.bySess[sessionID] == nil {
		s.bySess[sessionID] = map[turnKey]bool{}
	}
	s.bySess[sessionID][k] = true
	s.mu.Unlock()

	return k
}

func (s *TerminalSink) Done(k turnKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight[k] == nil
}

func (s *TerminalSink) complete(k turnKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := s.inflight[k]
	if ch == nil {
		return false
	}
	delete(s.inflight, k)
	if m := s.bySess[k.sessionID]; m != nil {
		delete(m, k)
		if len(m) == 0 {
			delete(s.bySess, k.sessionID)
		}
	}
	close(ch)
	return true
}

func (s *TerminalSink) completeSession(sessionID string) {
	s.mu.Lock()
	keys := make([]turnKey, 0, len(s.bySess[sessionID]))
	for k := range s.bySess[sessionID] {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	for _, k := range keys {
		s.complete(k)
	}
}

func (s *TerminalSink) observeTerminal(event any) {
	switch e := event.(type) {
	case backend.NativeSubmissionUncertain:
		// This settles the adapter invocation, not the native work. The Hub
		// retains its uncertain receipt; do not overwrite it with send_error.
		s.complete(turnKey{sessionID: e.SessionID, reqID: e.RequestID})
	case protocol.Done:
		s.complete(turnKey{sessionID: e.SessionID, reqID: e.RequestID})
	case protocol.Stopped:
		if e.RequestID != "" {
			s.complete(turnKey{sessionID: e.SessionID, reqID: e.RequestID})
		} else {
			s.completeSession(e.SessionID)
		}
	case protocol.Error:
		if e.SessionID == "" {
			return
		}
		if e.RequestID != "" {
			s.complete(turnKey{sessionID: e.SessionID, reqID: e.RequestID})
		} else {
			s.completeSession(e.SessionID)
		}
	}
}

// sendReliable enforces the Executor adapter contract for one turn. It lives
// below Mux.Send so optional capability detection still sees the raw backend.
func sendReliable(ctx context.Context, inner Executor, sink *TerminalSink, s *session.Session, reqID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) (err error) {
	k := sink.Begin(s.ID, reqID)
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("executor panic: %v", rec)
			log.Printf("[%s] %v", s.ID, err)
			if !sink.Done(k) {
				sink.Emit(backend.NewError(s.ID, reqID, backend.ErrPanic, err.Error()))
			}
		}
	}()
	err = inner.Send(ctx, s, reqID, content, images, files)
	if errors.Is(err, backend.ErrThreadActiveWriter) || errors.Is(err, backend.ErrNativeSubmissionBusy) {
		// The Hub converts this ownership conflict into a durable session_control
		// event plus a request-scoped rejection. Do not manufacture a generic red
		// send_error before it gets that opportunity.
		sink.complete(k)
		return err
	}
	if err != nil && !sink.Done(k) {
		sink.Emit(backend.NewError(s.ID, reqID, backend.ErrSend, err.Error()))
	}
	return err
}
