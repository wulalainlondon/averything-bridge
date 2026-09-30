package session

import (
	"errors"
	"log"
	"sync"
	"time"
)

// State is the explicit session lifecycle. Transitions:
//
//	Idle      --BeginTurn-->  Streaming
//	Streaming --EndTurn---->  Idle
//	Streaming --markStop--->  Stopping --EndTurn--> Idle
//	any       --Close------>  Closed   (terminal)
//
// State is owned here and exposed read-only; the connection core observes the
// executor's terminal events (done/stopped/error) and calls EndTurn, while the
// per-session worker calls beginTurn. No backend mutates state directly.
type State int

const (
	Idle State = iota
	Streaming
	Stopping
	Closed
)

func (st State) String() string {
	switch st {
	case Idle:
		return "idle"
	case Streaming:
		return "streaming"
	case Stopping:
		return "stopping"
	case Closed:
		return "closed"
	default:
		return "unknown"
	}
}

// mailboxSize bounds queued turns per session before Submit blocks. The app
// almost never pipelines turns for one session; this is headroom, not a design
// point.
const mailboxSize = 64

// turnWatchdog is a backstop, NOT a turn time limit: if a turn never produces a
// terminal event and is never stopped/cleared (an executor bug), it releases
// the worker so the session doesn't wedge forever. Real turns finish via
// EndTurn long before this fires.
const turnWatchdog = 2 * time.Hour

// State returns the current lifecycle state.
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// QueueLen reports how many turns are waiting behind the in-flight one. Surfaced
// as status_result.queued_commands.
func (s *Session) QueueLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.mailbox)
}

// CanDispatchAutomatic reports whether an automatic durable Run may be handed
// to this Session without pre-filling its in-memory mailbox. Human commands
// that race after this check are still serialized by Submit; this gate keeps a
// busy Session's automatic work authoritative in the durable Work queue.
func (s *Session) CanDispatchAutomatic() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != Idle {
		return false
	}
	return s.mailbox == nil || len(s.mailbox) == 0
}

// Submit enqueues a turn for serial execution. The worker runs fn, then waits
// for the turn's terminal event (EndTurn) before pulling the next one, so two
// turns for the same session never overlap. Returns false if the session is
// closed.
type queuedTurn struct {
	id   string
	run  func()
	held bool
}

var ErrQueueNotWaiting = errors.New("message is no longer waiting")
var ErrQueueBusy = errors.New("a queue operation is already in progress")
var ErrQueueNoActiveTurn = errors.New("no active turn to steer")

// Submit preserves blocking semantics for existing automatic/anonymous tasks.
func (s *Session) Submit(fn func()) bool { return s.submit("", fn, true) }

// SubmitNamed never blocks a transport handler if the bounded mailbox is full.
func (s *Session) SubmitNamed(id string, fn func()) bool { return s.submit(id, fn, false) }

func (s *Session) submit(id string, fn func(), wait bool) bool {
	for {
		s.mu.Lock()
		if s.state == Closed {
			s.mu.Unlock()
			return false
		}
		if !s.workerUp {
			s.workerUp = true
			s.quit = make(chan struct{})
			s.queueChanged = make(chan struct{})
			go s.runWorker(s.quit)
		}
		if id != "" {
			if s.activeQueuedID == id {
				s.mu.Unlock()
				return true
			}
			for _, item := range s.mailbox {
				if item.id == id {
					s.mu.Unlock()
					return true
				}
			}
		}
		if len(s.mailbox) < mailboxSize {
			s.mailbox = append(s.mailbox, &queuedTurn{id: id, run: fn})
			s.signalQueueLocked()
			s.mu.Unlock()
			return true
		}
		changed, quit := s.queueChanged, s.quit
		s.mu.Unlock()
		if !wait {
			return false
		}
		select {
		case <-changed:
		case <-quit:
			return false
		}
	}
}

func (s *Session) signalQueueLocked() {
	if s.queueChanged != nil {
		close(s.queueChanged)
	}
	s.queueChanged = make(chan struct{})
}

// ReserveQueued holds the dequeue boundary while a steering RPC is in flight.
// finish(true) removes the item; finish(false) restores its original FIFO place.
// The closure is idempotent and must be called after persisting the outcome.
func (s *Session) ReserveQueued(id string) (finish func(bool), err error) {
	return s.reserveQueued(id, true)
}
func (s *Session) ReserveWaiting(id string) (finish func(bool), err error) {
	return s.reserveQueued(id, false)
}
func (s *Session) reserveQueued(id string, requireActive bool) (finish func(bool), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if requireActive && s.state != Streaming {
		return nil, ErrQueueNoActiveTurn
	}
	if requireActive && s.queueHolds > 0 {
		return nil, ErrQueueBusy
	}
	var target *queuedTurn
	for _, item := range s.mailbox {
		if item.id == id {
			target = item
			break
		}
	}
	if target == nil {
		return nil, ErrQueueNotWaiting
	}
	if target.held {
		return nil, ErrQueueBusy
	}
	target.held = true
	s.queueHolds++
	var once sync.Once
	return func(remove bool) {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if target.held {
				target.held = false
				s.queueHolds--
			}
			if remove {
				for index, item := range s.mailbox {
					if item == target {
						s.mailbox = append(s.mailbox[:index], s.mailbox[index+1:]...)
						break
					}
				}
			}
			s.signalQueueLocked()
		})
	}, nil
}

func (s *Session) CancelQueued(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index, item := range s.mailbox {
		if item.id != id {
			continue
		}
		if item.held {
			return ErrQueueBusy
		}
		s.mailbox = append(s.mailbox[:index], s.mailbox[index+1:]...)
		s.signalQueueLocked()
		return nil
	}
	return ErrQueueNotWaiting
}

func (s *Session) runWorker(quit <-chan struct{}) {
	for {
		s.mu.Lock()
		if s.state == Closed {
			s.mu.Unlock()
			return
		}
		if len(s.mailbox) == 0 || s.queueHolds > 0 {
			changed := s.queueChanged
			s.mu.Unlock()
			select {
			case <-changed:
			case <-quit:
				return
			}
			continue
		}
		item := s.mailbox[0]
		s.mailbox = s.mailbox[1:]
		s.activeQueuedID = item.id
		done := s.beginTurnLocked()
		s.signalQueueLocked()
		s.mu.Unlock()
		item.run()
		select {
		case <-done:
		case <-quit:
			return
		case <-time.After(turnWatchdog):
			log.Printf("[%s] turn watchdog fired after %s — releasing queue", s.ID, turnWatchdog)
			s.EndTurn()
		}
		s.mu.Lock()
		s.activeQueuedID = ""
		s.mu.Unlock()
	}
}

// beginTurn moves Idle→Streaming and arms a fresh completion signal for the
// worker. Returns the channel the worker waits on. No-op intent if already
// closed (returns an already-fired channel so the worker won't block).
func (s *Session) beginTurnLocked() <-chan struct{} {
	if s.state == Closed {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	s.state = Streaming
	if s.futureConfig != nil {
		s.applyConfigurationLocked(*s.futureConfig)
	}
	s.lastActivity = nowSeconds()
	s.turnDone = make(chan struct{})
	return s.turnDone
}

// EndTurn marks the in-flight turn complete and releases the worker. Called by
// the connection core when the executor emits a terminal event (done/stopped/
// error) and by stop/clear/kill paths that forcibly cancel a turn. Idempotent.
func (s *Session) EndTurn() {
	s.PrepareEndTurn()()
}

// PrepareEndTurn marks the active turn idle immediately but holds the actor
// release signal until the returned closure is called. The Hub uses this tiny
// two-phase boundary to publish done -> terminal runtime in order before the
// next queued turn can begin. Close/duplicate terminal races cannot double
// close turnDone because ownership is detached under the Session lock.
func (s *Session) PrepareEndTurn() func() {
	s.mu.Lock()
	if s.state == Streaming || s.state == Stopping {
		s.state = Idle
		if s.futureConfig != nil {
			s.applyConfigurationLocked(*s.futureConfig)
		}
	}
	done := s.turnDone
	s.turnDone = nil
	s.lastActivity = nowSeconds()
	s.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			if done != nil {
				close(done)
			}
		})
	}
}

// MarkStopping records that a stop was requested for the in-flight turn. Purely
// observational (the actual interrupt is the executor's job); EndTurn follows
// when the turn unwinds.
func (s *Session) MarkStopping() {
	s.mu.Lock()
	if s.state == Streaming {
		s.state = Stopping
	}
	s.mu.Unlock()
}

// Close moves the session to the terminal state and shuts the turn worker down.
// Any queued turns are dropped; an in-flight turn's wait is released. Safe to
// call more than once.
func (s *Session) Close() {
	s.mu.Lock()
	if s.state == Closed {
		s.mu.Unlock()
		return
	}
	s.state = Closed
	if s.turnDone != nil {
		close(s.turnDone)
		s.turnDone = nil
	}
	// Stop the worker by signaling quit; never close the mailbox (a concurrent
	// Submit may still be selecting on it). Queued fns are simply dropped.
	if s.quit != nil {
		close(s.quit)
		s.quit = nil
	}
	for _, item := range s.mailbox {
		item.held = false
	}
	s.mailbox = nil
	s.queueHolds = 0
	s.signalQueueLocked()
	s.workerUp = false
	s.mu.Unlock()
}

func (s *Session) ActiveQueuedID() string { s.mu.Lock(); defer s.mu.Unlock(); return s.activeQueuedID }

// HoldQueue prevents a stop/clear RPC from racing the start of the next turn.
func (s *Session) HoldQueue() func() {
	s.mu.Lock()
	if s.state == Closed {
		s.mu.Unlock()
		return func() {}
	}
	s.queueHolds++
	s.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.state != Closed {
				s.queueHolds--
			}
			s.signalQueueLocked()
		})
	}
}
