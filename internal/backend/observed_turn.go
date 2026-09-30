package backend

import "everything-go/internal/session"

// NativeTurnObserver reconciles explicit lifecycle records without resuming or
// claiming the native client's thread.
type NativeTurnObserver interface {
	ObserveNativeLifecycle(*session.Session, string, string)
}

// ObservedTurn reports a native client's lifecycle without claiming the Bridge
// session actor or its queue. It is internal; clients receive session_runtime
// and the usual terminal events.
type ObservedTurn struct {
	SessionID string
	RequestID string
	Phase     string
	ErrorCode string
	Message   string
}
