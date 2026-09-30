package backend

import (
	"context"
	"everything-go/internal/recap"
	"everything-go/internal/session"
)

// RecapExecutor is independent from Send and never writes to the parent thread.
type RecapExecutor interface {
	SessionRecap(context.Context, *session.Session, bool, bool) (recap.Result, error)
}
