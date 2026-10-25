package backend

import (
	"context"
	"encoding/json"
)

type LineServiceProvider interface {
	LineServiceEnabled() bool
	ExecuteLineTool(context.Context, SessionControlCaller, string, json.RawMessage) (any, error)
}

// Caller and payload proof are derived from an actual provider-observed CLI command.
type LineOriginResolver interface {
	ResolveLineCaller(thread, operation, fingerprint string) (SessionControlCaller, error)
}
