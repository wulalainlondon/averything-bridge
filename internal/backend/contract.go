// Package backend defines the model/backend adapter contract used by the core
// session gateway. Backend definitions here are intentionally independent of
// the app-v1 JSON protocol and of the transport carrying client commands.
package backend

import (
	"context"
	"errors"

	"everything-go/internal/history"
	"everything-go/internal/session"
)

var ErrUnsupportedGoal = errors.New("goal mode is only supported for Codex sessions")
var (
	ErrUnsupportedSteer   = errors.New("same-turn steering is not supported by this backend")
	ErrSteerRejected      = errors.New("steering was rejected before applying input")
	ErrNoActiveTurn       = errors.New("there is no active turn to steer")
	ErrThreadActiveWriter = errors.New("codex thread is controlled by another client")
)

const (
	Claude   = "claude"
	Codex    = "codex"
	Ollama   = "ollama"
	RemoteWS = "remote-ws"
)

// Model describes one selectable model for a backend.
type Model struct {
	ID                        string
	Label                     string
	Description               string
	SupportedReasoningEfforts []string
	DefaultReasoningEffort    string
	InputModalities           []string
	SupportsPersonality       bool
	ServiceTiers              []ServiceTier
	DefaultServiceTier        string
	IsDefault                 bool
}

type ServiceTier struct {
	ID          string
	Name        string
	Description string
}

type CollaborationMode struct {
	Name            string
	Mode            string
	Model           string
	ReasoningEffort string
}

// Capabilities declares what the core and app may ask this backend to do.
// Backend implementations should expose capabilities here instead of requiring
// the client protocol or UI to special-case backend names.
type Capabilities struct {
	History      bool
	Usage        bool
	Interactions bool
	Sandbox      bool
	Images       bool
	Files        bool
	Remote       bool
	Steering     bool
}

// Definition is the backend registry entry advertised by the bridge. It is the
// neutral source of truth; clientproto adapters translate it to their wire
// representation.
type Definition struct {
	ID                 string
	Label              string
	DefaultModel       string
	Models             []Model
	Capabilities       Capabilities
	CollaborationModes []CollaborationMode
}

// CatalogProvider returns the runtime model catalog exposed by a backend.
// Implementations may start their local protocol server, so callers should use
// it off the connection read loop and retain the static registry as fallback.
type CatalogProvider interface {
	Catalog(context.Context) (Definition, error)
}

// ImageAttachment is an image attached to a user message. Data is raw base64;
// protocol adapters are responsible for stripping any transport-specific data
// URL prefix before this reaches a backend.
type ImageAttachment struct {
	Data      string
	MediaType string
}

// FileAttachment is a file attached to a user message. Content is the
// adapter-normalized payload used by the backend implementation.
type FileAttachment struct {
	AttachmentID string
	Name         string
	Content      string
	MediaType    string
	RemotePath   string
	SizeBytes    int64
}

// HistoryProvider is the optional backend capability for loading persisted
// transcript history and resumable sessions. The history package owns the
// slicing/hash domain logic; backend owns the capability boundary.
type HistoryProvider = history.Provider

// Sink receives normalized core events emitted by backend adapters. The current
// event vocabulary is protocol.* while the client protocol remains app-v1; new
// backends should emit through this sink rather than constructing client frames.
type Sink interface {
	Emit(event any)
}

// Executor is the backend adapter lifecycle. It owns all runtime-specific
// resources and reports progress only through Sink.
type Executor interface {
	Send(ctx context.Context, s *session.Session, reqID, content string, images []ImageAttachment, files []FileAttachment) error
	Stop(ctx context.Context, s *session.Session) error
	Clear(ctx context.Context, s *session.Session) error
	Close(ctx context.Context, s *session.Session) error
}

type sandboxOverrideContextKey struct{}

// WithSandboxOverride applies a server-authoritative sandbox to one submitted
// turn. Backends that support per-turn policy overrides must honor it instead
// of trusting the Session's ordinary interactive sandbox.
func WithSandboxOverride(ctx context.Context, sandbox string) context.Context {
	return context.WithValue(ctx, sandboxOverrideContextKey{}, sandbox)
}

// SandboxOverride returns the sandbox explicitly attached to this turn.
func SandboxOverride(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(sandboxOverrideContextKey{}).(string)
	return value, ok && value != ""
}

// SteerResult identifies the active turn that accepted a same-turn user
// message. RequestID is the original request that owns the turn; clients use it
// to keep subsequent streaming events attached to the same lifecycle.
type SteerResult struct {
	TurnID    string
	RequestID string
}

// SteeringExecutor is an optional capability implemented by backends that can
// inject additional user input into an already-running turn.
type SteeringExecutor interface {
	Steer(ctx context.Context, s *session.Session, clientUserMessageID, content string, images []ImageAttachment, files []FileAttachment) (SteerResult, error)
}

// GoalController is an optional capability for Codex app-server backed
// sessions. Implementations emit goal_update / goal_cleared through Sink.
type GoalController interface {
	SetGoal(ctx context.Context, s *session.Session, objective, status string, tokenBudget *int) error
	GetGoal(ctx context.Context, s *session.Session) error
	ClearGoal(ctx context.Context, s *session.Session) error
}

// SessionControlExecutor transfers a resumable session between the Bridge and
// another client of the shared backend daemon without clearing or archiving it.
type SessionControlExecutor interface {
	ReleaseSession(ctx context.Context, s *session.Session) error
	ClaimSession(ctx context.Context, s *session.Session) error
}
