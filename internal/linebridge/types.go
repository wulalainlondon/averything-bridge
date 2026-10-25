// Package linebridge owns account-scoped LINE operations. SDK clients and
// permanent credentials live in isolated account processes, never model tools.
package linebridge

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type Account struct {
	ID             string `json:"account_ref"`
	Label          string `json:"label"`
	ProfileName    string `json:"profile_name,omitempty"`
	Enabled        bool   `json:"enabled"`
	Revision       uint64 `json:"binding_revision"`
	AuthState      string `json:"auth_state"`
	CreatedAt      int64  `json:"created_at"`
	Owner          string `json:"-"`
	DataDir        string `json:"-"`
	KeychainHelper string `json:"-"`
	Principal      string `json:"-"`
}
type Caller struct {
	Subject string
	Admin   bool
}
type Target struct {
	HasAvatar     *bool  `json:"has_avatar,omitempty"`
	Alias         string `json:"alias,omitempty"`
	StatusMessage string `json:"status_message,omitempty"`
	Ref           string `json:"target_ref"`
	Account       string `json:"account_ref"`
	Name          string `json:"name"`
	Kind          string `json:"kind"`
	ExternalID    string `json:"-"`
}
type Content struct {
	Text        string `json:"text,omitempty"`
	ReplyTo     string `json:"reply_to,omitempty"`
	ResourceRef string `json:"resource_ref,omitempty"`
}
type Operation struct {
	ID              string  `json:"operation_id"`
	Account         string  `json:"account_ref"`
	Target          string  `json:"target_ref"`
	Kind            string  `json:"kind"`
	State           string  `json:"state"`
	Effect          string  `json:"effect_state"`
	RemoteID        string  `json:"message_id,omitempty"`
	Code            string  `json:"code,omitempty"`
	CreatedAt       int64   `json:"created_at"`
	UpdatedAt       int64   `json:"updated_at"`
	DueAt           int64   `json:"due_at,omitempty"`
	Timezone        string  `json:"timezone,omitempty"`
	Revision        uint64  `json:"revision"`
	Content         Content `json:"-"`
	Subject         string  `json:"-"`
	Intent          string  `json:"-"`
	Fingerprint     string  `json:"-"`
	BindingRevision uint64  `json:"-"`
	LateWindow      int64   `json:"-"`
}
type Message struct {
	TextTruncated bool   `json:"text_truncated,omitempty"`
	ID            string `json:"message_ref"`
	Target        string `json:"target_ref"`
	Direction     string `json:"direction"`
	Text          string `json:"text,omitempty"`
	ContentType   string `json:"content_type"`
	Timestamp     int64  `json:"timestamp"`
}
type RuntimeEvent struct {
	Kind    string   `json:"kind"`
	Key     string   `json:"event_key"`
	Target  string   `json:"target"`
	Cursor  string   `json:"cursor,omitempty"`
	Message *Message `json:"message,omitempty"`
	Code    string   `json:"code,omitempty"`
}
type Identity struct {
	Principal string `json:"principal"`
	Name      string `json:"name"`
}
type Driver interface {
	Invoke(context.Context, string, any, any) error
	Close() error
}
type Factory interface {
	Start(context.Context, Account, string, func(RuntimeEvent)) (Driver, error)
}
type EventSink interface {
	Commit(context.Context, string, string, string, []RuntimeEvent, string) error
	Cursor(context.Context, string, string) (string, error)
}
type Status struct {
	LastHealth   *HealthObservation `json:"last_health,omitempty"`
	Account      Account            `json:"account"`
	RuntimeState string             `json:"runtime_state"`
	Epoch        string             `json:"runtime_epoch,omitempty"`
	Capabilities map[string]string  `json:"capabilities"`
	CheckedAt    int64              `json:"checked_at"`
	Connectivity string             `json:"connectivity"`
}
type Failure struct {
	Code         string
	BeforeEffect bool
}

func (e *Failure) Error() string { return e.Code }
func Fail(code string) error     { return &Failure{Code: code, BeforeEffect: true} }
func SafeCode(err error) string {
	var e *Failure
	if errors.As(err, &e) {
		return e.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "line_timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "line_cancelled"
	}
	return "line_operation_failed"
}
func nowMS() int64                  { return time.Now().UnixMilli() }
func raw(value any) json.RawMessage { b, _ := json.Marshal(value); return b }
