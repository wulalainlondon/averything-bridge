// Package toolenv owns diagnostics and maintenance records, never chat lifecycle.
package toolenv

import (
	"errors"
	"regexp"
)

const Capability = "tool_environment_v1"

var validID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,100}$`)

func ValidID(s string) bool { return validID.MatchString(s) }

type Service struct {
	Name  string   `json:"name"`
	State string   `json:"state"`
	Auth  string   `json:"auth"`
	Tools []string `json:"tools"`
}

type Snapshot struct {
	Binding        string    `json:"-"`
	ThreadID       string    `json:"thread_id"`
	Generation     string    `json:"generation"`
	Revision       int64     `json:"revision"`
	CheckedAt      int64     `json:"checked_at_ms"`
	State          string    `json:"state"`
	Reason         string    `json:"reason"`
	Coverage       string    `json:"coverage"`
	Evidence       string    `json:"evidence"`
	RunningVersion string    `json:"running_version"`
	DiskVersion    string    `json:"disk_version"`
	ThreadState    string    `json:"thread_state"`
	Services       []Service `json:"services"`
	ReloadAllowed  bool      `json:"reload_allowed"`
	ForkAllowed    bool      `json:"fork_allowed"`
}

// Request contains no arbitrary executable code, config overrides or URLs.
type Request struct {
	Refresh              bool   `json:"refresh"`
	Action               string `json:"action"`
	Token                string `json:"repair_token"`
	OperationID          string `json:"operation_id"`
	MaintenanceConfirmed bool   `json:"maintenance_confirmed"`
}

type Identity struct {
	Authority string `json:"authority"`
	Device    string `json:"device"`
	Session   string `json:"session"`
	Thread    string `json:"thread"`
}

type Plan struct {
	Binding    string   `json:"-"`
	Token      string   `json:"repair_token"`
	Action     string   `json:"action"`
	Scope      string   `json:"scope"`
	Generation string   `json:"generation"`
	ExpiresAt  int64    `json:"expires_at_ms"`
	Identity   Identity `json:"-"`
}

type Operation struct {
	ThreadID   string   `json:"thread_id"`
	ID         string   `json:"operation_id"`
	Action     string   `json:"action"`
	Phase      string   `json:"phase"`
	Reason     string   `json:"reason"`
	Generation string   `json:"generation"`
	Revision   int64    `json:"revision"`
	UpdatedAt  int64    `json:"updated_at_ms"`
	Evidence   string   `json:"evidence"`
	NewThread  string   `json:"new_thread_id,omitempty"`
	NewSession string   `json:"new_session_id,omitempty"`
	Identity   Identity `json:"-"`
}

func (o Operation) Active() bool {
	return o.Phase == "waiting_idle" || o.Phase == "applying" || o.Phase == "verifying"
}

type Result struct {
	Snapshot  Snapshot
	NewThread string
}

// Fault codes are safe to send to clients. Provider errors can contain secrets.
type Fault struct{ Code string }

func (e *Fault) Error() string { return e.Code }
func Error(code string) error  { return &Fault{code} }
func Code(err error) string {
	var f *Fault
	if errors.As(err, &f) {
		return f.Code
	}
	return "inspection_failed"
}
