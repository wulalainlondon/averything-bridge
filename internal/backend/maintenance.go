package backend

import "context"

// Maintenance is a durable operation record, never a copy of conversation text.
type Maintenance struct {
	SessionID   string `json:"session_id"`
	RequestID   string `json:"request_id"`
	OperationID string `json:"operation_id"`
	ThreadID    string `json:"thread_id"`
	TurnID      string `json:"turn_id,omitempty"`
	Model       string `json:"model,omitempty"`
	State       string `json:"state"`
	Message     string `json:"message,omitempty"`
	StartedAt   int64  `json:"started_at"`
	UpdatedAt   int64  `json:"updated_at"`
	LogOffset   int64  `json:"log_offset,omitempty"`
}

func (m Maintenance) BlocksQueue() bool { return m.State != "completed" && m.State != "released" }

type MaintenanceProvider interface {
	MaintenanceRecords() []Maintenance
	ReconcileMaintenance(context.Context, string, bool) (Maintenance, error)
}
