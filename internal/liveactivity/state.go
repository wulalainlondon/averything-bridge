// Package liveactivity contains the bounded, read-only Live Activity contract.
package liveactivity

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
)

type State struct {
	Phase         string `json:"phase"`
	Stage         string `json:"stage"`
	Revision      uint64 `json:"revision"`
	UpdatedAt     int64  `json:"updatedAt"`
	StartedAt     int64  `json:"startedAt"`
	ResultPending bool   `json:"resultPending"`
}

func (s State) Terminal() bool {
	return s.Phase == "completed" || s.Phase == "failed" || s.Phase == "interrupted" || s.Phase == "closed"
}
func (s State) Valid() bool {
	switch s.Phase {
	case "queued", "running", "waiting", "stopping", "completed", "failed", "interrupted", "closed":
	default:
		return false
	}
	body, err := json.Marshal(s)
	return err == nil && len(body) < 2048 && len(s.Stage) <= 80 && s.Revision > 0 && s.Revision <= uint64(1<<53-1) && s.UpdatedAt > 0 && s.UpdatedAt < math.MaxInt64/2 && s.StartedAt >= 0
}
func (s State) Accepts(next State) bool {
	return next.Valid() && next.Revision > s.Revision && (!s.Terminal() || next.Terminal())
}
func ValidToken(token string) bool {
	if len(token) < 32 || len(token) > 1024 || len(token)%2 != 0 || token != strings.ToLower(token) {
		return false
	}
	_, err := hex.DecodeString(token)
	return err == nil
}

// Delivery is not arbitrary APNs JSON. The managed sender supplies the fixed
// app topic and retrieves the device-scoped enrolled token server-side.
type Delivery struct {
	ActivityID    string `json:"activity_id"`
	Timestamp     int64  `json:"timestamp"`
	Event         string `json:"event"`
	State         State  `json:"content_state"`
	StaleDate     int64  `json:"stale_date,omitempty"`
	DismissalDate int64  `json:"dismissal_date,omitempty"`
}

func (d Delivery) Valid() bool {
	if len(d.ActivityID) == 0 || len(d.ActivityID) > 160 || d.Timestamp <= 0 || !d.State.Valid() {
		return false
	}
	if d.Event == "end" {
		return d.State.Terminal() && d.DismissalDate >= d.Timestamp && d.DismissalDate <= d.Timestamp+3600
	}
	return d.Event == "update" && !d.State.Terminal() && d.StaleDate >= d.Timestamp && d.StaleDate <= d.Timestamp+3600 && d.DismissalDate == 0
}
