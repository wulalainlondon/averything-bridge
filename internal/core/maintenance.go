package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"time"
)

func (h *Hub) applyMaintenanceHold(r backend.Maintenance) {
	releaseHold := func() {}
	recovered := false
	h.messageQueueMu.Lock()
	if h.maintenanceHolds == nil {
		h.maintenanceHolds = map[string]func(){}
	}
	if r.BlocksQueue() {
		if h.maintenanceHolds[r.SessionID] == nil {
			if s, ok := h.registry.Get(r.SessionID); ok {
				h.maintenanceHolds[r.SessionID] = s.HoldQueue()
			}
		}
	} else if release := h.maintenanceHolds[r.SessionID]; release != nil {
		delete(h.maintenanceHolds, r.SessionID)
		releaseHold = release
		if r.State == "completed" && h.messageQueue != nil {
			_, recovered, _ = h.messageQueue.Transition(r.SessionID, r.RequestID, []messagequeue.State{messagequeue.Uncertain}, messagequeue.Completed, "壓縮結果已確認", "", r.TurnID)
		}
	}
	h.messageQueueMu.Unlock()
	if recovered {
		h.updateRuntime(r.SessionID, "completed", r.RequestID, h.sessionQueueLength(r.SessionID), "completed", "")
	}
	releaseHold()
	if h.messageQueue != nil {
		_ = h.messageQueue.Touch(r.SessionID)
	}
	h.publishMessageQueue(r.SessionID)
}
func (h *Hub) restoreMaintenanceHolds() {
	if p, ok := h.exec.(backend.MaintenanceProvider); ok {
		for _, r := range p.MaintenanceRecords() {
			if r.SessionID == "*" {
				for _, s := range h.registry.List() {
					copy := r
					copy.SessionID = s.ID
					h.applyMaintenanceHold(copy)
				}
			} else {
				h.applyMaintenanceHold(r)
			}
		}
		go h.reconcileMaintenance("", false)
	}
}
func (h *Hub) reconcileMaintenance(id string, release bool, expectedOperation ...string) {
	if !h.maintenanceCheck.TryLock() {
		return
	}
	defer h.maintenanceCheck.Unlock()
	if p, ok := h.exec.(backend.MaintenanceProvider); ok {
		for _, r := range p.MaintenanceRecords() {
			if release && (len(expectedOperation) == 0 || r.OperationID != expectedOperation[0]) {
				continue
			}
			if r.BlocksQueue() && (id == "" || r.SessionID == id) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, _ = p.ReconcileMaintenance(ctx, r.SessionID, release)
				cancel()
			}
		}
	}
}

func (h *Hub) StartMaintenanceRecovery(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.reconcileMaintenance("", false)
			}
		}
	}()
}
func (h *Hub) resumeMaintenanceQueue(c *Client, cmd clientproto.Command) {
	if !queueRequestValid(cmd) {
		return
	}
	if !h.controls.MobileMayWrite(cmd.SessionID) {
		h.queueError(c, cmd, "session_controlled_by_desktop", "請先收回對話控制權")
		return
	}
	h.reconcileMaintenance(cmd.SessionID, true, cmd.RequestID)
	h.sendMessageQueue(c, cmd)
	status := "accepted"
	message := "已確認，後續訊息會依序處理"
	if p, ok := h.exec.(backend.MaintenanceProvider); ok {
		for _, r := range p.MaintenanceRecords() {
			if r.SessionID == cmd.SessionID && r.BlocksQueue() {
				status = "uncertain"
				message = r.Message
			}
		}
	}
	h.queueResult(c, cmd, "resume", status, message, messagequeue.Entry{})
}
