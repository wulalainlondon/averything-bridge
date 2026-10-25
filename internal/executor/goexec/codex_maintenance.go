package goexec

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (c *Codex) loadMaintenance() {
	c.maintenanceMu.Lock()
	defer c.maintenanceMu.Unlock()
	c.maintenance = map[string]backend.Maintenance{}
	raw, err := os.ReadFile(filepath.Join(c.dataDir, "codex_maintenance.json"))
	if os.IsNotExist(err) {
		return
	}
	if err != nil || json.Unmarshal(raw, &c.maintenance) != nil {
		// A damaged journal must not silently authorize queued execution.
		c.maintenance = map[string]backend.Maintenance{"*": {SessionID: "*", State: "unknown", Message: "Maintenance journal could not be read"}}
	}
}
func (c *Codex) saveMaintenanceLocked() error {
	if c.dataDir == "" {
		return nil
	}
	if err := os.MkdirAll(c.dataDir, 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(c.maintenance)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(c.dataDir, ".maintenance-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(c.dataDir, "codex_maintenance.json")); err != nil {
		return err
	}
	dir, err := os.Open(c.dataDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func (c *Codex) MaintenanceRecords() []backend.Maintenance {
	c.maintenanceMu.Lock()
	defer c.maintenanceMu.Unlock()
	out := []backend.Maintenance{}
	for _, r := range c.maintenance {
		out = append(out, r)
	}
	return out
}
func (c *Codex) beginMaintenance(st *codexState) (string, error) {
	st.mu.Lock()
	thread, request := st.threadID, st.reqID
	st.mu.Unlock()
	c.mu.Lock()
	s := c.threadToSession[thread]
	c.mu.Unlock()
	if s == nil {
		return "", nil
	} // isolated unit-test states have no Session owner
	now := time.Now().UnixMilli()
	r := backend.Maintenance{SessionID: s.ID, RequestID: request, ThreadID: thread, OperationID: fmt.Sprintf("compact-%d", time.Now().UnixNano()), Model: s.Snapshot().Model, State: "running", StartedAt: now, UpdatedAt: now, Message: "正在整理上下文"}
	if info, err := os.Stat(c.findCodexSessionFile(thread)); err == nil {
		r.LogOffset = info.Size()
	}
	c.maintenanceMu.Lock()
	if c.maintenance == nil {
		c.maintenance = map[string]backend.Maintenance{}
	}
	c.maintenance[s.ID] = r
	err := c.saveMaintenanceLocked()
	c.maintenanceMu.Unlock()
	if err != nil {
		return s.ID, err
	}
	c.sink.Emit(r)
	return s.ID, nil
}
func (c *Codex) updateMaintenance(id, state, message, turn string, expectedOperation ...string) {
	if id == "" {
		return
	}
	c.maintenanceMu.Lock()
	r, ok := c.maintenance[id]
	if len(expectedOperation) > 0 && r.OperationID != expectedOperation[0] {
		c.maintenanceMu.Unlock()
		return
	}
	if !ok {
		c.maintenanceMu.Unlock()
		return
	}
	r.State = state
	r.Message = message
	r.UpdatedAt = time.Now().UnixMilli()
	if turn != "" {
		r.TurnID = turn
	}
	c.maintenance[id] = r
	if err := c.saveMaintenanceLocked(); err != nil {
		r.State = "unknown"
		r.Message = "Maintenance result could not be saved: " + err.Error()
		c.maintenance[id] = r
	}
	c.maintenanceMu.Unlock()
	c.sink.Emit(r)
}

// Check the durable Codex terminal event, never infer success from idle status.
// Streaming JSON decoding bounds memory even for a very large rollout.
func compactTerminal(ctx context.Context, path string, r *backend.Maintenance) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err = f.Seek(r.LogOffset, 0); err != nil {
		return "", err
	}
	reader := bufio.NewScanner(f)
	reader.Buffer(make([]byte, 65536), 32*1024*1024)
	candidate := ""
	compacted := false
	ambiguous := false
	for reader.Scan() {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		line := reader.Bytes()
		var v struct {
			Timestamp string `json:"timestamp"`
			Type      string `json:"type"`
			Payload   struct {
				Type   string          `json:"type"`
				TurnID string          `json:"turn_id"`
				Error  json.RawMessage `json:"error"`
			} `json:"payload"`
		}
		if json.Unmarshal(line, &v) != nil {
			continue
		}
		if r.TurnID == "" {
			// Some daemon versions omit compact turn/started on the socket.
			// Require a complete native sequence after the saved log offset;
			// no user message or competing turn may intervene.
			if v.Type == "event_msg" && v.Payload.Type == "user_message" {
				ambiguous = true
			}
			if v.Type == "event_msg" && v.Payload.Type == "task_started" {
				if candidate != "" {
					ambiguous = true
				}
				candidate = v.Payload.TurnID
			}
			if v.Type == "compacted" && candidate != "" {
				compacted = true
			}
			if !ambiguous && compacted && v.Type == "event_msg" && v.Payload.Type == "task_complete" && v.Payload.TurnID == candidate {
				r.TurnID = candidate
			}
		}
		if v.Type == "event_msg" && v.Payload.Type == "task_complete" && r.TurnID != "" && v.Payload.TurnID == r.TurnID {
			if len(v.Payload.Error) > 0 && string(v.Payload.Error) != "null" {
				return "failed", nil
			}
			return "completed", nil
		}
	}
	return "", reader.Err()
}
func (c *Codex) ReconcileMaintenance(ctx context.Context, id string, release bool) (backend.Maintenance, error) {
	c.maintenanceMu.Lock()
	r, ok := c.maintenance[id]
	c.maintenanceMu.Unlock()
	if !ok {
		return r, errors.New("no maintenance record")
	}
	if !r.BlocksQueue() {
		return r, nil
	}
	c.mu.Lock()
	st := c.states[id]
	c.mu.Unlock()
	if st != nil {
		st.mu.Lock()
		active := st.compactActive
		st.mu.Unlock()
		if active {
			return r, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return r, err
	}
	state, err := compactTerminal(ctx, c.findCodexSessionFile(r.ThreadID), &r)
	if err != nil || state == "" {
		if !release {
			return r, errors.New("尚未找到對應壓縮操作的完成紀錄，佇列保持暫停")
		}
		probe, openErr := c.openHealthProbe()
		if openErr != nil {
			return r, openErr
		}
		defer probe.close()
		raw, readErr := probe.call("thread/read", map[string]any{"threadId": r.ThreadID, "includeTurns": false})
		if readErr != nil {
			return r, readErr
		}
		var response struct {
			Thread struct {
				ID     string `json:"id"`
				Status struct {
					Type string `json:"type"`
				} `json:"status"`
			} `json:"thread"`
		}
		if json.Unmarshal(raw, &response) != nil || response.Thread.ID != r.ThreadID || response.Thread.Status.Type != "idle" {
			return r, errors.New("執行端仍非閒置，不能放行佇列")
		}
		// Explicit user consent plus confirmed idle permits continuing, but is
		// never reported as proof that the old operation succeeded.
		state = "released"
	}
	if release && state == "failed" {
		state = "released"
	}
	c.updateMaintenance(id, state, map[string]string{"completed": "上下文整理完成", "failed": "上下文整理失敗；後續訊息已保留，請確認後繼續", "released": "已確認繼續處理排隊訊息"}[state], r.TurnID, r.OperationID)
	for _, next := range c.MaintenanceRecords() {
		if next.SessionID == id {
			return next, nil
		}
	}
	return r, nil
}
