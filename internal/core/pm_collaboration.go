package core

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/coordination"
	"everything-go/internal/runtime"
	"everything-go/internal/session"
	"everything-go/internal/workitems"
)

type pmEvent struct {
	Type      string                          `json:"type"`
	Authority string                          `json:"authority_instance_id"`
	RequestID string                          `json:"request_id,omitempty"`
	Profile   coordination.Profile            `json:"profile"`
	Revision  uint64                          `json:"revision"`
	Projects  map[string]coordination.Project `json:"projects"`
	Tasks     map[string]coordination.Task    `json:"tasks"`
	Events    []coordination.Event            `json:"events"`
	Result    *coordination.Result            `json:"result,omitempty"`
	Error     string                          `json:"error_code,omitempty"`
}

func (h *Hub) pmSnapshot(requestID string, result *coordination.Result, failure error) pmEvent {
	e := pmEvent{Type: "pm_collaboration_snapshot", Authority: h.cfg.InstanceID, RequestID: requestID, Profile: coordination.DefaultProfile(), Projects: map[string]coordination.Project{}, Tasks: map[string]coordination.Task{}, Events: []coordination.Event{}, Result: result}
	if h.work != nil {
		if s, err := h.work.Collaboration(context.Background()); err == nil {
			e.Revision = s.Revision
			for id, p := range s.Projects {
				if p.EngineVersion != 2 {
					e.Projects[id] = p
				}
			}
			for id, t := range s.Tasks {
				if s.Projects[t.ProjectID].EngineVersion != 2 {
					e.Tasks[id] = t
				}
			}
			for _, event := range s.Events {
				if s.Projects[event.ProjectID].EngineVersion != 2 {
					e.Events = append(e.Events, event)
				}
			}
			if len(e.Events) > 200 {
				e.Events = e.Events[len(e.Events)-200:]
			}
		} else {
			failure = err
		}
	}
	if failure != nil {
		e.Error = failure.Error()
	}
	return e
}
func (h *Hub) broadcastPM() { h.Emit(h.pmSnapshot("", nil, nil)); h.broadcastCollaboration() }

// EnablePMCollaboration creates a private, authenticated MCP listener. There is
// no global writable HTTP session API and no access via a model-selected path.
func (h *Hub) EnablePMCollaboration(ctx context.Context) error {
	if h.work == nil {
		return errors.New("pm_work_store_required")
	}
	if err := h.recoverPMCollaboration(); err != nil {
		return err
	}
	h.pmSecret = make([]byte, 32)
	if _, err := rand.Read(h.pmSecret); err != nil {
		return err
	}
	dir := filepath.Join(h.cfg.DataDir, "pm-runtime")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	h.pmURL = "http://" + ln.Addr().String()
	h.pmEnabled = true
	srv := &http.Server{Handler: http.HandlerFunc(h.servePMMCP), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	go func() { <-ctx.Done(); _ = srv.Close() }()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.reconcilePM()
			}
		}
	}()
	return nil
}

// A committed admission proves that execution MAY have begun, not that it did
// or did not finish. Never automatically replay that turn after a Bridge crash.
func (h *Hub) recoverPMCollaboration() error {
	_, err := h.work.UpdateCollaboration(context.Background(), func(s *coordination.State) error {
		for id, t := range s.Tasks {
			if t.State == "running" {
				if s.Projects[t.ProjectID].EngineVersion == 2 {
					s.RecoverCollaborationTask(id, time.Now().UnixMilli())
					continue
				}
				t.State = "uncertain"
				t.Epoch++
				t.Result = "Bridge 在工作執行期間重新啟動。請先查看原對話與實際成果，再決定是否要求修改；系統不會自動重跑。"
				s.AddEvent(coordination.Principal{ID: "system"}, t.ProjectID, id, "execution_uncertain", t.Result, t.RequestID, time.Now().UnixMilli(), true)
				t.Revision = s.Revision
				s.Tasks[id] = t
			}
		}
		return nil
	})
	return err
}
func (h *Hub) pmToken(id string) string {
	mac := hmac.New(sha256.New, h.pmSecret)
	mac.Write([]byte(id))
	return hex.EncodeToString(mac.Sum(nil))
}
func (h *Hub) PMConfiguration(id string) (*backend.PMConfiguration, error) {
	if h.work == nil {
		if strings.HasPrefix(id, "pm_") {
			return nil, errors.New("pm_unavailable")
		}
		return nil, nil
	}
	s, err := h.work.Collaboration(context.Background())
	if err != nil {
		return nil, err
	}
	project, task, found := s.ProjectForSession(id)
	if found && task != nil && project.EngineVersion == 2 {
		return h.collaborationWorkerConfiguration(s, *task)
	}
	if !found || task != nil {
		if strings.HasPrefix(id, "pm_") {
			return nil, errors.New("pm_role_missing")
		}
		return nil, nil
	}
	if !h.pmEnabled {
		return nil, errors.New("pm_unavailable")
	}
	if project.ProfileID != coordination.ProfileID || project.ProfileVersion != 1 {
		return nil, errors.New("pm_role_version_unsupported")
	}
	instructions, toolset := coordination.PMInstructions, pmTools()
	if project.EngineVersion == 2 {
		instructions += collaborationPMInstructions
		toolset = collaborationPMTools()
	}
	return &backend.PMConfiguration{Instructions: instructions, MCPURL: h.pmURL + "/mcp/" + id, Token: h.pmToken(id), RuntimeDir: filepath.Join(h.cfg.DataDir, "pm-runtime"), Tools: toolset}, nil
}

func (h *Hub) IsReadOnlyCollaborationWorker(id string) (bool, error) {
	if h.work == nil {
		return false, nil
	}
	s, err := h.work.Collaboration(context.Background())
	if err != nil {
		return false, err
	}
	_, task, ok := s.ProjectForSession(id)
	return ok && task != nil && task.Backend == backend.Claude && task.Sandbox == "read-only", nil
}

func (h *Hub) handlePM(c *Client, cmd clientproto.Command) {
	var result coordination.Result
	var err error
	if !h.pmEnabled || h.work == nil {
		err = errors.New("pm_unavailable")
	} else if c.deviceID == "" || c.enrollmentOnly {
		err = coordination.ErrForbidden
	} else if cmd.PM != nil && cmd.PM.Action != "snapshot" {
		input := *cmd.PM
		if input.Action == "create" {
			input.Cwd = runtime.ExpandPath(input.Cwd)
			input.CwdAlias = input.Cwd
			input.Cwd, err = filepath.EvalSymlinks(input.Cwd)
			if err == nil {
				var fi os.FileInfo
				fi, err = os.Stat(input.Cwd)
				if err == nil && !fi.IsDir() {
					err = errors.New("pm_workspace_not_directory")
				}
			}
			if err == nil {
				input.ProjectID = "pmp_" + shortPMID(c.deviceID+":"+input.MutationID)
			}
		}
		if err == nil {
			result, err = h.applyPM(coordination.Principal{Human: true, ID: c.deviceID}, input)
		}
	}
	c.enqueueEvent(h.pmSnapshot(cmd.RequestID, &result, err))
}
func shortPMID(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:10])
}

func (h *Hub) applyPM(actor coordination.Principal, cmd coordination.Command) (coordination.Result, error) {
	h.pmMu.Lock()
	defer h.pmMu.Unlock()
	var out coordination.Result
	state, err := h.work.UpdateCollaboration(context.Background(), func(s *coordination.State) error {
		// Handback/accept cannot race a still-running human or worker turn.
		if cmd.Action == "return_to_pm" || cmd.Action == "accept" || cmd.Action == "rework" {
			if t, ok := s.Tasks[cmd.TaskID]; ok {
				if worker, ok := h.registry.Get(t.SessionID); ok && (worker.IsStreaming() || worker.QueueLen() > 0) {
					return errors.New("pm_wait_for_current_turn")
				}
			}
		}
		var err error
		out, err = s.Apply(actor, cmd, time.Now().UnixMilli())
		return err
	})
	if err != nil {
		return out, err
	}
	if cmd.Action == "create" {
		if err = h.provisionPMProject(state.Projects[out.ProjectID]); err != nil {
			return out, err
		}
	}
	h.broadcastPM()
	return out, nil
}

func (h *Hub) provisionPMProject(p coordination.Project) error {
	displayCwd := p.Cwd
	if p.CwdAlias != "" {
		displayCwd = p.CwdAlias
	}
	if _, err := h.work.GetProject(context.Background(), p.ID); errors.Is(err, workitems.ErrNotFound) {
		if _, err = h.work.CreateProject(context.Background(), workitems.CreateProjectInput{ID: p.ID, Name: p.Name, WorkspacePath: displayCwd}); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if existing, ok := h.registry.Get(p.PMSessionID); ok {
		if existing.Backend() != backend.Codex && existing.Backend() != backend.Claude {
			return errors.New("pm_backend_policy_mismatch")
		}
		return nil
	}
	h.registry.Create(p.PMSessionID, p.Name+" · 主對話", displayCwd, backend.Codex, "gpt-5.6-sol", "read-only", "")
	if err := h.registry.PersistDurably(); err != nil {
		return err
	}
	h.Emit(h.client.SessionsList(h.sessionSummaries()))
	return nil
}

// AdmitTurn is called by the mux for ALL dispatch paths, including Work,
// automation, relay, notifications and recovered user message queues.
func (h *Hub) AdmitTurn(s *session.Session, requestID, content string) (string, func(), error) {
	h.pmMu.Lock()
	release := func() { h.pmMu.Unlock() }
	if h.work == nil {
		release()
		return content, func() {}, nil
	}
	state, err := h.work.Collaboration(context.Background())
	if err != nil {
		release()
		return "", nil, err
	}
	p, t, ok := state.ProjectForSession(s.ID)
	if !ok {
		if strings.HasPrefix(s.ID, "pm_") {
			release()
			return "", nil, errors.New("pm_role_missing")
		}
		return content, release, nil
	}
	if !h.pmEnabled {
		release()
		return "", nil, errors.New("pm_unavailable")
	}
	if t == nil {
		if (s.Backend() != backend.Codex && s.Backend() != backend.Claude) || s.Snapshot().Sandbox != "read-only" {
			release()
			return "", nil, errors.New("pm_backend_policy_mismatch")
		}
		if strings.HasPrefix(requestID, "pmwake_") && (p.Mode != "active" || p.WakeRequestID != requestID) {
			release()
			return "", nil, errors.New("pm_wake_cancelled")
		}
		// The immutable system prompt is enforced by the restricted runtime at
		// every start/resume. Do not pollute native user history with role text.
		return content, release, nil
	}
	if !state.WorkerAllowed(s.ID, requestID) {
		release()
		return "", nil, errors.New("pm_control_changed")
	}
	if s.Backend() != t.Backend || s.Snapshot().Sandbox != t.Sandbox {
		release()
		return "", nil, errors.New("pm_worker_policy_changed")
	}
	// Serialize writable workers within this workspace; session control alone
	// is not filesystem isolation. Human-controlled tasks are included.
	if t.Sandbox == "workspace-write" {
		for _, other := range state.Tasks {
			if other.SessionID != s.ID && other.Sandbox == "workspace-write" && state.Projects[other.ProjectID].Cwd == p.Cwd {
				if peer, ok := h.registry.Get(other.SessionID); ok && peer.IsStreaming() {
					release()
					return "", nil, errors.New("pm_workspace_writer_busy")
				}
			}
		}
	}
	_, err = h.work.UpdateCollaboration(context.Background(), func(next *coordination.State) error {
		if p.EngineVersion == 2 {
			if err := next.AdmitCollaborationRun(s.ID, requestID, content, time.Now().UnixMilli()); err != nil {
				return err
			}
		}
		current := next.Tasks[t.ID]
		current.State = "running"
		next.AddEvent(coordination.Principal{ID: "system", SessionID: s.ID}, p.ID, t.ID, "turn_admitted", "執行已送入工作對話。", requestID, time.Now().UnixMilli(), false)
		current.Revision = next.Revision
		next.Tasks[t.ID] = current
		return nil
	})
	if err != nil {
		release()
		return "", nil, err
	}
	h.broadcastPM()
	return content, release, nil
}

func (h *Hub) rejectPMCommand(c *Client, cmd clientproto.Command) bool {
	if cmd.Kind == "new_session" && strings.HasPrefix(cmd.SessionID, "pm_") {
		c.enqueueEvent(h.client.Error(cmd.SessionID, "pm_reserved_session", "PM sessions must be created through the project coordinator"))
		return true
	}
	if h.work == nil || cmd.SessionID == "" {
		return false
	}
	// Order rejection with takeover, so a duplicate cannot race ahead of its
	// negative receipt during the owner transition.
	h.pmMu.Lock()
	defer h.pmMu.Unlock()
	state, err := h.work.Collaboration(context.Background())
	if err != nil {
		h.queueError(c, cmd, "queue_unavailable", "Cannot verify project control state")
		return true
	}
	_, task, ok := state.ProjectForSession(cmd.SessionID)
	if !ok {
		return false
	}
	switch cmd.Kind {
	case "switch_session_config", "set_effort":
		if current, ok := h.registry.Get(cmd.SessionID); ok {
			c.enqueueEvent(h.client.SessionConfigResult(cmd.SessionID, cmd.MutationID, false, "pm_managed_session", current.Snapshot()))
		}
		return true
	case "new_session", "clear_session", "close_session", "fork_session", "handoff_to_desktop", "codex_goal_set", "codex_goal_clear":
		c.enqueueEvent(h.client.Error(cmd.SessionID, "pm_managed_session", "Managed project conversations retain their role, policy and history"))
		return true
	case "steer_message", "promote_queued_message":
		if task == nil || task.Owner != "human" {
			h.queueError(c, cmd, "pm_takeover_required", "請先接管工作對話，再修改執行方向。")
			return true
		}
	case "message":
		if task != nil && task.Owner != "human" {
			if cmd.RequestID != "" {
				if h.messageQueue == nil {
					h.queueError(c, cmd, "queue_unavailable", "Message rejection storage is unavailable")
					return true
				}
				if err := h.messageQueue.Reject(cmd.SessionID, cmd.RequestID, "pm_takeover_required"); err != nil {
					h.queueError(c, cmd, "queue_persist_failed", "Could not persist the rejected request")
					return true
				}
			}
			h.queueError(c, cmd, "pm_takeover_required", "請先接管工作對話，再直接續聊。")
			return true
		}
	}
	return false
}

func (h *Hub) reconcilePM() {
	if !h.pmEnabled || h.work == nil {
		return
	}
	h.pmMu.Lock()
	defer h.pmMu.Unlock()
	beforeWork, _ := h.work.WorkRevision(context.Background())
	maintenanceChanged := false
	state, err := h.work.UpdateCollaboration(context.Background(), func(s *coordination.State) error {
		maintenanceChanged = s.SweepCollaboration(time.Now().UnixMilli())
		return nil
	})
	if err != nil {
		return
	}
	if maintenanceChanged {
		h.broadcastPM()
		h.broadcastWorkRange(beforeWork + 1)
	}
	h.stopCancelledCollaborationRuns(state)
	for _, p := range state.Projects {
		if err := h.provisionPMProject(p); err != nil {
			log.Printf("[pm] project provisioning: %v", err)
		}
	}
	ids := make([]string, 0, len(state.Tasks))
	for id := range state.Tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		t := state.Tasks[id]
		p := state.Projects[t.ProjectID]
		if t.State == "done" {
			if p.EngineVersion != 2 {
				h.reconcilePMAccept(t)
			}
			continue
		}
		if t.State != "provisioning" || p.Mode != "active" || t.Owner != "pm" || t.Epoch != t.DispatchEpoch {
			continue
		}
		if err := h.provisionPMTask(p, t); err != nil {
			log.Printf("[pm] task provisioning %s: %v", t.ID, err)
			continue
		}
		_, err = h.work.UpdateCollaboration(context.Background(), func(next *coordination.State) error {
			current := next.Tasks[t.ID]
			if current.State != "provisioning" {
				return nil
			}
			current.Provisioned = true
			current.State = "queued"
			next.AddEvent(coordination.Principal{ID: "system"}, p.ID, t.ID, "task_queued", "工作對話已建立並排入執行。", t.RequestID, time.Now().UnixMilli(), false)
			current.Revision = next.Revision
			next.Tasks[t.ID] = current
			return nil
		})
		if err == nil {
			h.broadcastPM()
			h.WakeWorkScheduler()
		}
	}
	state, err = h.work.Collaboration(context.Background())
	if err != nil {
		return
	}
	for _, p := range state.Projects {
		h.wakePM(p)
	}
}

func (h *Hub) provisionPMTask(p coordination.Project, t coordination.Task) error {
	displayCwd := p.Cwd
	if p.CwdAlias != "" {
		displayCwd = p.CwdAlias
	}
	ctx := context.Background()
	actor := workitems.Actor{Type: workitems.ActorAgent, DeviceID: p.PMSessionID}
	if _, ok := h.registry.Get(t.SessionID); !ok {
		h.registry.Create(t.SessionID, fmt.Sprintf("W%02d · %s", t.Number, t.Title), displayCwd, t.Backend, t.Model, t.Sandbox, "")
		if err := h.registry.PersistDurably(); err != nil {
			return err
		}
		h.Emit(h.client.SessionsList(h.sessionSummaries()))
	}
	item, err := h.work.GetItem(ctx, t.WorkItemID)
	if errors.Is(err, workitems.ErrNotFound) {
		item, err = h.work.CreateItem(ctx, workitems.CreateItemInput{ID: t.WorkItemID, ProjectID: p.ID, Title: t.Title, Description: t.Reason, Outcome: t.Instruction, AcceptanceCriteria: t.Acceptance, Actor: actor})
	}
	if err != nil {
		return err
	}
	snapshot, err := h.work.Snapshot(ctx)
	if err != nil {
		return err
	}
	linked := false
	for _, link := range snapshot.SessionLinks {
		if link.SessionID == t.SessionID && link.WorkItemID == t.WorkItemID && link.UnlinkedAt == nil {
			linked = true
		}
	}
	if !linked {
		_, item, err = h.work.LinkSession(ctx, workitems.LinkSessionInput{ID: "link_" + t.ID, WorkItemID: t.WorkItemID, SessionID: t.SessionID, Role: "primary", ExpectedVersion: item.Version, Actor: actor})
		if err != nil {
			return err
		}
	}
	for _, run := range snapshot.Runs {
		if run.ID == t.RunID {
			return nil
		}
	}
	if item.Lifecycle == workitems.LifecycleReview {
		_, err = h.work.DecideReview(ctx, workitems.ReviewDecisionInput{WorkItemID: item.ID, ExpectedVersion: item.Version, Decision: "request_changes", Feedback: t.Instruction, RunID: t.RunID, RequestID: t.RequestID, Actor: workitems.Actor{Type: workitems.ActorUser, DeviceID: t.ApprovedBy}})
		return err
	}
	if item.Lifecycle == workitems.LifecycleInbox {
		item, err = h.work.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version, Lifecycle: workitems.LifecycleReady, Actor: actor})
		if err != nil {
			return err
		}
	}
	instruction := fmt.Sprintf("[Delegated by project PM %s; task %s]\nReason: %s\nTask: %s\nAcceptance: %s\nReport evidence and remaining issues. Do not create subagents or expand authority. Deployment/publication/deletion is not authorized by this assignment.\n", p.PMSessionID, t.ID, t.Reason, t.Instruction, t.Acceptance)
	_, _, err = h.work.StartRun(ctx, workitems.StartRunInput{ID: t.RunID, WorkItemID: t.WorkItemID, SessionID: t.SessionID, RequestID: t.RequestID, Kind: "implementation", Instruction: instruction, ExpectedVersion: item.Version, Actor: actor})
	return err
}
func (h *Hub) reconcilePMAccept(t coordination.Task) {
	item, err := h.work.GetItem(context.Background(), t.WorkItemID)
	if err != nil || item.Lifecycle != workitems.LifecycleReview {
		return
	}
	_, _ = h.work.DecideReview(context.Background(), workitems.ReviewDecisionInput{WorkItemID: item.ID, ExpectedVersion: item.Version, Decision: "accept", Actor: workitems.Actor{Type: workitems.ActorUser, DeviceID: t.AcceptedBy}})
}

func (h *Hub) wakePM(p coordination.Project) {
	if p.Mode != "active" || p.PendingThrough <= p.ProcessedThrough || h.messageQueue == nil {
		return
	}
	pm, ok := h.registry.Get(p.PMSessionID)
	if !ok || pm.IsStreaming() || pm.QueueLen() > 0 {
		return
	}
	if p.WakeRequestID != "" {
		if entry, found, err := h.messageQueue.Get(p.PMSessionID, p.WakeRequestID); err != nil {
			return
		} else if found {
			if string(entry.State) == "queued" || string(entry.State) == "running" {
				return
			}
			// Completion callback normally handles this. Recover a receipt whose
			// process ended before the project transaction could commit.
			h.finishPMTurn(p.PMSessionID, p.WakeRequestID, "recovered", "Previous PM turn ended; inspect current project state before continuing.")
			return
		}
	}
	if p.Rounds >= p.MaxRounds {
		_, _ = h.work.UpdateCollaboration(context.Background(), func(s *coordination.State) error {
			current := s.Projects[p.ID]
			current.Mode = "paused"
			current.Note = "已達協調回合上限，請使用者確認後恢復。"
			s.Projects[p.ID] = current
			s.AddEvent(coordination.Principal{ID: "system"}, p.ID, "", "budget_limit", current.Note, "", time.Now().UnixMilli(), false)
			return nil
		})
		h.broadcastPM()
		return
	}
	requestID := p.WakeRequestID
	if requestID == "" {
		requestID = fmt.Sprintf("pmwake_%s_%d", p.ID, p.PendingThrough)
		_, err := h.work.UpdateCollaboration(context.Background(), func(s *coordination.State) error {
			current := s.Projects[p.ID]
			current.WakeRequestID = requestID
			current.WakeThrough = current.PendingThrough
			current.WakeState = "queued"
			if current.EngineVersion == 2 {
				current.WakeDispositionCount = s.ProjectDispositionCount(p.ID)
			}
			current.Rounds++
			s.Projects[p.ID] = current
			s.AddEvent(coordination.Principal{ID: "system"}, p.ID, "", "pm_wake_queued", "已安排 PM 檢查最新事件。", requestID, time.Now().UnixMilli(), false)
			return nil
		})
		if err != nil {
			return
		}
	}
	client := &Client{hub: h, deviceID: "pm-system", send: make(chan []byte, 64), quit: make(chan struct{}), ctx: context.Background()}
	h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: p.PMSessionID, RequestID: requestID, Content: "[Bridge coordination event] Read project_get_context and session_read_updates. Dispatch approved tasks, assess new evidence or handbacks, and report clearly. Do not execute implementation yourself."})
}

func (h *Hub) finishPMTurn(sessionID, requestID, status, text string) {
	if h.work == nil {
		return
	}
	changed := false
	beforeWork, _ := h.work.WorkRevision(context.Background())
	_, err := h.work.UpdateCollaboration(context.Background(), func(s *coordination.State) error {
		p, t, ok := s.ProjectForSession(sessionID)
		if !ok {
			return nil
		}
		if t == nil {
			if p.WakeRequestID != requestID {
				return nil
			}
			p.ProcessedThrough = p.WakeThrough
			p.WakeRequestID = ""
			p.WakeState = ""
			p.WakeThrough = 0
			needsDisposition := false
			if p.EngineVersion == 2 && status == "succeeded" {
				for _, a := range s.PendingActionables(p.ID, "pm") {
					if a.State == "open" || a.State == "in_progress" {
						needsDisposition = true
					}
				}
				if needsDisposition && s.ProjectDispositionCount(p.ID) == p.WakeDispositionCount {
					p.NoProgressRounds++
				} else {
					p.NoProgressRounds = 0
				}
				if p.NoProgressRounds >= 2 {
					p.Mode = "paused"
					p.Note = "PM 連續兩回合未處置待辦，已暫停；待處置事項仍保留。"
				}
			}
			if status != "succeeded" {
				p.Mode = "paused"
				p.Note = "PM 執行中斷；請檢查後恢復。"
			}
			s.Projects[p.ID] = p
			s.AddEvent(coordination.Principal{ID: "system"}, p.ID, "", "pm_cycle_"+status, "PM 已處理本次事件；後續仍以原始對話與工作證據為準。", requestID, time.Now().UnixMilli(), false)
			if needsDisposition && p.Mode == "active" {
				s.AddEvent(coordination.Principal{ID: "system"}, p.ID, "", "pm_actionables_pending", "仍有待處置回報；讀取並明確處置後才算完成。", "", time.Now().UnixMilli(), true)
			}
			changed = true
			return nil
		}
		if p.EngineVersion == 2 {
			changed = s.FinishCollaborationRun(sessionID, requestID, status, text, time.Now().UnixMilli())
			return nil
		}
		// A stale failed PM request cannot overwrite a newer human's result.
		if requestID != t.RequestID && t.Owner != "human" {
			return nil
		}
		key := "terminal:" + sessionID + ":" + requestID
		if _, ok := s.Receipts[key]; ok {
			return nil
		}
		s.Receipts[key] = json.RawMessage(`{}`)
		t.Result = truncateGraphemes(text, 12000)
		if status == "succeeded" {
			t.State = "review"
		} else {
			t.State = "failed"
		}
		s.AddEvent(coordination.Principal{ID: "system", SessionID: sessionID}, p.ID, t.ID, "worker_"+status, t.Result, requestID, time.Now().UnixMilli(), true)
		t.Revision = s.Revision
		s.Tasks[t.ID] = *t
		changed = true
		return nil
	})
	if err != nil {
		log.Printf("[pm] terminal: %v", err)
	} else if changed {
		h.broadcastPM()
		h.broadcastWorkRange(beforeWork + 1)
	}
}
