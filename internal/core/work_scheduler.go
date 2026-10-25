package core

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/workitems"
)

const workSchedulerInterval = 5 * time.Second

// StartWorkScheduler recovers durable queue leases once and continuously
// dispatches auto/recovered runs. Calling it more than once is harmless.
func (h *Hub) StartWorkScheduler(ctx context.Context) {
	if h.work == nil || !h.workScheduler.CompareAndSwap(false, true) {
		return
	}
	if recovered, err := h.work.RecoverQueue(ctx); err != nil {
		log.Printf("[work-queue] recover: %v", err)
	} else if recovered > 0 {
		log.Printf("[work-queue] recovered %d run(s)", recovered)
	}
	go func() {
		ticker := time.NewTicker(workSchedulerInterval)
		defer ticker.Stop()
		h.drainWorkQueue(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.drainWorkQueue(ctx)
			case <-h.workWake:
				h.drainWorkQueue(ctx)
			}
		}
	}()
}

func (h *Hub) WakeWorkScheduler() {
	select {
	case h.workWake <- struct{}{}:
	default:
	}
}

func (h *Hub) drainWorkQueue(ctx context.Context) {
	if queued, err := h.work.EnqueueAutomaticRuns(ctx); err != nil {
		log.Printf("[work-queue] auto enqueue: %v", err)
	} else if len(queued) > 0 {
		log.Printf("[work-queue] auto-enqueued %d run(s)", len(queued))
	}
	for dispatched := 0; dispatched < 4; dispatched++ {
		run, item, ok, err := h.work.ClaimNextRun(ctx, time.Now().UnixMilli())
		if err != nil {
			log.Printf("[work-queue] claim: %v", err)
			return
		}
		if !ok {
			return
		}
		h.broadcastWorkRevision(item.ActivityRevision)
		if !h.dispatchQueuedRun(ctx, run) {
			return
		}
	}
}

func (h *Hub) dispatchQueuedRun(ctx context.Context, run workitems.Run) bool {
	if h.work != nil {
		if pmState, err := h.work.Collaboration(ctx); err == nil {
			if project, task, managed := pmState.ProjectForSession(run.SessionID); managed && task != nil {
				if project.Mode != "active" {
					h.deferQueuedRun(ctx, run, time.Now().Add(2*time.Second), "pm_paused")
					return false
				}
				if !task.Provisioned {
					h.deferQueuedRun(ctx, run, time.Now().Add(time.Second), "pm_provisioning")
					return false
				}
				if !pmState.WorkerAllowed(run.SessionID, run.RequestID) {
					_, _ = h.work.AdvanceRun(ctx, run.SessionID, run.RequestID, "interrupted", "PM control or authorization changed; request was not executed")
					return true
				}
			}
		}
	}
	s, ok := h.registry.Get(run.SessionID)
	if !ok {
		h.deferQueuedRun(ctx, run, time.Now().Add(time.Minute), "session_unavailable")
		return false
	}
	if !s.CanDispatchAutomatic() {
		h.deferQueuedRun(ctx, run, time.Now().Add(5*time.Second), "session_busy")
		return false
	}
	if !h.controls.MobileMayWrite(run.SessionID) {
		h.deferQueuedRun(ctx, run, time.Now().Add(30*time.Second), "desktop_controlled")
		return false
	}
	// Relay review jobs must remain read-only at the moment of dispatch, not
	// merely when the remote envelope was accepted. Codex supports a per-turn
	// sandbox override, so an ordinary writable conversation can safely host a
	// review turn without permanently changing its interactive policy. Other
	// backends still require a read-only Session.
	reviewOnly := strings.HasPrefix(run.RequestID, "rjob_")
	snap := s.Snapshot()
	if reviewOnly && snap.Sandbox != "read-only" && snap.Backend != backend.Codex {
		h.deferQueuedRun(ctx, run, time.Now().Add(30*time.Second), "review_only_session_required")
		return false
	}
	if resetAt, limited := h.workQuotaLimited(ctx, s); limited {
		h.deferQueuedRun(ctx, run, resetAt, "quota_limited")
		return false
	}
	pack, err := h.work.BuildContextPack(ctx, run.WorkItemID, 24_000)
	if err != nil {
		h.deferQueuedRun(ctx, run, time.Now().Add(time.Minute), "context_failed")
		return false
	}
	for i := range pack.Attachments {
		pack.Attachments[i] = h.materializeWorkAttachment(pack.Attachments[i])
	}
	pack.Prompt, pack.Truncated = workitems.RenderContextPrompt(pack, 24_000, pack.Truncated)
	content := h.workRunContent(pack, run.Instruction, h.cfg.Port)
	if state, err := h.work.Collaboration(ctx); err != nil {
		h.deferQueuedRun(ctx, run, time.Now().Add(time.Minute), "collaboration_unavailable")
		return false
	} else if project, task, ok := state.ProjectForSession(run.SessionID); ok && task != nil && project.EngineVersion == 2 {
		meta := state.Collaboration.Tasks[task.ID]
		manifest := state.Collaboration.Manifests[meta.ManifestID]
		content = manifest.Required + "\nPhase: " + meta.Phase + "\n" + collaborationWorkerInstructions + "\nRead work_get_context for saved answers and pending review findings.\n"
	}
	accepted := s.Submit(func() {
		if err := h.work.MarkRunSubmitted(context.Background(), run.ID); err != nil {
			log.Printf("[work-queue] mark submitted %s: %v", run.ID, err)
			h.updateRuntime(run.SessionID, "failed", run.RequestID, 0, "failed", "durable dispatch receipt failed")
			s.EndTurn()
			return
		}
		h.updateRuntime(run.SessionID, "running", run.RequestID, s.QueueLen(), "", "")
		sendCtx := context.Background()
		if reviewOnly {
			sendCtx = backend.WithSandboxOverride(sendCtx, "read-only")
		}
		if err := h.exec.Send(sendCtx, s, run.RequestID, content, nil, nil); err != nil {
			if errors.Is(err, backend.ErrThreadActiveWriter) {
				h.markDesktopWriter(s)
			}
			h.updateRuntime(run.SessionID, "failed", run.RequestID, 0, "failed", err.Error())
		}
	})
	if !accepted {
		h.updateRuntime(run.SessionID, "failed", run.RequestID, 0, "failed", "session is closed")
		return false
	}
	h.updateRuntime(run.SessionID, "queued", run.RequestID, s.QueueLen(), "", "")
	return true
}

func (h *Hub) deferQueuedRun(ctx context.Context, run workitems.Run, until time.Time, reason string) {
	_, item, err := h.work.DeferRun(ctx, run.ID, until.UnixMilli(), reason)
	if err != nil {
		log.Printf("[work-queue] defer %s: %v", run.ID, err)
		return
	}
	h.broadcastWorkRevision(item.ActivityRevision)
}

func (h *Hub) workQuotaLimited(ctx context.Context, s *session.Session) (time.Time, bool) {
	router, ok := h.exec.(usageRouter)
	if !ok {
		return time.Time{}, false
	}
	provider, ok := router.UsageFor(s)
	if !ok {
		return time.Time{}, false
	}
	report, err := provider.FetchUsage(ctx)
	if err != nil || report == nil || report.FiveHour == nil || report.FiveHour.Utilization == nil || *report.FiveHour.Utilization < 0.98 {
		return time.Time{}, false
	}
	reset := time.Now().Add(5 * time.Minute)
	if report.FiveHour.ResetsAt != nil {
		if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*report.FiveHour.ResetsAt)); err == nil && parsed.After(time.Now()) {
			reset = parsed.Add(5 * time.Second)
		}
	}
	return reset, true
}
