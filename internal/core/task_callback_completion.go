package core

import (
	"context"
	"database/sql"
	"errors"
	"everything-go/internal/callbacks"
	"everything-go/internal/messagequeue"
	"everything-go/internal/workitems"
)

// Root readiness is independent from any child final or a green idle badge.
func (h *Hub) callbackRootReady(ctx context.Context, r callbacks.Record) bool {
	if h.delegations != nil {
		children, e := h.delegations.ListByParent(ctx, r.Session)
		if e != nil {
			return false
		}
		for _, child := range children {
			if child.OriginRequestID != r.Request {
				continue
			}
			if child.LogicalID != "" {
				latest, e := h.delegations.LatestRound(ctx, child.LogicalID)
				if e != nil {
					return false
				}
				if latest.ID != child.ID {
					continue
				}
			}
			if child.State != "terminal" || (child.TerminalStatus != "succeeded" && child.TerminalStatus != "completed") || child.DeliveryState != "delivered" {
				return false
			}
			logical := child.LogicalID
			if logical == "" {
				logical = child.ID
			}
			{
				owner, qa, e := h.delegations.Responsibility(ctx, logical)
				if e != nil {
					return false
				}
				if owner == logical && qa != "" {
					_, verdict, e := h.delegations.QAForRound(ctx, child.ID)
					if e != nil || verdict != "PASS" {
						return false
					}
				}
			}
		}
	}
	for _, journal := range h.apiJournals() {
		if journal == nil {
			continue
		}
		original, e := journal.OriginalCommandForRequest(ctx, r.Session, r.Request)
		if errors.Is(e, sql.ErrNoRows) {
			continue
		}
		if e != nil {
			return false
		}
		task, e := h.apiTask(ctx, original)
		if e != nil {
			return false
		}
		axes, _ := task["axes"].(map[string]string)
		if axes["final"] != "sealed" || axes["execution"] != "succeeded" || (axes["qa"] != "not_requested" && axes["qa"] != "passed") {
			return false
		}
	}
	return true
}

func (h *Hub) callbackWorkSource(ctx context.Context, session, request string) (workitems.CallbackSource, bool, error) {
	if h.work == nil {
		return workitems.CallbackSource{}, false, nil
	}
	return h.work.CallbackSource(ctx, session, request)
}

// callbackExecution follows only a persisted strict native steering proof. It
// never derives a root from the latest busy session or commentary.
func (h *Hub) callbackExecution(r callbacks.Record) (callbacks.Record, messagequeue.Entry, bool) {
	entry, found, e := h.messageQueue.Get(r.Session, r.Request)
	if e != nil || !found {
		return r, entry, false
	}
	if entry.State != messagequeue.Steered {
		return r, entry, true
	}
	if !h.messageQueue.StrictSteerProven(entry) {
		return r, entry, false
	}
	s, ok := h.registry.Get(r.Session)
	if !ok || entry.ActiveRequestID == "" || entry.TurnID == "" || s.ResumeID() != r.Thread || s.SettingsSnapshot().ConfigRevision != r.Config {
		return r, entry, false
	}
	if h.nativeTaskTurn(s, entry.ActiveRequestID) != entry.TurnID {
		return r, entry, false
	}
	active, found, e := h.messageQueue.Get(r.Session, entry.ActiveRequestID)
	if e != nil || !found || active.State == messagequeue.Steered {
		return r, entry, false
	}
	if !h.taskAdmission(entry).ExpectedTarget.matches(s) || !h.taskAdmission(active).ExpectedTarget.matches(s) {
		return r, entry, false
	}
	r.Request = entry.ActiveRequestID
	return r, active, true
}
func (h *Hub) callbackReportCurrent(ctx context.Context, r callbacks.Record) bool {
	effective, _, found := h.callbackExecution(r)
	if r.Report != nil && r.Report.ExecutionRequest != "" {
		if !found || effective.Request != r.Report.ExecutionRequest {
			return false
		}
		if s, ok := h.registry.Get(r.Session); !ok || (r.Report.Turn != "" && h.nativeTaskTurn(s, effective.Request) != r.Report.Turn) {
			return false
		}
	}
	if !h.callbackRootReady(ctx, effective) {
		return false
	}
	if r.Report != nil && r.Report.SourceKind == "workitem" {
		w, found, e := h.callbackWorkSource(ctx, r.Session, effective.Request)
		return e == nil && found && w.Latest && w.Lifecycle == "review" && w.ItemID == r.Report.SourceID && w.Revision == r.Report.SourceRevision
	}
	return true
}
func (h *Hub) callbackResult(ctx context.Context, r callbacks.Record, entry messagequeue.Entry, found bool) (status, text string, ready bool) {
	if found && entry.State == messagequeue.Steered {
		var valid bool
		r, entry, valid = h.callbackExecution(r)
		if !valid {
			return "", "", false
		}
	}
	if !h.callbackRootReady(ctx, r) {
		return "", "", false
	}
	work, owned, e := h.callbackWorkSource(ctx, r.Session, r.Request)
	if e != nil {
		return "", "", false
	}
	if owned {
		if !work.Latest || work.Lifecycle != "review" {
			return "", "", false
		}
		return "review_ready", "工作「" + work.Title + "」已提交驗收。請到原工作項目查看成果與驗收條件；目前尚未由你確認完成。", true
	}
	if found && entry.State == messagequeue.Completed {
		return "result_ready", "", true
	}
	if found && entry.State == messagequeue.Failed && r.Failures {
		return "failed", "原任務回報失敗，請查看原對話的錯誤與需要處理的事項。", true
	}
	return "", "", false
}
