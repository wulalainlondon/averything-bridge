package workitems

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const maxReviewFeedbackRunes = 8000

type ReviewDecisionInput struct {
	WorkItemID      string
	ExpectedVersion uint64
	Decision        string
	Feedback        string
	CommentID       string
	RunID           string
	RequestID       string
	Actor           Actor
}

type ReviewDecisionResult struct {
	Item     WorkItem
	Comment  *Comment
	Run      *Run
	Decision ReviewDecisionRecord
}

// DecideReview is the single authoritative human-review transaction. It
// persists the decision and feedback with the lifecycle change. Decisions that
// require more agent work also enqueue a durable run against the primary linked
// Session before committing, so a Bridge restart cannot lose the hand-back.
func (s *Store) DecideReview(ctx context.Context, in ReviewDecisionInput) (ReviewDecisionResult, error) {
	if in.Actor.Type != ActorUser && in.Actor.Type != ActorDesktop && in.Actor.Type != ActorMobile {
		return ReviewDecisionResult{}, ErrHumanRequired
	}
	decision := strings.TrimSpace(in.Decision)
	feedback := strings.TrimSpace(in.Feedback)
	if utf8.RuneCountInString(feedback) > maxReviewFeedbackRunes {
		return ReviewDecisionResult{}, fmt.Errorf("review feedback exceeds %d characters", maxReviewFeedbackRunes)
	}
	if decision != "accept" && decision != "request_changes" && decision != "needs_more_info" && decision != "reopen" {
		return ReviewDecisionResult{}, errors.New("invalid review decision")
	}
	if decision != "accept" && feedback == "" {
		return ReviewDecisionResult{}, errors.New("review feedback is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReviewDecisionResult{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return ReviewDecisionResult{}, err
	}
	if item.Version != in.ExpectedVersion {
		return ReviewDecisionResult{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	if decision == "reopen" {
		if item.Lifecycle != LifecycleDone {
			return ReviewDecisionResult{}, fmt.Errorf("%w: reopen from %s", ErrInvalidTransition, item.Lifecycle)
		}
	} else if item.Lifecycle != LifecycleReview {
		return ReviewDecisionResult{}, fmt.Errorf("%w: decide review from %s", ErrInvalidTransition, item.Lifecycle)
	}

	now := s.now().UnixMilli()
	record := ReviewDecisionRecord{Decision: decision, Feedback: feedback, CreatedAt: now}
	payload := ChangePayload{ReviewDecision: &record}
	var comment *Comment
	if feedback != "" {
		commentID := strings.TrimSpace(in.CommentID)
		if commentID == "" {
			commentID = newID("wc")
		}
		created := Comment{ID: commentID, WorkItemID: item.ID, AuthorType: in.Actor.Type,
			AuthorDeviceID: in.Actor.DeviceID, Body: feedback, CreatedAt: now}
		if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_comments
			(id,work_item_id,author_type,author_device_id,body,created_at) VALUES(?,?,?,?,?,?)`,
			created.ID, created.WorkItemID, created.AuthorType, created.AuthorDeviceID, created.Body, now); err != nil {
			return ReviewDecisionResult{}, err
		}
		comment = &created
		payload.Comment = comment
	}

	kind := "review_accepted"
	if decision == "accept" {
		item.Lifecycle = LifecycleDone
		item.AcceptedAt = &now
	} else {
		var active int
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM work_item_runs
			WHERE work_item_id=? AND finished_at IS NULL)`, item.ID).Scan(&active); err != nil {
			return ReviewDecisionResult{}, err
		}
		if active == 1 {
			return ReviewDecisionResult{}, errors.New("work item already has an active run")
		}
		var link SessionLink
		err := tx.QueryRowContext(ctx, `SELECT id,work_item_id,session_id,thread_id_snapshot,role,linked_at,unlinked_at
			FROM work_item_session_links WHERE work_item_id=? AND unlinked_at IS NULL
			ORDER BY CASE role WHEN 'primary' THEN 0 ELSE 1 END, linked_at DESC LIMIT 1`, item.ID).Scan(
			&link.ID, &link.WorkItemID, &link.SessionID, &link.ThreadIDSnapshot, &link.Role, &link.LinkedAt, &link.UnlinkedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return ReviewDecisionResult{}, errors.New("review feedback requires a linked session")
		}
		if err != nil {
			return ReviewDecisionResult{}, err
		}
		runID, requestID := strings.TrimSpace(in.RunID), strings.TrimSpace(in.RequestID)
		if runID == "" {
			runID = newID("wr")
		}
		if requestID == "" {
			requestID = newID("review")
		}
		instruction := "Human review requested changes. Address the feedback below, update the durable Work Item with evidence, and request human review again. Do not mark the work done.\n\nFeedback:\n" + feedback
		kind = "review_changes_requested"
		if decision == "needs_more_info" {
			instruction = "Human review requires more information. Gather or explain only the missing evidence below, update the durable Work Item, and request human review again. Do not mark the work done.\n\nMissing information:\n" + feedback
			kind = "review_more_info_requested"
		} else if decision == "reopen" {
			instruction = "The human reopened an accepted outcome. Address the feedback below, update the durable Work Item with evidence, and request human review again. Do not mark the work done.\n\nReopen feedback:\n" + feedback
			kind = "review_reopened"
		}
		run := Run{ID: runID, WorkItemID: item.ID, SessionLinkID: link.ID, RequestID: requestID,
			Kind: "verification", Status: "queued", StartedAt: now, SessionID: link.SessionID,
			Instruction: instruction, AvailableAt: now, MaxAttempts: 3, QueueReason: "human_review_feedback"}
		if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_runs
			(id,work_item_id,session_link_id,request_id,kind,status,started_at,session_id,instruction,
			 available_at,attempt,max_attempts,queue_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			run.ID, run.WorkItemID, run.SessionLinkID, run.RequestID, run.Kind, run.Status, run.StartedAt,
			run.SessionID, run.Instruction, run.AvailableAt, run.Attempt, run.MaxAttempts, run.QueueReason); err != nil {
			return ReviewDecisionResult{}, err
		}
		record.RunID, record.SessionID = run.ID, run.SessionID
		payload.ReviewDecision = &record
		payload.Run = &run
		item.Lifecycle = LifecycleActive
		item.AcceptedAt = nil
	}
	item.BlockedReasonCode = ""
	item.BlockedNote = ""
	item.Version++
	item.UpdatedAt = now
	updated, err := s.writeItemMutation(ctx, tx, item, kind, in.Actor, payload)
	if err != nil {
		return ReviewDecisionResult{}, err
	}
	return ReviewDecisionResult{Item: updated, Comment: comment, Run: payload.Run, Decision: record}, nil
}
