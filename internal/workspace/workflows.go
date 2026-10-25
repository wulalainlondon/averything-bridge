package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type WorkflowInput struct {
	Title       string      `json:"title"`
	Instruction string      `json:"instruction"`
	ThreadID    string      `json:"thread_id"`
	Researcher  string      `json:"researcher"`
	Writer      string      `json:"writer"`
	Reviewer    string      `json:"reviewer"`
	References  []Reference `json:"references"`
	DocumentID  string      `json:"document_id"`
}

func (s *Store) startWorkflow(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	if !a.Owner {
		return Result{}, errors.New("paired_owner_workflow_approval_required")
	}
	var in WorkflowInput
	if err := decode(c.Data, &in); err != nil {
		return Result{}, err
	}
	var err error
	if in.Title, err = text(in.Title, 1, 160); err != nil {
		return Result{}, err
	}
	if in.Instruction, err = text(in.Instruction, 1, 32000); err != nil {
		return Result{}, err
	}
	bindings := map[string]Agent{}
	for _, id := range []string{in.Researcher, in.Writer, in.Reviewer} {
		e, e2 := getEntity(ctx, tx, r.ID, id)
		if e2 != nil || e.Kind != "agent" {
			return Result{}, errors.New("agent_unavailable")
		}
		b, e2 := bodyOf[Agent](e)
		if e2 != nil || b.Disabled {
			return Result{}, errors.New("agent_unavailable")
		}
		bindings[id] = b
	}
	writer, reviewer := bindings[in.Writer], bindings[in.Reviewer]
	if in.Writer == in.Reviewer || writer.SessionID == reviewer.SessionID || (writer.ThreadID != "" && writer.ThreadID == reviewer.ThreadID) {
		return Result{}, errors.New("independent_qa_required")
	}
	if in.DocumentID != "" {
		doc, e2 := getEntity(ctx, tx, r.ID, in.DocumentID)
		if e2 != nil || doc.Kind != "document" {
			return Result{}, errors.New("document_unavailable")
		}
	}
	w := Workflow{Title: in.Title, Instruction: in.Instruction, ThreadID: in.ThreadID, Researcher: in.Researcher, Writer: in.Writer, Reviewer: in.Reviewer, References: in.References, State: "research", Draft: 1, MaxDrafts: 3, Calls: 1, Deadline: s.now().Add(30 * time.Minute).UnixMilli(), DocumentID: in.DocumentID}
	id := s.entityID("workflow", a, c.MutationID)
	if w.ThreadID == "" {
		rootID := id + "_thread"
		if _, err = s.put(ctx, tx, a, r, Entity{ID: rootID, Kind: "message"}, Message{Body: in.Instruction, ThreadID: rootID, Attachments: []Reference{}, Mentions: []string{}}); err != nil {
			return Result{}, err
		}
		w.ThreadID = rootID
	}
	research, err := s.createTask(ctx, tx, a, r, id+"/research", TaskInput{Title: in.Title + " · 研究", Instruction: "Research the following goal. Verify current facts when necessary; preserve citations and distinguish unavailable sources. Produce a research brief for the writer.\n\n" + in.Instruction, AgentID: w.Researcher, ThreadID: w.ThreadID, References: w.References}, id, "research", 1, "", nil)
	if err != nil {
		return Result{}, err
	}
	t, _ := bodyOf[Task](research)
	w.References = t.Manifest.References
	w.ActiveTask = research.ID
	if err = s.saveGrant(ctx, tx, id, a, r.ACLRevision); err != nil {
		return Result{}, err
	}
	e, err := s.put(ctx, tx, a, r, Entity{ID: id, Kind: "workflow"}, w)
	return withEntity(Result{}, e, err)
}

// Observation is accepted only from the native adapter. Commands cannot supply
// a verdict, a seal, a native turn, or an execution-completed assertion.
type Observation struct {
	Admission      string
	Execution      string
	Reason         string
	CanonicalID    string
	WorkItemID     string
	RunID          string
	NativeThreadID string
	NativeTurnID   string
	SealID         string
	Text           string
	Hash           string
}

func (s *Store) Observe(ctx context.Context, roomID, taskID string, o Observation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	e, err := getEntity(ctx, tx, roomID, taskID)
	if err != nil || e.Kind != "task" {
		return errors.New("task_unavailable")
	}
	t, err := bodyOf[Task](e)
	if err != nil {
		return err
	}
	r, err := roomByID(ctx, tx, roomID)
	if err != nil {
		return err
	}
	actor := Actor{ID: "agent:" + t.AgentID, Name: t.Role.Name}
	prior, _ := json.Marshal(t)
	if o.WorkItemID != "" || o.RunID != "" {
		if o.WorkItemID == "" || o.RunID == "" || (t.WorkItemID != "" && (t.WorkItemID != o.WorkItemID || t.RunID != o.RunID)) {
			return errors.New("original_work_link_conflict")
		}
		t.WorkItemID, t.RunID = o.WorkItemID, o.RunID
	}
	if t.Axes.Final == "sealed" {
		if o.Hash != "" && o.Hash != t.FinalHash {
			return errors.New("immutable_final_conflict")
		}
		return nil
	}
	if o.CanonicalID != "" {
		t.CanonicalID = o.CanonicalID
	}
	if o.Admission != "" {
		t.Admission = o.Admission
	}
	if o.Reason != "" {
		t.Reason = o.Reason
	}
	if o.Execution == "running" {
		if o.NativeThreadID == "" || o.NativeTurnID == "" || (t.NativeThreadID != "" && t.NativeThreadID != o.NativeThreadID) {
			return errors.New("exact_native_evidence_required")
		}
		t.NativeThreadID = o.NativeThreadID
		t.NativeTurnID = o.NativeTurnID
		t.Axes.Execution = "running"
	} else if o.Execution != "" {
		switch o.Execution {
		case "waiting", "terminal", "unknown":
			t.Axes.Execution = o.Execution
		default:
			return errors.New("execution_state_invalid")
		}
	}
	if o.Hash != "" {
		if o.SealID == "" || o.NativeThreadID == "" || o.NativeTurnID == "" || digest([]byte(o.Text)) != o.Hash || (t.NativeThreadID != "" && t.NativeThreadID != o.NativeThreadID) {
			return errors.New("sealed_native_result_required")
		}
		if r.ACLRevision != t.Manifest.ACLRevision {
			// The original seal stays in the original native/task store. A stale
			// room context is never automatically published to a newer audience.
			t.Admission = "held"
			t.Axes.Execution = "terminal"
			t.Axes.Final = "unavailable"
			t.Reason = "room_context_changed_result_withheld"
		} else if len(o.Text) > MaxResultBytes {
			t.Admission = "held"
			t.Axes.Execution = "terminal"
			t.Axes.Final = "unavailable"
			t.Reason = "result_exceeds_room_artifact_limit_original_seal_retained"
		} else {
			t.NativeThreadID = o.NativeThreadID
			t.NativeTurnID = o.NativeTurnID
			t.SealID = o.SealID
			t.FinalHash = o.Hash
			t.FinalBytes = len(o.Text)
			t.FinalText = resultPreview(o.Text)
			t.FinalTruncated = len(t.FinalText) != len(o.Text)
			if t.FinalTruncated {
				hash, err := s.putBlob([]byte(o.Text))
				if err != nil {
					return err
				}
				fileID := e.ID + "_full_result"
				if _, err = s.put(ctx, tx, actor, r, Entity{ID: fileID, Kind: "attachment"}, Attachment{Name: t.Title + ".txt", MIME: "text/plain", Hash: hash, Size: int64(len(o.Text)), CreatedBy: actor.ID}); err != nil {
					return err
				}
				t.ResultAttachmentID = fileID
			}
			t.Axes.Execution = "terminal"
			t.Axes.Final = "sealed"
			t.Admission = "completed"
			t.Reason = ""
		}
	}
	if t.Admission == "cancelled" || t.Admission == "failed" || t.Admission == "rejected" {
		t.Axes.Execution = "terminal"
		if t.Axes.Final != "sealed" {
			t.Axes.Final = "unavailable"
		}
	}
	if t.Admission == "unknown" {
		t.Axes.Execution = "unknown"
	}
	current, _ := json.Marshal(t)
	if string(current) == string(prior) {
		if _, err = tx.ExecContext(ctx, "UPDATE native_jobs SET updated_at=? WHERE task_id=?", s.now().UnixMilli(), e.ID); err != nil {
			return err
		}
		return tx.Commit()
	}
	if t.Axes.Final == "sealed" {
		messageID := e.ID + "_result"
		thread := t.ThreadID
		if thread == "" {
			thread = messageID
		}
		attachments := []Reference{}
		if t.ResultAttachmentID != "" {
			file, e2 := getEntity(ctx, tx, r.ID, t.ResultAttachmentID)
			if e2 != nil {
				return e2
			}
			attachments = append(attachments, referenceTo(file))
		}
		if _, err = s.put(ctx, tx, actor, r, Entity{ID: messageID, Kind: "message"}, Message{Body: t.FinalText, ThreadID: thread, TaskID: e.ID, Attachments: attachments, Mentions: []string{}}); err != nil {
			return err
		}
		t.Axes.Delivery = "delivered"
	}
	updated, err := s.put(ctx, tx, actor, r, e, t)
	if err != nil {
		return err
	}
	state := t.Admission
	if state == "prepared" && t.Axes.Execution == "running" {
		state = "running"
	}
	if _, err = tx.ExecContext(ctx, "UPDATE native_jobs SET state=?,updated_at=? WHERE task_id=?", state, s.now().UnixMilli(), e.ID); err != nil {
		return err
	}
	if t.Axes.Final == "sealed" {
		if t.OwnerTaskID != "" {
			if err = s.recordReview(ctx, tx, actor, r, updated, t); err != nil {
				return err
			}
		}
		if t.WorkflowID != "" {
			if err = s.advanceWorkflow(ctx, tx, r, updated, t); err != nil {
				return err
			}
		}
		if err = s.notify(ctx, tx, actor, r, t.CreatedBy, "result", e.ID); err != nil {
			return err
		}
	} else if t.Axes.Execution == "terminal" && t.WorkflowID != "" {
		w, e2 := getEntity(ctx, tx, r.ID, t.WorkflowID)
		if e2 != nil {
			return e2
		}
		wf, e2 := bodyOf[Workflow](w)
		if e2 != nil {
			return e2
		}
		if wf.ActiveTask == e.ID {
			wf.State = "needs_human"
			wf.Reason = t.Reason
			if _, err = s.put(ctx, tx, actor, r, w, wf); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *Store) recordReview(ctx context.Context, tx *sql.Tx, a Actor, r Room, qa Entity, qt Task) error {
	owner, err := getEntity(ctx, tx, r.ID, qt.OwnerTaskID)
	if err != nil || owner.Kind != "task" {
		return errors.New("owner_round_unavailable")
	}
	ot, err := bodyOf[Task](owner)
	if err != nil || owner.ID != qt.OwnerRoundID || ot.Axes.Final != "sealed" || qt.SessionID == ot.SessionID || qt.NativeThreadID == ot.NativeThreadID {
		return errors.New("independent_qa_required")
	}
	var parsed struct {
		OwnerRoundID string `json:"owner_round_id"`
		Verdict      string `json:"verdict"`
		Findings     string `json:"findings"`
	}
	// Only the exact sealed final is parsed, never an ordinary message or a
	// caller-provided verdict. Malformed review leaves QA unknown.
	verdict := "unknown"
	findings := "sealed_review_format_invalid"
	fullReview, err := s.ResultText(ctx, tx, r, qt)
	if err != nil {
		return err
	}
	if len(fullReview) <= MaxTextBytes && json.Unmarshal([]byte(strings.TrimSpace(fullReview)), &parsed) == nil && parsed.OwnerRoundID == owner.ID && (parsed.Verdict == "PASS" || parsed.Verdict == "FAIL") {
		verdict = parsed.Verdict
		findings = parsed.Findings
		if len(findings) > MaxTextBytes {
			findings = "sealed_review_findings_too_large"
			verdict = "unknown"
		}
	}
	review := Review{OwnerRoundID: owner.ID, QARoundID: qa.ID, OwnerHash: ot.FinalHash, QAHash: qt.FinalHash, Verdict: verdict, Findings: findings}
	if _, err = s.put(ctx, tx, a, r, Entity{ID: qa.ID + "_review", Kind: "review"}, review); err != nil {
		return err
	}
	ot.Axes.QA = verdict
	_, err = s.put(ctx, tx, a, r, owner, ot)
	return err
}

func (s *Store) advanceWorkflow(ctx context.Context, tx *sql.Tx, r Room, completed Entity, t Task) error {
	e, err := getEntity(ctx, tx, r.ID, t.WorkflowID)
	if err != nil || e.Kind != "workflow" {
		return errors.New("workflow_unavailable")
	}
	w, err := bodyOf[Workflow](e)
	if err != nil {
		return err
	}
	if w.ActiveTask != completed.ID || w.State == "cancel_requested" || w.State == "cancelled" {
		return nil
	}
	g, err := grant(ctx, tx, e.ID)
	if err != nil {
		return err
	}
	a := actorForGrant(g)
	if _, _, err = s.access(ctx, tx, a, r.ID, "dispatch"); err != nil || r.ACLRevision != g.ACLRevision || w.Deadline <= s.now().UnixMilli() || w.Calls >= 12 {
		w.State = "needs_human"
		w.Reason = "workflow_authorization_or_budget_exhausted"
		_, err = s.put(ctx, tx, Actor{ID: "workflow:" + e.ID}, r, e, w)
		return err
	}
	var input TaskInput
	var step, owner string
	extra := []Reference{referenceTo(completed)}
	sources, err := researchReferences(ctx, tx, r.ID, completed.ID)
	if err != nil {
		return err
	}
	extra = append(extra, sources...)
	switch t.Step {
	case "research":
		input = TaskInput{Title: w.Title + " · 草稿 1", Instruction: "Write the requested content using the pinned research. Preserve citations, distinguish proposals from verified facts, and include unresolved limitations.\n\n" + w.Instruction, AgentID: w.Writer, ThreadID: w.ThreadID, References: w.References}
		step = "draft"
	case "draft", "rewrite":
		w.LatestDraft = completed.ID
		input = TaskInput{Title: w.Title + " · 獨立審查", Instruction: "Review this exact draft against the original goal, evidence and citations. Return PASS only when required scope is covered; otherwise return actionable FAIL findings.\n\nOriginal goal:\n" + w.Instruction, AgentID: w.Reviewer, ThreadID: w.ThreadID, References: w.References}
		step = "review"
		owner = completed.ID
		ownerTask := t
		ownerTask.Axes.QA = "pending"
		if _, err = s.put(ctx, tx, Actor{ID: "workflow:" + e.ID}, r, completed, ownerTask); err != nil {
			return err
		}
	case "review":
		draftEntity, err := getEntity(ctx, tx, r.ID, w.LatestDraft)
		if err != nil {
			return err
		}
		draftTask, err := bodyOf[Task](draftEntity)
		if err != nil {
			return err
		}
		switch draftTask.Axes.QA {
		case "PASS":
			fullDraft, e2 := s.ResultText(ctx, tx, r, draftTask)
			if e2 != nil {
				return e2
			}
			if len(fullDraft) > MaxTextBytes {
				w.State = "needs_human"
				w.Reason = "full_result_retained_canvas_text_limit_exceeded"
				_, err = s.put(ctx, tx, Actor{ID: "workflow:" + e.ID}, r, e, w)
				return err
			}
			w.State = "awaiting_human"
			w.Reason = ""
			w.ActiveTask = ""
			if w.DocumentID == "" {
				doc := Entity{ID: e.ID + "_document", Kind: "document"}
				doc, err = s.saveDocument(ctx, tx, Actor{ID: "agent:" + draftTask.AgentID}, r, doc, w.Title, fullDraft, []Reference{referenceTo(draftEntity)}, draftEntity.ID)
				if err != nil {
					return err
				}
				w.DocumentID = doc.ID
			} else {
				doc, err := getEntity(ctx, tx, r.ID, w.DocumentID)
				if err != nil {
					return err
				}
				current, err := bodyOf[Document](doc)
				if err != nil {
					return err
				}
				proposal := DocumentProposal{DocumentID: doc.ID, Base: doc.Revision, Title: current.Title, Body: fullDraft, State: "proposed", TaskID: draftEntity.ID}
				if _, err = s.put(ctx, tx, Actor{ID: "agent:" + draftTask.AgentID}, r, Entity{ID: draftEntity.ID + "_proposal", Kind: "document_proposal"}, proposal); err != nil {
					return err
				}
			}
			if _, err = s.put(ctx, tx, Actor{ID: "workflow:" + e.ID}, r, e, w); err != nil {
				return err
			}
			return s.notify(ctx, tx, Actor{ID: "workflow:" + e.ID}, r, g.PrincipalID, "accept_result", draftEntity.ID)
		case "FAIL":
			if w.Draft >= w.MaxDrafts {
				w.State = "needs_human"
				w.Reason = "max_draft_versions_reached"
				_, err = s.put(ctx, tx, Actor{ID: "workflow:" + e.ID}, r, e, w)
				return err
			}
			w.Draft++
			input = TaskInput{Title: w.Title + " · 改稿 " + uintString(uint64(w.Draft)), Instruction: "Revise the prior draft using the exact sealed review findings. Keep supported material and explain unresolved issues.\n\nOriginal goal:\n" + w.Instruction, AgentID: w.Writer, ThreadID: w.ThreadID, References: w.References}
			step = "rewrite"
			extra = append(extra, referenceTo(draftEntity))
		default:
			w.State = "needs_human"
			w.Reason = "sealed_qa_unknown"
			_, err = s.put(ctx, tx, Actor{ID: "workflow:" + e.ID}, r, e, w)
			return err
		}
	default:
		return nil
	}
	key := e.ID + "/" + step + "/" + uintString(uint64(w.Draft))
	if _, err = tx.ExecContext(ctx, "SAVEPOINT room_next_stage"); err != nil {
		return err
	}
	next, err := s.createTask(ctx, tx, a, r, key, input, e.ID, step, w.Draft, owner, extra)
	if err != nil {
		if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO room_next_stage"); rollbackErr != nil {
			return rollbackErr
		}
		if _, releaseErr := tx.ExecContext(ctx, "RELEASE room_next_stage"); releaseErr != nil {
			return releaseErr
		}
		w.State = "needs_human"
		w.Reason = err.Error()
		_, err = s.put(ctx, tx, Actor{ID: "workflow:" + e.ID}, r, e, w)
		return err
	}
	if _, err = tx.ExecContext(ctx, "RELEASE room_next_stage"); err != nil {
		return err
	}
	w.ActiveTask = next.ID
	w.State = step
	w.Calls++
	_, err = s.put(ctx, tx, Actor{ID: "workflow:" + e.ID}, r, e, w)
	return err
}

// Claim marks an original prepared request before handing it to the existing
// durable queue. Recovery must inspect that same queue ID before any further
// submission; no replacement request is ever minted.
func (s *Store) Claim(ctx context.Context, roomID, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	e, err := getEntity(ctx, tx, roomID, taskID)
	if err != nil {
		return err
	}
	t, err := bodyOf[Task](e)
	if err != nil {
		return err
	}
	if t.Admission != "prepared" {
		return errors.New("task_already_claimed")
	}
	var active int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM native_jobs WHERE state IN ('dispatching','queued','running','unknown','cancel_pending')").Scan(&active); err != nil {
		return err
	}
	if active >= 3 {
		return errors.New("workspace_parallel_limit")
	}
	t.Admission = "dispatching"
	r, err := roomByID(ctx, tx, roomID)
	if err != nil {
		return err
	}
	if _, err = s.put(ctx, tx, Actor{ID: "gateway"}, r, e, t); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE native_jobs SET state='dispatching',updated_at=? WHERE task_id=? AND state='prepared'", s.now().UnixMilli(), taskID); err != nil {
		return err
	}
	return tx.Commit()
}
