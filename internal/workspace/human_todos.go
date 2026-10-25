package workspace

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
)

type HumanReply struct {
	Author string `json:"author"`
	Text   string `json:"text"`
	At     int64  `json:"at"`
}
type HumanTodo struct {
	Title      string       `json:"title"`
	Body       string       `json:"body"`
	Assignee   string       `json:"assignee"`
	Kind       string       `json:"todo_kind"`
	State      string       `json:"state"`
	DueAt      int64        `json:"due_at,omitempty"`
	References []Reference  `json:"references"`
	Replies    []HumanReply `json:"replies"`
	Resolution string       `json:"resolution,omitempty"`
	ResolvedBy string       `json:"resolved_by,omitempty"`
}
type HumanTodoInput struct {
	Title      string      `json:"title"`
	Body       string      `json:"body"`
	Assignee   string      `json:"assignee"`
	Kind       string      `json:"todo_kind"`
	DueAt      int64       `json:"due_at"`
	References []Reference `json:"references"`
}

// Only authenticated human mutations reach this path. Neither a notification
// read, a model result, a QA verdict nor a schedule resolves a human question.
func (s *Store) humanTodoMutation(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	_, role, err := s.access(ctx, tx, a, r.ID, "comment")
	if err != nil {
		return Result{}, err
	}
	var e Entity
	var todo HumanTodo
	if c.Action != "create_human_todo" {
		e, err = s.getForUpdate(ctx, tx, c, "human_todo")
		if err != nil {
			return Result{}, err
		}
		todo, err = bodyOf[HumanTodo](e)
		if err != nil {
			return Result{}, err
		}
	}
	manage := permits(role, "manage") || e.CreatedBy == a.ID
	kind := "human_todo_updated"
	target := todo.Assignee
	switch c.Action {
	case "create_human_todo", "update_human_todo":
		if c.Action == "update_human_todo" && (!manage || todo.State != "open") {
			return Result{}, errors.New("human_todo_edit_not_allowed")
		}
		var in HumanTodoInput
		if err = decode(c.Data, &in); err != nil {
			return Result{}, err
		}
		if in.Title, err = text(in.Title, 1, 160); err != nil {
			return Result{}, err
		}
		if in.Body, err = text(in.Body, 0, 16000); err != nil {
			return Result{}, err
		}
		if in.Kind != "todo" && in.Kind != "question" {
			return Result{}, errors.New("human_todo_kind_invalid")
		}
		if in.DueAt < 0 || in.DueAt > 253402300799000 {
			return Result{}, errors.New("human_todo_due_invalid")
		}
		var member Member
		err = tx.QueryRowContext(ctx, "SELECT principal_id,role,active FROM members WHERE room_id=? AND principal_id=?", r.ID, in.Assignee).Scan(&member.PrincipalID, &member.Role, &member.Active)
		if err != nil || !member.Active || !permits(member.Role, "comment") {
			return Result{}, errors.New("human_todo_active_writer_required")
		}
		if len(in.References) > 16 {
			return Result{}, errors.New("human_todo_reference_limit")
		}
		if err = references(ctx, tx, r, in.References); err != nil {
			return Result{}, err
		}
		if c.Action == "create_human_todo" {
			e = Entity{ID: s.entityID("human_todo", a, c.MutationID), Kind: "human_todo"}
			todo = HumanTodo{State: "open", Replies: []HumanReply{}}
			kind = "human_todo_assigned"
		}
		todo.Title, todo.Body, todo.Assignee, todo.Kind, todo.DueAt, todo.References = in.Title, in.Body, in.Assignee, in.Kind, in.DueAt, in.References
		target = in.Assignee
	case "respond_human_todo":
		if todo.Assignee != a.ID || todo.State != "open" {
			return Result{}, errors.New("human_todo_original_assignee_required")
		}
		var in struct {
			Answer string `json:"answer"`
		}
		if err = decode(c.Data, &in); err != nil {
			return Result{}, err
		}
		minimum := 0
		if todo.Kind == "question" {
			minimum = 1
		}
		if len(todo.Replies) >= 8 {
			return Result{}, errors.New("human_todo_reply_limit_new_linked_item_required")
		}
		if in.Answer, err = text(in.Answer, minimum, 4000); err != nil {
			return Result{}, err
		}
		todo.Replies = append(todo.Replies, HumanReply{Author: a.ID, Text: in.Answer, At: s.now().UnixMilli()})
		todo.State = "completed"
		if todo.Kind == "question" {
			todo.State = "answered"
		}
		kind = "human_todo_answered"
		target = e.CreatedBy
	case "resolve_human_todo":
		if !manage {
			return Result{}, errors.New("human_todo_creator_or_manager_required")
		}
		var in struct {
			Action string `json:"action"`
			Reason string `json:"reason"`
		}
		if err = decode(c.Data, &in); err != nil {
			return Result{}, err
		}
		if in.Reason, err = text(in.Reason, 0, 8000); err != nil {
			return Result{}, err
		}
		switch in.Action {
		case "accept":
			if todo.State != "answered" {
				return Result{}, errors.New("human_question_answer_required")
			}
			todo.State = "completed"
		case "reopen":
			if todo.State != "answered" && todo.State != "completed" {
				return Result{}, errors.New("human_todo_not_resolved")
			}
			todo.State = "open"
		case "cancel":
			if todo.State == "cancelled" {
				return Result{}, errors.New("human_todo_already_cancelled")
			}
			todo.State = "cancelled"
		default:
			return Result{}, errors.New("human_todo_resolution_invalid")
		}
		todo.Resolution, todo.ResolvedBy = in.Reason, a.ID
		kind = "human_todo_resolved"
	default:
		return Result{}, errors.New("workspace_action_unsupported")
	}
	e, err = s.put(ctx, tx, a, r, e, todo)
	if err != nil {
		return Result{}, err
	}
	// Each explicit revision has its own notice while the notice still routes
	// to the stable todo entity, rather than an unrelated latest task.
	if target != "" && target != a.ID {
		var active bool
		if tx.QueryRowContext(ctx, "SELECT active FROM members WHERE room_id=? AND principal_id=?", r.ID, target).Scan(&active) == nil && active {
			notice := Entity{ID: "cw_notice_" + s.keyed("human-todo-notice", r.ID+"/"+target+"/"+e.ID+"/"+strconv.FormatUint(e.Revision, 10))[:24], Kind: "notification"}
			notice, err = s.put(ctx, tx, a, r, notice, Notification{PrincipalID: target, Kind: kind, EntityID: e.ID})
			if err != nil {
				return Result{}, err
			}
			if err = s.queuePush(ctx, tx, r, target, notice.ID); err != nil {
				return Result{}, err
			}
		}
	}
	return Result{Entity: &e}, nil
}
