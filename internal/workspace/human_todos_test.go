package workspace

import "testing"

func TestHumanQuestionAssignmentAnswerAndExplicitAcceptanceNeverDispatchAI(t *testing.T) {
	f := setup(t)
	guest, _ := f.invite("editor")
	create := f.command(Command{Action: "create_human_todo", RoomID: f.room.ID, Data: raw(HumanTodoInput{Title: "Choose the draft", Body: "Which direction should the human writer use?", Assignee: guest.ID, Kind: "question"})})
	created, err := f.s.Apply(f.ctx, f.owner, create)
	if err != nil {
		t.Fatal(err)
	}
	todo := *created.Entity
	if body, _ := bodyOf[HumanTodo](todo); body.State != "open" || body.Assignee != guest.ID {
		t.Fatal(body)
	}
	again, err := f.s.Apply(f.ctx, f.owner, create)
	if err != nil || again.Entity.ID != todo.ID {
		t.Fatal("duplicate human assignment", err)
	}
	snap := f.call(guest, Command{Action: "snapshot", RoomID: f.room.ID}).Snapshot
	if len(snap.Data["human_todo"].Items) != 1 || len(snap.Data["notification"].Items) != 1 {
		t.Fatal("assignment missing from scoped inbox")
	}
	n := snap.Data["notification"].Items[0]
	f.call(guest, Command{Action: "read_notification", RoomID: f.room.ID, EntityID: n.ID, ExpectedRevision: ptr(n.Revision)})
	current := f.call(guest, Command{Action: "get", RoomID: f.room.ID, EntityID: todo.ID}).Entity
	if body, _ := bodyOf[HumanTodo](*current); body.State != "open" {
		t.Fatal("read receipt resolved question")
	}
	f.reject(f.owner, Command{Action: "respond_human_todo", RoomID: f.room.ID, EntityID: todo.ID, ExpectedRevision: ptr(todo.Revision), Data: raw(map[string]any{"answer": "Owner cannot impersonate the assignee"})}, "original_assignee")
	respond := f.command(Command{Action: "respond_human_todo", RoomID: f.room.ID, EntityID: todo.ID, ExpectedRevision: ptr(todo.Revision), Data: raw(map[string]any{"answer": "Use the concise direction; human answer only."})})
	answered, err := f.s.Apply(f.ctx, guest, respond)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := bodyOf[HumanTodo](*answered.Entity)
	if body.State != "answered" || len(body.Replies) != 1 || body.Replies[0].Author != guest.ID {
		t.Fatal("actual author lost", body)
	}
	f.reject(guest, Command{Action: "resolve_human_todo", RoomID: f.room.ID, EntityID: todo.ID, ExpectedRevision: ptr(answered.Entity.Revision), Data: raw(map[string]any{"action": "accept"})}, "creator_or_manager")
	resolved := f.call(f.owner, Command{Action: "resolve_human_todo", RoomID: f.room.ID, EntityID: todo.ID, ExpectedRevision: ptr(answered.Entity.Revision), Data: raw(map[string]any{"action": "accept", "reason": "Human direction confirmed"})})
	body, _ = bodyOf[HumanTodo](*resolved.Entity)
	if body.State != "completed" || body.ResolvedBy != f.owner.ID || len(body.Replies) != 1 {
		t.Fatal(body)
	}
	f.reject(f.owner, Command{Action: "update_human_todo", RoomID: f.room.ID, EntityID: todo.ID, ExpectedRevision: ptr(todo.Revision), Data: create.Data}, "revision_conflict")
	old, err := f.s.Apply(f.ctx, guest, respond)
	if err != nil || old.Entity.Revision != answered.Entity.Revision {
		t.Fatal("unknown/lost ACK changed original response", err)
	}
	var jobs int
	if err = f.s.db.QueryRow("SELECT COUNT(*) FROM native_jobs").Scan(&jobs); err != nil || jobs != 0 {
		t.Fatal("human response created native work", jobs, err)
	}
}

func TestHumanTodoViewerAndRevokedAssigneeCannotWriteOrRead(t *testing.T) {
	f := setup(t)
	viewer, _ := f.invite("viewer")
	guest, _ := f.invite("editor")
	f.reject(f.owner, Command{Action: "create_human_todo", RoomID: f.room.ID, Data: raw(HumanTodoInput{Title: "Bad destination", Assignee: viewer.ID, Kind: "todo"})}, "active_writer")
	f.reject(viewer, Command{Action: "create_human_todo", RoomID: f.room.ID, Data: raw(HumanTodoInput{Title: "Viewer cannot write", Assignee: guest.ID, Kind: "todo"})}, "permission")
	e := f.call(f.owner, Command{Action: "create_human_todo", RoomID: f.room.ID, Data: raw(HumanTodoInput{Title: "Assigned task", Assignee: guest.ID, Kind: "todo"})}).Entity
	snap := f.call(f.owner, Command{Action: "snapshot", RoomID: f.room.ID}).Snapshot
	for _, member := range snap.Members {
		if member.PrincipalID == guest.ID {
			f.call(f.owner, Command{Action: "update_member", RoomID: f.room.ID, ExpectedRevision: ptr(member.Revision), Data: raw(map[string]any{"principal_id": guest.ID, "role": "editor", "active": false})})
		}
	}
	f.reject(guest, Command{Action: "get", RoomID: f.room.ID, EntityID: e.ID}, "permission")
	f.reject(guest, Command{Action: "respond_human_todo", RoomID: f.room.ID, EntityID: e.ID, ExpectedRevision: ptr(e.Revision), Data: raw(map[string]any{"answer": "revoked"})}, "permission")
}
