package workspace

import (
	"context"
	"errors"
)

// WorkProjectionState is host-only metadata for the existing WorkItem service.
// It is not a wire action or a way to retrieve task/source text as a guest.
func (s *Store) WorkProjectionState(ctx context.Context, roomID, taskID string) (Room, Task, error) {
	r, err := roomByID(ctx, s.db, roomID)
	if err != nil {
		return r, Task{}, err
	}
	e, err := getEntity(ctx, s.db, roomID, taskID)
	if err != nil || e.Kind != "task" {
		return r, Task{}, errors.New("task_unavailable")
	}
	t, err := bodyOf[Task](e)
	return r, t, err
}
