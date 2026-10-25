package workspace

import (
	"context"
	"database/sql"
	"errors"
)

func (s *Store) RoomSessionID(a Actor, roomID, mutationID string) string {
	return "s_cw_" + s.keyed("room-session", a.ID+"/"+roomID+"/"+mutationID)[:28]
}
func (s *Store) Lookup(ctx context.Context, a Actor, roomID, id string) (Entity, error) {
	if _, _, err := s.Authorize(ctx, a, roomID, "read"); err != nil {
		return Entity{}, err
	}
	return getEntity(ctx, s.db, roomID, id)
}

type PendingAgent struct {
	Entity Entity
	Agent  Agent
	Room   Room
	Grant  Grant
}

func (s *Store) PendingAgents(ctx context.Context) ([]PendingAgent, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+entityColumns+" FROM entities WHERE kind='agent' AND json_extract(body,'$.ready')=0 AND json_extract(body,'$.room_exclusive')=1 AND json_extract(body,'$.disabled')=0 LIMIT 20")
	if err != nil {
		return nil, err
	}
	entities := []Entity{}
	for rows.Next() {
		e, e2 := scanEntity(rows)
		if e2 != nil {
			rows.Close()
			return nil, e2
		}
		entities = append(entities, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := []PendingAgent{}
	for _, e := range entities {
		a, err := bodyOf[Agent](e)
		if err != nil {
			return nil, err
		}
		r, err := roomByID(ctx, s.db, e.RoomID)
		if err != nil {
			return nil, err
		}
		g, err := grant(ctx, s.db, e.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, PendingAgent{e, a, r, g})
	}
	return out, nil
}
func (s *Store) AgentReady(ctx context.Context, roomID, id, sessionID string, ready bool, reason string) (Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Entity{}, err
	}
	defer tx.Rollback()
	e, err := getEntity(ctx, tx, roomID, id)
	if err != nil || e.Kind != "agent" {
		return Entity{}, errors.New("agent_unavailable")
	}
	a, err := bodyOf[Agent](e)
	if err != nil || a.SessionID != sessionID || !a.RoomExclusive {
		return Entity{}, errors.New("agent_binding_changed")
	}
	if a.Ready == ready && a.SetupReason == reason {
		return e, nil
	}
	a.Ready = ready
	a.SetupReason = reason
	r, err := roomByID(ctx, tx, roomID)
	if err != nil {
		return Entity{}, err
	}
	e, err = s.put(ctx, tx, Actor{ID: "workspace-provisioner"}, r, e, a)
	if err != nil {
		return Entity{}, err
	}
	return e, tx.Commit()
}
func (s *Store) NativeRequest(ctx context.Context, roomID, id string) (string, string, error) {
	e, err := getEntity(ctx, s.db, roomID, id)
	if err != nil || e.Kind != "task" {
		return "", "", errors.New("task_unavailable")
	}
	t, err := bodyOf[Task](e)
	return t.SessionID, t.RequestID, err
}
func (s *Store) TaskGrant(ctx context.Context, taskID string) (Grant, error) {
	return grant(ctx, s.db, taskID)
}

// Keep the transaction helper type checked in this file as provisioning is
// deliberately driven by the original persisted grant rather than a fake Client.
var _ sqlReader = (*sql.Tx)(nil)
