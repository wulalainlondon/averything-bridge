package workitems

import (
	"context"
	"database/sql"
	"encoding/json"
	"everything-go/internal/coordination"
	"everything-go/internal/taskapi"
	"time"
)

// DispatchAPI runs the SAME coordination policy and normalized-state writes,
// together with the API intent/outbox, under the original work-store writer.
func (s *Service) DispatchAPI(ctx context.Context, api taskapi.AuthorizedCommand, intent taskapi.IntentRecord, principal coordination.Principal, command coordination.CollaborationCommand, legacy coordination.Command) (taskapi.IntentRecord, bool, coordination.State, error) {
	var record taskapi.IntentRecord
	created := false
	var state coordination.State
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var previous map[string]collaborationRecord
		var err error
		state, previous, err = readCollaboration(ctx, tx)
		if err != nil {
			return err
		}
		record, created, err = s.TaskJournal().ClaimTx(tx, api, intent)
		if err != nil || !created {
			return err
		}
		if state.Projects[command.ProjectID].EngineVersion == 2 {
			_, err = state.ApplyCollaboration(principal, command, time.Now().UnixMilli())
		} else {
			_, err = state.Apply(principal, legacy, time.Now().UnixMilli())
		}
		if err != nil {
			return err
		}
		task := state.Tasks[command.TaskID]
		if task.SessionID != intent.SessionID {
			return taskapi.Failure("stale_revision", "known_none", "refresh_identity")
		}
		record.RequestID = task.RequestID
		if state.Projects[command.ProjectID].EngineVersion == 2 && state.Collaboration != nil {
			record.RequestID = state.Collaboration.Tasks[task.ID].ActiveRequestID
		}
		if _, err = tx.Exec("UPDATE task_api_intents SET request_id=? WHERE namespace_key=?", record.RequestID, record.Key); err != nil {
			return err
		}
		legacyState, records, err := collaborationRecords(state)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(legacyState)
		if err != nil {
			return err
		}
		if err = s.store.writeCollaborationRecords(ctx, tx, previous, records, state); err != nil {
			return err
		}
		if _, err = tx.Exec("INSERT INTO work_pm_state(id,payload) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", encoded); err != nil {
			return err
		}
		return taskapi.ChangeTx(tx, task.SessionID, record.RequestID, "admission")
	})
	return record, created, state, err
}
