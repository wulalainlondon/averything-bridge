package delegation

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"everything-go/internal/handoff"
)

const handoffDDL = `
CREATE TABLE IF NOT EXISTS delegation_return_contexts (
 id TEXT PRIMARY KEY, parent TEXT NOT NULL, identity TEXT NOT NULL,
 snapshot_json BLOB NOT NULL, content TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS delegation_handoff_revisions (
 parent TEXT NOT NULL, path TEXT NOT NULL, revision INTEGER NOT NULL,
 sha256 TEXT NOT NULL, PRIMARY KEY(parent,path)
);`

type ReturnContext struct {
	Snapshot handoff.Snapshot `json:"snapshot"`
	Content  string           `json:"content"`
}

// Identity includes the actual sealed result and original source binding; no
// caller may transplant a snapshot to another parent/return/source/result.
func returnIdentity(r Record, thread string, revision uint64) string {
	b, _ := json.Marshal([]any{r.ID, r.ParentSessionID, r.ParentRequestID, r.ChildSessionID, r.ChildRequestID, r.OriginRequestID, r.ToolCallID, thread, revision, r.TerminalStatus, r.Result, r.Error, r.Artifacts})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func (s *Store) ReturnContext(ctx context.Context, r Record, thread string, revision uint64) (ReturnContext, bool, error) {
	var c ReturnContext
	var parent, identity string
	var b []byte
	err := s.db.QueryRowContext(ctx, "SELECT parent,identity,snapshot_json,content FROM delegation_return_contexts WHERE id=?", r.ID).Scan(&parent, &identity, &b, &c.Content)
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}
	if parent != r.ParentSessionID || identity != returnIdentity(r, thread, revision) {
		return c, false, errors.New("handoff_return_identity_conflict")
	}
	c.Snapshot, err = handoff.DecodeSnapshot(b)
	return c, err == nil, err
}

// SealReturnContext is additive: it never changes delegation completion or
// delivery states. First writer wins, including disabled/unavailable snapshots.
func (s *Store) SealReturnContext(ctx context.Context, r Record, thread string, revision uint64, snapshot handoff.Snapshot, original string) (ReturnContext, error) {
	var out ReturnContext
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var parent, request, child, childRequest, result, status, source string
		var rev uint64
		err := tx.QueryRowContext(ctx, `SELECT d.parent_session_id,d.parent_request_id,d.child_session_id,d.child_request_id,d.result,d.terminal_status,b.thread,b.revision
   FROM session_delegations d JOIN delegation_source_bindings b ON b.id=d.id WHERE d.id=? AND d.state='terminal'`, r.ID).Scan(&parent, &request, &child, &childRequest, &result, &status, &source, &rev)
		if err != nil {
			return err
		}
		if parent != r.ParentSessionID || request != r.ParentRequestID || child != r.ChildSessionID || childRequest != r.ChildRequestID || result != r.Result || status != r.TerminalStatus || source != thread || rev != revision {
			return errors.New("handoff_return_identity_conflict")
		}
		var identity string
		var data []byte
		err = tx.QueryRowContext(ctx, "SELECT identity,snapshot_json,content FROM delegation_return_contexts WHERE id=? AND parent=?", r.ID, parent).Scan(&identity, &data, &out.Content)
		if err == nil {
			if identity != returnIdentity(r, thread, revision) {
				return errors.New("handoff_return_identity_conflict")
			}
			out.Snapshot, err = handoff.DecodeSnapshot(data)
			return err
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		digest := sha256.Sum256([]byte(r.Result))
		snapshot.ResultSHA256 = hex.EncodeToString(digest[:])
		snapshot.DelegationID = r.ID
		snapshot = snapshot.Bounded()
		if snapshot.Status == "available" {
			var previous uint64
			var sha string
			err = tx.QueryRowContext(ctx, "SELECT revision,sha256 FROM delegation_handoff_revisions WHERE parent=? AND path=?", parent, snapshot.Path).Scan(&previous, &sha)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil && (snapshot.Revision < previous || (snapshot.Revision == previous && snapshot.SHA256 != sha)) {
				snapshot.Status = "unavailable"
				snapshot.Reason = "revision_drift"
				snapshot.Goal = ""
				snapshot.Progress = ""
				snapshot.NextStep = ""
			} else {
				_, err = tx.ExecContext(ctx, `INSERT INTO delegation_handoff_revisions VALUES(?,?,?,?) ON CONFLICT(parent,path) DO UPDATE SET revision=excluded.revision,sha256=excluded.sha256`, parent, snapshot.Path, snapshot.Revision, snapshot.SHA256)
				if err != nil {
					return err
				}
			}
		}
		out = ReturnContext{Snapshot: snapshot, Content: original + snapshot.Reminder()}
		data, err = json.Marshal(snapshot)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO delegation_return_contexts VALUES(?,?,?,?,?)", r.ID, parent, returnIdentity(r, thread, revision), data, out.Content)
		return err
	})
	return out, err
}
