package delegation

import (
	"context"
	"database/sql"
	"errors"
)

// Explicit, append-only authority epochs. Original source bindings, rounds,
// results and frozen returns are never rewritten or implicitly upgraded.
const reauthorizationDDL = `CREATE TABLE IF NOT EXISTS delegation_source_epochs (
 logical_id TEXT NOT NULL, epoch INTEGER NOT NULL, parent TEXT NOT NULL,
 authority TEXT NOT NULL, thread TEXT NOT NULL, from_revision INTEGER NOT NULL,
 to_revision INTEGER NOT NULL, grant_digest TEXT NOT NULL, peer TEXT NOT NULL,
 origin_request TEXT NOT NULL, origin_call TEXT NOT NULL,
 PRIMARY KEY(logical_id,epoch), UNIQUE(logical_id,from_revision));`

type SourceEpoch struct {
	LogicalID, Parent, Authority, Thread, GrantDigest, Peer, Request, Call string
	Epoch, FromRevision, ToRevision                                        uint64
}

func (s *Store) SourceEpoch(ctx context.Context, logical string) (SourceEpoch, bool, error) {
	var a SourceEpoch
	err := s.db.QueryRowContext(ctx, `SELECT logical_id,epoch,parent,authority,thread,from_revision,to_revision,grant_digest,peer,origin_request,origin_call FROM delegation_source_epochs WHERE logical_id=? ORDER BY epoch DESC LIMIT 1`, logical).Scan(&a.LogicalID, &a.Epoch, &a.Parent, &a.Authority, &a.Thread, &a.FromRevision, &a.ToRevision, &a.GrantDigest, &a.Peer, &a.Request, &a.Call)
	if errors.Is(err, sql.ErrNoRows) {
		return a, false, nil
	}
	return a, err == nil, err
}
func (s *Store) ReauthorizePair(ctx context.Context, pair [2]SourceEpoch, validate func() error) error {
	return s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		if err := validate(); err != nil {
			return err
		}
		if pair[0].LogicalID == pair[1].LogicalID || pair[0].Peer != pair[1].LogicalID || pair[1].Peer != pair[0].LogicalID {
			return errors.New("delegation_independent_qa_required")
		}
		for _, a := range pair {
			var parent, thread string
			var original, current, epoch uint64
			if err := tx.QueryRowContext(ctx, `SELECT d.parent_session_id,b.thread,b.revision FROM session_delegations d JOIN delegation_source_bindings b ON d.id=b.id WHERE d.id=?`, a.LogicalID).Scan(&parent, &thread, &original); err != nil {
				return err
			}
			current = original
			err := tx.QueryRowContext(ctx, `SELECT epoch,to_revision FROM delegation_source_epochs WHERE logical_id=? ORDER BY epoch DESC LIMIT 1`, a.LogicalID).Scan(&epoch, &current)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if parent != a.Parent || thread != a.Thread || current != a.FromRevision || a.ToRevision <= current || a.Epoch != epoch+1 {
				return errors.New("delegation_reauthorization_stale")
			}
			var pending int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM session_delegations d JOIN delegation_rounds r ON r.id=d.id WHERE r.logical_id=? AND (d.state!='terminal' OR d.delivery_state!='delivered')`, a.LogicalID).Scan(&pending); err != nil {
				return err
			}
			if pending != 0 {
				return errors.New("delegation_reauthorization_pending_work")
			}
			// Existing responsibility cannot be replaced through reauthorization.
			var owner, qa string
			err = tx.QueryRowContext(ctx, `SELECT owner,qa FROM delegation_responsibilities WHERE owner=? OR qa=?`, a.LogicalID, a.LogicalID).Scan(&owner, &qa)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil && (owner != pair[0].LogicalID || qa != pair[1].LogicalID) {
				return errors.New("delegation_responsibility_immutable")
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO delegation_source_epochs VALUES(?,?,?,?,?,?,?,?,?,?,?)`, a.LogicalID, a.Epoch, a.Parent, a.Authority, a.Thread, a.FromRevision, a.ToRevision, a.GrantDigest, a.Peer, a.Request, a.Call)
			if err != nil {
				return err
			}
		}
		return validate()
	})
}
