package workitems

import (
	"context"
	"database/sql"
	"errors"
)

// CallbackSource exposes authoritative review readiness without accepting the
// work or interpreting its desired outcome as a completed deliverable.
type CallbackSource struct {
	Request, ItemID, Title, Lifecycle, RunState string
	Revision                                    uint64
	Created                                     int64
	Latest                                      bool
}

func (s *Store) CallbackSource(ctx context.Context, session, request string) (CallbackSource, bool, error) {
	var x CallbackSource
	var latest int
	e := s.db.QueryRowContext(ctx, `SELECT r.request_id,w.id,w.title,w.lifecycle,r.status,w.activity_revision,r.started_at,
 NOT EXISTS(SELECT 1 FROM work_item_runs n WHERE n.work_item_id=r.work_item_id AND (n.started_at>r.started_at OR (n.started_at=r.started_at AND n.rowid>r.rowid)))
 FROM work_item_runs r JOIN work_item_session_links l ON l.id=r.session_link_id JOIN work_items w ON w.id=r.work_item_id
 WHERE l.session_id=? AND r.request_id=? AND l.unlinked_at IS NULL AND w.archived_at IS NULL`, session, request).Scan(&x.Request, &x.ItemID, &x.Title, &x.Lifecycle, &x.RunState, &x.Revision, &x.Created, &latest)
	if errors.Is(e, sql.ErrNoRows) {
		return x, false, nil
	}
	x.Latest = latest == 1
	return x, e == nil, e
}
func (s *Service) CallbackSource(ctx context.Context, session, request string) (CallbackSource, bool, error) {
	return s.store.CallbackSource(ctx, session, request)
}
func (s *Service) CallbackSources(ctx context.Context, session string) ([]CallbackSource, error) {
	rows, e := s.store.db.QueryContext(ctx, `SELECT r.request_id FROM work_item_runs r JOIN work_item_session_links l ON l.id=r.session_link_id WHERE l.session_id=? AND l.unlinked_at IS NULL ORDER BY r.started_at DESC LIMIT 20`, session)
	if e != nil {
		return nil, e
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return nil, e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	out := []CallbackSource{}
	for _, id := range ids {
		x, found, e := s.CallbackSource(ctx, session, id)
		if e != nil {
			return nil, e
		}
		if found {
			out = append(out, x)
		}
	}
	return out, nil
}
