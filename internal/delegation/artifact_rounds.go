package delegation

import (
	"context"
	"errors"
)

// Membership metadata only, in the ORIGINAL canonical round store. No body,
// external file owner, latest-round substitution or separate ledger.
func (s *Store) ArtifactRounds(ctx context.Context, logical, parent, child, request string) ([]Round, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT r.id,r.logical_id,r.version,r.previous_id,r.authority,r.grant_digest,r.mode,r.execution_request FROM delegation_rounds r JOIN session_delegations s ON s.id=r.id WHERE r.logical_id=? AND s.parent_session_id=? AND s.child_session_id=? AND r.mode='queue' AND (?='' OR s.child_request_id=?) ORDER BY r.version LIMIT 1025`, logical, parent, child, request, request)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Round{}
	for rows.Next() {
		r, e := scanRound(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	if e := rows.Err(); e != nil {
		return nil, e
	}
	if len(out) > 1024 {
		return nil, errors.New("artifact_owner_selector_required")
	}
	return out, nil
}
