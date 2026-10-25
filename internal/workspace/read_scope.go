package workspace

import (
	"context"
	"encoding/json"
)

// MembershipFingerprint binds original TaskAPI frozen pages to this human's
// current room membership without disclosing room names, content or counts.
func (s *Store) MembershipFingerprint(ctx context.Context, a Actor) (string, error) {
	if err := s.actorOK(ctx, s.db, a); err != nil {
		return "", err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.id,r.acl_revision,m.revision FROM rooms r JOIN members m ON m.room_id=r.id WHERE m.principal_id=? AND m.active=1 ORDER BY r.id`, a.ID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var values []any
	for rows.Next() {
		var id string
		var acl, member uint64
		if err := rows.Scan(&id, &acl, &member); err != nil {
			return "", err
		}
		values = append(values, []any{id, acl, member})
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	data, _ := json.Marshal(values)
	return digest(data), nil
}
