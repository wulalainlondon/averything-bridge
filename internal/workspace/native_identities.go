package workspace

import "context"

// PrivateNativeThreadIDs is a host-only catalog, never a wire read action.
func (s *Store) PrivateNativeThreadIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT n.native_thread_id FROM room_native_identity n JOIN room_sessions r ON r.session_id=n.session_id WHERE r.exclusive=1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
