package messagequeue

// ActiveWorkspaceRequests is a read-only startup projection of the existing
// queue. It is not another admission ledger or a permission grant.
func (s *Store) ActiveWorkspaceRequests() ([][2]string, error) {
	rows, err := s.db.Query(`SELECT session_id,request_id FROM queue_commands WHERE state IN ('queued','running','uncertain','steering','steered') AND CASE WHEN json_valid(CAST(payload AS TEXT)) THEN json_extract(CAST(payload AS TEXT),'$.workspace_task.task_id') ELSE NULL END IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := [][2]string{}
	for rows.Next() {
		var pair [2]string
		if err = rows.Scan(&pair[0], &pair[1]); err != nil {
			return nil, err
		}
		out = append(out, pair)
	}
	return out, rows.Err()
}
