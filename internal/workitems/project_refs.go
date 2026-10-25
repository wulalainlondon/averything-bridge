package workitems

import "context"

// ProjectRef deliberately omits private filesystem paths and project context.
type ProjectRef struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version uint64 `json:"version"`
}

func (s *Service) ProjectRefs(ctx context.Context) ([]ProjectRef, error) {
	rows, err := s.store.db.QueryContext(ctx, `SELECT id,name,version FROM work_projects WHERE authority_instance_id=? AND archived_at IS NULL ORDER BY name,id LIMIT 1000`, s.store.instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ProjectRef{}
	for rows.Next() {
		var p ProjectRef
		if err := rows.Scan(&p.ID, &p.Name, &p.Version); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
