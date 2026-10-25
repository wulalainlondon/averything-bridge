package delegation

import (
	"context"
	"strings"
)

// SessionSource is a read-only, content-free relationship projection. It reads
// no instructions, results, artifacts, caller grants or transcript history.
type SessionSource struct {
	ID, ParentSessionID, ChildSessionID, ChildRequestID, OriginRequestID string
	State, TerminalStatus, DeliveryState                                 string
	FinalObserved                                                        bool
	UpdatedAt                                                            int64
}

func (s *Store) SessionSources(ctx context.Context, childIDs []string) ([]SessionSource, error) {
	out := []SessionSource{}
	// Bound SQLite parameters; no limit/truncation of historical relationships.
	for start := 0; start < len(childIDs); start += 200 {
		end := start + 200
		if end > len(childIDs) {
			end = len(childIDs)
		}
		args := make([]any, 0, end-start)
		marks := make([]string, 0, end-start)
		for _, id := range childIDs[start:end] {
			args = append(args, id)
			marks = append(marks, "?")
		}
		rows, err := s.db.QueryContext(ctx, `SELECT id,parent_session_id,child_session_id,child_request_id,origin_request_id,state,terminal_status,delivery_state,(state='terminal' AND terminal_status='completed' AND length(trim(result))>0),updated_at FROM session_delegations WHERE child_session_id IN (`+strings.Join(marks, ",")+`) ORDER BY updated_at DESC,id`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var r SessionSource
			if err = rows.Scan(&r.ID, &r.ParentSessionID, &r.ChildSessionID, &r.ChildRequestID, &r.OriginRequestID, &r.State, &r.TerminalStatus, &r.DeliveryState, &r.FinalObserved, &r.UpdatedAt); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, r)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
