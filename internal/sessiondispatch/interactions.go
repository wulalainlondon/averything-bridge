package sessiondispatch

import (
	"context"
	"encoding/json"
	"errors"
)

func (s *Store) ByExecution(ctx context.Context, session, request string) (Record, bool, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT payload FROM session_dispatches WHERE json_extract(payload,'$.target_session_id')=? AND (json_extract(payload,'$.request_id')=? OR json_extract(payload,'$.execution_request_id')=?)`, session, request, request)
	if e != nil {
		return Record{}, false, e
	}
	defer rows.Close()
	var r Record
	n := 0
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return r, false, e
		}
		if e = json.Unmarshal(b, &r); e != nil {
			return r, false, e
		}
		n++
	}
	if n > 1 {
		return Record{}, false, errors.New("ambiguous_interaction_execution")
	}
	return r, n == 1, rows.Err()
}
