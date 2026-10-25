package messagequeue

import "encoding/json"

type CallbackAdmission struct{ Session, Request, Owner, Binding string }

func (s *Store) CallbackAdmissions() ([]CallbackAdmission, error) {
	rows, e := s.db.Query("SELECT session_id,request_id,metadata FROM task_admissions WHERE json_extract(metadata,'$.callback_on_complete')=1 ORDER BY rowid DESC LIMIT 500")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []CallbackAdmission{}
	for rows.Next() {
		var v CallbackAdmission
		var data []byte
		if e = rows.Scan(&v.Session, &v.Request, &data); e != nil {
			return nil, e
		}
		var m struct {
			Owner   string `json:"owner_device"`
			Binding string `json:"callback_binding"`
		}
		if e = json.Unmarshal(data, &m); e != nil {
			return nil, e
		}
		v.Owner = m.Owner
		v.Binding = m.Binding
		out = append(out, v)
	}
	return out, rows.Err()
}
