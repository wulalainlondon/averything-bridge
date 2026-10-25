package history

import (
	"bytes"
	"encoding/json"
)

type nativeIndexRow struct {
	Type    string `json:"type"`
	Payload struct {
		Type   string          `json:"type"`
		Role   string          `json:"role"`
		Phase  string          `json:"phase"`
		Name   string          `json:"name"`
		TurnID string          `json:"turn_id"`
		Error  json.RawMessage `json:"error"`
	} `json:"payload"`
}

// For an oversized line inspect only actual JSON header fields, never strings
// inside the body. Missing trailing phase/name stays unknown and conservative.
func nativeWindowHeader(prefix []byte) (nativeIndexRow, bool) {
	var row nativeIndexRow
	if json.Unmarshal(prefix, &row) == nil {
		return row, true
	}
	d := json.NewDecoder(bytes.NewReader(prefix))
	if token, e := d.Token(); e != nil || token != json.Delim('{') {
		return row, false
	}
	for d.More() {
		key, e := d.Token()
		if e != nil {
			return row, false
		}
		if key == "type" {
			if d.Decode(&row.Type) != nil {
				return row, false
			}
			continue
		}
		if key != "payload" {
			var value json.RawMessage
			if d.Decode(&value) != nil {
				return row, false
			}
			continue
		}
		if token, e := d.Token(); e != nil || token != json.Delim('{') {
			return row, false
		}
		for d.More() {
			key, e := d.Token()
			if e != nil {
				return row, false
			}
			var dest *string
			switch key {
			case "type":
				dest = &row.Payload.Type
			case "role":
				dest = &row.Payload.Role
			case "phase":
				dest = &row.Payload.Phase
			case "name":
				dest = &row.Payload.Name
			case "turn_id":
				dest = &row.Payload.TurnID
			case "content", "arguments", "input", "output":
				return row, false
			}
			if dest != nil {
				if d.Decode(dest) != nil {
					return row, false
				}
			} else {
				var value json.RawMessage
				if d.Decode(&value) != nil {
					return row, false
				}
			}
		}
		return row, false
	}
	return row, false
}
