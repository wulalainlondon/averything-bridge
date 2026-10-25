package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"
)

// ResearchObservation is provider-observed tool metadata, not a model claim that
// a URL was fetched or that its content is true. Provider output remains data.
type ResearchObservation struct {
	TaskID     string   `json:"task_id"`
	RequestID  string   `json:"request_id"`
	ToolID     string   `json:"tool_id"`
	Tool       string   `json:"tool"`
	Query      string   `json:"query"`
	ObservedAt int64    `json:"observed_at"`
	State      string   `json:"state"`
	URLs       []string `json:"urls"`
	Excerpt    string   `json:"excerpt"`
	OutputHash string   `json:"output_hash,omitempty"`
	Truncated  bool     `json:"truncated"`
}

var observedURLPattern = regexp.MustCompile(`https?://[^\s<>"\x60]+`)

func clipUTF8(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit]
}
func webTool(name string) bool {
	switch strings.ToLower(strings.ReplaceAll(name, "_", "")) {
	case "websearch", "webfetch":
		return true
	}
	return false
}
func observationURLs(output string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, raw := range observedURLPattern.FindAllString(output, 100) {
		raw = strings.TrimRight(raw, ".,;:!?)\u3002\uff09]")
		u, err := url.Parse(raw)
		if err != nil || u.User != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || len(raw) > 2000 || seen[raw] {
			continue
		}
		seen[raw] = true
		out = append(out, raw)
		if len(out) >= 32 {
			break
		}
	}
	return out
}
func (s *Store) RecordResearchTool(ctx context.Context, session, request, toolID, name, query, output string, result bool) error {
	if toolID == "" || (!result && !webTool(name)) {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var roomID, taskID string
	if err = tx.QueryRowContext(ctx, "SELECT room_id,task_id FROM native_jobs WHERE session_id=? AND request_id=?", session, request).Scan(&roomID, &taskID); errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return err
	}
	grant, err := grant(ctx, tx, taskID)
	if err != nil {
		return err
	}
	a := actorForGrant(grant)
	r, _, err := s.access(ctx, tx, a, roomID, "read")
	if err != nil || r.ACLRevision != grant.ACLRevision {
		return errors.New("research_scope_revoked")
	}
	id := "cw_source_" + s.keyed("research-tool", taskID+"/"+toolID)[:24]
	e := Entity{ID: id, Kind: "research_source"}
	o := ResearchObservation{TaskID: taskID, RequestID: request, ToolID: toolID, Tool: name, Query: clipUTF8(query, 8000), ObservedAt: s.now().UnixMilli(), State: "provider_tool_started", URLs: []string{}}
	if previous, e2 := getEntity(ctx, tx, r.ID, id); e2 == nil {
		e = previous
		if json.Unmarshal(e.Body, &o) != nil {
			return errors.New("research_observation_invalid")
		}
	} else if result {
		return nil
	}
	if result {
		o.State = "provider_output_observed_not_independent_fact_verification"
		o.OutputHash = digest([]byte(output))
		o.Excerpt = clipUTF8(output, 16000)
		o.Truncated = len(o.Excerpt) != len(output)
		o.URLs = observationURLs(output)
	}
	e, err = s.put(ctx, tx, Actor{ID: "provider:" + session}, r, e, o)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func researchReferences(ctx context.Context, q sqlReader, room, task string) ([]Reference, error) {
	rows, err := q.QueryContext(ctx, "SELECT "+entityColumns+" FROM entities WHERE room_id=? AND kind='research_source' AND json_extract(body,'$.task_id')=? ORDER BY room_seq LIMIT 33", room, task)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Reference{}
	for rows.Next() {
		e, err := scanEntity(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, referenceTo(e))
	}
	if len(out) > 32 {
		return nil, errors.New("research_source_reference_limit")
	}
	return out, rows.Err()
}

func (s *Store) NativeRequestByProvider(ctx context.Context, sessionID, requestID string) (string, string, error) {
	var room, task string
	err := s.db.QueryRowContext(ctx, "SELECT room_id,task_id FROM native_jobs WHERE session_id=? AND request_id=?", sessionID, requestID).Scan(&room, &task)
	return room, task, err
}
