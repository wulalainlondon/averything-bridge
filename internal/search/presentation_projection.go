package search

import (
	"database/sql"
	"strings"

	"everything-go/internal/backend"
)

// Repair only the rebuildable search projection written by release117. Never
// touch native transcripts, request receipts, IDs, timestamps, or source cursors.
func (idx *Index) refreshPresentationProjection() (int64, error) {
	const key = "presentation_context_projection_version"
	var current string
	err := idx.db.QueryRow("SELECT value FROM schema_meta WHERE key=?", key).Scan(&current)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	if current == "1" {
		return 0, nil
	}
	tx, err := idx.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	const auto = "[Averything presentation context: conversation-visual.v1]\n"
	const plain = "[Averything presentation context: text-v1]\n"
	rows, err := tx.Query(`SELECT rowid,session_id,content FROM messages WHERE role='user'
	 AND (content LIKE ? OR content LIKE ?) AND instr(content,?) BETWEEN 1 AND 12000 LIMIT 1000`, auto+"%", plain+"%", "[/Averything presentation context]\n\n")
	if err != nil {
		return 0, err
	}
	type row struct {
		id               int64
		session, content string
	}
	var pending []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.session, &r.content); err != nil {
			rows.Close()
			return 0, err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	var changed int64
	for _, r := range pending {
		original := backend.OriginalPresentationContent(r.content)
		if original == r.content {
			continue
		}
		if _, err := tx.Exec("UPDATE messages SET content=? WHERE rowid=? AND content=?", original, r.id, r.content); err != nil {
			return 0, err
		}
		changed++
		var name string
		if err := tx.QueryRow("SELECT COALESCE(display_name,'') FROM sessions WHERE session_id=?", r.session).Scan(&name); err != nil {
			return 0, err
		}
		if strings.HasPrefix(name, strings.TrimSpace(auto)) || strings.HasPrefix(name, strings.TrimSpace(plain)) {
			var first string
			if err := tx.QueryRow("SELECT content FROM messages WHERE session_id=? AND role='user' ORDER BY ts,rowid LIMIT 1", r.session).Scan(&first); err != nil {
				return 0, err
			}
			if _, err := tx.Exec("UPDATE sessions SET display_name=? WHERE session_id=? AND display_name=?", collapseWS(backend.OriginalPresentationContent(first), 80), r.session, name); err != nil {
				return 0, err
			}
		}
	}
	// Large pre-existing projections finish in bounded batches on later passes.
	if len(pending) < 1000 {
		if _, err := tx.Exec("INSERT INTO schema_meta(key,value) VALUES(?,'1') ON CONFLICT(key) DO UPDATE SET value='1'", key); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return changed, nil
}
