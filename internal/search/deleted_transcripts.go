package search

import "everything-go/internal/nativewatch"

// Prune only derived search rows. Raw transcripts, Bridge request receipts and
// drafts are not owned by this index and are never modified here.
func (idx *Index) pruneDeletedTranscripts() (int64, error) {
	roots := map[string]string{}
	for _, src := range idx.sources {
		switch s := src.(type) {
		case claudeSource:
			roots[s.name()] = s.root
		case codexSource:
			roots[s.name()] = s.root
		}
	}
	catalog := nativewatch.ScanTranscriptCatalog(roots)
	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()
	rows, err := idx.db.Query("SELECT session_id,source,source_path FROM sessions")
	if err != nil {
		return 0, err
	}
	type missing struct{ id, backend, path string }
	var candidates []missing
	for rows.Next() {
		var row missing
		if err = rows.Scan(&row.id, &row.backend, &row.path); err != nil {
			rows.Close()
			return 0, err
		}
		if catalog.MissingPath(row.backend, row.path) {
			candidates = append(candidates, row)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(candidates) == 0 {
		return 0, err
	}
	tx, err := idx.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var removed int64
	for _, row := range candidates {
		if !catalog.MissingPath(row.backend, row.path) {
			continue
		}
		if _, err = tx.Exec("DELETE FROM messages WHERE session_id=?", row.id); err != nil {
			return 0, err
		}
		if _, err = tx.Exec("DELETE FROM sessions WHERE session_id=? AND source_path=?", row.id, row.path); err != nil {
			return 0, err
		}
		if _, err = tx.Exec("DELETE FROM ingest_state WHERE source_path=?", row.path); err != nil {
			return 0, err
		}
		removed++
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return removed, nil
}
