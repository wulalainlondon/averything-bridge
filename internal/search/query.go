package search

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

// sprintfWhere splices the dynamic WHERE clause into a query template's single
// %s. The clause is built only from fixed fragments + bind placeholders (never
// user text), so this is not an injection vector.
func sprintfWhere(tmpl, where string) string { return fmt.Sprintf(tmpl, where) }

// --- Wire result types (match bridge/search query dataclasses) --------------

type Hit struct {
	SessionID          string  `json:"session_id"`
	SessionDisplayName *string `json:"session_display_name"`
	Cwd                *string `json:"cwd"`
	ProjectDir         string  `json:"project_dir"`
	Backend            *string `json:"backend"`
	SessionLastTS      *string `json:"session_last_ts"`
	SessionMsgCount    int     `json:"session_msg_count"`
	MsgUUID            string  `json:"msg_uuid"`
	Role               string  `json:"role"`
	MsgTS              string  `json:"msg_ts"`
	Snippet            string  `json:"snippet"`
	Rank               float64 `json:"rank"`
}

type SearchResponse struct {
	Type          string   `json:"type"`
	Query         string   `json:"query"`
	Hits          []Hit    `json:"hits"`
	ReturnedCount int      `json:"returned_count"`
	Total         int      `json:"total"`
	Warnings      []string `json:"warnings"`
	ElapsedMs     float64  `json:"elapsed_ms"`
}

type Filters struct {
	ProjectDir       string
	Since            string
	Role             string
	ExcludeSubagents bool
	Source           string
	MaxPerSession    int
}

const maxQueryLen = 200

var (
	ftsToken     = regexp.MustCompile(`"[^"]*"|\S+`)
	ftsSpecial   = regexp.MustCompile(`['";]`)
	shortASCII   = regexp.MustCompile(`\b([A-Za-z]{1,2})\b`)
	ftsKeywords  = map[string]bool{"AND": true, "OR": true, "NOT": true}
	cjkRun       = regexp.MustCompile(`[\x{3400}-\x{4dbf}\x{4e00}-\x{9fff}\x{f900}-\x{faff}\x{3040}-\x{30ff}\x{ac00}-\x{d7af}]+`)
	quotedPhrase = regexp.MustCompile(`"[^"]*"`)
)

func buildFTSMatch(userQuery string) string {
	tokens := ftsToken.FindAllString(strings.TrimSpace(userQuery), -1)
	var parts []string
	for _, tok := range tokens {
		if ftsKeywords[tok] {
			if len(parts) > 0 {
				parts = append(parts, tok)
			}
		} else if len(tok) >= 2 && strings.HasPrefix(tok, `"`) && strings.HasSuffix(tok, `"`) {
			parts = append(parts, tok)
		} else {
			clean := ftsSpecial.ReplaceAllString(tok, "")
			if clean != "" {
				parts = append(parts, `"`+clean+`"`)
			}
		}
	}
	for len(parts) > 0 && ftsKeywords[parts[len(parts)-1]] {
		parts = parts[:len(parts)-1]
	}
	return strings.Join(parts, " ")
}

func shortCJKTokens(query string) []string {
	bare := quotedPhrase.ReplaceAllString(query, " ")
	var out []string
	for _, t := range cjkRun.FindAllString(bare, -1) {
		if n := len([]rune(t)); n >= 1 && n <= 2 {
			out = append(out, t)
		}
	}
	return out
}

func collectWarnings(query string) []string {
	bare := quotedPhrase.ReplaceAllString(query, " ")
	var short []string
	for _, m := range shortASCII.FindAllStringSubmatch(bare, -1) {
		if !ftsKeywords[strings.ToUpper(m[1])] {
			short = append(short, "'"+m[1]+"'")
		}
	}
	if len(short) > 0 {
		return []string{"Short ASCII token(s) " + strings.Join(short, ", ") +
			" are < 3 chars and cannot be matched by the trigram index — they will produce no results."}
	}
	return nil
}

// Search executes a full-text search, falling back to LIKE for short CJK terms.
func (idx *Index) Search(query string, f Filters, limit, offset int) SearchResponse {
	t0 := time.Now()
	if f.MaxPerSession <= 0 {
		f.MaxPerSession = 3
	}
	resp := SearchResponse{Type: "search_result", Query: query, Hits: []Hit{}, Warnings: []string{}}

	stripped := strings.TrimSpace(query)
	if stripped == "" {
		resp.Warnings = []string{"Query is empty."}
		return resp
	}
	if len([]rune(stripped)) > maxQueryLen {
		resp.Warnings = []string{"Query exceeds maximum length of 200 characters."}
		return resp
	}

	var hits []Hit
	if short := shortCJKTokens(stripped); len(short) > 0 {
		quoted := make([]string, len(short))
		for i, s := range short {
			quoted[i] = "'" + s + "'"
		}
		resp.Warnings = append(resp.Warnings,
			"Short CJK token(s) "+strings.Join(quoted, ", ")+" use LIKE fallback (slower, no ranking).")
		hits = idx.runLikeFallback(short, f, limit, offset)
	} else {
		resp.Warnings = append(resp.Warnings, collectWarnings(stripped)...)
		hits = idx.runFTS(buildFTSMatch(stripped), f, limit, offset)
		hasFilter := f.ProjectDir != "" || f.Role != "" || f.ExcludeSubagents || f.Source != ""
		if offset > 0 && hasFilter {
			resp.Warnings = append(resp.Warnings, "pagination may skip results due to FTS5 + filter interaction")
		}
	}

	resp.Hits = hits
	resp.ReturnedCount = len(hits)
	resp.Total = len(hits)
	resp.ElapsedMs = float64(time.Since(t0).Microseconds()) / 1000.0
	if resp.Warnings == nil {
		resp.Warnings = []string{}
	}
	return resp
}

const ftsSQL = `
WITH _inner AS (
	SELECT s.session_id, s.display_name, s.cwd, s.project_dir, s.backend,
		s.last_ts, s.msg_count, m.msg_uuid, m.role, m.ts,
		snippet(messages_fts, 0, '<<', '>>', '…', 24) AS snippet,
		bm25(messages_fts) AS rank, m.is_subagent, s.source
	FROM messages_fts
	JOIN messages m ON m.rowid = messages_fts.rowid
	JOIN sessions s ON s.session_id = m.session_id
	WHERE %s
	ORDER BY rank
	LIMIT ?
),
_ranked AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY rank) AS per_sess_rank
	FROM _inner
)
SELECT session_id, display_name, cwd, project_dir, backend,
	last_ts, msg_count, msg_uuid, role, ts, snippet, rank, is_subagent, source
FROM _ranked WHERE per_sess_rank <= ? ORDER BY rank`

func (idx *Index) runFTS(ftsQuery string, f Filters, limit, offset int) []Hit {
	where := "messages_fts MATCH ?"
	args := []any{ftsQuery}
	if f.Since != "" {
		where += " AND m.ts >= ?"
		args = append(args, f.Since)
	}
	internalLimit := (offset + limit) * f.MaxPerSession * 10
	if internalLimit > 2000 {
		internalLimit = 2000
	}
	args = append(args, internalLimit, f.MaxPerSession)

	rows, err := idx.db.Query(sprintfWhere(ftsSQL, where), args...)
	if err != nil {
		return []Hit{}
	}
	defer rows.Close()
	return scanHits(rows, f, limit, offset)
}

const likeSQL = `
SELECT s.session_id, s.display_name, s.cwd, s.project_dir, s.backend,
	s.last_ts, s.msg_count, m.msg_uuid, m.role, m.ts, m.content, 0 AS rank,
	m.is_subagent, s.source
FROM messages m JOIN sessions s ON s.session_id = m.session_id
WHERE %s
ORDER BY m.ts DESC LIMIT ?`

func (idx *Index) runLikeFallback(tokens []string, f Filters, limit, offset int) []Hit {
	var likeClauses []string
	var args []any
	for _, t := range tokens {
		likeClauses = append(likeClauses, "m.content LIKE ?")
		args = append(args, "%"+t+"%")
	}
	where := strings.Join(likeClauses, " AND ")
	if f.Since != "" {
		where += " AND m.ts >= ?"
		args = append(args, f.Since)
	} else {
		since := time.Now().Add(-90 * 24 * time.Hour).Format(time.RFC3339)
		where += " AND m.ts >= ?"
		args = append(args, since)
	}
	internalLimit := (offset + limit) * f.MaxPerSession * 10
	if internalLimit > 500 {
		internalLimit = 500
	}
	args = append(args, internalLimit)

	rows, err := idx.db.Query(sprintfWhere(likeSQL, where), args...)
	if err != nil {
		return []Hit{}
	}
	defer rows.Close()
	hits := scanHits(rows, f, limit, offset)
	// Build snippets around the first token (FTS path already has snippet()).
	for i := range hits {
		hits[i].Snippet = likeSnippet(hits[i].Snippet, tokens)
	}
	return hits
}

// scanHits reads the 14-column row shape, applies post-MATCH filters and the
// per-session cap in Go, then slices [offset:offset+limit]. For the LIKE path
// column 11 is raw content (used as snippet seed); for FTS it is the snippet.
func scanHits(rows interface {
	Next() bool
	Scan(...any) error
}, f Filters, limit, offset int) []Hit {
	type raw struct {
		Hit
		isSubagent int
		source     string
	}
	var all []raw
	for rows.Next() {
		var r raw
		var dn, cwd, backend, lastTS *string
		if err := rows.Scan(&r.SessionID, &dn, &cwd, &r.ProjectDir, &backend,
			&lastTS, &r.SessionMsgCount, &r.MsgUUID, &r.Role, &r.MsgTS,
			&r.Snippet, &r.Rank, &r.isSubagent, &r.source); err != nil {
			continue
		}
		r.SessionDisplayName, r.Cwd, r.Backend, r.SessionLastTS = dn, cwd, backend, lastTS
		all = append(all, r)
	}

	perSession := map[string]int{}
	var capped []raw
	for _, r := range all {
		if f.ProjectDir != "" && r.ProjectDir != f.ProjectDir {
			continue
		}
		if f.Role != "" && r.Role != f.Role {
			continue
		}
		if f.ExcludeSubagents && r.isSubagent != 0 {
			continue
		}
		if f.Source != "" && r.source != f.Source {
			continue
		}
		// LIKE path enforces the per-session cap here (FTS already capped in SQL).
		if perSession[r.SessionID] >= f.MaxPerSession {
			continue
		}
		perSession[r.SessionID]++
		capped = append(capped, r)
	}

	lo := offset
	if lo > len(capped) {
		lo = len(capped)
	}
	hi := lo + limit
	if hi > len(capped) {
		hi = len(capped)
	}
	out := make([]Hit, 0, hi-lo)
	for _, r := range capped[lo:hi] {
		out = append(out, r.Hit)
	}
	return out
}

func likeSnippet(content string, tokens []string) string {
	raw := content
	if len(tokens) > 0 {
		if pos := strings.Index(content, tokens[0]); pos >= 0 {
			start := pos - 24
			if start < 0 {
				start = 0
			}
			end := start + 80
			if end > len(content) {
				end = len(content)
			}
			raw = content[start:end]
		} else if len(content) > 80 {
			raw = content[:80]
		}
	}
	for _, t := range tokens {
		raw = strings.ReplaceAll(raw, t, "<<"+t+">>")
	}
	return raw
}

// --- Health -----------------------------------------------------------------

type HealthResponse struct {
	Type              string         `json:"type"`
	IndexedSessions   int            `json:"indexed_sessions"`
	IndexedMessages   int            `json:"indexed_messages"`
	DBSizeMB          float64        `json:"db_size_mb"`
	IngestLagSeconds  *float64       `json:"ingest_lag_seconds"`
	LastFullRebuildAt *string        `json:"last_full_rebuild_at"`
	ErrorsLast24h     int            `json:"errors_last_24h"`
	Ready             bool           `json:"ready"`
	IngestProgress    map[string]any `json:"ingest_progress"`
	StaleFiles        int            `json:"stale_files"`
	PendingBytes      int64          `json:"pending_bytes"`
	ProjectionLagSecs float64        `json:"projection_lag_seconds"`
}

func (idx *Index) Health() HealthResponse {
	h := HealthResponse{Type: "search_health", IngestProgress: map[string]any{}}
	idx.db.QueryRow("SELECT COUNT(*) FROM sessions").Scan(&h.IndexedSessions)
	idx.db.QueryRow("SELECT COUNT(*) FROM messages").Scan(&h.IndexedMessages)

	var lastIngest *float64
	idx.db.QueryRow("SELECT MAX(last_ingest_at) FROM ingest_state").Scan(&lastIngest)
	if lastIngest != nil {
		lag := float64(time.Now().UnixNano())/1e9 - *lastIngest
		h.IngestLagSeconds = &lag
	}
	cutoff := float64(time.Now().Add(-24*time.Hour).UnixNano()) / 1e9
	idx.db.QueryRow("SELECT COALESCE(SUM(errors),0) FROM ingest_state WHERE last_ingest_at >= ?", cutoff).Scan(&h.ErrorsLast24h)

	if info, err := os.Stat(idx.path); err == nil {
		h.DBSizeMB = float64(info.Size()) / (1024 * 1024)
	}
	// Ingestion runs out-of-process, so derive readiness from the DB (any
	// ingested file or message) as well as the in-memory flag the bridge sets
	// after the first child indexer run.
	h.Ready = idx.isReady() || h.IndexedMessages > 0 || lastIngest != nil
	h.StaleFiles, h.PendingBytes, h.ProjectionLagSecs = idx.projectionFreshness(time.Now())
	p := idx.snapshotProgress()
	status := p.status
	if status == "" {
		if h.Ready {
			status = "ready"
		} else {
			status = "ingesting"
		}
	}
	h.IngestProgress["status"] = status
	h.IngestProgress["files_total"] = p.filesTotal
	h.IngestProgress["files_done"] = p.filesDone
	h.IngestProgress["last_added"] = p.lastAdded
	if p.lastMetrics.Mode != "" {
		h.IngestProgress["mode"] = p.lastMetrics.Mode
		h.IngestProgress["files_seen"] = p.lastMetrics.FilesSeen
		h.IngestProgress["files_changed"] = p.lastMetrics.FilesChanged
		h.IngestProgress["files_queued"] = p.lastMetrics.FilesQueued
		h.IngestProgress["message_conflicts"] = p.lastMetrics.MessageConflicts
		h.IngestProgress["bytes_read"] = p.lastMetrics.BytesRead
		h.IngestProgress["duration_ms"] = p.lastMetrics.DurationMS
		h.IngestProgress["maintenance_rows"] = p.lastMetrics.MaintenanceRows
		h.IngestProgress["wal_bytes_before"] = p.lastMetrics.WALBytesBefore
		h.IngestProgress["wal_bytes_after"] = p.lastMetrics.WALBytesAfter
		h.IngestProgress["checkpoint_busy"] = p.lastMetrics.CheckpointBusy
		h.IngestProgress["checkpoint_log_pages"] = p.lastMetrics.CheckpointLog
		h.IngestProgress["checkpointed_pages"] = p.lastMetrics.CheckpointDone
	}
	if p.currentFile != "" {
		h.IngestProgress["current_file"] = p.currentFile
	}
	if p.currentSource != "" {
		h.IngestProgress["current_source"] = p.currentSource
	}
	if p.lastError != "" {
		h.IngestProgress["last_error"] = p.lastError
	}
	if !p.cycleStarted.IsZero() {
		h.IngestProgress["cycle_started_at"] = p.cycleStarted.Format(time.RFC3339)
	}
	if !p.cycleDone.IsZero() {
		h.IngestProgress["cycle_done_at"] = p.cycleDone.Format(time.RFC3339)
	}
	return h
}

func (idx *Index) projectionFreshness(now time.Time) (staleFiles int, pendingBytes int64, maxLagSeconds float64) {
	rows, err := idx.db.Query("SELECT source_path,last_mtime,last_offset FROM ingest_state")
	if err != nil {
		return 0, 0, 0
	}
	defer rows.Close()
	for rows.Next() {
		var path string
		var indexedMtime float64
		var offset int64
		if rows.Scan(&path, &indexedMtime, &offset) != nil {
			continue
		}
		info, statErr := os.Stat(path)
		if statErr != nil {
			continue
		}
		currentMtime := float64(info.ModTime().UnixNano()) / 1e9
		if info.Size() <= offset && currentMtime <= indexedMtime+0.000001 {
			continue
		}
		staleFiles++
		if info.Size() > offset {
			pendingBytes += info.Size() - offset
		}
		lag := now.Sub(info.ModTime()).Seconds()
		if lag > maxLagSeconds {
			maxLagSeconds = lag
		}
	}
	return staleFiles, pendingBytes, maxLagSeconds
}

// --- Session list -----------------------------------------------------------

type SessionItem struct {
	SessionID   string  `json:"session_id"`
	DisplayName *string `json:"display_name"`
	Cwd         *string `json:"cwd"`
	ProjectDir  string  `json:"project_dir"`
	Backend     *string `json:"backend"`
	FirstTS     *string `json:"first_ts"`
	LastTS      *string `json:"last_ts"`
	MsgCount    int     `json:"msg_count"`
	IsPinned    bool    `json:"is_pinned"`
}

type SessionListResponse struct {
	Type          string        `json:"type"`
	Items         []SessionItem `json:"items"`
	NextCursor    *string       `json:"next_cursor"`
	TotalFiltered *int          `json:"total_filtered"`
}

func (idx *Index) ListSessions(cursor string, limit int, projectDir string, includeHidden bool, includeSubagents bool) SessionListResponse {
	var where []string
	var args []any
	if !includeHidden {
		where = append(where, "is_hidden = 0")
	}
	if !includeSubagents {
		where = append(where, "session_id NOT LIKE ?")
		args = append(args, "%:subagent:%")
	}
	if projectDir != "" {
		where = append(where, "project_dir = ?")
		args = append(args, projectDir)
	}
	if cursor != "" {
		where = append(where, "last_ts < ?")
		args = append(args, cursor)
	}
	whereSQL := ""
	if len(where) > 0 {
		whereSQL = "WHERE " + strings.Join(where, " AND ")
	}
	args = append(args, limit+1)
	sql := "SELECT session_id, display_name, cwd, project_dir, backend, first_ts, last_ts, msg_count, is_pinned FROM sessions " +
		whereSQL + " ORDER BY is_pinned DESC, last_ts DESC LIMIT ?"

	resp := SessionListResponse{Type: "session_list", Items: []SessionItem{}}
	rows, err := idx.db.Query(sql, args...)
	if err != nil {
		return resp
	}
	defer rows.Close()
	var items []SessionItem
	for rows.Next() {
		var it SessionItem
		var pinned int
		if err := rows.Scan(&it.SessionID, &it.DisplayName, &it.Cwd, &it.ProjectDir,
			&it.Backend, &it.FirstTS, &it.LastTS, &it.MsgCount, &pinned); err != nil {
			continue
		}
		it.IsPinned = pinned != 0
		items = append(items, it)
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	resp.Items = items
	if hasMore && len(items) > 0 {
		resp.NextCursor = items[len(items)-1].LastTS
	}
	return resp
}

// --- Search context ---------------------------------------------------------

type ContextMessage struct {
	MsgUUID  string `json:"msg_uuid"`
	Role     string `json:"role"`
	TS       string `json:"ts"`
	Content  string `json:"content"`
	IsTarget bool   `json:"is_target"`
}

type ContextResponse struct {
	Type               string           `json:"type"`
	SessionID          string           `json:"session_id"`
	SessionDisplayName *string          `json:"session_display_name"`
	Cwd                *string          `json:"cwd"`
	Backend            *string          `json:"backend"`
	TargetMsgUUID      string           `json:"target_msg_uuid"`
	Messages           []ContextMessage `json:"messages"`
	ElapsedMs          float64          `json:"elapsed_ms"`
}

func (idx *Index) GetContext(sessionID, msgUUID string, around int) ContextResponse {
	t0 := time.Now()
	if around < 1 {
		around = 1
	}
	if around > 30 {
		around = 30
	}
	resp := ContextResponse{Type: "search_context", SessionID: sessionID,
		TargetMsgUUID: msgUUID, Messages: []ContextMessage{}}

	var dn, cwd, backend *string
	idx.db.QueryRow("SELECT display_name, cwd, backend FROM sessions WHERE session_id = ? LIMIT 1",
		sessionID).Scan(&dn, &cwd, &backend)
	resp.SessionDisplayName, resp.Cwd, resp.Backend = dn, cwd, backend

	var targetRowid int64
	if err := idx.db.QueryRow("SELECT rowid FROM messages WHERE session_id = ? AND msg_uuid = ? LIMIT 1",
		sessionID, msgUUID).Scan(&targetRowid); err != nil {
		resp.ElapsedMs = float64(time.Since(t0).Microseconds()) / 1000.0
		return resp
	}
	rows, err := idx.db.Query(`SELECT msg_uuid, role, ts, content FROM messages
		WHERE session_id = ? AND rowid BETWEEN ? AND ? ORDER BY rowid`,
		sessionID, targetRowid-int64(around), targetRowid+int64(around))
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var m ContextMessage
			if rows.Scan(&m.MsgUUID, &m.Role, &m.TS, &m.Content) == nil {
				m.IsTarget = m.MsgUUID == msgUUID
				resp.Messages = append(resp.Messages, m)
			}
		}
	}
	resp.ElapsedMs = float64(time.Since(t0).Microseconds()) / 1000.0
	return resp
}

// RecentMsg is a lightweight role+text pair returned by RecentMessagesBySession.
type RecentMsg struct {
	Role string
	Text string
}

// SessionUID is a (backend, uid) pair used by RecentMessagesByUID.
// For claude the uid is the Claude UUID (search DB key = "claude:{uid}").
// For codex the uid is the 36-char suffix of the rollout filename (the search
// DB key is "codex:rollout-{timestamp}-{uid}", so we match by suffix).
type SessionUID struct {
	// HubID is the hub session ID used as the return-map key.
	HubID   string
	Backend string
	UID     string
}

// SessionPreview bundles the recent-message preview with the real last-activity
// timestamp (unix seconds) derived from the search index. LastTS is the source
// of truth for "last active" — the session store's last_used can be stale or
// flattened, so callers prefer this when it is > 0.
type SessionPreview struct {
	Recent          []RecentMsg
	LastTS          int64 // unix seconds of the newest indexed message; 0 if unknown
	LastAssistantTS int64 // newest canonical assistant message, used for restart reconciliation
	LastUserTS      int64 // newest canonical user message, used while a turn is active
}

// parseISOUnix converts an ISO-8601 timestamp (e.g. "2026-06-07T09:18:58.954Z")
// to unix seconds, returning 0 on failure.
func parseISOUnix(s string) int64 {
	if s == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		// Some rows omit the timezone; try a looser layout.
		t, err = time.Parse("2006-01-02T15:04:05.999999999", strings.TrimSuffix(s, "Z"))
		if err != nil {
			return 0
		}
	}
	return t.Unix()
}

// RecentMessagesByUID returns, per HubID, the last n non-subagent messages plus
// the real last-activity timestamp. The map is keyed by HubID (not the search
// DB session_id).
func (idx *Index) RecentMessagesByUID(uids []SessionUID, n int) map[string]*SessionPreview {
	if len(uids) == 0 {
		return nil
	}

	// Split into exact-match (claude) and suffix-match (codex) groups.
	type entry struct {
		hubID string
		uid   string // codex: last 36 chars
	}
	var exactArgs []any
	var exactKeys []string
	hubByExact := map[string]string{}
	var codexEntries []entry

	for _, u := range uids {
		switch u.Backend {
		case "claude":
			key := "claude:" + u.UID
			exactArgs = append(exactArgs, key)
			exactKeys = append(exactKeys, key)
			hubByExact[key] = u.HubID
		case "codex":
			if len(u.UID) == 36 {
				codexEntries = append(codexEntries, entry{hubID: u.HubID, uid: u.UID})
			}
		}
	}

	out := make(map[string]*SessionPreview, len(uids))
	get := func(hubID string) *SessionPreview {
		if p := out[hubID]; p != nil {
			return p
		}
		p := &SessionPreview{}
		out[hubID] = p
		return p
	}
	addRow := func(hubID, role, content, ts string) {
		p := get(hubID)
		u := parseISOUnix(ts)
		if u > p.LastTS {
			p.LastTS = u
		}
		if role == "assistant" && u > p.LastAssistantTS {
			p.LastAssistantTS = u
		}
		if role == "user" && u > p.LastUserTS {
			p.LastUserTS = u
		}
		text := strings.TrimSpace(content)
		if text == "" {
			return
		}
		text = truncateRunes(text, 200)
		p.Recent = append(p.Recent, RecentMsg{Role: role, Text: text})
	}

	// --- claude: exact IN query -----------------------------------------------
	if len(exactKeys) > 0 {
		ph := strings.Repeat("?,", len(exactKeys))
		ph = ph[:len(ph)-1]
		q := fmt.Sprintf(`
			SELECT session_id, role, content, ts FROM (
				SELECT session_id, role, content, ts,
					ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY ts DESC) AS rn
				FROM messages
				WHERE session_id IN (%s) AND is_subagent = 0
			) WHERE rn <= ?
			ORDER BY session_id, rn DESC`, ph)
		args := append(exactArgs, n)
		rows, err := idx.db.Query(q, args...)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var sid, role, content, ts string
				if rows.Scan(&sid, &role, &content, &ts) != nil {
					continue
				}
				addRow(hubByExact[sid], role, content, ts)
			}
		}
	}

	// --- codex: two-step lookup to avoid full messages-table scan --------------
	// Old approach: substr(session_id,-36)=? on the messages table (no index,
	// O(messages) per session). New: resolve UIDs via the small sessions table
	// first, then do a single indexed IN query on messages.
	if len(codexEntries) > 0 {
		uidToHub := make(map[string]string, len(codexEntries))
		uidArgs := make([]any, len(codexEntries))
		for i, ce := range codexEntries {
			uidToHub[ce.uid] = ce.hubID
			uidArgs[i] = ce.uid
		}
		ph := strings.Repeat("?,", len(codexEntries))
		ph = ph[:len(ph)-1]
		sidRows, sidErr := idx.db.Query(fmt.Sprintf(
			"SELECT session_id, substr(session_id,-36) FROM sessions WHERE source='codex' AND substr(session_id,-36) IN (%s)", ph),
			uidArgs...)
		if sidErr == nil {
			var codexSidArgs []any
			hubByCodexSid := map[string]string{}
			func() {
				defer sidRows.Close()
				for sidRows.Next() {
					var sid, uid string
					if sidRows.Scan(&sid, &uid) == nil {
						if hubID, ok := uidToHub[uid]; ok {
							codexSidArgs = append(codexSidArgs, sid)
							hubByCodexSid[sid] = hubID
						}
					}
				}
			}()
			if len(codexSidArgs) > 0 {
				ph2 := strings.Repeat("?,", len(codexSidArgs))
				ph2 = ph2[:len(ph2)-1]
				q := fmt.Sprintf(`
					SELECT session_id, role, content, ts FROM (
						SELECT session_id, role, content, ts,
							ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY ts DESC) AS rn
						FROM messages
						WHERE session_id IN (%s) AND is_subagent = 0
					) WHERE rn <= ?
					ORDER BY session_id, rn DESC`, ph2)
				cArgs := append(codexSidArgs, n)
				rows, err := idx.db.Query(q, cArgs...)
				if err == nil {
					defer rows.Close()
					for rows.Next() {
						var sid, role, content, ts string
						if rows.Scan(&sid, &role, &content, &ts) != nil {
							continue
						}
						addRow(hubByCodexSid[sid], role, content, ts)
					}
				}
			}
		}
	}

	return out
}

// RecentMessagesBySession returns the last n non-subagent messages for each
// session in sessionIDs. Results are ordered oldest-first within each session.
func (idx *Index) RecentMessagesBySession(sessionIDs []string, n int) map[string][]RecentMsg {
	if len(sessionIDs) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(sessionIDs))
	placeholders = placeholders[:len(placeholders)-1]

	args := make([]any, len(sessionIDs)+1)
	for i, id := range sessionIDs {
		args[i] = id
	}
	args[len(sessionIDs)] = n

	// For each session pick the last N rows then return them oldest-first.
	q := fmt.Sprintf(`
		SELECT session_id, role, content FROM (
			SELECT session_id, role, content,
				ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY ts DESC) AS rn
			FROM messages
			WHERE session_id IN (%s) AND is_subagent = 0
		) WHERE rn <= ?
		ORDER BY session_id, rn DESC`, placeholders)

	rows, err := idx.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()

	out := make(map[string][]RecentMsg, len(sessionIDs))
	for rows.Next() {
		var sid, role, content string
		if rows.Scan(&sid, &role, &content) != nil {
			continue
		}
		text := strings.TrimSpace(content)
		if text == "" {
			continue
		}
		text = truncateRunes(text, 200)
		out[sid] = append(out[sid], RecentMsg{Role: role, Text: text})
	}
	return out
}

func truncateRunes(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
