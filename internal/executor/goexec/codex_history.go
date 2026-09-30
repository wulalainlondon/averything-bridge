package goexec

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/history"
)

const codexHistoryCommentaryMaxRunes = 16_000

func appendCodexHistoryCommentary(current, next string) string {
	if current != "" {
		current += "\n\n"
	}
	current += next
	runes := []rune(current)
	if len(runes) <= codexHistoryCommentaryMaxRunes {
		return current
	}
	// Keep the most recent progress because it best describes the current state.
	return "…" + string(runes[len(runes)-codexHistoryCommentaryMaxRunes+1:])
}

func (c *Codex) LoadHistory(resumeID string, opts history.Opts) (*history.Result, error) {
	path := c.findCodexSessionFile(resumeID)
	if path == "" {
		return history.Slice(nil, opts), nil
	}
	if fi, err := os.Stat(path); err == nil && !fi.IsDir() {
		key := history.FileKey{Path: path, MtimeNS: fi.ModTime().UnixNano(), Size: fi.Size()}
		// Parsed cache v2 preserves native turn boundaries for request identity.
		cacheName := "codex:turns-v2:" + resumeID
		if messages, ok := history.DefaultCache().Load(cacheName, key); ok {
			if !opts.IncludeThinking {
				messages = history.StripThinkingBlocks(messages)
			}
			res := history.Slice(c.withHistoryRequestIDs(messages, resumeID), opts)
			if fi.Size() > int64(history.LoadMaxBytes()) {
				res.HasMoreBefore = true
			}
			return res, nil
		}
		messages, truncated, err := c.readCodexHistory(path, resumeID)
		if err != nil {
			return history.Slice(nil, opts), nil
		}
		// A bounded tail is still canonical for this exact file key. Persist it so
		// completed large sessions do not rescan hundreds of MB on every open or
		// after a Bridge restart.
		history.DefaultCache().SaveAsync(cacheName, key, messages)
		if !opts.IncludeThinking {
			messages = history.StripThinkingBlocks(messages)
		}
		res := history.Slice(c.withHistoryRequestIDs(messages, resumeID), opts)
		if truncated {
			res.HasMoreBefore = true
		}
		return res, nil
	}
	messages, _, err := c.readCodexHistory(path, resumeID)
	if err != nil {
		return history.Slice(nil, opts), nil
	}
	if !opts.IncludeThinking {
		messages = history.StripThinkingBlocks(messages)
	}
	return history.Slice(c.withHistoryRequestIDs(messages, resumeID), opts), nil
}

func (c *Codex) ResumableSessions(limit int) ([]history.ResumableSession, error) {
	limit = history.ClampLimit(limit)
	paths := c.codexRolloutFiles()
	sort.Slice(paths, func(i, j int) bool {
		return filepath.Base(paths[i]) > filepath.Base(paths[j])
	})
	out := make([]history.ResumableSession, 0, min(limit, len(paths)))
	seen := map[string]bool{}
	for _, path := range paths {
		if len(out) >= limit {
			break
		}
		// Codex multi-agent workers have their own rollout files, but app-server
		// deliberately rejects direct turn/start input for those child threads.
		// They belong in the live agent tree, not in the resumable-session picker.
		if codexRolloutIsSubagent(path) {
			continue
		}
		uid := codexRolloutUID(filepath.Base(path))
		if uid == "" || seen[uid] {
			continue
		}
		seen[uid] = true
		cwd, name := c.codexCwdAndName(path, uid)
		lastUsed := codexTimestampFromFilename(filepath.Base(path))
		if lastUsed == 0 {
			if fi, err := os.Stat(path); err == nil {
				lastUsed = fi.ModTime().Unix()
			}
		}
		out = append(out, history.ResumableSession{
			ID: uid, Name: sanitizeCodexSessionName(name, uid[:min(8, len(uid))]),
			ClaudeUUID: uid, LastUsed: lastUsed, Cwd: cwd, Backend: backend.Codex,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastUsed > out[j].LastUsed })
	return out, nil
}

// codexRolloutIsSubagent inspects the rollout's session_meta record. Newer
// Codex versions expose both thread_source and source.subagent; older versions
// may expose only one of them. parent_thread_id is also specific to an agent
// child. Deliberately do not classify forked_from_id alone as a sub-agent:
// user-created resumable forks may legitimately carry that field.
func codexRolloutIsSubagent(path string) bool {
	r, closeFn, err := openCodexRollout(path)
	if err != nil {
		return false
	}
	defer closeFn()

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for i := 0; i < 8 && sc.Scan(); i++ {
		var row codexHistoryRow
		if json.Unmarshal(sc.Bytes(), &row) != nil || row.Type != "session_meta" {
			continue
		}
		var meta struct {
			ThreadSource   string          `json:"thread_source"`
			ParentThreadID string          `json:"parent_thread_id"`
			Source         json.RawMessage `json:"source"`
		}
		if json.Unmarshal(row.Payload, &meta) != nil {
			return false
		}
		if meta.ThreadSource == "subagent" || meta.ParentThreadID != "" {
			return true
		}
		var source map[string]json.RawMessage
		return json.Unmarshal(meta.Source, &source) == nil && source["subagent"] != nil
	}
	return false
}

// TranscriptPath returns the on-disk .jsonl rollout file for a resume id, used
// by fork_session to copy a session's history. Satisfies the core's
// transcriptLocator capability. .gz files are excluded because fork.go copies
// raw bytes directly into a new .jsonl; decompressing is out of scope.
func (c *Codex) TranscriptPath(resumeID string) (string, bool) {
	if p := c.findCodexSessionFile(resumeID); p != "" && !strings.HasSuffix(p, ".gz") {
		return p, true
	}
	return "", false
}

func (c *Codex) findCodexSessionFile(resumeID string) string {
	if resumeID == "" {
		return ""
	}
	for _, force := range []bool{false, true} {
		_, byID := c.codexRolloutCatalog(force)
		if path := byID[resumeID]; path != "" {
			return path
		}
	}
	return ""
}

func (c *Codex) codexRolloutFiles() []string {
	paths, _ := c.codexRolloutCatalog(false)
	return paths
}

func (c *Codex) codexRolloutCatalog(force bool) ([]string, map[string]string) {
	c.rolloutMu.Lock()
	defer c.rolloutMu.Unlock()
	if !force && c.rolloutRoot == c.sessionsRoot && len(c.rolloutPaths) > 0 && time.Since(c.rolloutScannedAt) < 30*time.Second {
		return append([]string(nil), c.rolloutPaths...), cloneStringMap(c.rolloutByID)
	}
	var out []string
	_ = filepath.WalkDir(c.sessionsRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, "rollout-") && (strings.HasSuffix(name, ".jsonl") || strings.HasSuffix(name, ".jsonl.gz")) {
			out = append(out, path)
		}
		return nil
	})
	byID := make(map[string]string, len(out))
	for _, path := range out {
		if uid := codexRolloutUID(filepath.Base(path)); uid != "" {
			byID[uid] = path
		}
	}
	c.rolloutRoot = c.sessionsRoot
	c.rolloutScannedAt = time.Now()
	c.rolloutPaths = append(c.rolloutPaths[:0], out...)
	c.rolloutByID = byID
	return append([]string(nil), out...), cloneStringMap(byID)
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// readCodexHistory parses the rollout and returns its wire messages plus whether
// the file was tail-truncated (too large to hold whole).
func (c *Codex) readCodexHistory(path, resumeID string) ([]map[string]any, bool, error) {
	var lines []history.TailLine
	var truncated bool
	if strings.HasSuffix(path, ".gz") {
		r, closeFn, err := openCodexRollout(path)
		if err != nil {
			return nil, false, err
		}
		defer closeFn()
		lines, truncated, err = history.StreamTailLines(r, history.LoadMaxBytes())
		if err != nil {
			return nil, truncated, err
		}
	} else {
		cache := history.DefaultCache()
		prior, hasPrior := cache.LoadTailState(path)
		var next history.TailState
		var err error
		lines, truncated, next, err = history.ReadTailFileLines(path, history.LoadMaxBytes(), prior, hasPrior)
		if err != nil {
			return nil, truncated, err
		}
		cache.SaveTailState(next)
	}
	return parseCodexHistoryLines(lines, resumeID, truncated)
}

func parseCodexHistoryLines(lines []history.TailLine, resumeID string, truncated bool) ([]map[string]any, bool, error) {

	type rec struct {
		lineNo int
		row    codexHistoryRow
		turnID string
	}
	var records []rec
	toolOutputs := map[string]string{}
	var messages []map[string]any
	turnID := ""
	turnBySource := map[string]string{}
	for _, ln := range lines {
		var row codexHistoryRow
		if json.Unmarshal(ln.Data, &row) != nil {
			continue
		}
		if row.Type == "turn_context" || row.Type == "event_msg" {
			var event struct {
				Type   string `json:"type"`
				TurnID string `json:"turn_id"`
			}
			if json.Unmarshal(row.Payload, &event) == nil {
				if row.Type == "turn_context" || event.Type == "task_started" {
					turnID = event.TurnID
				} else if event.Type == "task_complete" || event.Type == "turn_aborted" {
					turnID = ""
				}
			}
		}
		if row.Type != "response_item" {
			continue
		}
		records = append(records, rec{lineNo: ln.LineNo, row: row, turnID: turnID})
		turnBySource["codex:"+resumeID+":line:"+itoa(ln.LineNo)] = turnID
		if id, out, ok := codexResponseToolOutput(row.Payload); ok {
			toolOutputs[id] = out
		}
	}

	pendingCommentary := ""
	pendingCommentaryLine := 0
	var pendingCommentaryTS int64
	flushPendingCommentary := func() {
		if pendingCommentary == "" {
			return
		}
		messages = append(messages, history.CompleteMsg(
			"codex", resumeID, "codex:"+resumeID+":line:"+itoa(pendingCommentaryLine),
			"assistant", pendingCommentary, pendingCommentaryTS,
			[]map[string]any{{"type": "text", "text": pendingCommentary}},
		))
		pendingCommentary = ""
	}
	attachPendingCommentary := func(blocks []map[string]any) []map[string]any {
		if pendingCommentary == "" {
			return blocks
		}
		merged := make([]map[string]any, 0, len(blocks)+1)
		merged = append(merged, map[string]any{"type": "text", "text": pendingCommentary})
		merged = append(merged, blocks...)
		pendingCommentary = ""
		return merged
	}

	previousTurn := ""
	for _, rc := range records {
		if rc.turnID != previousTurn {
			flushPendingCommentary()
		}
		previousTurn = rc.turnID
		if tool, ok := normalizeCodexResponseTool(rc.row.Payload, toolOutputs[codexPayloadCallID(rc.row.Payload)]); ok {
			blocks := attachPendingCommentary([]map[string]any{tool.historyBlock()})
			messages = append(messages, history.CompleteMsg(
				"codex", resumeID, "codex:"+resumeID+":line:"+itoa(rc.lineNo),
				"assistant", firstNonEmpty(tool.Command, tool.Name), parseCodexISOms(rc.row.Timestamp), blocks,
			))
			continue
		}
		payload := parseCodexHistoryPayload(rc.row.Payload)
		if payload.Type != "message" {
			continue
		}
		role := payload.Role
		if role != "user" && role != "assistant" {
			continue
		}
		text := stripCodexBridgeHandoff(extractCodexText(payload.Content))
		if text == "" || (role == "user" && isCodexBootstrapText(text)) {
			continue
		}
		ts := parseCodexISOms(rc.row.Timestamp)
		if role == "assistant" && payload.Phase == "commentary" {
			pendingCommentary = appendCodexHistoryCommentary(pendingCommentary, text)
			pendingCommentaryLine = rc.lineNo
			pendingCommentaryTS = ts
			continue
		}
		if role == "user" {
			// An unfinished prior turn may end with commentary only. Preserve it
			// before starting the next user turn instead of attaching it across turns.
			flushPendingCommentary()
		}
		blocks := []map[string]any{{"type": "text", "text": text}}
		if role == "assistant" {
			blocks = attachPendingCommentary(blocks)
		}
		messages = append(messages, history.CompleteMsg(
			"codex", resumeID, "codex:"+resumeID+":line:"+itoa(rc.lineNo),
			role, text, ts, blocks,
		))
	}
	flushPendingCommentary()
	for _, message := range messages {
		sourceID, _ := message["source_message_id"].(string)
		if turn := turnBySource[sourceID]; turn != "" {
			message["source_turn_id"] = turn
		}
	}
	return messages, truncated, nil
}

type codexHistoryRow struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

type codexHistoryPayload struct {
	Type    string          `json:"type"`
	Role    string          `json:"role"`
	Phase   string          `json:"phase"`
	Content json.RawMessage `json:"content"`
	Cwd     string          `json:"cwd"`
}

func parseCodexHistoryPayload(raw json.RawMessage) codexHistoryPayload {
	var p codexHistoryPayload
	_ = json.Unmarshal(raw, &p)
	return p
}

func codexPayloadCallID(raw json.RawMessage) string {
	var p struct {
		CallID  string `json:"call_id"`
		CallID2 string `json:"callId"`
		ID      string `json:"id"`
	}
	_ = json.Unmarshal(raw, &p)
	return firstNonEmpty(p.CallID, p.CallID2, p.ID)
}

func (c *Codex) codexCwdAndName(path, uid string) (string, string) {
	cwd := "~"
	name := uid[:min(8, len(uid))]
	r, closeFn, err := openCodexRollout(path)
	if err != nil {
		return cwd, name
	}
	defer closeFn()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		var row codexHistoryRow
		if json.Unmarshal(sc.Bytes(), &row) != nil {
			continue
		}
		payload := parseCodexHistoryPayload(row.Payload)
		switch {
		case row.Type == "session_meta" && payload.Cwd != "":
			cwd = payload.Cwd
		case row.Type == "response_item" && payload.Role == "user":
			text := extractCodexText(payload.Content)
			if text != "" && !isCodexBootstrapText(text) {
				name = text
				return cwd, name
			}
		}
	}
	return cwd, name
}

func openCodexRollout(path string) (io.Reader, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, func() {}, err
	}
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, func() {}, err
		}
		return gz, func() { _ = gz.Close(); _ = f.Close() }, nil
	}
	return f, func() { _ = f.Close() }, nil
}

func codexRolloutUID(name string) string {
	name = strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".jsonl")
	if len(name) < 36 {
		return ""
	}
	return name[len(name)-36:]
}

func codexTimestampFromFilename(name string) int64 {
	if len(name) < len("rollout-2006-01-02T15-04-05") {
		return 0
	}
	raw := name[len("rollout-") : len("rollout-")+len("2006-01-02T15-04-05")]
	t, err := time.Parse("2006-01-02T15-04-05", raw)
	if err != nil {
		return 0
	}
	return t.UTC().Unix()
}

func parseCodexISOms(value string) int64 {
	if value == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339Nano, strings.ReplaceAll(value, "Z", "+00:00"))
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}

func extractCodexText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return cleanCodexText(s)
	}
	var items []struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &items) == nil {
		var parts []string
		for _, it := range items {
			if strings.TrimSpace(it.Text) != "" {
				parts = append(parts, it.Text)
			}
		}
		return cleanCodexText(strings.Join(parts, "\n"))
	}
	return ""
}

func cleanCodexText(text string) string {
	for {
		start := strings.Index(strings.ToLower(text), "<turn_aborted>")
		if start < 0 {
			break
		}
		end := strings.Index(strings.ToLower(text[start:]), "</turn_aborted>")
		if end < 0 {
			break
		}
		end += start + len("</turn_aborted>")
		text = text[:start] + text[end:]
	}
	return strings.TrimSpace(text)
}

func isCodexBootstrapText(text string) bool {
	stripped := strings.TrimLeft(text, " \t\r\n")
	return (strings.HasPrefix(stripped, "<environment_context>") && strings.Contains(stripped, "</environment_context>") && strings.Contains(stripped, "<cwd>")) ||
		(strings.HasPrefix(stripped, "# AGENTS.md instructions") && (strings.Contains(stripped, "<environment_context>") || strings.Contains(stripped, "<INSTRUCTIONS>")))
}

func stripCodexBridgeHandoff(text string) string {
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, "<bridge_session_handoff") {
		return text
	}
	end := strings.Index(trimmed, "</bridge_session_handoff>")
	if end < 0 {
		return ""
	}
	rest := strings.TrimSpace(trimmed[end+len("</bridge_session_handoff>"):])
	if strings.HasPrefix(rest, "<current_user_request>") && strings.HasSuffix(rest, "</current_user_request>") {
		rest = strings.TrimPrefix(rest, "<current_user_request>")
		rest = strings.TrimSuffix(rest, "</current_user_request>")
	}
	return strings.TrimSpace(rest)
}

func sanitizeCodexSessionName(raw, fallback string) string {
	s := strings.Join(strings.Fields(raw), " ")
	s = strings.Trim(s, "`'\"[]{}()<>")
	if s == "" {
		return fallback
	}
	if len(s) > 80 {
		s = strings.TrimSpace(s[:80])
	}
	return s
}
