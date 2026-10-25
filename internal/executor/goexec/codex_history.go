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
	path, findErr := c.findCodexSessionFileChecked(resumeID)
	if findErr != nil {
		return nil, findErr
	}
	if path == "" {
		return history.Slice(nil, opts), nil
	}
	if err := history.CheckFile("codex", path); err != nil {
		return nil, err
	}
	if opts.Scoped || opts.Before != "" || opts.Mode == "continuation" || opts.Mode == "beginning" {
		if _, ok := physicalHistoryCursor("codex", resumeID, opts); ok {
			return loadNativeWindowPage("codex", resumeID, opts, func(cursor int) (history.FileWindow, error) { return c.historyFileWindow(path, cursor) }, func(window history.FileWindow) ([]map[string]any, error) {
				messages, _, e := parseCodexHistoryWindow(window.Lines, resumeID, window.MoreBefore, !window.MoreAfter, opts.Scoped)
				if e != nil {
					return nil, e
				}
				if !opts.IncludeThinking {
					messages = history.StripThinkingBlocks(messages)
				}
				messages = c.withHistoryRequestIDs(messages, resumeID)
				if opts.Scoped && opts.DeferredAssistantRequestID != "" {
					committed := make([]map[string]any, 0, len(messages))
					for _, message := range messages {
						if message["role"] == "assistant" && message["request_id"] == opts.DeferredAssistantRequestID {
							continue
						}
						committed = append(committed, message)
					}
					messages = committed
				}
				return messages, nil
			})
		}
	}
	if fi, err := os.Stat(path); err == nil && !fi.IsDir() {
		key := history.FileKey{Path: path, MtimeNS: fi.ModTime().UnixNano(), Size: fi.Size()}
		// Reparse older tails whose typed tool-output parts were saved empty.
		cacheName := "codex:turns-v5-item-ids:" + resumeID
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
			return nil, history.SourceError("codex", path, err)
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
		return nil, history.SourceError("codex", path, err)
	}
	if !opts.IncludeThinking {
		messages = history.StripThinkingBlocks(messages)
	}
	return history.Slice(c.withHistoryRequestIDs(messages, resumeID), opts), nil
}

func (c *Codex) ResumableSessions(limit int) ([]history.ResumableSession, error) {
	limit = history.ClampLimit(limit)
	paths, _, err := c.codexRolloutCatalogChecked(false)
	if err != nil {
		return nil, err
	}
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
		if err := history.CheckFile("codex", path); err != nil {
			return nil, err
		}
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
	path, _ := c.findCodexSessionFileChecked(resumeID)
	return path
}

func (c *Codex) findCodexSessionFileChecked(resumeID string) (string, error) {
	if resumeID == "" {
		return "", nil
	}
	for _, force := range []bool{false, true} {
		_, byID, err := c.codexRolloutCatalogChecked(force)
		if err != nil {
			return "", err
		}
		if path := byID[resumeID]; path != "" {
			return path, nil
		}
	}
	return "", nil
}

func (c *Codex) codexRolloutFiles() []string {
	paths, _ := c.codexRolloutCatalog(false)
	return paths
}

func (c *Codex) codexRolloutCatalog(force bool) ([]string, map[string]string) {
	paths, ids, _ := c.codexRolloutCatalogChecked(force)
	return paths, ids
}

func (c *Codex) codexRolloutCatalogChecked(force bool) ([]string, map[string]string, error) {
	if err := history.CheckDirectory("codex", c.sessionsRoot); err != nil {
		return nil, nil, err
	}
	c.rolloutMu.Lock()
	defer c.rolloutMu.Unlock()
	if !force && c.rolloutRoot == c.sessionsRoot && len(c.rolloutPaths) > 0 && time.Since(c.rolloutScannedAt) < 30*time.Second {
		return append([]string(nil), c.rolloutPaths...), cloneStringMap(c.rolloutByID), nil
	}
	var out []string
	walkErr := filepath.WalkDir(c.sessionsRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return history.SourceError("codex", path, err)
		}
		if d == nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, "rollout-") && (strings.HasSuffix(name, ".jsonl") || strings.HasSuffix(name, ".jsonl.gz")) {
			out = append(out, path)
		}
		return nil
	})
	if walkErr != nil {
		return nil, nil, walkErr
	}
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
	return append([]string(nil), out...), cloneStringMap(byID), nil
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
	return parseCodexHistoryWindow(lines, resumeID, truncated, true, false)
}
func parseCodexHistoryWindow(lines []history.TailLine, resumeID string, truncated, completeTail, scoped bool) ([]map[string]any, bool, error) {

	type rec struct {
		lineNo int
		row    codexHistoryRow
		turnID string
	}
	var records []rec
	toolOutputs := map[string]string{}
	var messages []map[string]any
	turnID := ""
	tailClosed := false
	turnBySource := map[string]string{}
	// A bounded tail may begin inside a turn, before its exact closing marker.
	unboundLeadingTail := truncated
	for _, ln := range lines {
		var row codexHistoryRow
		if json.Unmarshal(ln.Data, &row) != nil {
			continue
		}
		if row.Type == "turn_context" || row.Type == "event_msg" {
			var event struct {
				Type   string          `json:"type"`
				TurnID string          `json:"turn_id"`
				Error  json.RawMessage `json:"error"`
			}
			if json.Unmarshal(row.Payload, &event) == nil {
				if row.Type == "turn_context" || event.Type == "task_started" {
					unboundLeadingTail = false
					tailClosed = false
					turnID = event.TurnID
				} else if event.Type == "task_complete" || event.Type == "turn_aborted" {
					if unboundLeadingTail && turnID == "" && event.Type == "task_complete" && event.TurnID != "" &&
						(len(event.Error) == 0 || string(event.Error) == "null") && len(records) > 0 {
						last := len(records) - 1
						final := parseCodexHistoryPayload(records[last].row.Payload)
						finalCount := 0
						for _, candidate := range records {
							payload := parseCodexHistoryPayload(candidate.row.Payload)
							if payload.Type == "message" && payload.Role == "assistant" && payload.Phase == "final_answer" {
								finalCount++
							}
						}
						// Only the single final immediately before this exact close is
						// recovered. Other rows and unknown/aborted/legacy turns remain
						// unbound; the persisted turn/request journal is still required.
						if finalCount == 1 && final.Type == "message" && final.Role == "assistant" && final.Phase == "final_answer" {
							turnBySource["codex:"+resumeID+":line:"+itoa(records[last].lineNo)] = event.TurnID
						}
					}
					unboundLeadingTail = false
					tailClosed = true
					turnID = ""
				}
			}
		}
		if row.Type != "response_item" {
			continue
		}
		tailClosed = false
		records = append(records, rec{lineNo: ln.LineNo, row: row, turnID: turnID})
		turnBySource["codex:"+resumeID+":line:"+itoa(ln.LineNo)] = turnID
		if id, out, ok := codexResponseToolOutput(row.Payload); ok {
			toolOutputs[id] = out
		}
	}

	pendingCommentary := ""
	var pendingCommentaryBlocks []map[string]any
	pendingCommentaryLine := 0
	var pendingCommentaryTS int64
	flushPendingCommentary := func() {
		if pendingCommentary == "" {
			return
		}
		message := history.CompleteMsg(
			"codex", resumeID, "codex:"+resumeID+":line:"+itoa(pendingCommentaryLine),
			"assistant", pendingCommentary, pendingCommentaryTS,
			pendingCommentaryBlocks,
		)
		message["history_read_result_verified"] = false
		messages = append(messages, message)
		pendingCommentary = ""
		pendingCommentaryBlocks = nil
	}
	attachPendingCommentary := func(blocks []map[string]any) []map[string]any {
		if pendingCommentary == "" {
			return blocks
		}
		merged := make([]map[string]any, 0, len(blocks)+1)
		merged = append(merged, pendingCommentaryBlocks...)
		merged = append(merged, blocks...)
		pendingCommentary = ""
		pendingCommentaryBlocks = nil
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
			pendingCommentaryBlocks = appendNativeCommentaryBlock(pendingCommentaryBlocks, text, payload.ID)
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
		if role == "assistant" && payload.ID != "" {
			blocks[0]["source_item_id"] = payload.ID
		}
		if role == "assistant" {
			blocks = attachPendingCommentary(blocks)
		}
		message := history.CompleteMsg(
			"codex", resumeID, "codex:"+resumeID+":line:"+itoa(rc.lineNo),
			role, text, ts, blocks,
		)
		message["history_read_result_verified"] = role == "assistant" && payload.Phase == "final_answer"
		messages = append(messages, message)
	}
	// Open progress may later attach to a tool/final with a different source
	// ID. It remains in the live stream until an exact native close; never mint
	// a transient canonical cursor that disappears at streaming -> done.
	if completeTail && (!scoped || tailClosed) {
		flushPendingCommentary()
	}
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
	ID      string          `json:"id"`
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
