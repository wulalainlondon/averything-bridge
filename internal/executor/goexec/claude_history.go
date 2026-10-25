package goexec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"everything-go/internal/history"
)

const maxToolOutput = 256 * 1024

// TranscriptPath returns the on-disk .jsonl transcript for a resume id, used by
// fork_session to copy a session's history. Satisfies the core's transcript
// locator capability. Empty resume id or missing file → ok=false.
func (c *Claude) TranscriptPath(resumeID string) (string, bool) {
	if resumeID == "" {
		return "", false
	}
	if p := c.findSessionFile(resumeID); p != "" {
		return p, true
	}
	return "", false
}

// findSessionFile locates <uuid>.jsonl under any ~/.claude/projects/* dir.
func (c *Claude) findSessionFile(uuid string) string {
	path, _ := c.findSessionFileChecked(uuid)
	return path
}

func (c *Claude) findSessionFileChecked(uuid string) (string, error) {
	if uuid == "" {
		return "", nil
	}
	entries, err := os.ReadDir(c.projectsDir)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", history.SourceError("claude", c.projectsDir, err)
	}
	for _, proj := range entries {
		if !proj.IsDir() {
			continue
		}
		project := filepath.Join(c.projectsDir, proj.Name())
		if err := history.CheckDirectory("claude", project); err != nil {
			return "", err
		}
		candidate := filepath.Join(project, uuid+".jsonl")
		fi, err := os.Stat(candidate)
		if err == nil && !fi.IsDir() {
			return candidate, nil
		}
		if err != nil && !os.IsNotExist(err) {
			return "", history.SourceError("claude", candidate, err)
		}
	}
	return "", nil
}

type claudeRow struct {
	UUID                      string `json:"uuid"`
	Type                      string `json:"type"`
	IsSidechain               bool   `json:"isSidechain"`
	IsCompactSummary          bool   `json:"isCompactSummary"`
	IsVisibleInTranscriptOnly bool   `json:"isVisibleInTranscriptOnly"`
	Timestamp                 string `json:"timestamp"`
	Cwd                       string `json:"cwd"`
	Message                   struct {
		Content    json.RawMessage `json:"content"`
		StopReason string          `json:"stop_reason"`
	} `json:"message"`
}

type claudeBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
}

// LoadHistory parses the session JSONL into wire messages and slices it.
func (c *Claude) LoadHistory(resumeID string, opts history.Opts) (*history.Result, error) {
	path, findErr := c.findSessionFileChecked(resumeID)
	if findErr != nil {
		return nil, findErr
	}
	if path == "" {
		return history.Slice(nil, opts), nil
	}
	if err := history.CheckFile("claude", path); err != nil {
		return nil, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, history.SourceError("claude", path, err)
	}
	if fi.IsDir() {
		return history.Slice(nil, opts), nil
	}
	if opts.Scoped || opts.Before != "" || opts.Mode == "continuation" || opts.Mode == "beginning" {
		if _, ok := physicalHistoryCursor("claude", resumeID, opts); ok {
			return loadNativeWindowPage("claude", resumeID, opts, func(cursor int) (history.FileWindow, error) {
				return history.ReadFileWindow(path, cursor, history.LoadMaxBytes())
			}, func(window history.FileWindow) ([]map[string]any, error) {
				messages, _, e := parseClaudeHistoryLines(window.Lines, resumeID, fi.ModTime().UnixMilli(), window.MoreBefore)
				if e != nil {
					return nil, e
				}
				if !opts.IncludeThinking {
					messages = history.StripThinkingBlocks(messages)
				}
				return messages, nil
			})
		}
	}
	mtimeNS := fi.ModTime().UnixNano()
	size := fi.Size()
	key := history.FileKey{Path: path, MtimeNS: mtimeNS, Size: size}
	cacheName := "claude:" + resumeID
	if messages, ok := history.DefaultCache().Load(cacheName, key); ok {
		if !opts.IncludeThinking {
			messages = history.StripThinkingBlocks(messages)
		}
		return history.Slice(messages, opts), nil
	}

	messages, truncated, loadErr := loadClaudeHistoryMessagesChecked(path, resumeID, fi.ModTime().UnixMilli())
	if loadErr != nil {
		return nil, history.SourceError("claude", path, loadErr)
	}
	if !truncated {
		history.DefaultCache().SaveAsync(cacheName, key, messages)
	}
	if !opts.IncludeThinking {
		messages = history.StripThinkingBlocks(messages)
	}
	result := history.Slice(messages, opts)
	result.HasMoreBefore = result.HasMoreBefore || truncated
	return result, nil
}

// loadClaudeHistoryMessages parses the transcript and returns its wire messages
// plus whether the file was tail-truncated (too large to hold whole). A
// truncated result must not be cached, since it is only the most recent window.
func loadClaudeHistoryMessages(path, resumeID string, fileMtimeMs int64) ([]map[string]any, bool) {
	messages, truncated, _ := loadClaudeHistoryMessagesChecked(path, resumeID, fileMtimeMs)
	return messages, truncated
}

func loadClaudeHistoryMessagesChecked(path, resumeID string, fileMtimeMs int64) ([]map[string]any, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	lines, truncated, err := history.StreamTailLines(f, history.LoadMaxBytes())
	if err != nil {
		return nil, truncated, err
	}
	return parseClaudeHistoryLines(lines, resumeID, fileMtimeMs, truncated)
}

func parseClaudeHistoryLines(lines []history.TailLine, resumeID string, fileMtimeMs int64, truncated bool) ([]map[string]any, bool, error) {
	type rec struct {
		lineNo int
		row    claudeRow
	}
	var records []rec
	for _, ln := range lines {
		var r claudeRow
		if json.Unmarshal(ln.Data, &r) != nil {
			continue
		}
		records = append(records, rec{lineNo: ln.LineNo, row: r})
	}

	// Pass 1: tool_use_id -> output.
	toolOutputs := map[string]string{}
	for _, rc := range records {
		blocks := parseBlocks(rc.row.Message.Content)
		for _, b := range blocks {
			if b.Type == "tool_result" && b.ToolUseID != "" {
				out := flattenToolResult(b.Content)
				if len(out) > maxToolOutput {
					out = out[:maxToolOutput] + "\n…(truncated)"
				}
				toolOutputs[b.ToolUseID] = out
			}
		}
	}

	// Pass 2: build messages.
	var messages []map[string]any
	for _, rc := range records {
		d := rc.row
		if d.IsSidechain || d.IsCompactSummary || d.IsVisibleInTranscriptOnly {
			continue
		}
		if d.Type != "user" && d.Type != "assistant" {
			continue
		}
		text, blocks := buildClaudeBlocks(d.Message.Content, d.Type, toolOutputs)
		hasThinking := false
		for _, b := range blocks {
			if b["type"] == "thinking" {
				hasThinking = true
				break
			}
		}
		// Text-less rows are dropped (tool_result user rows, meta rows, and —
		// deliberately, for Python-shape parity — tool_use-only assistant rows),
		// EXCEPT thinking rows, which clients need to keep the process fold
		// visible after a history reload.
		if (text == "" && !hasThinking) || strings.HasPrefix(text, "<") || strings.HasPrefix(text, "[Request interrupted") {
			continue
		}
		if strings.HasPrefix(text, "Base directory for this skill:") {
			continue
		}
		if len(blocks) == 0 {
			blocks = []map[string]any{{"type": "text", "text": text}}
		}
		tsMs := parseISOms(d.Timestamp)
		verifiedTimestamp := tsMs > 0
		if tsMs == 0 {
			tsMs = fileMtimeMs
		}
		message := history.CompleteMsg(
			"claude", resumeID,
			"claude:"+resumeID+":line:"+itoa(rc.lineNo),
			d.Type, text, tsMs, blocks,
		)
		message["history_timestamp_verified"] = verifiedTimestamp
		// A fresh thinking/tool/progress row from the same API request is not
		// proof that the completed answer has reached native history.
		message["history_read_result_verified"] = d.Type == "assistant" && d.Message.StopReason == "end_turn" && strings.TrimSpace(text) != ""
		messages = append(messages, message)
	}

	return messages, truncated, nil
}

func parseBlocks(content json.RawMessage) []claudeBlock {
	if len(content) == 0 {
		return nil
	}
	var blocks []claudeBlock
	if json.Unmarshal(content, &blocks) == nil {
		return blocks
	}
	return nil
}

// buildClaudeBlocks returns the joined text and the wire blocks for one message.
func buildClaudeBlocks(content json.RawMessage, role string, toolOutputs map[string]string) (string, []map[string]any) {
	// content may be a plain string.
	var asStr string
	if json.Unmarshal(content, &asStr) == nil {
		return asStr, nil
	}
	blocks := parseBlocks(content)
	var textParts []string
	var out []map[string]any
	for _, b := range blocks {
		switch {
		case b.Type == "text" && b.Text != "":
			textParts = append(textParts, b.Text)
			out = append(out, map[string]any{"type": "text", "text": b.Text})
		case b.Type == "thinking" && b.Thinking != "" && role == "assistant":
			// Kept out of the joined text (content stays text-only, matching the
			// Python shape) — clients render this as a collapsed thinking block.
			out = append(out, map[string]any{"type": "thinking", "thinking": b.Thinking})
		case b.Type == "tool_use" && role == "assistant":
			out = append(out, map[string]any{
				"type":        "tool_call",
				"tool_use_id": b.ID,
				"name":        b.Name,
				"command":     extractCommand(b.Input),
				"output":      toolOutputs[b.ID],
			})
		}
	}
	return strings.Join(textParts, "\n"), out
}

func flattenToolResult(content json.RawMessage) string {
	if len(content) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(content, &s) == nil {
		return s
	}
	var items []struct {
		Text    string `json:"text"`
		Content string `json:"content"`
	}
	if json.Unmarshal(content, &items) == nil {
		var parts []string
		for _, it := range items {
			if it.Text != "" {
				parts = append(parts, it.Text)
			} else if it.Content != "" {
				parts = append(parts, it.Content)
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

// ResumableSessions scans ~/.claude/projects for session transcripts.
func (c *Claude) ResumableSessions(limit int) ([]history.ResumableSession, error) {
	projs, err := os.ReadDir(c.projectsDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, history.SourceError("claude", c.projectsDir, err)
	}
	var sessions []history.ResumableSession
	for _, proj := range projs {
		if !proj.IsDir() {
			continue
		}
		projPath := filepath.Join(c.projectsDir, proj.Name())
		files, err := os.ReadDir(projPath)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, history.SourceError("claude", projPath, err)
		}
		for _, entry := range files {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
				continue
			}
			uuid := strings.TrimSuffix(entry.Name(), ".jsonl")
			fi, err := entry.Info()
			if err != nil {
				return nil, history.SourceError("claude", filepath.Join(projPath, entry.Name()), err)
			}
			if err := history.CheckFile("claude", filepath.Join(projPath, entry.Name())); err != nil {
				return nil, err
			}
			cwd, name := scanCwdAndName(filepath.Join(projPath, entry.Name()))
			if cwd == "" {
				cwd = decodeProjectPath(proj.Name())
			}
			if name == "" {
				name = uuid
				if len(uuid) > 8 {
					name = uuid[:8]
				}
			}
			sessions = append(sessions, history.ResumableSession{
				ID: uuid, Name: name, ClaudeUUID: uuid,
				LastUsed: fi.ModTime().Unix(), Cwd: cwd, Backend: "claude",
			})
		}
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].LastUsed > sessions[j].LastUsed })
	if len(sessions) > limit {
		sessions = sessions[:limit]
	}
	return sessions, nil
}

// scanCwdAndName reads a transcript for its cwd and first user-text name. Both
// live in the first handful of lines, so it streams and stops early instead of
// reading the whole file — a runaway session can be 800MB+, and this runs once
// per transcript when building the resumable-sessions list (get_resumable_sessions).
func scanCwdAndName(path string) (cwd, name string) {
	f, err := os.Open(path)
	if err != nil {
		return "", ""
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 64*1024)
	for i := 0; i < 400; i++ {
		line, readErr := br.ReadBytes('\n')
		if raw := bytes.TrimSpace(line); len(raw) > 0 {
			var d claudeRow
			if json.Unmarshal(raw, &d) == nil {
				if cwd == "" && strings.TrimSpace(d.Cwd) != "" {
					cwd = strings.TrimSpace(d.Cwd)
				}
				if name == "" && d.Type == "user" {
					text, _ := buildClaudeBlocks(d.Message.Content, "user", nil)
					if text != "" && !strings.HasPrefix(text, "<") {
						if len(text) > 50 {
							text = text[:50]
						}
						name = strings.TrimSpace(text)
					}
				}
				if cwd != "" && name != "" {
					return cwd, name
				}
			}
		}
		if readErr != nil {
			break
		}
	}
	return cwd, name
}

// decodeProjectPath best-effort reverses Claude's path encoding ('/'→'-', leading '-').
func decodeProjectPath(projName string) string {
	s := strings.TrimPrefix(projName, "-")
	return "/" + strings.ReplaceAll(s, "-", "/")
}

func parseISOms(ts string) int64 {
	if ts == "" {
		return 0
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, ts); err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// NativeTaskFinalAnchor retains the existing line-based public anchor while
// verifying the exact UUID in the provider's canonical native transcript.
// Stream stdout alone is never the final anchor authority.
func (c *Claude) NativeTaskFinalAnchor(conversation, uuid, text string) (string, bool) {
	path := c.findSessionFile(conversation)
	if path == "" || uuid == "" {
		return "", false
	}
	f, err := os.Open(path)
	if err != nil {
		return "", false
	}
	defer f.Close()
	lines, _, err := history.StreamTailLines(f, history.LoadMaxBytes())
	if err != nil {
		return "", false
	}
	match := ""
	for _, ln := range lines {
		var r claudeRow
		if json.Unmarshal(ln.Data, &r) != nil || r.UUID != uuid {
			continue
		}
		if r.Type != "assistant" || r.IsSidechain || r.IsCompactSummary || r.IsVisibleInTranscriptOnly || r.Message.StopReason != "end_turn" {
			return "", false
		}
		content, _ := buildClaudeBlocks(r.Message.Content, "assistant", map[string]string{})
		if content != text {
			return "", false
		}
		id := "claude:" + conversation + ":line:" + itoa(ln.LineNo)
		if match != "" && match != id {
			return "", false
		}
		match = id
	}
	return match, match != ""
}
