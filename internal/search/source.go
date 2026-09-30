package search

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"everything-go/internal/sourcepolicy"
)

// searchableMessage is the normalized unit indexed into the messages table.
// Mirrors bridge/search/sources/base.py SearchableMessage.
type searchableMessage struct {
	Source     string // claude | codex | ollama
	SessionID  string // "{source}:{native_id}"
	MsgUUID    string
	ParentUUID string
	Role       string // user | assistant | system
	Timestamp  string // ISO8601 ("" if missing)
	Text       string
	IsSubagent bool
	Cwd        string
}

// sessionMeta is the per-file metadata used to upsert the sessions row.
type sessionMeta struct {
	Cwd         string
	ProjectDir  string
	FirstTS     string
	DisplayName string
}

// source abstracts a JSONL conversation store (Claude / Codex).
type source interface {
	name() string
	enabled() bool
	owns(path string) bool
	discover() []string
	// iterMessages reads complete lines from startOffset to EOF, returning the
	// extracted messages and the byte offset after the last complete line.
	iterMessages(path string, startOffset int64) ([]searchableMessage, int64)
	headSignature(path string) string
	sessionIDFor(path string) string
	sessionMeta(path string) sessionMeta
}

func headSig(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	buf := make([]byte, 4096)
	n, _ := f.Read(buf)
	sum := sha256.Sum256(buf[:n])
	return hex.EncodeToString(sum[:])
}

// lineReader streams complete (newline-terminated) lines from startOffset,
// invoking fn(raw, nextOffset) per line. An incomplete trailing line is left
// for a later resume. Returns the offset after the last complete line.
func lineReader(path string, startOffset int64, fn func(raw []byte, lineNum int, nextOffset int64)) int64 {
	return lineReaderUntil(path, startOffset, func(raw []byte, lineNum int, nextOffset int64) bool {
		fn(raw, lineNum, nextOffset)
		return false
	})
}

// lineReaderUntil is lineReader with an early-stop signal. Metadata readers use
// it to stop physical I/O as soon as cwd/timestamp/display name are known.
func lineReaderUntil(path string, startOffset int64, fn func(raw []byte, lineNum int, nextOffset int64) bool) int64 {
	f, err := os.Open(path)
	if err != nil {
		return startOffset
	}
	defer f.Close()
	if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
		return startOffset
	}
	br := bufio.NewReaderSize(f, 1<<20)
	offset := startOffset
	lineNum := 0
	for {
		raw, err := br.ReadBytes('\n')
		if len(raw) > 0 && raw[len(raw)-1] == '\n' {
			next := offset + int64(len(raw))
			lineNum++
			stop := fn(raw, lineNum, next)
			offset = next
			if stop {
				return offset
			}
		}
		if err != nil {
			break // EOF or incomplete trailing line (not emitted)
		}
	}
	return offset
}

// --- Claude source ----------------------------------------------------------

type claudeSource struct{ root string }

func newClaudeSource() claudeSource {
	if configured := strings.TrimSpace(os.Getenv("EVERYTHING_GO_CLAUDE_PROJECTS_DIR")); configured != "" {
		return claudeSource{root: configured}
	}
	home, _ := os.UserHomeDir()
	return claudeSource{root: filepath.Join(home, ".claude", "projects")}
}

func (c claudeSource) name() string { return "claude" }

func (c claudeSource) enabled() bool {
	info, err := os.Stat(c.root)
	return err == nil && info.IsDir()
}

func (c claudeSource) owns(path string) bool {
	return pathWithinRoot(path, c.root) && strings.HasSuffix(filepath.Base(path), ".jsonl")
}

func (c claudeSource) discover() []string {
	var out []string
	projects, err := os.ReadDir(c.root)
	if err != nil {
		return out
	}
	for _, pj := range projects {
		if !pj.IsDir() {
			continue
		}
		pdir := filepath.Join(c.root, pj.Name())
		if matches, _ := filepath.Glob(filepath.Join(pdir, "*.jsonl")); matches != nil {
			out = append(out, matches...)
		}
		if matches, _ := filepath.Glob(filepath.Join(pdir, "*", "subagents", "agent-*.jsonl")); matches != nil {
			out = append(out, matches...)
		}
	}
	return out
}

func (c claudeSource) headSignature(path string) string { return headSig(path) }

func (c claudeSource) sessionIDFor(path string) string {
	if strings.Contains(path, "/subagents/") {
		// .../projects/<proj>/<session-uuid>/subagents/agent-<id>.jsonl
		sessionUUID := filepath.Base(filepath.Dir(filepath.Dir(path)))
		agentID := strings.TrimPrefix(strings.TrimSuffix(filepath.Base(path), ".jsonl"), "agent-")
		return "claude:" + sessionUUID + ":subagent:" + agentID
	}
	return "claude:" + strings.TrimSuffix(filepath.Base(path), ".jsonl")
}

type claudeRecord struct {
	Type        string          `json:"type"`
	UUID        string          `json:"uuid"`
	ParentUUID  string          `json:"parentUuid"`
	Timestamp   string          `json:"timestamp"`
	Cwd         string          `json:"cwd"`
	IsSidechain bool            `json:"isSidechain"`
	Message     json.RawMessage `json:"message"`
}

var claudeNoiseTypes = map[string]bool{
	"attachment": true, "permission-mode": true, "file-history-snapshot": true,
	"deferred_tools_delta": true, "progress": true, "last-prompt": true,
	"queue-operation": true, "system": true,
}

func (c claudeSource) iterMessages(path string, startOffset int64) ([]searchableMessage, int64) {
	if !c.enabled() {
		return nil, startOffset
	}
	isSub := strings.Contains(path, "/subagents/")
	sid := c.sessionIDFor(path)
	stem := strings.TrimSuffix(filepath.Base(path), ".jsonl")
	var cwdCache string
	cwdResolved := false
	var msgs []searchableMessage

	final := lineReader(path, startOffset, func(raw []byte, lineNum int, _ int64) {
		var rec claudeRecord
		if json.Unmarshal(raw, &rec) != nil {
			return
		}
		if !cwdResolved && rec.Cwd != "" {
			cwdCache = rec.Cwd
			cwdResolved = true
		}
		if claudeNoiseTypes[rec.Type] {
			return
		}
		if rec.Type != "user" && rec.Type != "assistant" {
			return
		}
		if rec.Type == "user" && rec.IsSidechain {
			return
		}
		text := extractClaudeText(rec.Message)
		if text == "" {
			return
		}
		msgUUID := rec.UUID
		if msgUUID == "" {
			msgUUID = stem + ":line:" + itoa(lineNum)
		}
		msgs = append(msgs, searchableMessage{
			Source: "claude", SessionID: sid, MsgUUID: msgUUID,
			ParentUUID: rec.ParentUUID, Role: rec.Type, Timestamp: rec.Timestamp,
			Text: text, IsSubagent: isSub, Cwd: cwdCache,
		})
	})
	return msgs, final
}

// extractClaudeText pulls text from message.content (string or block list),
// then strips framework wrappers. Mirrors sources/claude.py _extract_text.
func extractClaudeText(rawMsg json.RawMessage) string {
	if len(rawMsg) == 0 {
		return ""
	}
	var m struct {
		Content json.RawMessage `json:"content"`
	}
	if json.Unmarshal(rawMsg, &m) != nil || len(m.Content) == 0 {
		return ""
	}
	return blocksToText(m.Content, []string{"text"})
}

func (c claudeSource) sessionMeta(path string) sessionMeta {
	meta := sessionMeta{}
	if strings.Contains(path, "/subagents/") {
		meta.ProjectDir = filepath.Dir(filepath.Dir(filepath.Dir(path)))
	} else {
		meta.ProjectDir = filepath.Dir(path)
	}
	cwdSet := false
	lineReaderUntil(path, 0, func(raw []byte, _ int, _ int64) bool {
		var rec claudeRecord
		if json.Unmarshal(raw, &rec) != nil {
			return false
		}
		if !cwdSet && rec.Cwd != "" {
			meta.Cwd = rec.Cwd
			cwdSet = true
		}
		if meta.FirstTS == "" && rec.Timestamp != "" {
			meta.FirstTS = rec.Timestamp
		}
		if meta.DisplayName == "" && rec.Type == "user" {
			text := extractClaudeText(rec.Message)
			if text != "" && !isFrameworkNoise(text) {
				meta.DisplayName = collapseWS(text, 80)
			}
		}
		return meta.Cwd != "" && meta.FirstTS != "" && meta.DisplayName != ""
	})
	return meta
}

// --- Codex source -----------------------------------------------------------

type codexSource struct {
	root               string
	ignoreCWDGlobs     []string
	ignoreNamePrefixes []string
}

func newCodexSource() codexSource {
	return codexSource{
		root:               sourcepolicy.CodexSessionsDir(),
		ignoreCWDGlobs:     sourcepolicy.CodexIgnoreCWDGlobs(),
		ignoreNamePrefixes: sourcepolicy.CodexIgnoreNamePrefixes(),
	}
}

func (c codexSource) name() string { return "codex" }

func (c codexSource) enabled() bool {
	info, err := os.Stat(c.root)
	return err == nil && info.IsDir()
}

func (c codexSource) owns(path string) bool {
	name := filepath.Base(path)
	return pathWithinRoot(path, c.root) && strings.HasPrefix(name, "rollout-") && strings.HasSuffix(name, ".jsonl")
}

func (c codexSource) discover() []string {
	matches, _ := filepath.Glob(filepath.Join(c.root, "*", "*", "*", "rollout-*.jsonl"))
	return matches
}

func (c codexSource) includeMeta(meta sessionMeta) bool {
	return !sourcepolicy.IgnoreCodexSession(
		meta.Cwd,
		meta.DisplayName,
		c.ignoreCWDGlobs,
		c.ignoreNamePrefixes,
	)
}

func (c codexSource) headSignature(path string) string { return headSig(path) }

func (c codexSource) sessionIDFor(path string) string {
	return "codex:" + strings.TrimSuffix(filepath.Base(path), ".jsonl")
}

type codexRecord struct {
	Type        string          `json:"type"`
	UUID        string          `json:"uuid"`
	ParentUUID  string          `json:"parent_uuid"`
	ParentUUID2 string          `json:"parentUuid"`
	Timestamp   string          `json:"timestamp"`
	Payload     json.RawMessage `json:"payload"`
}

type codexPayload struct {
	Type    string          `json:"type"`
	Role    string          `json:"role"`
	Phase   string          `json:"phase"`
	Cwd     string          `json:"cwd"`
	Content json.RawMessage `json:"content"`
}

func (c codexSource) iterMessages(path string, startOffset int64) ([]searchableMessage, int64) {
	sid := c.sessionIDFor(path)
	stem := strings.TrimSuffix(filepath.Base(path), ".jsonl")
	var cwdCache string
	cwdResolved := false
	var msgs []searchableMessage

	final := lineReader(path, startOffset, func(raw []byte, _ int, nextOffset int64) {
		var rec codexRecord
		if json.Unmarshal(raw, &rec) != nil {
			return
		}
		if !cwdResolved && rec.Type == "session_meta" {
			var pl codexPayload
			if json.Unmarshal(rec.Payload, &pl) == nil {
				cwdCache = pl.Cwd
				cwdResolved = true
			}
			return
		}
		if rec.Type != "response_item" {
			return
		}
		var pl codexPayload
		if json.Unmarshal(rec.Payload, &pl) != nil || pl.Type != "message" {
			return
		}
		if pl.Role != "user" && pl.Role != "assistant" {
			return
		}
		if pl.Role == "assistant" && pl.Phase == "commentary" {
			return
		}
		text := blocksToText(pl.Content, []string{"text", "input_text", "output_text"})
		if text == "" {
			return
		}
		if pl.Role == "user" && (isCodexBootstrap(text) || isFrameworkNoise(text)) {
			return
		}
		msgUUID := rec.UUID
		if msgUUID == "" {
			// Incremental reads restart their local line counter at 1. Using that
			// counter made later batches collide with the first batch, so SQLite
			// silently discarded every new UUID-less Codex message.
			msgUUID = stem + ":offset:" + strconv.FormatInt(nextOffset, 10)
		}
		parent := rec.ParentUUID
		if parent == "" {
			parent = rec.ParentUUID2
		}
		msgs = append(msgs, searchableMessage{
			Source: "codex", SessionID: sid, MsgUUID: msgUUID,
			ParentUUID: parent, Role: pl.Role, Timestamp: rec.Timestamp,
			Text: text, IsSubagent: false, Cwd: cwdCache,
		})
	})
	return msgs, final
}

func (c codexSource) sessionMeta(path string) sessionMeta {
	meta := sessionMeta{ProjectDir: filepath.Dir(path)}
	cwdSet := false
	lineReaderUntil(path, 0, func(raw []byte, _ int, _ int64) bool {
		var rec codexRecord
		if json.Unmarshal(raw, &rec) != nil {
			return false
		}
		if meta.FirstTS == "" && rec.Timestamp != "" {
			meta.FirstTS = rec.Timestamp
		}
		if !cwdSet && rec.Type == "session_meta" {
			var pl codexPayload
			if json.Unmarshal(rec.Payload, &pl) == nil {
				meta.Cwd = pl.Cwd
				cwdSet = true
			}
		}
		if meta.DisplayName == "" && rec.Type == "response_item" {
			var pl codexPayload
			if json.Unmarshal(rec.Payload, &pl) == nil && pl.Type == "message" && pl.Role == "user" {
				text := blocksToText(pl.Content, []string{"text", "input_text", "output_text"})
				if text != "" && !isCodexBootstrap(text) && !isFrameworkNoise(text) {
					meta.DisplayName = collapseWS(text, 80)
				}
			}
		}
		return meta.Cwd != "" && meta.FirstTS != "" && meta.DisplayName != ""
	})
	return meta
}

func isCodexBootstrap(text string) bool {
	s := strings.TrimLeft(text, " \t\r\n")
	return strings.HasPrefix(s, "# AGENTS.md instructions") &&
		strings.Contains(s, "<environment_context>") &&
		strings.Contains(s, "<INSTRUCTIONS>")
}

// pathWithinRoot resolves symlinks before comparing paths. Dirty paths enter
// the indexer through a local child-process boundary, so the source must still
// reject traversal and symlink escapes rather than trusting its caller.
func pathWithinRoot(path, root string) bool {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(root) == "" {
		return false
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(resolvedRoot, resolvedPath)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// blocksToText extracts text from a content field that is either a JSON string
// or a list of {type,text} blocks, joining wanted block types with newlines,
// then strips framework wrappers.
func blocksToText(raw json.RawMessage, wantTypes []string) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return stripFrameworkWrappers(strings.TrimSpace(s))
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) != nil {
		return ""
	}
	want := map[string]bool{}
	for _, t := range wantTypes {
		want[t] = true
	}
	var parts []string
	for _, b := range blocks {
		if want[b.Type] {
			if t := strings.TrimSpace(b.Text); t != "" {
				parts = append(parts, t)
			}
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return stripFrameworkWrappers(strings.Join(parts, "\n"))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
