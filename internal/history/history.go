// Package history owns transcript slicing, resumable-session discovery shapes,
// and the canonical content hash used for client-side dedup. It intentionally
// preserves the Python bridge's history message shape so existing clients can
// replay mixed backend histories without per-backend branching.
package history

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

const (
	defaultLimit = 100
	maxLimit     = 10000
)

// Opts mirror the request_history parameters.
type Opts struct {
	Limit     int
	KnownLast string // known_last_source_message_id
	Mode      string // "auto" | "delta" | "snapshot"
	Before    string // before_source_message_id
	// IncludeThinking keeps assistant thinking blocks in the reply. Off by
	// default: legacy clients validate history blocks with a strict
	// text/tool_call union and would reject the whole event otherwise.
	IncludeThinking            bool
	Scoped                     bool   // opt-in authenticated continuity pages
	DeferredAssistantRequestID string // exact running request; native tool outputs can still append
}

// Result is the output of Slice — either a snapshot or a delta page.
type Result struct {
	Kind              string // "snapshot" | "delta"
	Messages          []map[string]any
	SourceCount       int
	KnownIDFound      bool
	SnapshotReason    string
	HasMoreBefore     bool
	HasMoreAfter      bool
	SourceGeneration  string
	SourceGenerations map[string]string
}

// ResumableSession is one entry in the resumable_sessions list.
type ResumableSession struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	ClaudeUUID string `json:"claude_uuid"`
	LastUsed   int64  `json:"last_used"`
	Cwd        string `json:"cwd"`
	Backend    string `json:"backend,omitempty"`
}

// Provider is the optional history capability an executor may implement.
type Provider interface {
	LoadHistory(resumeID string, opts Opts) (*Result, error)
	ResumableSessions(limit int) ([]ResumableSession, error)
}

func ClampLimit(n int) int {
	if n <= 0 {
		return defaultLimit
	}
	if n > maxLimit {
		return maxLimit
	}
	return n
}

func normalizeText(s string) string { return strings.TrimRight(s, " \t\r\n\v\f") }

// canonicalJSON marshals like Python json.dumps(sort_keys=True,
// separators=(",",":"), ensure_ascii=False): sorted keys, compact, and crucially
// WITHOUT Go's default HTML escaping of < > &.
func canonicalJSON(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// ContentHash reproduces canonical_content_hash from history.py.
func ContentHash(role, content string, blocks []map[string]any) string {
	normBlocks := make([]map[string]any, 0, len(blocks))
	for _, b := range blocks {
		switch b["type"] {
		case "text":
			normBlocks = append(normBlocks, map[string]any{
				"type": "text", "text": normalizeText(asString(b["text"])),
			})
		case "tool_call":
			normBlocks = append(normBlocks, map[string]any{
				"type":        "tool_call",
				"tool_use_id": asString(b["tool_use_id"]),
				"name":        asString(b["name"]),
				"command":     asString(b["command"]),
				"output":      normalizeText(asString(b["output"])),
			})
		case "thinking":
			normBlocks = append(normBlocks, map[string]any{
				"type": "thinking", "thinking": normalizeText(asString(b["thinking"])),
			})
		}
	}
	payload := map[string]any{
		"role":             role,
		"normalizedText":   normalizeText(content),
		"normalizedBlocks": normBlocks,
	}
	sum := sha256.Sum256(canonicalJSON(payload))
	return hex.EncodeToString(sum[:])
}

// CompleteMsg builds a wire history message (mirrors complete_history_message).
func CompleteMsg(source, sourceSessionID, sourceMessageID, role, content string, timestamp int64, blocks []map[string]any) map[string]any {
	m := map[string]any{
		"role":              role,
		"content":           content,
		"source":            source,
		"source_session_id": sourceSessionID,
		"source_message_id": sourceMessageID,
		"content_hash":      ContentHash(role, content, blocks),
	}
	if timestamp != 0 {
		m["timestamp"] = timestamp
	}
	if len(blocks) > 0 {
		m["blocks"] = blocks
	}
	return m
}

func sourceID(m map[string]any) string { return asString(m["source_message_id"]) }

// Slice reproduces slice_history from history.py.
func Slice(messages []map[string]any, opts Opts) *Result {
	limit := ClampLimit(opts.Limit)
	count := len(messages)

	if opts.Mode == "continuation" && opts.KnownLast != "" {
		if idx := indexOf(messages, opts.KnownLast); idx >= 0 {
			end := idx + limit
			if end > count {
				end = count
			}
			return &Result{Kind: "snapshot", Messages: messages[idx:end], SourceCount: count, KnownIDFound: true, SnapshotReason: "continuation_page", HasMoreBefore: idx > 0, HasMoreAfter: end < count}
		}
		return &Result{Kind: "snapshot", Messages: nil, SourceCount: count, KnownIDFound: false, SnapshotReason: "continuation_page"}
	}
	if opts.Mode == "beginning" {
		end := limit
		if end > count {
			end = count
		}
		return &Result{Kind: "snapshot", Messages: messages[:end], SourceCount: count, KnownIDFound: true, SnapshotReason: "beginning_page", HasMoreAfter: end < count}
	}
	if opts.Before != "" {
		idx := indexOf(messages, opts.Before)
		if idx >= 0 {
			start := idx - limit
			if start < 0 {
				start = 0
			}
			return &Result{Kind: "snapshot", Messages: messages[start:idx], SourceCount: count,
				KnownIDFound: true, SnapshotReason: "before_page", HasMoreBefore: start > 0}
		}
		return &Result{Kind: "snapshot", Messages: tail(messages, limit), SourceCount: count,
			KnownIDFound: false, SnapshotReason: "before_page", HasMoreBefore: count > limit}
	}

	if (opts.Mode == "auto" || opts.Mode == "delta") && opts.KnownLast != "" {
		idx := indexOf(messages, opts.KnownLast)
		if idx >= 0 {
			delta := messages[idx+1:]
			return &Result{Kind: "delta", Messages: tail(delta, limit), SourceCount: count,
				KnownIDFound: true, HasMoreBefore: true}
		}
		if opts.Mode == "delta" {
			return &Result{Kind: "snapshot", Messages: tail(messages, limit), SourceCount: count,
				KnownIDFound: false, SnapshotReason: "known_id_not_found", HasMoreBefore: count > limit}
		}
	}

	reason := "initial"
	if opts.Mode == "snapshot" {
		reason = "requested_snapshot"
	} else if opts.KnownLast != "" {
		reason = "known_id_not_found"
	}
	return &Result{Kind: "snapshot", Messages: tail(messages, limit), SourceCount: count,
		KnownIDFound: opts.KnownLast == "", SnapshotReason: reason, HasMoreBefore: count > limit}
}

// StripThinkingBlocks returns messages with assistant thinking blocks removed,
// for clients that did not opt in via include_thinking. Cached message maps are
// never mutated — a message whose blocks change is shallow-copied. Messages
// left with no blocks and no content (thinking-only rows) are dropped entirely,
// reproducing the legacy shape.
func StripThinkingBlocks(messages []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(messages))
	for _, m := range messages {
		raw, has := m["blocks"]
		if !has {
			out = append(out, m)
			continue
		}
		kept, removed := filterThinking(raw)
		if !removed {
			out = append(out, m)
			continue
		}
		if len(kept) == 0 && asString(m["content"]) == "" {
			continue
		}
		cp := make(map[string]any, len(m))
		for k, v := range m {
			cp[k] = v
		}
		if len(kept) == 0 {
			delete(cp, "blocks")
		} else {
			cp["blocks"] = kept
		}
		out = append(out, cp)
	}
	return out
}

// filterThinking handles both block slice shapes: []map[string]any (freshly
// built) and []any (round-tripped through the JSON cache).
func filterThinking(raw any) (kept []any, removed bool) {
	switch bs := raw.(type) {
	case []map[string]any:
		for _, b := range bs {
			if asString(b["type"]) == "thinking" {
				removed = true
				continue
			}
			kept = append(kept, b)
		}
	case []any:
		for _, b := range bs {
			if bm, ok := b.(map[string]any); ok && asString(bm["type"]) == "thinking" {
				removed = true
				continue
			}
			kept = append(kept, b)
		}
	default:
		kept = append(kept, raw)
	}
	return kept, removed
}

func indexOf(messages []map[string]any, sourceMessageID string) int {
	for i, m := range messages {
		if sourceID(m) == sourceMessageID {
			return i
		}
	}
	return -1
}

func tail(messages []map[string]any, limit int) []map[string]any {
	if len(messages) > limit {
		return messages[len(messages)-limit:]
	}
	return messages
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
