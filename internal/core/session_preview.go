package core

import (
	"regexp"
	"strings"

	"everything-go/internal/protocol"
	"everything-go/internal/search"
	"github.com/rivo/uniseg"
)

const sessionPreviewPolicyVersion = 2

var previewWhitespace = regexp.MustCompile(`\s+`)

// selectSessionPreview is the sole row-preview selection policy. Active work
// describes the current user request, waiting work shows the assistant's
// question, and terminal/idle work shows the latest assistant result with a
// user fallback. Clients render this projection instead of reselecting a
// different message from their local cache.
func selectSessionPreview(messages []protocol.RecentMessage, phase string) (text, role string) {
	want := "assistant"
	if phase == "queued" || phase == "running" || phase == "stopping" {
		want = "user"
	}
	if value := lastPreviewByRole(messages, want); value != "" {
		return value, want
	}
	fallback := "user"
	if want == "user" {
		fallback = "assistant"
	}
	return lastPreviewByRole(messages, fallback), fallback
}

// resolveSessionPreview arbitrates the two legitimate preview producers. A
// Bridge-owned accepted/completed turn is persisted in the Registry; the
// search index may only supersede it when an external CLI wrote a newer row.
func resolveSessionPreview(persistedText, persistedRole string, persistedAt int64, messages []protocol.RecentMessage, indexed *search.SessionPreview, phase string) (text, role string, updatedAt int64) {
	text, role, updatedAt = persistedText, persistedRole, persistedAt
	if indexed == nil {
		return
	}
	indexedText, indexedRole := selectSessionPreview(messages, phase)
	indexedAt := indexed.LastAssistantTS * 1000
	if indexedRole == "user" {
		indexedAt = indexed.LastUserTS * 1000
	}
	if indexedText != "" && (text == "" || indexedAt > updatedAt) {
		return indexedText, indexedRole, indexedAt
	}
	return
}

// historyAssistantPreview extracts only human-facing assistant text. Codex
// history intentionally exposes tool calls as assistant messages so the chat
// can render command cards; their content field is the command itself and must
// never become a Session-row preview. Commentary attached to a tool call is a
// text block and remains eligible.
func historyAssistantPreview(message map[string]any) string {
	blocks, hasBlocks := message["blocks"]
	if hasBlocks {
		var textParts []string
		appendBlock := func(block map[string]any) {
			if kind, _ := block["type"].(string); kind != "text" {
				return
			}
			if value, _ := block["text"].(string); strings.TrimSpace(value) != "" {
				textParts = append(textParts, value)
			}
		}
		switch values := blocks.(type) {
		case []map[string]any:
			for _, block := range values {
				appendBlock(block)
			}
		case []any:
			for _, value := range values {
				if block, ok := value.(map[string]any); ok {
					appendBlock(block)
				}
			}
		}
		return strings.Join(textParts, "\n")
	}
	text, _ := message["content"].(string)
	return text
}

func lastPreviewByRole(messages []protocol.RecentMessage, role string) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != role {
			continue
		}
		text := normalizePreviewText(messages[i].Text)
		if text != "" {
			return truncateGraphemes(text, 160)
		}
	}
	return ""
}

func normalizePreviewText(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.TrimSpace(previewWhitespace.ReplaceAllString(value, " "))
}

func truncateGraphemes(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	g := uniseg.NewGraphemes(value)
	var b strings.Builder
	for count := 0; count < limit && g.Next(); count++ {
		b.WriteString(g.Str())
	}
	return b.String()
}
