package goexec

import (
	"everything-go/internal/history"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func physicalHistoryCursor(kind, resumeID string, opts history.Opts) (int, bool) {
	id := opts.Before
	if id == "" && opts.Mode == "continuation" {
		id = opts.KnownLast
	}
	if id == "" {
		if opts.Mode == "beginning" {
			return 1, true
		}
		return 0, opts.Scoped
	}
	prefix := kind + ":" + resumeID + ":line:"
	if !strings.HasPrefix(id, prefix) {
		return 0, false
	}
	raw := strings.TrimPrefix(id, prefix)
	n, e := strconv.Atoi(raw)
	return n, e == nil && n > 0 && strconv.Itoa(n) == raw
}

func (c *Codex) HistorySourceGeneration(id string) (string, error) {
	path, e := c.findCodexSessionFileChecked(id)
	if e != nil {
		return "", e
	}
	if path == "" {
		return "", fmt.Errorf("history source not available")
	}
	if e = history.CheckFile("codex", path); e != nil {
		return "", e
	}
	if strings.HasSuffix(path, ".gz") {
		return history.ImmutableFileGeneration(path)
	}
	return history.FileWindowGeneration(path)
}
func (c *Claude) HistorySourceGeneration(id string) (string, error) {
	path, e := c.findSessionFileChecked(id)
	if e != nil {
		return "", e
	}
	if path == "" {
		return "", fmt.Errorf("history source not available")
	}
	if e = history.CheckFile("claude", path); e != nil {
		return "", e
	}
	return history.FileWindowGeneration(path)
}
func (c *Codex) historyFileWindow(path string, cursor int) (history.FileWindow, error) {
	if !strings.HasSuffix(path, ".gz") {
		return history.ReadFileWindow(path, cursor, history.LoadMaxBytes())
	}
	generation, e := history.ImmutableFileGeneration(path)
	if e != nil {
		return history.FileWindow{}, e
	}
	window, e := history.ReadStreamWindow(func() (io.Reader, func(), error) { return openCodexRollout(path) }, cursor, history.LoadMaxBytes())
	if e != nil {
		return window, e
	}
	current, e := history.ImmutableFileGeneration(path)
	if e != nil {
		return history.FileWindow{}, e
	}
	if current != generation {
		return history.FileWindow{}, fmt.Errorf("compressed history changed during read")
	}
	window.Generation = generation
	return window, nil
}

// Skip raw metadata-only gaps without inventing a canonical cursor. The client
// anchor is verified in the first window; subsequent physical windows overlap
// and advance strictly, with bounded memory and a finite source-scan budget.
func loadNativeWindowPage(kind, resumeID string, opts history.Opts, read func(int) (history.FileWindow, error), parse func(history.FileWindow) ([]map[string]any, error)) (*history.Result, error) {
	cursor, ok := physicalHistoryCursor(kind, resumeID, opts)
	if !ok {
		return nil, fmt.Errorf("invalid native history cursor")
	}
	window, e := read(cursor)
	if e != nil {
		return nil, e
	}
	generation := window.Generation
	messages, e := parse(window)
	if e != nil {
		return nil, e
	}
	result := history.Slice(messages, opts)
	if (opts.Before != "" || opts.Mode == "continuation") && !result.KnownIDFound {
		result.SourceGeneration = generation
		return result, nil
	}
	originalCursor := cursor
	previousPosition := -1
	var anchor map[string]any
	if opts.Mode == "continuation" && len(result.Messages) > 0 {
		anchor = result.Messages[0]
	}
	finished := false
	for scans := 0; scans < 256; scans++ {
		if opts.Mode == "continuation" {
			if len(result.Messages) >= history.ClampLimit(opts.Limit) || !window.MoreAfter {
				result.HasMoreAfter = result.HasMoreAfter || window.MoreAfter
				finished = true
				break
			}
			if window.EndLine <= previousPosition {
				return nil, fmt.Errorf("history physical continuation made no progress")
			}
			previousPosition = window.EndLine
			next, e := read(window.EndLine + 1)
			if e != nil {
				return nil, e
			}
			if next.Generation != generation {
				return nil, fmt.Errorf("history generation changed between windows")
			}
			window = next
			extra, e := parse(window)
			if e != nil {
				return nil, e
			}
			byID := map[string]int{}
			for i, m := range messages {
				byID[m["source_message_id"].(string)] = i
			}
			for _, m := range extra {
				source, _ := m["source_message_id"].(string)
				n, valid := physicalHistoryCursor(kind, resumeID, history.Opts{Before: source})
				if !valid || n < originalCursor {
					continue
				}
				if i, ok := byID[source]; ok {
					messages[i] = m
				} else {
					byID[source] = len(messages)
					messages = append(messages, m)
				}
			}
			if anchor != nil {
				found := false
				for _, m := range messages {
					if m["source_message_id"] == anchor["source_message_id"] {
						found = true
						break
					}
				}
				if !found {
					messages = append([]map[string]any{anchor}, messages...)
				}
			}
			result = history.Slice(messages, opts)
			continue
		}
		if len(result.Messages) > 0 || !window.MoreBefore {
			result.HasMoreBefore = result.HasMoreBefore || window.MoreBefore
			finished = true
			break
		}
		nextLine := window.StartLine - 1
		if nextLine < 1 || previousPosition >= 0 && nextLine >= previousPosition {
			return nil, fmt.Errorf("history physical backfill made no progress")
		}
		previousPosition = nextLine
		next, e := read(nextLine)
		if e != nil {
			return nil, e
		}
		if next.Generation != generation {
			return nil, fmt.Errorf("history generation changed between windows")
		}
		window = next
		messages, e = parse(window)
		if e != nil {
			return nil, e
		}
		if opts.Before != "" {
			kept := messages[:0]
			for _, m := range messages {
				source, _ := m["source_message_id"].(string)
				n, valid := physicalHistoryCursor(kind, resumeID, history.Opts{Before: source})
				if valid && n < originalCursor {
					kept = append(kept, m)
				}
			}
			messages = kept
		}
		sliceOpts := opts
		sliceOpts.Before = ""
		sliceOpts.KnownLast = ""
		sliceOpts.Mode = "snapshot"
		result = history.Slice(messages, sliceOpts)
		result.KnownIDFound = true
		if opts.Before != "" {
			result.SnapshotReason = "before_page"
		}
		// The newer empty windows were scanned already, so an initial tail located
		// in an older physical window still ends at the actual latest canonical row.
		if opts.Before == "" {
			result.HasMoreAfter = false
		}
		if scans == 255 {
			return nil, fmt.Errorf("history bounded scan requires a smaller source range")
		}
	}
	if !finished {
		return nil, fmt.Errorf("history bounded scan requires another source range")
	}
	if opts.Mode == "beginning" {
		result.HasMoreAfter = result.HasMoreAfter || window.MoreAfter
	}
	result.SourceGeneration = generation
	if opts.Before != "" {
		result.HasMoreAfter = true
	}
	return result, nil
}
