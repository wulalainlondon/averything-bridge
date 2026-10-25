package goexec

// Keep provider item identities through the outer-tool history projection.
// Legacy entries without an ID retain their original combined text shape.
func appendNativeCommentaryBlock(blocks []map[string]any, text, id string) []map[string]any {
	if id == "" && len(blocks) > 0 && blocks[len(blocks)-1]["source_item_id"] == nil {
		blocks[len(blocks)-1]["text"] = appendCodexHistoryCommentary(blocks[len(blocks)-1]["text"].(string), text)
	} else {
		block := map[string]any{"type": "text", "text": text}
		if id != "" {
			block["source_item_id"] = id
		}
		blocks = append(blocks, block)
	}
	// Bound the combined pending run while retaining each surviving item ID.
	remaining := codexHistoryCommentaryMaxRunes
	for i := len(blocks) - 1; i >= 0; i-- {
		runes := []rune(blocks[i]["text"].(string))
		if len(runes) > remaining {
			if remaining <= 1 {
				return blocks[i+1:]
			}
			blocks[i]["text"] = "…" + string(runes[len(runes)-remaining+1:])
			return blocks[i:]
		}
		remaining -= len(runes) + 2
	}
	return blocks
}
