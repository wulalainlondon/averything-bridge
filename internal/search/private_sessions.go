package search

import "encoding/json"

// ProtectNativeSession keeps room transcripts out of the legacy global index
// views. Room search has its own current-membership and source-version checks.
// The host supplies provider-observed native identities; callers cannot do so.
func (idx *Index) ProtectNativeSession(uid string) {
	if uid == "" {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.privateUIDs == nil {
		idx.privateUIDs = map[string]bool{}
	}
	idx.privateUIDs[uid] = true
}

func (idx *Index) publicSessionClause(alias string) (string, []any) {
	// Generated workdirs also close the interval before a provider's first
	// thread identity is observed, and protect historical orphaned index rows.
	clause := "COALESCE(" + alias + ".cwd,'') NOT LIKE '%/collaboration-workspace-v1/workdirs/%' AND COALESCE(" + alias + ".cwd,'') NOT LIKE '%/.bridge-workspaces/%'"
	idx.mu.Lock()
	defer idx.mu.Unlock()
	ids := make([]string, 0, len(idx.privateUIDs))
	for uid := range idx.privateUIDs {
		ids = append(ids, uid)
	}
	// One bound JSON array avoids SQLite's expression-depth/parameter limits
	// when many rooms have original native threads.
	encoded, _ := json.Marshal(ids)
	clause += " AND NOT EXISTS(SELECT 1 FROM json_each(?) private WHERE instr(" + alias + ".session_id,private.value)>0)"
	return "(" + clause + ")", []any{string(encoded)}
}
