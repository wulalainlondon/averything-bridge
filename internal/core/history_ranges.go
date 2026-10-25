package core

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"fmt"
	"reflect"
	"strings"
)

type historyGenerationProvider interface{ HistorySourceGeneration(string) (string, error) }

func historySourceThread(cursor string, threads []string) int {
	for i, id := range threads {
		if strings.HasPrefix(cursor, "codex:"+id+":line:") || strings.HasPrefix(cursor, "claude:"+id+":line:") {
			return i
		}
	}
	return -1
}

// Apply the cursor once in the actual owning physical thread, then walk
// adjacent archived generations. No unknown cursor falls back to a new tail.
func loadScopedLogicalHistory(provider backend.HistoryProvider, ids []string, opts history.Opts) (*history.Result, error) {
	if len(ids) == 0 {
		return history.Slice(nil, opts), nil
	}
	limit := history.ClampLimit(opts.Limit)
	if opts.Mode == "continuation" && limit < 2 {
		limit = 2
	}
	opts.Limit = limit
	result := &history.Result{SourceGenerations: map[string]string{}, Kind: "snapshot", KnownIDFound: true, SnapshotReason: "requested_snapshot"}
	var total int
	if opts.Before != "" {
		at := historySourceThread(opts.Before, ids)
		if at < 0 {
			return &history.Result{Kind: "snapshot", SnapshotReason: "before_page"}, nil
		}
		for i := at; i >= 0; i-- {
			pageOpts := opts
			pageOpts.Limit = limit - len(result.Messages)
			if i != at {
				pageOpts.Before = ""
				pageOpts.Mode = "snapshot"
				pageOpts.KnownLast = ""
			}
			p, e := provider.LoadHistory(ids[i], pageOpts)
			if e != nil {
				return nil, e
			}
			if i == at && !p.KnownIDFound {
				return &history.Result{Kind: "snapshot", SnapshotReason: "before_page"}, nil
			}
			if p.SourceGeneration != "" {
				result.SourceGenerations[ids[i]] = p.SourceGeneration
			}
			total += p.SourceCount
			result.Messages = append(append([]map[string]any(nil), p.Messages...), result.Messages...)
			result.HasMoreBefore = p.HasMoreBefore || i > 0
			if p.HasMoreBefore || len(result.Messages) >= limit {
				break
			}
		}
		result.SourceCount = total
		result.SnapshotReason = "before_page"
		result.HasMoreAfter = true
		return result, nil
	}
	if opts.Mode == "continuation" {
		at := historySourceThread(opts.KnownLast, ids)
		if at < 0 {
			return &history.Result{Kind: "snapshot", SnapshotReason: "continuation_page"}, nil
		}
		for i := at; i < len(ids); i++ {
			pageOpts := opts
			pageOpts.Limit = limit - len(result.Messages)
			if i != at {
				pageOpts.Mode = "beginning"
				pageOpts.KnownLast = ""
			}
			p, e := provider.LoadHistory(ids[i], pageOpts)
			if e != nil {
				return nil, e
			}
			if i == at && !p.KnownIDFound {
				return &history.Result{Kind: "snapshot", SnapshotReason: "continuation_page"}, nil
			}
			if p.SourceGeneration != "" {
				result.SourceGenerations[ids[i]] = p.SourceGeneration
			}
			total += p.SourceCount
			result.Messages = append(result.Messages, p.Messages...)
			result.HasMoreAfter = p.HasMoreAfter || i < len(ids)-1
			if p.HasMoreAfter || len(result.Messages) >= limit {
				break
			}
		}
		result.SourceCount = total
		result.SnapshotReason = "continuation_page"
		result.HasMoreBefore = true
		return result, nil
	}
	for i := len(ids) - 1; i >= 0; i-- {
		pageOpts := opts
		pageOpts.Limit = limit - len(result.Messages)
		pageOpts.Mode = "snapshot"
		p, e := provider.LoadHistory(ids[i], pageOpts)
		if e != nil {
			return nil, e
		}
		if p.SourceGeneration != "" {
			result.SourceGenerations[ids[i]] = p.SourceGeneration
		}
		total += p.SourceCount
		result.Messages = append(append([]map[string]any(nil), p.Messages...), result.Messages...)
		result.HasMoreBefore = p.HasMoreBefore || i > 0
		if p.HasMoreBefore || len(result.Messages) >= limit {
			break
		}
	}
	result.SourceCount = total
	return result, nil
}

func (h *Hub) buildHistoryRangeProof(provider backend.HistoryProvider, s *session.Session, ids []string, active string, cmd clientproto.Command, res *history.Result) (*protocol.HistoryRangeProof, error) {
	if !cmd.HistoryContinuity || cmd.HistoryRequestID == "" {
		return nil, nil
	}
	if len(cmd.HistoryRequestID) > 128 || strings.TrimSpace(cmd.HistoryRequestID) != cmd.HistoryRequestID {
		return nil, fmt.Errorf("invalid history read correlation")
	}
	if active == "" || s.Snapshot().ResumeID != active || !reflect.DeepEqual(s.ResumeIDs(), ids) {
		return nil, fmt.Errorf("history source binding changed while reading")
	}
	source, ok := provider.(historyGenerationProvider)
	if !ok {
		return nil, fmt.Errorf("history provider lacks scoped source generations")
	}
	generations := map[string]string{}
	for _, id := range ids {
		gen, e := source.HistorySourceGeneration(id)
		if e != nil {
			return nil, e
		}
		if gen == "" || res.SourceGenerations[id] != "" && gen != res.SourceGenerations[id] {
			return nil, fmt.Errorf("history source generation changed while reading")
		}
		generations[id] = gen
	}
	if len(generations) == 0 {
		return nil, fmt.Errorf("history source is not available yet")
	}
	kind, cursor := "initial", ""
	if cmd.Before != "" {
		kind, cursor = "before", cmd.Before
	} else if cmd.Mode == "continuation" {
		kind, cursor = "continuation", cmd.KnownLast
	}
	first, last := "", ""
	seen := map[string]bool{}
	for i, m := range res.Messages {
		id, _ := m["source_message_id"].(string)
		native, _ := m["source_session_id"].(string)
		if id == "" || seen[id] || generations[native] == "" || historySourceThread(id, []string{native}) < 0 {
			return nil, fmt.Errorf("history source identity missing or duplicated")
		}
		seen[id] = true
		if i == 0 {
			first = id
		}
		last = id
	}
	if kind == "continuation" && res.KnownIDFound && (len(res.Messages) == 0 || first != cursor) {
		return nil, fmt.Errorf("history continuation did not include its canonical anchor")
	}
	return &protocol.HistoryRangeProof{Version: 1, RequestID: cmd.HistoryRequestID, Authority: h.cfg.InstanceID, SessionID: s.ID, NativeThread: active, Threads: ids, Generations: generations, ServerEpoch: h.gen, Sequence: h.historySequence.Add(1), Kind: kind, Cursor: cursor, First: first, Last: last, HasMoreBefore: res.HasMoreBefore, HasMoreAfter: res.HasMoreAfter, KnownIDFound: res.KnownIDFound}, nil
}

func (h *Hub) supportsNativeHistoryRanges() bool {
	router, ok := h.exec.(historyRouter)
	if !ok {
		return false
	}
	providers := router.AllProviders()
	if len(providers) == 0 {
		return false
	}
	for _, p := range providers {
		if _, ok := p.(historyGenerationProvider); !ok {
			return false
		}
	}
	return true
}

// Leave room below the native client's16MiB WebSocket frame ceiling for the
// scoped envelope and projected IDs. Trimming moves only a page boundary;
// no message body is truncated or silently skipped.
func boundScopedHistoryFrame(input *history.Result, cmd clientproto.Command) (*history.Result, error) {
	const budget = 12 << 20
	result := *input
	messages := input.Messages
	size := 0
	kept := 0
	if cmd.Mode == "continuation" {
		for _, m := range messages {
			raw, e := json.Marshal(m)
			if e != nil {
				return nil, e
			}
			if len(raw) > budget {
				return nil, fmt.Errorf("single history row exceeds bounded frame budget")
			}
			if size+len(raw) > budget {
				break
			}
			size += len(raw)
			kept++
		}
		if kept < len(messages) {
			if kept < 2 {
				return nil, fmt.Errorf("history anchor and next row exceed bounded frame budget")
			}
			result.Messages = messages[:kept]
			result.HasMoreAfter = true
		}
	} else {
		for i := len(messages) - 1; i >= 0; i-- {
			raw, e := json.Marshal(messages[i])
			if e != nil {
				return nil, e
			}
			if len(raw) > budget {
				return nil, fmt.Errorf("single history row exceeds bounded frame budget")
			}
			if size+len(raw) > budget {
				break
			}
			size += len(raw)
			kept++
		}
		if kept < len(messages) {
			result.Messages = messages[len(messages)-kept:]
			result.HasMoreBefore = true
		}
	}
	return &result, nil
}
