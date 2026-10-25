package core

import (
	"context"
	"everything-go/internal/delegation"
	"everything-go/internal/protocol"
	"time"
)

// Called only after sessionSummaries has applied its existing authority/root
// scope. Query only visible child IDs and never expose an out-of-scope parent.
// Failure preserves rows as unknown rather than silently losing conversations.
func (h *Hub) projectSessionSources(out []protocol.SessionSummary) {
	visible := make(map[string]int, len(out))
	ids := make([]string, 0, len(out))
	for i := range out {
		visible[out[i].ID] = i
		ids = append(ids, out[i].ID)
		out[i].SessionSource = &protocol.SessionSourceMetadata{Version: 1, Origin: "unknown", Coverage: "unknown", Relationships: []protocol.SessionSourceRelationship{}}
	}
	// Native identity already belongs to the authorized session registry;
	// it allows search-index IDs to correlate without reading transcript history.
	if h.registry != nil {
		for i := range out {
			if session, ok := h.registry.Get(out[i].ID); ok {
				out[i].SessionSource.NativeThreadID = session.Snapshot().ResumeID
			}
		}
	}
	if h.delegations == nil {
		for i := range out {
			out[i].SessionSource.MetadataError = "unavailable"
		}
		return
	}
	readContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows, err := h.delegations.SessionSources(readContext, ids)
	if err != nil {
		for i := range out {
			out[i].SessionSource.MetadataError = "unavailable"
		}
		return
	}
	nativeRequests := map[string]string{}
	nativePhases := map[string]string{}
	if h.runtimes != nil {
		for _, view := range h.runtimes.Snapshot("", ids) {
			nativeRequests[view.SessionID] = view.ActiveRequestID
			nativePhases[view.SessionID] = view.Phase
		}
	}
	byChild := make(map[string][]delegation.SessionSource)
	for _, row := range rows {
		byChild[row.ChildSessionID] = append(byChild[row.ChildSessionID], row)
	}
	for _, row := range rows {
		index, ok := visible[row.ChildSessionID]
		if !ok {
			continue
		}
		source := out[index].SessionSource
		source.Origin = "delegation"
		source.Coverage = "mapped"
		source.CreatedBy = "bridge_delegation"
		parent := ""
		_, available := visible[row.ParentSessionID]
		if available {
			parent = row.ParentSessionID
		}
		final := "unknown"
		if row.FinalObserved {
			final = "observed"
		}
		execution := "unknown"
		if nativeRequests[row.ChildSessionID] == row.ChildRequestID {
			execution = nativePhases[row.ChildSessionID]
		}
		root, lineage := sessionSourceRoot(row, byChild, visible)
		originRequest := ""
		if available {
			originRequest = row.OriginRequestID
		}
		source.Relationships = append(source.Relationships, protocol.SessionSourceRelationship{
			OriginRequestID: originRequest, RootSessionID: root, LineageStatus: lineage, LineageScope: "authorized_ledger",
			Execution: execution, DelegationID: row.ID, ParentSessionID: parent, ParentAvailable: available, ChildRequestID: row.ChildRequestID,
			State: row.State, TerminalStatus: row.TerminalStatus, Final: final, Delivery: row.DeliveryState, QA: "unknown", UpdatedAtUnixMS: row.UpdatedAt,
		})
	}
}

// A root is the first source of an exact request-bound chain in this authorized
// ledger projection. This does not claim complete historic/global ancestry.
// A parent with other incoming tasks cannot be assigned one arbitrary ancestor.
func sessionSourceRoot(row delegation.SessionSource, byChild map[string][]delegation.SessionSource, visible map[string]int) (string, string) {
	seen := map[string]bool{row.ChildSessionID: true}
	for {
		if row.ParentSessionID == "" {
			return "", "unknown"
		}
		if _, ok := visible[row.ParentSessionID]; !ok {
			return "", "incomplete"
		}
		if seen[row.ParentSessionID] {
			return "", "cycle"
		}
		seen[row.ParentSessionID] = true
		if row.OriginRequestID == "" {
			return "", "unknown"
		}
		incoming := byChild[row.ParentSessionID]
		if len(incoming) == 0 {
			return row.ParentSessionID, "known"
		}
		matches := []delegation.SessionSource{}
		for _, candidate := range incoming {
			if candidate.ChildRequestID == row.OriginRequestID {
				matches = append(matches, candidate)
			}
		}
		if len(matches) > 1 {
			return "", "ambiguous"
		}
		if len(matches) == 0 {
			return "", "unknown"
		}
		row = matches[0]
	}
}
