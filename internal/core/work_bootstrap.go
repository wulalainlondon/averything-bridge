package core

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"strings"

	"everything-go/internal/clientproto"
	"everything-go/internal/projectbootstrap"
	"everything-go/internal/protocol"
	"everything-go/internal/runtime"
	"everything-go/internal/search"
	"everything-go/internal/workitems"
)

func (h *Hub) generateProjectBootstrap(ctx context.Context, cmd clientproto.Command) (workitems.SaveBootstrapResult, error) {
	workspace := filepath.Clean(runtime.ExpandPath(strings.TrimSpace(cmd.WorkspacePath)))
	projectName := strings.TrimSpace(cmd.Name)
	projectID := strings.TrimSpace(cmd.ProjectID)
	projectVersion := uint64(0)
	formal := false
	if projectID != "" {
		project, err := h.work.GetProject(ctx, projectID)
		if err != nil {
			return workitems.SaveBootstrapResult{}, err
		}
		formal = true
		projectVersion = project.Version
		if workspace == "" || workspace == "." {
			workspace = filepath.Clean(runtime.ExpandPath(project.WorkspacePath))
		} else if project.WorkspacePath != "" && workspace != filepath.Clean(runtime.ExpandPath(project.WorkspacePath)) {
			return workitems.SaveBootstrapResult{}, errors.New("bootstrap workspace does not match the formal project")
		}
		if projectName == "" {
			projectName = project.Name
		}
	}
	if workspace == "" || workspace == "." || workspace == string(filepath.Separator) || !filepath.IsAbs(workspace) {
		return workitems.SaveBootstrapResult{}, errors.New("bootstrap workspace must be an absolute non-root directory")
	}
	if projectName == "" {
		projectName = filepath.Base(workspace)
	}

	requested := make(map[string]bool, len(cmd.SessionIDs))
	for _, id := range cmd.SessionIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			requested[id] = true
		}
	}
	type candidate struct {
		id           string
		name         string
		backend      string
		resumeID     string
		lastActivity float64
		messages     []projectbootstrap.SessionMessage
	}
	var candidates []candidate
	matchedRequested := make(map[string]bool, len(requested))
	for _, registered := range h.registry.List() {
		snapshot := registered.Snapshot()
		if snapshot.Hidden || filepath.Clean(runtime.ExpandPath(snapshot.Cwd)) != workspace {
			continue
		}
		if len(requested) > 0 && !requested[snapshot.ID] {
			continue
		}
		matchedRequested[snapshot.ID] = true
		candidates = append(candidates, candidate{id: snapshot.ID, name: snapshot.Name, backend: snapshot.Backend,
			resumeID: snapshot.ResumeID, lastActivity: snapshot.LastActivity})
	}
	if len(requested) > 0 && len(matchedRequested) != len(requested) {
		return workitems.SaveBootstrapResult{}, errors.New("one or more bootstrap Sessions do not belong to this workspace")
	}
	if !formal && len(candidates) == 0 {
		return workitems.SaveBootstrapResult{}, errors.New("folder project must be authorized by at least one Bridge Session")
	}
	if h.search != nil && len(candidates) > 0 {
		uids := make([]search.SessionUID, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate.resumeID != "" && candidate.backend != "" {
				uids = append(uids, search.SessionUID{HubID: candidate.id, Backend: candidate.backend, UID: candidate.resumeID})
			}
		}
		previews := h.search.RecentMessagesByUID(uids, 12)
		for i := range candidates {
			preview := previews[candidates[i].id]
			if preview == nil {
				continue
			}
			if preview.LastTS > 0 {
				candidates[i].lastActivity = float64(preview.LastTS)
			}
			for _, message := range preview.Recent {
				candidates[i].messages = append(candidates[i].messages,
					projectbootstrap.SessionMessage{Role: message.Role, Text: message.Text})
			}
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].lastActivity > candidates[j].lastActivity })
	if len(candidates) > 5 {
		candidates = candidates[:5]
	}
	sessions := make([]projectbootstrap.SessionEvidence, 0, len(candidates))
	for _, candidate := range candidates {
		sessions = append(sessions, projectbootstrap.SessionEvidence{ID: candidate.id, Name: candidate.name,
			Backend: candidate.backend, LastActivity: int64(candidate.lastActivity * 1000), Messages: candidate.messages})
	}
	readRoot, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return workitems.SaveBootstrapResult{}, err
	}
	buildCtx, cancel := context.WithTimeout(ctx, projectbootstrap.DefaultTimeout())
	defer cancel()
	inventory, err := projectbootstrap.Build(buildCtx, readRoot, projectName, sessions)
	if err != nil {
		return workitems.SaveBootstrapResult{}, err
	}
	sources := make([]workitems.BootstrapSource, 0, len(inventory.Sources))
	for _, source := range inventory.Sources {
		sources = append(sources, workitems.BootstrapSource{ID: source.ID, Kind: source.Kind, Label: source.Label,
			Path: source.Path, Excerpt: source.Excerpt, Fingerprint: source.Fingerprint, ModifiedAt: source.ModifiedAt})
	}
	suggestions := make([]workitems.BootstrapSuggestion, 0, len(inventory.Suggestions))
	for _, suggestion := range inventory.Suggestions {
		suggestions = append(suggestions, workitems.BootstrapSuggestion{ID: suggestion.ID, WorkItemID: suggestion.WorkItemID,
			SessionID: suggestion.SessionID, Title: suggestion.Title, Description: suggestion.Description,
			Outcome: suggestion.Outcome, NextStep: suggestion.NextStep, AcceptanceCriteria: suggestion.AcceptanceCriteria,
			EvidenceRefs: suggestion.EvidenceRefs})
	}
	sessionIDs := make([]string, 0, len(sessions))
	for _, session := range sessions {
		sessionIDs = append(sessionIDs, session.ID)
	}
	return h.work.SaveBootstrapDraft(ctx, workitems.SaveBootstrapInput{
		ProjectID: projectID, ProjectVersion: projectVersion, ProjectName: projectName, WorkspacePath: workspace,
		Fingerprint: inventory.Fingerprint, Objective: inventory.Objective, CurrentState: inventory.CurrentState,
		NextStep: inventory.NextStep, AcceptanceCriteria: inventory.AcceptanceCriteria,
		Constraints: inventory.Constraints, Decisions: inventory.Decisions, OpenQuestions: inventory.OpenQuestions,
		Suggestions: suggestions, Sources: sources, SessionIDs: sessionIDs,
	})
}

func (h *Hub) approveProjectBootstrap(ctx context.Context, cmd clientproto.Command, actor workitems.Actor) (workitems.ApproveBootstrapResult, error) {
	draft, err := h.work.GetBootstrap(ctx, cmd.BootstrapID)
	if err != nil {
		return workitems.ApproveBootstrapResult{}, err
	}
	selected := make(map[string]bool, len(cmd.SelectedSuggestionIDs))
	for _, id := range cmd.SelectedSuggestionIDs {
		selected[strings.TrimSpace(id)] = true
	}
	for _, suggestion := range draft.Suggestions {
		if !selected[suggestion.ID] || suggestion.SessionID == "" {
			continue
		}
		if _, ok := h.registry.Get(suggestion.SessionID); !ok {
			return workitems.ApproveBootstrapResult{}, errors.New("a selected bootstrap Session is no longer available")
		}
	}
	return h.work.ApproveBootstrap(ctx, workitems.ApproveBootstrapInput{
		BootstrapID: cmd.BootstrapID, ExpectedVersion: cmd.ExpectedVersion, ProjectName: cmd.Name,
		Objective: cmd.Objective, CurrentState: cmd.CurrentState, NextStep: cmd.NextStep,
		AcceptanceCriteria: cmd.AcceptanceCriteria, SelectedSuggestionIDs: cmd.SelectedSuggestionIDs, Actor: actor,
	})
}

func bootstrapAckRevision(ctx context.Context, service *workitems.Service) uint64 {
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		return 0
	}
	return snapshot.Revision
}

func bootstrapMutationAck(mutationID string, result workitems.ApproveBootstrapResult) protocol.WorkMutationAck {
	return protocol.WorkMutationAck{Type: "work_mutation_ack", MutationID: mutationID,
		EntityVersion: result.Draft.Version, Revision: result.LastRevision, Bootstrap: &result.Draft,
		Project: &result.Project, Items: result.Items, Links: result.Links}
}
