package workitems

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

const (
	contextPackVersion       = 1
	defaultContextCharacters = 24_000
	maxContextComments       = 20
	maxContextRuns           = 10
)

const contextExecutionContract = "\n## Execution contract\nWork only toward the desired outcome and acceptance criteria. Record durable progress through the Bridge Work API when useful. Never mark this item done; only the user can accept it.\n"

// BuildContextPack compiles the complete, ordered WorkItem context used by
// every execution surface. It is intentionally deterministic so retries and
// Bridge restarts submit equivalent instructions.
func (s *Store) BuildContextPack(ctx context.Context, itemID string, maxCharacters int) (ContextPack, error) {
	if maxCharacters <= 0 {
		maxCharacters = defaultContextCharacters
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return ContextPack{}, err
	}
	pack := ContextPack{Version: contextPackVersion, GeneratedAt: s.now().UnixMilli()}
	for _, item := range snapshot.Items {
		if item.ID == itemID {
			pack.Item = item
			break
		}
	}
	if pack.Item.ID == "" {
		return ContextPack{}, ErrNotFound
	}
	for _, project := range snapshot.Projects {
		if project.ID == pack.Item.ProjectID {
			pack.Project = project
			break
		}
	}
	dependencyIDs := map[string]bool{}
	for _, dependency := range snapshot.Dependencies {
		if dependency.WorkItemID == itemID {
			dependencyIDs[dependency.DependsOn] = true
		}
	}
	for _, item := range snapshot.Items {
		if dependencyIDs[item.ID] {
			pack.Dependencies = append(pack.Dependencies, item)
		}
	}
	for _, comment := range snapshot.Comments {
		if comment.WorkItemID == itemID && comment.DeletedAt == nil {
			pack.Comments = append(pack.Comments, comment)
		}
	}
	for _, run := range snapshot.Runs {
		if run.WorkItemID == itemID {
			pack.Runs = append(pack.Runs, run)
		}
	}
	for _, attachment := range snapshot.Attachments {
		if attachment.WorkItemID == itemID && attachment.RemovedAt == nil {
			pack.Attachments = append(pack.Attachments, attachment)
		}
	}
	for _, link := range snapshot.SessionLinks {
		if link.WorkItemID == itemID && link.UnlinkedAt == nil {
			pack.SessionLinks = append(pack.SessionLinks, link)
		}
	}
	sort.Slice(pack.Dependencies, func(i, j int) bool { return pack.Dependencies[i].ID < pack.Dependencies[j].ID })
	sort.Slice(pack.Comments, func(i, j int) bool {
		if pack.Comments[i].CreatedAt == pack.Comments[j].CreatedAt {
			return pack.Comments[i].ID < pack.Comments[j].ID
		}
		return pack.Comments[i].CreatedAt < pack.Comments[j].CreatedAt
	})
	if len(pack.Comments) > maxContextComments {
		pack.Comments = append([]Comment(nil), pack.Comments[len(pack.Comments)-maxContextComments:]...)
		pack.Truncated = true
	}
	sort.Slice(pack.Runs, func(i, j int) bool {
		if pack.Runs[i].StartedAt == pack.Runs[j].StartedAt {
			return pack.Runs[i].ID < pack.Runs[j].ID
		}
		return pack.Runs[i].StartedAt < pack.Runs[j].StartedAt
	})
	if len(pack.Runs) > maxContextRuns {
		pack.Runs = append([]Run(nil), pack.Runs[len(pack.Runs)-maxContextRuns:]...)
		pack.Truncated = true
	}
	sort.Slice(pack.Attachments, func(i, j int) bool {
		if pack.Attachments[i].SortKey == pack.Attachments[j].SortKey {
			return pack.Attachments[i].ID < pack.Attachments[j].ID
		}
		return pack.Attachments[i].SortKey < pack.Attachments[j].SortKey
	})
	sort.Slice(pack.SessionLinks, func(i, j int) bool {
		if pack.SessionLinks[i].LinkedAt == pack.SessionLinks[j].LinkedAt {
			return pack.SessionLinks[i].ID < pack.SessionLinks[j].ID
		}
		return pack.SessionLinks[i].LinkedAt < pack.SessionLinks[j].LinkedAt
	})
	pack.Prompt, pack.Truncated = RenderContextPrompt(pack, maxCharacters, pack.Truncated)
	return pack, nil
}

// RenderContextPrompt rebuilds a pack after core has materialized attachment
// availability and URLs for the current connection environment.
func RenderContextPrompt(pack ContextPack, maxCharacters int, alreadyTruncated bool) (string, bool) {
	if maxCharacters <= 0 {
		maxCharacters = defaultContextCharacters
	}
	var b strings.Builder
	writeSection := func(title, body string) {
		body = strings.TrimSpace(body)
		if body == "" {
			return
		}
		fmt.Fprintf(&b, "\n## %s\n%s\n", title, body)
	}
	b.WriteString("[Bridge Work Item]\nTitle: ")
	b.WriteString(pack.Item.Title)
	b.WriteString("\n[Bridge Work Context v1]\n")
	fmt.Fprintf(&b, "Work item: %s\nProject: %s\nWorkspace: %s\nLifecycle: %s\nPriority: %s\n",
		pack.Item.ID, pack.Project.Name, pack.Project.WorkspacePath, pack.Item.Lifecycle, pack.Item.Priority)
	writeSection("Title", pack.Item.Title)
	writeSection("Desired outcome", pack.Item.Outcome)
	writeSection("Current next step", pack.Item.NextStep)
	writeSection("Acceptance criteria", pack.Item.AcceptanceCriteria)
	writeSection("Project context", pack.Project.Context)
	writeSection("Background", pack.Item.Description)
	if pack.Item.BlockedReasonCode != "" || pack.Item.BlockedNote != "" {
		writeSection("Current blocker", strings.TrimSpace(pack.Item.BlockedReasonCode+"\n"+pack.Item.BlockedNote))
	}
	if len(pack.Dependencies) > 0 {
		var lines []string
		for _, item := range pack.Dependencies {
			lines = append(lines, fmt.Sprintf("- %s [%s]: %s — outcome: %s", item.ID, item.Lifecycle, item.Title, item.Outcome))
		}
		writeSection("Dependencies", strings.Join(lines, "\n"))
	}
	if len(pack.Comments) > 0 {
		var lines []string
		for _, comment := range pack.Comments {
			lines = append(lines, fmt.Sprintf("- %s (%s): %s", comment.ID, comment.AuthorType, comment.Body))
		}
		writeSection("Recent decisions and comments", strings.Join(lines, "\n"))
	}
	if len(pack.Attachments) > 0 {
		var lines []string
		for _, attachment := range pack.Attachments {
			lines = append(lines, fmt.Sprintf("- %s: %s [%s] %s", attachment.AttachmentID, attachment.DisplayName, attachment.Status, attachment.URL))
		}
		writeSection("Attachments", strings.Join(lines, "\n"))
	}
	if len(pack.Runs) > 0 {
		var lines []string
		for _, run := range pack.Runs {
			lines = append(lines, fmt.Sprintf("- %s (%s/%s): %s", run.ID, run.Kind, run.Status, run.TerminalReason))
		}
		writeSection("Previous runs", strings.Join(lines, "\n"))
	}
	if len(pack.SessionLinks) > 0 {
		var lines []string
		for _, link := range pack.SessionLinks {
			lines = append(lines, fmt.Sprintf("- %s role=%s thread=%s", link.SessionID, link.Role, link.ThreadIDSnapshot))
		}
		writeSection("Linked execution sessions", strings.Join(lines, "\n"))
	}
	b.WriteString(contextExecutionContract)
	prompt := b.String()
	if len([]rune(prompt)) <= maxCharacters {
		return prompt, alreadyTruncated
	}
	marker := "\n\n[Context truncated to budget]\n"
	contract := []rune(contextExecutionContract)
	markerRunes := []rune(marker)
	if maxCharacters <= len(contract)+len(markerRunes) {
		return string(append(markerRunes, contract...)), true
	}
	runes := []rune(strings.TrimSuffix(prompt, contextExecutionContract))
	bodyBudget := maxCharacters - len(contract) - len(markerRunes)
	return string(runes[:bodyBudget]) + marker + contextExecutionContract, true
}
