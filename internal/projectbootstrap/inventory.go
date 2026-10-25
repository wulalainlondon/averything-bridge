// Package projectbootstrap builds a bounded, read-only evidence bundle for a
// workspace. It deliberately avoids recursive watchers and arbitrary file
// traversal: Project Bootstrap is an explicit, finite operation.
package projectbootstrap

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	maxDocuments        = 8
	maxDocumentBytes    = 32 * 1024
	maxAllDocumentBytes = 128 * 1024
	maxEvidenceExcerpt  = 4 * 1024
	maxSessionPreview   = 1200
)

type SessionMessage struct {
	Role string `json:"role"`
	Text string `json:"text"`
}

type SessionEvidence struct {
	ID           string           `json:"id"`
	Name         string           `json:"name"`
	Backend      string           `json:"backend,omitempty"`
	LastActivity int64            `json:"last_activity"`
	Messages     []SessionMessage `json:"messages"`
}

type Source struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Label       string `json:"label"`
	Path        string `json:"path,omitempty"`
	Excerpt     string `json:"excerpt,omitempty"`
	Fingerprint string `json:"fingerprint"`
	ModifiedAt  int64  `json:"modified_at,omitempty"`
}

type Suggestion struct {
	ID                 string   `json:"id"`
	WorkItemID         string   `json:"work_item_id"`
	SessionID          string   `json:"session_id,omitempty"`
	Title              string   `json:"title"`
	Description        string   `json:"description,omitempty"`
	Outcome            string   `json:"outcome,omitempty"`
	NextStep           string   `json:"next_step,omitempty"`
	AcceptanceCriteria string   `json:"acceptance_criteria,omitempty"`
	EvidenceRefs       []string `json:"evidence_refs"`
}

type Result struct {
	Fingerprint        string       `json:"fingerprint"`
	Objective          string       `json:"objective"`
	CurrentState       string       `json:"current_state"`
	NextStep           string       `json:"next_step"`
	AcceptanceCriteria string       `json:"acceptance_criteria"`
	Constraints        []string     `json:"constraints"`
	Decisions          []string     `json:"decisions"`
	OpenQuestions      []string     `json:"open_questions"`
	Suggestions        []Suggestion `json:"suggestions"`
	Sources            []Source     `json:"sources"`
}

type document struct {
	rel     string
	content string
	info    os.FileInfo
}

// Build performs a finite inventory. workspace must already have been
// authorized from a server-owned Project or Session mapping by the caller.
func Build(ctx context.Context, workspace, projectName string, sessions []SessionEvidence) (Result, error) {
	workspace = filepath.Clean(strings.TrimSpace(workspace))
	if workspace == "" || workspace == "." || workspace == string(filepath.Separator) {
		return Result{}, errors.New("project workspace must be a non-root directory")
	}
	info, err := os.Stat(workspace)
	if err != nil {
		return Result{}, fmt.Errorf("project workspace: %w", err)
	}
	if !info.IsDir() {
		return Result{}, errors.New("project workspace is not a directory")
	}
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	default:
	}

	documents, err := readDocuments(workspace)
	if err != nil {
		return Result{}, err
	}
	if len(sessions) > 5 {
		sessions = sessions[:5]
	}

	result := Result{
		Constraints: []string{}, Decisions: []string{}, OpenQuestions: []string{},
		Suggestions: []Suggestion{}, Sources: []Source{},
	}
	var readme *document
	manifestNames := make([]string, 0, 4)
	for i := range documents {
		doc := &documents[i]
		source := Source{ID: "doc:" + doc.rel, Kind: "document", Label: doc.rel,
			Path: doc.rel, Excerpt: truncate(redact(doc.content), maxEvidenceExcerpt),
			Fingerprint: digest(doc.content), ModifiedAt: doc.info.ModTime().UnixMilli()}
		result.Sources = append(result.Sources, source)
		base := strings.ToLower(filepath.Base(doc.rel))
		if strings.HasPrefix(base, "readme") && readme == nil {
			readme = doc
		}
		if base == "agents.md" || base == "rtk.md" {
			result.Constraints = append(result.Constraints, "Follow the repository guidance in "+doc.rel+".")
		}
		if isManifest(base) {
			manifestNames = append(manifestNames, doc.rel)
		}
	}

	branch, head, status, logText, gitOK := gitEvidence(ctx, workspace)
	if gitOK {
		excerpt := strings.TrimSpace(fmt.Sprintf("Branch: %s\nHEAD: %s\nStatus:\n%s\nRecent commits:\n%s", branch, head, status, logText))
		result.Sources = append(result.Sources, Source{ID: "git:state", Kind: "git", Label: "Git repository state",
			Excerpt: truncate(redact(excerpt), maxEvidenceExcerpt), Fingerprint: digest(excerpt)})
		if len(strings.Fields(status)) == 0 {
			result.CurrentState = fmt.Sprintf("Git branch %s at %s has no tracked working-tree changes.", emptyAs(branch, "unknown"), emptyAs(head, "unknown"))
		} else {
			changes := len(strings.Split(strings.TrimSpace(status), "\n"))
			result.CurrentState = fmt.Sprintf("Git branch %s at %s has %d working-tree change(s).", emptyAs(branch, "unknown"), emptyAs(head, "unknown"), changes)
			result.OpenQuestions = append(result.OpenQuestions, "Confirm which uncommitted changes belong to the current project work.")
		}
	}

	if readme != nil {
		result.Objective = readmeObjective(readme.content)
	}
	if result.Objective == "" {
		if strings.TrimSpace(projectName) == "" {
			projectName = filepath.Base(workspace)
		}
		result.Objective = "Clarify and advance the documented work for " + projectName + "."
		result.OpenQuestions = append(result.OpenQuestions, "The project objective is not explicitly documented in a README.")
	}
	if len(manifestNames) > 0 {
		sort.Strings(manifestNames)
		result.Decisions = append(result.Decisions, "Project tooling is represented by "+strings.Join(manifestNames, ", ")+".")
	}

	sort.SliceStable(sessions, func(i, j int) bool { return sessions[i].LastActivity > sessions[j].LastActivity })
	for _, session := range sessions {
		preview := sessionPreview(session.Messages)
		sourceID := "session:" + session.ID
		body, _ := json.Marshal(session)
		result.Sources = append(result.Sources, Source{ID: sourceID, Kind: "session", Label: session.Name,
			Excerpt: truncate(redact(preview), maxSessionPreview), Fingerprint: digest(string(body)), ModifiedAt: session.LastActivity})
		if strings.TrimSpace(session.Name) == "" {
			continue
		}
		suggestionID := stableID("pbs", workspace+"\x00"+session.ID)
		result.Suggestions = append(result.Suggestions, Suggestion{
			ID: suggestionID, WorkItemID: stableID("wi", workspace+"\x00"+session.ID), SessionID: session.ID,
			Title: truncate(strings.TrimSpace(session.Name), 160), Description: truncate(preview, 1200),
			NextStep:           "Review the latest verified state in this conversation and choose the next concrete action.",
			AcceptanceCriteria: "The owner reviews the resulting evidence and explicitly accepts or redirects the work.",
			EvidenceRefs:       []string{sourceID},
		})
	}
	if len(sessions) > 0 {
		latest := sessions[0]
		preview := sessionPreview(latest.Messages)
		if preview != "" {
			if result.CurrentState != "" {
				result.CurrentState += " "
			}
			result.CurrentState += "Latest conversation: " + latest.Name + " — " + truncate(preview, 500)
		}
		result.NextStep = "Review and continue the most recent conversation: " + latest.Name + "."
	} else {
		result.OpenQuestions = append(result.OpenQuestions, "No recent Bridge Session evidence was available for this workspace.")
	}
	result.AcceptanceCriteria = acceptanceFor(documents)
	result.Fingerprint = resultFingerprint(result.Sources)
	return result, nil
}

func readDocuments(root string) ([]document, error) {
	var rels []string
	collect := func(dir, prefix string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		for _, entry := range entries {
			if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
				continue
			}
			name := entry.Name()
			if allowedDocument(prefix, name) {
				rels = append(rels, filepath.Join(prefix, name))
			}
		}
		return nil
	}
	if err := collect(root, ""); err != nil {
		return nil, err
	}
	if err := collect(filepath.Join(root, "docs"), "docs"); err != nil {
		return nil, err
	}
	sort.Strings(rels)
	if len(rels) > maxDocuments {
		rels = rels[:maxDocuments]
	}
	remaining := maxAllDocumentBytes
	out := make([]document, 0, len(rels))
	for _, rel := range rels {
		path := filepath.Join(root, rel)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		limit := maxDocumentBytes
		if remaining < limit {
			limit = remaining
		}
		if limit <= 0 {
			break
		}
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		data := make([]byte, limit+1)
		n, readErr := file.Read(data)
		_ = file.Close()
		if readErr != nil && n == 0 {
			continue
		}
		if n > limit {
			n = limit
		}
		remaining -= n
		out = append(out, document{rel: filepath.ToSlash(rel), content: string(data[:n]), info: info})
	}
	return out, nil
}

func allowedDocument(prefix, name string) bool {
	lower := strings.ToLower(name)
	if prefix == "" {
		if strings.HasPrefix(lower, "readme") || lower == "agents.md" || lower == "rtk.md" ||
			lower == "contributing.md" || lower == "roadmap.md" || lower == "todo.md" || lower == "changelog.md" {
			return true
		}
		return isManifest(lower)
	}
	return prefix == "docs" && (strings.HasPrefix(lower, "readme") || strings.Contains(lower, "architecture") || strings.Contains(lower, "roadmap")) && strings.HasSuffix(lower, ".md")
}

func isManifest(name string) bool {
	switch name {
	case "package.json", "go.mod", "cargo.toml", "pyproject.toml", "podfile", "pubspec.yaml":
		return true
	default:
		return false
	}
}

func gitEvidence(ctx context.Context, root string) (branch, head, status, logText string, ok bool) {
	if _, err := os.Stat(filepath.Join(root, ".git")); err != nil {
		return "", "", "", "", false
	}
	run := func(args ...string) string {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		out, err := cmd.Output()
		if err != nil {
			return ""
		}
		return truncate(strings.TrimSpace(string(out)), 16*1024)
	}
	branch = run("branch", "--show-current")
	head = run("rev-parse", "--short=12", "HEAD")
	status = run("status", "--short", "--untracked-files=no")
	logText = run("log", "-5", "--pretty=format:%h %s")
	return branch, head, status, logText, branch != "" || head != ""
}

func readmeObjective(content string) string {
	scanner := bufio.NewScanner(strings.NewReader(redact(content)))
	var title string
	var paragraph []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			if len(paragraph) > 0 {
				break
			}
			continue
		}
		if strings.HasPrefix(line, "#") && title == "" {
			title = strings.TrimSpace(strings.TrimLeft(line, "#"))
			continue
		}
		if strings.HasPrefix(line, "[") || strings.HasPrefix(line, "!") || strings.HasPrefix(line, "<") || strings.HasPrefix(line, "```") {
			continue
		}
		paragraph = append(paragraph, line)
		if len(strings.Join(paragraph, " ")) >= 500 {
			break
		}
	}
	body := truncate(strings.Join(paragraph, " "), 600)
	if body != "" {
		return body
	}
	return title
}

func sessionPreview(messages []SessionMessage) string {
	var lines []string
	for _, message := range messages {
		text := strings.TrimSpace(message.Text)
		if text == "" {
			continue
		}
		lines = append(lines, strings.TrimSpace(message.Role)+": "+text)
	}
	return truncate(strings.Join(lines, "\n"), maxSessionPreview)
}

func acceptanceFor(documents []document) string {
	for _, doc := range documents {
		switch strings.ToLower(filepath.Base(doc.rel)) {
		case "go.mod":
			return "Relevant Go tests pass and the owner verifies the requested outcome."
		case "package.json":
			return "Relevant package tests and build checks pass, then the owner verifies the requested outcome."
		}
	}
	return "The owner verifies the result against documented project requirements and explicitly accepts it."
}

func redact(input string) string {
	var out []string
	privateBlock := false
	for _, line := range strings.Split(input, "\n") {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "begin ") && strings.Contains(lower, "private key") {
			privateBlock = true
			out = append(out, "[redacted private key block]")
			continue
		}
		if privateBlock {
			if strings.Contains(lower, "end ") && strings.Contains(lower, "private key") {
				privateBlock = false
			}
			continue
		}
		if strings.Contains(lower, "\"token\"") || strings.Contains(lower, "\"secret\"") ||
			strings.Contains(lower, "\"password\"") ||
			strings.Contains(lower, "api_key") || strings.Contains(lower, "apikey") ||
			strings.Contains(lower, "password=") || strings.Contains(lower, "secret=") ||
			strings.Contains(lower, "token=") {
			out = append(out, "[redacted potentially sensitive line]")
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func resultFingerprint(sources []Source) string {
	h := sha256.New()
	for _, source := range sources {
		_, _ = h.Write([]byte(source.ID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(source.Fingerprint))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func stableID(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return prefix + "_" + hex.EncodeToString(sum[:12])
}

func truncate(value string, limit int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return strings.TrimSpace(string(runes[:limit])) + "…"
}

func emptyAs(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// Keep the package's explicit operation bounded even when a caller forgets a
// deadline. The core currently supplies a shorter deadline; this value is also
// useful to downstream callers and tests.
func DefaultTimeout() time.Duration { return 5 * time.Second }
