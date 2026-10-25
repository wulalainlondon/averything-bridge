package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"path/filepath"
)

func retainedScope(p backend.TaskAPIProvider, s *session.Session, want taskapi.ChildScope, cwd, provider string) error {
	fail := func() error { return taskapi.Failure("unsupported", "known_none", "read_capabilities") }
	if p == nil || s == nil || s.Backend() != provider || s.ResumeID() == "" || s.SettingsSnapshot().Sandbox != "read-only" {
		return fail()
	}
	actual, err := p.TaskWorkerScope(s)
	if err != nil {
		return err
	}
	if actual == nil {
		return fail()
	}
	a, _ := json.Marshal(actual)
	b, _ := json.Marshal(want)
	if string(a) != string(b) || want.Sandbox != "read-only" || want.Network != "deny" || want.MaxChildren != 0 || len(want.Operations) != 1 || want.Operations[0] != "read" || len(want.Roots) == 0 {
		return fail()
	}
	root, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return fail()
	}
	current, err := filepath.EvalSymlinks(s.SettingsSnapshot().Cwd)
	if err != nil || root != current {
		return fail()
	}
	tools, err := p.TaskTools(s)
	if err != nil || (len(tools) != 2 && len(tools) != 3) {
		return fail()
	}
	seen := map[string]bool{}
	for _, tool := range tools {
		name, _ := tool["name"].(string)
		if name != "task_capabilities" && name != "task_read_input" && name != "task_interactions" {
			return fail()
		}
		if seen[name] {
			return fail()
		}
		seen[name] = true
	}
	if !seen["task_capabilities"] || !seen["task_read_input"] {
		return fail()
	}
	return nil
}

func (c *Claude) ExistingTaskScopeMode() string { return "retained_bounded_only" }
func (c *Claude) ValidateExistingTaskScope(s *session.Session, scope taskapi.ChildScope, cwd string) error {
	if c.taskVersion() != "2.1.296" || c.taskMCP == nil {
		return taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	return retainedScope(c.taskProvider, s, scope, cwd, backend.Claude)
}
func (c *Codex) ExistingTaskScopeMode() string { return "retained_bounded_only" }
func (c *Codex) ValidateExistingTaskScope(s *session.Session, scope taskapi.ChildScope, cwd string) error {
	if c.taskDaemonVersion() != "0.162.1" {
		return taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	return retainedScope(c.taskProvider, s, scope, cwd, backend.Codex)
}
