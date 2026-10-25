package taskapi

import (
	"path/filepath"
	"strings"
)

type ChildScope struct {
	Roots            []string
	Operations       []string
	Sandbox, Network string
	MaxChildren      int
}

// ValidateChildScope checks the original effective server authorization AND
// enforceability. A subset check or cwd is never a filesystem/network sandbox.
func ValidateChildScope(parent, child ChildScope, enforcement ScopeEnforcement) error {
	denied := Failure("permission", "known_none", "request_scope_change")
	if child.Sandbox != "read-only" && child.Sandbox != "workspace-write" {
		return denied
	}
	if child.Sandbox == "workspace-write" && parent.Sandbox == "read-only" {
		return denied
	}
	if child.MaxChildren < 0 || child.MaxChildren > parent.MaxChildren {
		return denied
	}
	if child.Network != "deny" && child.Network != "inherit_authorized" {
		return denied
	}
	if parent.Network == "deny" && child.Network != "deny" {
		return denied
	}
	operations := map[string]bool{}
	for _, op := range parent.Operations {
		operations[op] = true
	}
	for _, op := range child.Operations {
		if !operations[op] {
			return denied
		}
	}
	if len(child.Roots) == 0 {
		return denied
	}
	parents := []string{}
	for _, root := range parent.Roots {
		real, err := filepath.EvalSymlinks(root)
		if err != nil || !filepath.IsAbs(real) {
			return denied
		}
		parents = append(parents, real)
	}
	for _, root := range child.Roots {
		real, err := filepath.EvalSymlinks(root)
		if err != nil || !filepath.IsAbs(real) {
			return denied
		}
		contained := false
		for _, parent := range parents {
			rel, err := filepath.Rel(parent, real)
			if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				contained = true
			}
		}
		if !contained {
			return denied
		}
	}
	if (enforcement.Mode != "native_sandbox" && enforcement.Mode != "gateway_only_tools") || !enforcement.Roots || !enforcement.Tools || !enforcement.Network || !enforcement.Delegation {
		return Failure("unsupported", "known_none", "read_capabilities")
	}
	return nil
}
