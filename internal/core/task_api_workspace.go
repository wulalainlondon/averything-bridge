package core

import (
	"os"
	"path/filepath"
	"strings"

	"everything-go/internal/taskapi"
)

func taskPathRelative(root, path string) (string, error) {
	if !filepath.IsAbs(root) || !filepath.IsAbs(path) {
		return "", taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	relative, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return "", taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	return relative, nil
}

// Capture every child directory through the SAME confined parent handle. A
// mutable path checked by Authorize is never separately Stat'ed as an approved
// root. Saved identities come from opened directory handles, not an alias that
// can be retargeted between the check and persistence.
func captureTaskWorkspace(authority, parent, cwd string, roots []string) ([]string, map[string]string, string, error) {
	denied := taskapi.Failure("permission", "known_none", "request_scope_change")
	if authority == "" {
		authority = parent
	}
	relative, err := taskPathRelative(authority, parent)
	if err != nil {
		return nil, nil, "", err
	}
	authorityRoot, err := os.OpenRoot(authority)
	if err != nil {
		return nil, nil, "", denied
	}
	defer authorityRoot.Close()
	parentRoot, err := authorityRoot.OpenRoot(relative)
	if err != nil {
		return nil, nil, "", denied
	}
	defer parentRoot.Close()
	names := []string{}
	identities := map[string]string{}
	capture := func(path string) (string, string, error) {
		relative, err := taskPathRelative(parent, path)
		if err != nil {
			return "", "", err
		}
		opened, err := parentRoot.OpenRoot(relative)
		if err != nil {
			return "", "", denied
		}
		defer opened.Close()
		info, err := opened.Stat(".")
		if err != nil {
			return "", "", denied
		}
		identity, err := taskRootInfoIdentity(info)
		if err != nil {
			return "", "", taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
		// The immutable normalized name and its identity refer to this captured
		// directory. Later read_input must match its opened-root identity again.
		return filepath.Clean(path), identity, nil
	}
	cwdName, cwdIdentity, err := capture(cwd)
	if err != nil {
		return nil, nil, "", err
	}
	identities[cwdName] = cwdIdentity
	for _, root := range roots {
		name, identity, err := capture(root)
		if err != nil {
			return nil, nil, "", err
		}
		names = append(names, name)
		identities[name] = identity
	}
	return names, identities, cwdName, nil
}
