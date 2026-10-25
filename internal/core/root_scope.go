package core

import (
	"fmt"
	"path/filepath"
	"strings"

	"everything-go/internal/runtime"
)

// scopedPath keeps file-picker requests inside a restricted bridge's root.
// Unrestricted bridges retain their existing home/relative-path behavior.
func (h *Hub) scopedPath(raw string) (string, error) {
	if h.cfg.RootDir == "" {
		return runtime.ExpandPath(raw), nil
	}
	root := realpath(runtime.ExpandPath(h.cfg.RootDir))
	path := raw
	if path == "" || path == "~" {
		path = root
	} else if !filepath.IsAbs(path) && !strings.HasPrefix(path, "~/") {
		path = filepath.Join(root, path)
	} else {
		path = runtime.ExpandPath(path)
	}
	path = realpath(path)
	if !pathInsideRoot(path, root) {
		return "", fmt.Errorf("path is outside bridge root")
	}
	return path, nil
}

func (h *Hub) cwdInScope(cwd string) bool {
	return h.cfg.RootDir == "" || (cwd != "" && pathInsideRoot(runtime.ExpandPath(cwd), runtime.ExpandPath(h.cfg.RootDir)))
}
