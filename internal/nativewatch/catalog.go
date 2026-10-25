package nativewatch

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// TranscriptCatalog contains filename identities, never conversation content.
// A source is authoritative only after its entire root was readable. Missing
// roots, permission failures and symlink trees cannot prove a deletion.
type TranscriptCatalog struct {
	roots   map[string]string
	healthy map[string]bool
	ids     map[string]map[string]bool
}

func ScanTranscriptCatalog(roots map[string]string) TranscriptCatalog {
	c := TranscriptCatalog{roots: map[string]string{}, healthy: map[string]bool{}, ids: map[string]map[string]bool{}}
	for backend, root := range roots {
		if root == "" {
			continue
		}
		root, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		c.roots[backend] = root
		c.ids[backend] = map[string]bool{}
		info, err := os.Lstat(root)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Type()&os.ModeSymlink != 0 {
				return fs.ErrInvalid
			}
			if d.IsDir() {
				return nil
			}
			name := strings.TrimSuffix(strings.TrimSuffix(d.Name(), ".gz"), ".zst")
			if !strings.HasSuffix(name, ".jsonl") {
				return nil
			}
			id := strings.TrimSuffix(name, ".jsonl")
			if backend == BackendCodex {
				if !strings.HasPrefix(id, "rollout-") || len(id) < 36 {
					return nil
				}
				id = id[len(id)-36:]
			}
			if uuidRE.MatchString(id) {
				c.ids[backend][id] = true
			}
			return nil
		})
		c.healthy[backend] = err == nil
	}
	return c
}

func (c TranscriptCatalog) MissingSession(backend, id string) bool {
	if !uuidRE.MatchString(id) || !c.healthy[backend] || c.ids[backend][id] {
		return false
	}
	info, err := os.Stat(c.roots[backend])
	return err == nil && info.IsDir()
}

// MissingPath only invalidates a derived row for an absent file inside the
// same fully inspected source root. Archived/compressed files remain evidence.
func (c TranscriptCatalog) MissingPath(backend, path string) bool {
	if !c.healthy[backend] || path == "" {
		return false
	}
	root := c.roots[backend]
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	if _, err = os.Lstat(abs); !os.IsNotExist(err) {
		return false
	}
	if _, err = os.Lstat(abs + ".gz"); !os.IsNotExist(err) {
		return false
	}
	if _, err = os.Lstat(abs + ".zst"); !os.IsNotExist(err) {
		return false
	}
	// Recheck source availability; unplugging a root is never a deletion.
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		return false
	}
	return true
}
