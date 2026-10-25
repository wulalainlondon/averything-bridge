package linebridge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// VerifyBundle checks the staged artifact before a child can access credentials.
// The signed .app seals the manifest; the manifest also pins the external Node.
func VerifyBundle(manifest, node, entry, shared string) error {
	data, err := os.ReadFile(manifest)
	if err != nil || len(data) > 4*1024*1024 {
		return Fail("line_bundle_invalid")
	}
	var spec struct {
		Version int `json:"schema_version"`
		Node    struct {
			SHA string `json:"sha256"`
		} `json:"node"`
		Files map[string]string `json:"files"`
	}
	if json.Unmarshal(data, &spec) != nil || spec.Version != 1 || len(spec.Files) < 1 {
		return Fail("line_bundle_invalid")
	}
	root, err := os.OpenRoot(filepath.Dir(manifest))
	if err != nil {
		return Fail("line_bundle_invalid")
	}
	defer root.Close()
	for name, expected := range spec.Files {
		if filepath.IsAbs(name) || strings.Contains(name, "\\") || filepath.Clean(name) != name || strings.HasPrefix(name, "..") || len(expected) != 64 {
			return Fail("line_bundle_invalid")
		}
		f, err := root.Open(name)
		if err != nil {
			return Fail("line_bundle_invalid")
		}
		digest := sha256.New()
		_, err = io.Copy(digest, f)
		f.Close()
		if err != nil || hex.EncodeToString(digest.Sum(nil)) != expected {
			return Fail("line_bundle_changed")
		}
	}
	for _, path := range []string{entry, filepath.Join(shared, "credentials.mjs"), filepath.Join(shared, "route-worker.mjs")} {
		rel, err := filepath.Rel(filepath.Dir(manifest), path)
		if err != nil || spec.Files[filepath.ToSlash(rel)] == "" {
			return Fail("line_bundle_invalid")
		}
	}
	f, err := os.Open(node)
	if err != nil {
		return Fail("line_node_unavailable")
	}
	defer f.Close()
	digest := sha256.New()
	_, err = io.Copy(digest, f)
	if err != nil || hex.EncodeToString(digest.Sum(nil)) != spec.Node.SHA {
		return Fail("line_node_binary_changed")
	}
	return nil
}
