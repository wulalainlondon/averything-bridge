package handoff

import (
    "os"
    "path/filepath"
)

func writeProbeFile(p Policy, raw string) error {
    return os.WriteFile(filepath.Join(p.Workspace, p.Path), []byte(raw), 0600)
}
