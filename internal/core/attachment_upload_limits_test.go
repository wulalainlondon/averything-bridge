package core

import (
	"strings"
	"testing"
)

func TestAttachmentUpload2048MiBBoundaryForVideoAndGenericFiles(t *testing.T) {
	if maxVideoUploadBytes != 2147483648 || maxFileUploadBytes != 2147483648 {
		t.Fatal("attachment caps must be exactly 2048 MiB")
	}
	for _, kind := range []string{"video", "file"} {
		t.Run(kind, func(t *testing.T) {
			h, _ := newTestHub(t)
			h.cfg.DataDir = t.TempDir()
			h.registry.Create("large-file", "Boundary", t.TempDir(), "codex", "", "read-only", "")
			c := newTestClient(h)
			c.deviceID = "large-fixture"
			c.uploads = newAttachmentUploads(c, h.cfg.DataDir)
			defer c.uploads.close()
			c.uploads.initKind("large-file", "exact-limit", "boundary.mov", "video/quicktime", maxFileUploadBytes, kind)
			ready := waitForType(t, c, "attachment_upload_ready")
			if ready["protocol_version"] != float64(2) || ready["received_bytes"] != float64(0) {
				t.Fatal(ready)
			}
			c.uploads.initKind("large-file", "over-limit", "too-big.mov", "video/quicktime", maxFileUploadBytes+1, kind)
			failed := waitForType(t, c, "attachment_upload_error")
			if !strings.Contains(failed["message"].(string), "2048 MB") {
				t.Fatal(failed)
			}
		})
	}
}
