package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/executor/goexec"
)

// Opt-in: synthetic fixtures and isolated conversations on the existing
// subscription. ConnectExistingDaemon never starts or restarts the daemon.
func TestDesktopWorkspaceLiveAttachmentReading(t *testing.T) {
	root := os.Getenv("BRIDGE_WORKSPACE_LIVE_FIXTURES")
	if root == "" {
		t.Skip("opt-in synthetic attachment acceptance")
	}
	for _, backendID := range []string{"codex", "claude"} {
		t.Run(backendID, func(t *testing.T) {
			h, _ := newTestHub(t)
			h.cfg.DataDir = t.TempDir()
			s := h.registry.Create("workspace-live-"+backendID, "[bridge-eval] workspace attachments", root, backendID, "", "read-only", "")
			if backendID == "codex" {
				c := goexec.NewCodex(h, "codex")
				c.SetDataDir(h.cfg.DataDir)
				if err := c.ConnectExistingDaemon(); err != nil {
					t.Fatal(err)
				}
				h.SetExecutor(c)
				s.SetModel("gpt-6-astra")
				s.SetEffort("low")
				t.Cleanup(func() { _ = c.Close(context.Background(), s) })
			} else {
				c := goexec.NewClaude(h, "claude")
				h.SetExecutor(c)
				s.SetModel("sonnet")
				t.Cleanup(func() { _ = c.Close(context.Background(), s) })
			}
			client := newTestClient(h)
			pdf, err := os.ReadFile(filepath.Join(root, "workspace-reference.pdf"))
			if err != nil {
				t.Fatal(err)
			}
			png, err := os.ReadFile(filepath.Join(root, "workspace-image.png"))
			if err != nil {
				t.Fatal(err)
			}
			text, err := os.ReadFile(filepath.Join(root, "workspace-note.txt"))
			if err != nil {
				t.Fatal(err)
			}
			h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: s.ID, RequestID: "attachment-reading", Content: "Isolated attachment QA. Read the attached PDF and report the verification code on PAGE 2 and the count/color of circles on that page. Read the attached image and report its check code and the two shapes/colors. Read the text attachment and report its check code. You must actually inspect the attachment contents. Read only these synthetic fixtures; do not modify anything, execute attachments, inspect unrelated files, or start agents. Answer concisely.",
				Images: []backend.ImageAttachment{{Data: base64.StdEncoding.EncodeToString(png), MediaType: "image/png"}},
				Files:  []backend.FileAttachment{{Name: "workspace-reference.pdf", MediaType: "application/pdf", Content: base64.StdEncoding.EncodeToString(pdf)}, {Name: "workspace-note.txt", MediaType: "text/plain", Content: string(text)}}})
			var answer strings.Builder
			deadline := time.NewTimer(150 * time.Second)
			defer deadline.Stop()
			for {
				select {
				case data := <-client.send:
					var event map[string]any
					if json.Unmarshal(data, &event) != nil {
						continue
					}
					if event["request_id"] != "attachment-reading" {
						continue
					}
					switch event["type"] {
					case "text_chunk":
						if value, ok := event["content"].(string); ok {
							answer.WriteString(value)
						}
					case "error":
						t.Fatalf("attachment turn rejected: %v", event)
					case "done":
						for _, expected := range []string{"COPPER-482", "MAPLE-294", "RIVER-615"} {
							if !strings.Contains(answer.String(), expected) {
								t.Fatalf("missing verified content %s: %s", expected, answer.String())
							}
						}
						t.Logf("VERIFIED session=%s thread=%s answer=%s", s.ID, s.ResumeID(), answer.String())
						return
					}
				case <-deadline.C:
					t.Fatalf("attachment reading timed out; response=%s", answer.String())
				}
			}
		})
	}
}
