package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func TestDesktopFileUploadResumesAndKeepsOriginalIntent(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.registry.Create("resume-file", "Resume", t.TempDir(), "codex", "", "read-only", "")
	data := bytes.Repeat([]byte("original binary bytes\n"), 52000)
	first := newTestClient(h)
	first.deviceID = "desktop-resume"
	first.uploads = newAttachmentUploads(first, h.cfg.DataDir)
	first.uploads.initKind("resume-file", "stable-upload", "原始文件.zip", "application/zip", int64(len(data)), "file")
	id := waitForType(t, first, "attachment_upload_ready")["upload_id"].(string)
	frame := func(offset int, chunk []byte) []byte {
		out := append([]byte(attachmentMagicV2+id), make([]byte, 8)...)
		binary.BigEndian.PutUint64(out[len(out)-8:], uint64(offset))
		return append(out, chunk...)
	}
	first.uploads.writeFrame(frame(0, data[:attachmentChunkBytes]))
	waitForType(t, first, "attachment_upload_ack")
	first.uploads.close()
	second := newTestClient(h)
	second.deviceID = first.deviceID
	second.uploads = newAttachmentUploads(second, h.cfg.DataDir)
	second.uploads.initKind("resume-file", "stable-upload", "原始文件.zip", "application/zip", int64(len(data)), "file")
	if ready := waitForType(t, second, "attachment_upload_ready"); ready["received_bytes"] != float64(attachmentChunkBytes) {
		t.Fatal(ready)
	}
	// Repeated old chunk is acknowledged at the authoritative offset, not appended.
	second.uploads.writeFrame(frame(0, data[:32]))
	if ack := waitForType(t, second, "attachment_upload_ack"); ack["received_bytes"] != float64(attachmentChunkBytes) {
		t.Fatal(ack)
	}
	for offset := attachmentChunkBytes; offset < len(data); {
		end := min(offset+attachmentChunkBytes, len(data))
		second.uploads.writeFrame(frame(offset, data[offset:end]))
		waitForType(t, second, "attachment_upload_ack")
		offset = end
	}
	second.uploads.finish(id)
	complete := waitForType(t, second, "attachment_upload_complete")
	digest := sha256.Sum256(data)
	if complete["sha256"] != hex.EncodeToString(digest[:]) {
		t.Fatal(complete)
	}
	content, _, err := h.resolveUploadedVideos("resume-file", "", []backend.FileAttachment{{AttachmentID: id}})
	if err != nil || !strings.Contains(content, "原始文件.zip") {
		t.Fatal(content, err)
	}
	second.uploads.initKind("resume-file", "stable-upload", "不同文件.zip", "application/zip", int64(len(data)), "file")
	if event := waitForType(t, second, "attachment_upload_error"); event["message"] != "Upload intent conflict" {
		t.Fatal(event)
	}
}

func TestDesktopPDFMaterializationCannotFollowEscapingSymlink(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	root := filepath.Join(h.cfg.DataDir, "uploads")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	_, err := h.materializeInlinePDF("escape", backend.FileAttachment{Name: "test.pdf", MediaType: "application/pdf", Content: base64.StdEncoding.EncodeToString([]byte("%PDF-1.4\ntest"))})
	if err == nil {
		t.Fatal("PDF wrote through symlink")
	}
	entries, _ := os.ReadDir(outside)
	if len(entries) != 0 {
		t.Fatal("outside root was modified")
	}
}

func TestNextMessageSettingsPreserveActiveAndAlreadyQueuedSnapshots(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	s := h.registry.Create("next", "Next", t.TempDir(), "codex", "gpt-6-astra", "read-only", "")
	s.SetEffort("low")
	starts := make(chan session.Snapshot, 3)
	fe.onSend = func(s *session.Session, _, _ string) { starts <- s.Snapshot() }
	route(h, c, `{"type":"message","session_id":"next","request_id":"first","content":"first"}`)
	select {
	case snap := <-starts:
		if snap.Effort != "low" {
			t.Fatal(snap)
		}
	case <-time.After(time.Second):
		t.Fatal("first did not start")
	}
	route(h, c, `{"type":"message","session_id":"next","request_id":"old-queued","content":"old queue"}`)
	route(h, c, `{"type":"switch_session_config","session_id":"next","mutation_id":"future","expected_config_revision":0,"config_scope":"next_message","effort":"high"}`)
	result := waitForType(t, c, "session_config_result")
	for result["mutation_id"] != "future" {
		result = waitForType(t, c, "session_config_result")
	}
	if result["accepted"] != true || result["effective_boundary"] != "new_messages_only" || result["config_revision"] != float64(1) {
		t.Fatal(result)
	}
	if s.Snapshot().Effort != "low" || s.SettingsSnapshot().Effort != "high" {
		t.Fatal("active settings changed", s.Snapshot(), s.SettingsSnapshot())
	}
	// ACK loss must not turn a replay into a config conflict, or enqueue it a
	// second time under the new settings.
	route(h, c, `{"type":"message","session_id":"next","request_id":"old-queued","content":"old queue"}`)
	if s.QueueLen() != 1 {
		t.Fatal("retry duplicated the waiting message")
	}
	route(h, c, `{"type":"message","session_id":"next","request_id":"new-queued","content":"new queue"}`)
	h.Emit(protocol.NewDone(s.ID, "first"))
	select {
	case snap := <-starts:
		if snap.Effort != "low" || snap.ConfigRevision != 0 {
			t.Fatal("old queue changed", snap)
		}
	case <-time.After(time.Second):
		t.Fatal("old queue did not start")
	}
	h.Emit(protocol.NewDone(s.ID, "old-queued"))
	select {
	case snap := <-starts:
		if snap.Effort != "high" || snap.ConfigRevision != 1 {
			t.Fatal("new queue lost settings", snap)
		}
	case <-time.After(time.Second):
		t.Fatal("new queue did not start")
	}
	h.Emit(protocol.NewDone(s.ID, "new-queued"))
	if s.SettingsSnapshot().Effort != "high" {
		t.Fatal("default settings lost")
	}
}

func TestNextMessageSettingsCASAndPermissionFence(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	s := h.registry.Create("next", "Next", t.TempDir(), "codex", "", "read-only", "")
	route(h, c, `{"type":"switch_session_config","session_id":"next","mutation_id":"one","expected_config_revision":0,"config_scope":"next_message","effort":"high"}`)
	if result := waitForType(t, c, "session_config_result"); result["accepted"] != true {
		t.Fatal(result)
	}
	route(h, c, `{"type":"switch_session_config","session_id":"next","mutation_id":"two","expected_config_revision":0,"config_scope":"next_message","effort":"low"}`)
	if result := waitForType(t, c, "session_config_result"); result["reason"] != "config_revision_conflict" {
		t.Fatal(result)
	}
	route(h, c, `{"type":"switch_session_config","session_id":"next","mutation_id":"three","expected_config_revision":1,"config_scope":"next_message","sandbox":"danger-full-access"}`)
	if result := waitForType(t, c, "session_config_result"); result["reason"] != "next_message_cannot_change_backend_or_permissions" {
		t.Fatal(result)
	}
	if s.SettingsSnapshot().Sandbox != "read-only" {
		t.Fatal("permissions changed")
	}
	// Immediate edit while idle must compare with the pending default revision,
	// not with the previous active turn's revision.
	route(h, c, `{"type":"switch_session_config","session_id":"next","mutation_id":"four","expected_config_revision":1,"effort":""}`)
	if result := waitForType(t, c, "session_config_result"); result["accepted"] != true || result["effort"] != "" || result["config_revision"] != float64(2) {
		t.Fatal(result)
	}
}

func TestDesktopConfigurationRevisionRejectsSecondDeviceStaleWrite(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	route(h, c, `{"type":"new_session","session_id":"cfg","name":"Config","backend":"claude"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"switch_session_config","session_id":"cfg","mutation_id":"one","expected_config_revision":0,"effort":"high"}`)
	one := waitForType(t, c, "session_config_result")
	if one["accepted"] != true || one["config_revision"] != float64(1) {
		t.Fatal(one)
	}
	route(h, c, `{"type":"switch_session_config","session_id":"cfg","mutation_id":"two","expected_config_revision":0,"effort":"low"}`)
	two := waitForType(t, c, "session_config_result")
	if two["accepted"] != false || two["reason"] != "config_revision_conflict" || two["effort"] != "high" {
		t.Fatal(two)
	}
}

func TestDesktopGenericUploadAndIntegrityFence(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.registry.Create("files", "Files", t.TempDir(), "codex", "", "read-only", "")
	c := newTestClient(h)
	c.deviceID = "desktop"
	c.uploads = newAttachmentUploads(c, h.cfg.DataDir)
	data := []byte("original document bytes")
	c.uploads.initKind("files", "upload-file", "brief.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", int64(len(data)), "file")
	ready := waitForType(t, c, "attachment_upload_ready")
	id := ready["upload_id"].(string)
	c.uploads.writeFrame(append([]byte(attachmentMagicV1+id), data...))
	c.uploads.finish(id)
	complete := waitForType(t, c, "attachment_upload_complete")
	path := complete["remote_path"].(string)
	if _, _, err := h.resolveUploadedVideos("files", "Read by ID", []backend.FileAttachment{{AttachmentID: id}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.resolveUploadedVideos("other", "", []backend.FileAttachment{{AttachmentID: id}}); err == nil {
		t.Fatal("cross-session attachment ID allowed")
	}
	content, inline, err := h.resolveUploadedVideos("files", "Read this", []backend.FileAttachment{{RemotePath: path}})
	if err != nil || len(inline) != 0 || !strings.Contains(content, path) {
		t.Fatal(content, err)
	}
	if _, _, err = h.resolveUploadedVideos("other", "", []backend.FileAttachment{{RemotePath: path}}); err == nil {
		t.Fatal("cross session attachment allowed")
	}
	if err = os.WriteFile(path, []byte(strings.Repeat("X", len(data))), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = h.resolveUploadedVideos("files", "", []backend.FileAttachment{{RemotePath: path}}); err == nil {
		t.Fatal("same-size content drift not detected")
	}
}

func TestDesktopCodexInlinePDFIsMaterializedNotBase64Prompt(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	h.registry.Create("pdf", "PDF", t.TempDir(), "codex", "", "read-only", "")
	encoded := base64.StdEncoding.EncodeToString([]byte("%PDF-1.4\ntransport fixture only"))
	content, inline, err := h.resolveUploadedVideos("pdf", "Read the PDF", []backend.FileAttachment{{Name: "brief.pdf", Content: encoded, MediaType: "application/pdf"}})
	if err != nil || len(inline) != 0 || strings.Contains(content, encoded) || !strings.Contains(content, "document.pdf") {
		t.Fatal(content, err)
	}
	if _, _, err = h.resolveUploadedVideos("pdf", "", []backend.FileAttachment{{Name: "fake.pdf", Content: "bm90IHBkZg==", MediaType: "application/pdf"}}); err == nil {
		t.Fatal("non-PDF bytes accepted")
	}
}
