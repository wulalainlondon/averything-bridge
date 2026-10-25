package attachmentjournal

import (
	"path/filepath"
	"testing"
	"time"

	"everything-go/internal/protocol"
)

func TestPerDeviceAckAndRestartPersistence(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	path := filepath.Join(dir, "same.png")
	first, added := s.Add(protocol.Media{Type: "media", SessionID: "s1", RequestID: "r1", MediaType: "image", Path: path})
	if !added {
		t.Fatal("first attachment was not added")
	}
	if _, duplicate := s.Add(protocol.Media{Type: "media", SessionID: "s1", RequestID: "r1", MediaType: "image", Path: path}); duplicate {
		t.Fatal("same turn/path must be idempotent")
	}
	second, added := s.Add(protocol.Media{Type: "media", SessionID: "s1", RequestID: "r2", MediaType: "image", Path: path})
	if !added || second.AttachmentID == first.AttachmentID {
		t.Fatal("same path in another turn must remain distinct")
	}

	if got := s.Ack("desktop", []string{first.AttachmentID}); got != 1 {
		t.Fatalf("desktop ACK committed %d records", got)
	}
	if pending := s.Pending("desktop", "s1", 10); len(pending) != 1 || pending[0].AttachmentID != second.AttachmentID {
		t.Fatalf("desktop pending=%+v", pending)
	}
	if pending := s.Pending("phone", "s1", 10); len(pending) != 2 {
		t.Fatalf("desktop ACK consumed phone cursor: %+v", pending)
	}

	reloaded := New(dir)
	if pending := reloaded.Pending("desktop", "s1", 10); len(pending) != 1 || pending[0].AttachmentID != second.AttachmentID {
		t.Fatalf("restart lost desktop cursor: %+v", pending)
	}
	if pending := reloaded.Pending("phone", "s1", 10); len(pending) != 2 {
		t.Fatalf("restart lost phone pending records: %+v", pending)
	}
}

func TestWorkPinSurvivesRestartAndPreventsTTLCollection(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.now = func() time.Time { return time.UnixMilli(1_800_000_000_000) }
	record, added := s.Add(protocol.Media{Type: "media", SessionID: "s1", RequestID: "r1", MediaType: "image", Path: filepath.Join(dir, "pinned.png")})
	found, pinned := s.Pin(record.AttachmentID, "wi1")
	if !added || !found || !pinned {
		t.Fatalf("record=%+v added=%v", record, added)
	}

	s = New(dir)
	s.now = func() time.Time { return time.UnixMilli(1_800_000_000_000).Add(ttl + time.Hour) }
	if got, ok := s.Get(record.AttachmentID); !ok || !got.PinnedByWork["wi1"] {
		t.Fatalf("pinned record missing after restart: %+v ok=%v", got, ok)
	}
	if pending := s.Pending("other-device", "s1", 10); len(pending) != 1 {
		t.Fatalf("pinned expired record was collected: %+v", pending)
	}
	s.Unpin(record.AttachmentID, "wi1")
	if pending := s.Pending("other-device", "s1", 10); len(pending) != 0 {
		t.Fatalf("unpinned expired record retained: %+v", pending)
	}
}

func TestTTLRemovesExpiredRecords(t *testing.T) {
	s := New("")
	now := time.Unix(2_000_000, 0)
	s.now = func() time.Time { return now }
	r, _ := s.Add(protocol.Document{Type: "document", SessionID: "s1", RequestID: "r1", Path: "/tmp/a.pdf", DocType: "pdf"})
	if r.MIMEType != "application/pdf" {
		t.Fatalf("mime=%q", r.MIMEType)
	}
	now = now.Add(ttl + time.Second)
	if got := s.Pending("phone", "s1", 10); len(got) != 0 {
		t.Fatalf("expired record remained pending: %+v", got)
	}
}
