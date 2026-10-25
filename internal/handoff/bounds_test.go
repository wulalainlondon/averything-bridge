package handoff

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestHandoffBoundsDepthBeforeDescent(t *testing.T) {
	for _, depth := range []int{MaxJSONDepth, MaxJSONDepth + 1, 8190, 10001, 32000} {
		for _, object := range []bool{false, true} {
			t.Run(fmt.Sprintf("depth-%d-object-%t", depth, object), func(t *testing.T) {
				open, close := "[", "]"
				if object {
					open, close = `{"x":`, "}"
				}
				raw := strings.Repeat(open, depth) + "0" + strings.Repeat(close, depth)
				d := json.NewDecoder(strings.NewReader(raw))
				err := uniqueKeys(d)
				if depth == MaxJSONDepth {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				if err == nil || err.Error() != "json_depth_oversize" || d.InputOffset() > int64(len(open)*(MaxJSONDepth+1)) {
					t.Fatalf("prepass descended too far: offset=%d error=%v", d.InputOffset(), err)
				}
			})
		}
	}
	for _, raw := range []string{`{"x":1,"x":2}`, `[{"x":1,"x":2}]`} {
		if err := uniqueKeys(json.NewDecoder(strings.NewReader(raw))); err == nil || err.Error() != "duplicate_key" {
			t.Fatal(err)
		}
	}
}

func TestHandoffBoundsAllDecodePathsAndBytes(t *testing.T) {
	p, d := fixture(t)
	config, _ := json.Marshal(Config{SchemaVersion: 1, Parents: map[string]Policy{"parent": p}})
	padded := string(config) + strings.Repeat(" ", MaxConfigBytes-len(config))
	if _, err := ReadConfig(strings.NewReader(padded)); err != nil {
		t.Fatal("exact config cap", err)
	}
	if _, err := ReadConfig(strings.NewReader(padded + " ")); err == nil {
		t.Fatal("config cap+1 accepted")
	}
	doc, _ := json.Marshal(d)
	if err := os.WriteFile(filepath.Join(p.Workspace, p.Path), append(doc, []byte(strings.Repeat(" ", MaxFileBytes-len(doc)))...), 0600); err != nil {
		t.Fatal(err)
	}
	if got := Read(p, p.Workspace, testNow); got.Status != "available" {
		t.Fatal("exact file cap", got)
	}
	if err := os.WriteFile(filepath.Join(p.Workspace, p.Path), []byte(strings.Repeat(" ", MaxFileBytes+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if got := Read(p, p.Workspace, testNow); got.Reason != "file_oversize" {
		t.Fatal(got)
	}
	for _, raw := range []string{
		strings.Repeat("[", 8000) + "0" + strings.Repeat("]", 8000),
		`{"schema_version":1,"revision":1,"goal":` + strings.Repeat("[", 100) + "0" + strings.Repeat("]", 100) + `}`,
	} {
		if err := os.WriteFile(filepath.Join(p.Workspace, p.Path), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if got := Read(p, p.Workspace, testNow); got.Reason != "metadata_invalid" {
			t.Fatal(got)
		}
		if _, err := ReadConfig(strings.NewReader(raw)); err == nil {
			t.Fatal("config deep input accepted")
		}
		if _, err := DecodeSnapshot([]byte(raw)); err == nil {
			t.Fatal("snapshot deep input accepted")
		}
	}
	for _, raw := range []string{`{"status":"available","status":"unavailable"}`, `{"status":"available","foreign":1}`, `{} ` + strings.Repeat("[", 32000) + "0" + strings.Repeat("]", 32000)} {
		if _, err := DecodeSnapshot([]byte(raw)); err == nil {
			t.Fatal("snapshot malformed accepted")
		}
	}
	var target Config
	if err := decode([]byte(strings.Repeat(" ", MaxConfigBytes+1)), &target, MaxConfigBytes); err == nil || err.Error() != "json_oversize" {
		t.Fatal("length guard before token parse", err)
	}
	// Loading an old stored snapshot must not normalize or rebuild its receipt.
	legacy := Snapshot{Status: "unavailable", Reason: "reminder_oversize", DelegationID: "dg_legacy", ResultSHA256: strings.Repeat("a", 64), Path: "/fixture/handoff.json", Revision: 3, SHA256: strings.Repeat("b", 64), UpdatedAt: "2026-10-10T07:59:00." + strings.Repeat("0", 7000) + "Z", CapturedAt: "2026-10-10T08:00:00Z"}
	legacyBytes, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeSnapshot(legacyBytes); err != nil || !reflect.DeepEqual(got, legacy) {
		t.Fatal("legacy stored snapshot rewritten", err)
	}
	snapshot := `{"status":"unavailable","captured_at":"2026-10-10T08:00:00Z"}`
	if _, err := DecodeSnapshot([]byte(snapshot + strings.Repeat(" ", MaxConfigBytes-len(snapshot)))); err != nil {
		t.Fatal("stored compatibility cap", err)
	}
	if _, err := DecodeSnapshot([]byte(strings.Repeat(" ", MaxConfigBytes+1))); err == nil {
		t.Fatal("stored cap+1 accepted")
	}
}

func TestHandoffBoundsTimestampAssociation(t *testing.T) {
	for _, tc := range []struct {
		stamp string
		valid bool
	}{
		{"2026-10-10T15:59:00.123456789+08:00", true}, // exact 35-byte valid timestamp
		{"2026-10-10T15:59:00.1234567890+08:00", false},
		{"2026-10-10T07:59:00." + strings.Repeat("0", 7000) + "Z", false},
		{"2026-10-10T07:59:00." + strings.Repeat("0", MaxFileBytes) + "Z", false},
	} {
		t.Run(fmt.Sprint(len(tc.stamp)), func(t *testing.T) {
			p, d := fixture(t)
			d.UpdatedAt = tc.stamp
			writeDoc(t, p, d)
			s := Read(p, p.Workspace, testNow)
			s.DelegationID = "dg_original"
			s.ResultSHA256 = strings.Repeat("a", 64)
			if (s.Status == "available") != tc.valid {
				t.Fatal(s.Status, s.Reason)
			}
			if tc.valid && s.UpdatedAt != "2026-10-10T07:59:00.123456789Z" {
				t.Fatal(s.UpdatedAt)
			}
			if !tc.valid && len(tc.stamp) < MaxFileBytes {
				raw, err := os.ReadFile(filepath.Join(p.Workspace, p.Path))
				if err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(raw)
				if s.Reason != "updated_at_invalid" || s.Revision != d.Revision || s.SHA256 != hex.EncodeToString(sum[:]) || s.UpdatedAt != "" {
					t.Fatal("rejected timestamp lost stable file association", s)
				}
			}
			assertReminderAssociation(t, s)
		})
	}
}
func assertReminderAssociation(t *testing.T, s Snapshot) {
	t.Helper()
	reminder := s.Reminder()
	if len(reminder) > MaxReminderBytes || !strings.HasPrefix(reminder, reminderWarning) {
		t.Fatal("reminder cap/warning", len(reminder))
	}
	var got Snapshot
	if err := json.Unmarshal([]byte(strings.TrimPrefix(reminder, reminderWarning)), &got); err != nil {
		t.Fatal(err)
	}
	if got.DelegationID != s.DelegationID || got.ResultSHA256 != s.ResultSHA256 || got.Path != s.Path || got.Revision != s.Revision || got.SHA256 != s.SHA256 || got.CapturedAt != s.CapturedAt {
		t.Fatal("identity fields lost", got, s)
	}
}
func TestHandoffBoundsFallbackMaxMetadataAndLegacyTimestamp(t *testing.T) {
	p, d := fixture(t)
	d.Goal = strings.Repeat("\t", 1023) + "g"
	d.Progress = d.Goal
	d.NextStep = d.Goal
	writeDoc(t, p, d)
	s := Read(p, p.Workspace, testNow)
	if s.Status != "available" {
		t.Fatal(s)
	}
	// JSON-escaped metadata at the previously permitted path/identity bounds.
	s.Path = "/" + strings.Repeat("<", 340) + "abc" // encoded 2046 bytes
	s.DelegationID = strings.Repeat("<", 320)
	s.ResultSHA256 = strings.Repeat("a", 64)
	for _, stamp := range []string{s.UpdatedAt, "2026-10-10T07:59:00." + strings.Repeat("0", 7000) + "Z"} {
		s.UpdatedAt = stamp
		b := s.Bounded()
		if b.Status != "unavailable" || b.Reason != "reminder_oversize" || b.Goal != "" || b.Progress != "" || b.NextStep != "" {
			t.Fatal("expected summary fallback", b.Status, b.Reason)
		}
		if !reflect.DeepEqual(b, b.Bounded()) {
			t.Fatal("bounds not idempotent")
		}
		assertReminderAssociation(t, b)
		if b.DelegationID != s.DelegationID || b.ResultSHA256 != s.ResultSHA256 || b.Path != s.Path || b.Revision != s.Revision || b.SHA256 != s.SHA256 || b.CapturedAt != s.CapturedAt {
			t.Fatal("max metadata changed")
		}
		if len(b.UpdatedAt) > MaxTimestampBytes {
			t.Fatal("legacy timestamp not normalized")
		}
	}
	// Even malformed internal values use stable digest placeholders within 6KiB.
	bad := strings.Repeat("x", MaxConfigBytes+1)
	s = Snapshot{DelegationID: bad, ResultSHA256: bad, Path: bad, SHA256: bad, UpdatedAt: bad, CapturedAt: bad, Goal: bad, Progress: bad, NextStep: bad, Status: bad, Reason: bad, Revision: ^uint64(0)}
	b := s.Bounded()
	if !reflect.DeepEqual(b, b.Bounded()) {
		t.Fatal("digest placeholders changed on replay")
	}
	for _, v := range []string{b.DelegationID, b.ResultSHA256, b.Path, b.SHA256, b.UpdatedAt, b.CapturedAt} {
		if !metadataDigest(v) {
			t.Fatal("malformed association silently lost")
		}
	}
	assertReminderAssociation(t, b)
}

func TestHandoffBoundsConfigParentCountAndRootContainment(t *testing.T) {
	p, _ := fixture(t)
	parents := map[string]Policy{}
	for i := 0; i < 100; i++ {
		parents[fmt.Sprint(i)] = p
	}
	raw, _ := json.Marshal(Config{SchemaVersion: 1, Parents: parents})
	if len(raw) > MaxConfigBytes {
		t.Fatal("fixture too large")
	}
	if _, err := ReadConfig(strings.NewReader(string(raw))); err != nil {
		t.Fatal(err)
	}
	parents["101"] = p
	raw, _ = json.Marshal(Config{SchemaVersion: 1, Parents: parents})
	if _, err := ReadConfig(strings.NewReader(string(raw))); err == nil {
		t.Fatal("101 parents accepted")
	}
	root := filepath.Dir(p.Workspace)
	if got := Read(p, root, testNow); got.Status != "available" {
		t.Fatal(got)
	}
	// A lexical prefix is not root confinement; failure happens before file read.
	for _, allowed := range []string{p.Workspace + "-sibling", filepath.Join(p.Workspace, "child"), filepath.Join(root, "other-root")} {
		if got := Read(p, allowed, testNow); got.Reason != "path_unavailable_or_outside_scope" {
			t.Fatal(allowed, got)
		}
	}
	// Streaming readers stop at the size cap; an unread tail is never parsed.
	r := &countingReader{remaining: MaxConfigBytes * 4}
	if _, err := ReadConfig(r); err == nil || r.read != MaxConfigBytes+1 {
		t.Fatal("unbounded config read", r.read, err)
	}
}

type countingReader struct{ remaining, read int }

func (r *countingReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = ' '
	}
	r.remaining -= n
	r.read += n
	return n, nil
}
