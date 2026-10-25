package partners

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPartnerDurableCASAndReplay(t *testing.T) {
	root := t.TempDir()
	s, e := Open(root)
	if e != nil {
		t.Fatal(e)
	}
	p := Profile{SessionID: "s1", NativeThreadID: "original", Name: "小橋", Color: "mint", Description: "role v1", Main: true}
	first, e := s.Save("device-a", "mutation-001", 0, p)
	if e != nil {
		t.Fatal(e)
	}
	second := first
	second.Name = "新名字"
	second.Description = "role v2"
	second, e = s.Save("device-a", "mutation-002", 1, second)
	if e != nil {
		t.Fatal(e)
	}
	if second.RoleRevision != 2 || second.Revision != 2 {
		t.Fatal(second)
	}
	replay, e := s.Save("device-a", "mutation-001", 0, p)
	if e != nil || replay != first {
		t.Fatalf("original receipt lost: %v", e)
	}
	p.Name = "another body"
	if _, e = s.Save("device-a", "mutation-001", 0, p); e == nil {
		t.Fatal("accepted key reuse")
	}
	if _, e = s.Save("device-a", "mutation-003", 1, second); e == nil {
		t.Fatal("accepted stale revision")
	}
	second.NativeThreadID = "different"
	if _, e = s.Save("device-a", "mutation-004", 2, second); e == nil {
		t.Fatal("rebound native thread")
	}
	s, e = Open(root)
	if e != nil {
		t.Fatal(e)
	}
	if got, _ := s.ForSession("s1"); got.Name != "新名字" {
		t.Fatal(got)
	}
	if _, ok := s.Lookup("device-b", "mutation-001"); ok {
		t.Fatal("cross principal receipt")
	}
}
func TestPartnerSingleMainAndFailedWriteIsNotApplied(t *testing.T) {
	root := t.TempDir()
	s, _ := Open(root)
	a, _ := s.Save("human", "mutation-a", 0, Profile{SessionID: "a", Name: "a", Color: "mint", Main: true})
	_, e := s.Save("human", "mutation-b", 0, Profile{SessionID: "b", Name: "b", Color: "blue", Main: true})
	if e != nil {
		t.Fatal(e)
	}
	a, _ = s.ForSession("a")
	if a.Main || a.Revision != 2 {
		t.Fatal(a)
	}
	if e = os.Remove(filepath.Join(root, "partners-v1", "profiles.json")); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(filepath.Join(root, "partners-v1", "profiles.json"), 0700); e != nil {
		t.Fatal(e)
	}
	a.Name = "must not commit"
	if _, e = s.Save("human", "mutation-fail", 2, a); e == nil {
		t.Fatal("write should fail")
	}
	if got, _ := s.ForSession("a"); got.Name != "a" {
		t.Fatal("failed write mutated memory")
	}
}
func TestPartnerUnicodeAndInputLimits(t *testing.T) {
	p := Profile{Name: strings.Repeat("👨‍👩‍👧‍👦", 40), Color: "mint"}
	if e := Validate(&p); e != nil {
		t.Fatal(e)
	}
	p.Name += "x"
	if e := Validate(&p); e == nil {
		t.Fatal("name overflow")
	}
	p.Name = "x"
	p.Avatar = "../../secret"
	if e := Validate(&p); e == nil {
		t.Fatal("unsafe digest")
	}
	root := t.TempDir()
	s, _ := Open(root)
	if _, e := s.Avatar("../../secret"); e == nil {
		t.Fatal("path traversal")
	}
	if _, e := s.PutAvatar([]byte("fake image")); e == nil {
		t.Fatal("invalid image")
	}
	if e := os.WriteFile(filepath.Join(root, "partners-v1", "profiles.json"), []byte("corrupt"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := Open(root); e == nil {
		t.Fatal("silently reset broken metadata")
	}
}

func TestPartnerAtomicMoveHandlesEqualRanksAndReplaysOnce(t *testing.T) {
	s, _ := Open(t.TempDir())
	for _, id := range []string{"a", "b", "c"} {
		if _, e := s.Save("human", "initial-"+id, 0, Profile{SessionID: id, Name: id, Color: "mint"}); e != nil {
			t.Fatal(e)
		}
	}
	rev, rows := s.Snapshot()
	middle := rows[1]
	moved, e := s.Move("human", "move-original", middle.SessionID, middle.Revision, rev, -1)
	if e != nil {
		t.Fatal(e)
	}
	after, ordered := s.Snapshot()
	if ordered[0].SessionID != middle.SessionID {
		t.Fatal("move did not change relative order", ordered)
	}
	replay, e := s.Move("human", "move-original", middle.SessionID, middle.Revision, rev, -1)
	if e != nil || replay != moved {
		t.Fatal("lost original move receipt", e)
	}
	last, _ := s.Snapshot()
	if after != last {
		t.Fatal("replay moved twice")
	}
	if _, e = s.Move("human", "move-stale", moved.SessionID, moved.Revision, rev, 1); e == nil {
		t.Fatal("stale roster accepted")
	}
	if e = s.PinInitialThread(moved.SessionID, "native-one"); e != nil {
		t.Fatal(e)
	}
	if e = s.PinInitialThread(moved.SessionID, "native-two"); e == nil {
		t.Fatal("initial pin replaced")
	}
}
