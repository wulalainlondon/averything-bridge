package session

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func readRetentionEntries(t *testing.T, filename string) map[string]map[string]json.RawMessage {
	t.Helper()
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	// Always decode into a fresh map: Unmarshal does not remove keys from an
	// existing map, which could hide an eviction from the on-disk index.
	var entries map[string]map[string]json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatal(err)
	}
	return entries
}

func writeRetentionEntries(t *testing.T, filename string, entries any) {
	t.Helper()
	data, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRetainsLargeIndexAcrossSavesAndRestarts(t *testing.T) {
	for _, count := range []int{501, 509, 1200} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "saved_sessions.json")
			seed := make(map[string]map[string]any, count)
			old := time.Now().Add(-90 * 24 * time.Hour).Unix()
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("s-%04d", i)
				seed[id] = map[string]any{
					"name": id, "backend": "codex", "cwd": "/retention-test",
					"resume_id": "thread-" + id, "claude_uuid": "thread-" + id,
					"last_used": old + int64(i), "created_at": old,
					"pinned": i == 0, "hidden": i == 1,
					"foreign_metadata": map[string]any{"value": i},
				}
			}
			writeRetentionEntries(t, filename, seed)
			for restart := 0; restart < 3; restart++ {
				registry := NewRegistry()
				registry.AttachStore(NewStore(filename))
				if got := len(registry.List()); got != count {
					t.Fatalf("restart %d restored %d sessions, want %d", restart, got, count)
				}
				for save := 0; save < 3; save++ {
					if err := registry.PersistDurably(); err != nil {
						t.Fatal(err)
					}
					entries := readRetentionEntries(t, filename)
					if len(entries) != count {
						t.Fatalf("restart %d save %d retained %d entries, want %d", restart, save, len(entries), count)
					}
					for id := range seed {
						if _, ok := entries[id]; !ok {
							t.Fatalf("missing entry %s", id)
						}
						if len(entries[id]["foreign_metadata"]) == 0 {
							t.Fatalf("foreign metadata lost for %s", id)
						}
					}
					if string(entries["s-0000"]["pinned"]) != "true" || string(entries["s-0001"]["hidden"]) != "true" {
						t.Fatal("pinned or hidden session metadata lost")
					}
				}
			}
		})
	}
}

func TestStoreRetainsOldForeignEntriesButHonorsExplicitDeletion(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "saved_sessions.json")
	registry := NewRegistry()
	registry.AttachStore(NewStore(filename))
	registry.Create("delete-me", "known session", "/test", "codex", "", "", "known-thread")
	if err := registry.PersistDurably(); err != nil {
		t.Fatal(err)
	}
	entries := readRetentionEntries(t, filename)
	oldForeign, err := json.Marshal(map[string]any{
		"name": "foreign", "backend": "codex", "cwd": "/foreign",
		"last_used": 1, "resume_id": "foreign-thread", "pinned": true,
		"foreign_metadata": "must survive",
	})
	if err != nil {
		t.Fatal(err)
	}
	var foreign map[string]json.RawMessage
	if err := json.Unmarshal(oldForeign, &foreign); err != nil {
		t.Fatal(err)
	}
	entries["foreign"] = foreign // Written by another process after our Load.
	writeRetentionEntries(t, filename, entries)
	registry.Delete("delete-me")
	if err := registry.PersistDurably(); err != nil {
		t.Fatal(err)
	}
	got := readRetentionEntries(t, filename)
	if _, ok := got["delete-me"]; ok {
		t.Fatal("explicitly deleted session was resurrected")
	}
	if _, ok := got["foreign"]; !ok {
		t.Fatal("old foreign entry was automatically evicted")
	}
	if string(got["foreign"]["foreign_metadata"]) != `"must survive"` {
		t.Fatal("foreign metadata changed")
	}
}

func TestStoreRetainsLargeIndexFromIndependentWriters(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "saved_sessions.json")
	a, b := NewStore(filename), NewStore(filename)
	a.Load()
	b.Load()
	var sessionsA, sessionsB []*Session
	for i := 0; i < 600; i++ {
		id := fmt.Sprintf("%04d", i)
		sessionsA = append(sessionsA, &Session{ID: "a-" + id, name: "A", backend: "codex", resumeID: "thread-a-" + id, lastActivity: 1})
		sessionsB = append(sessionsB, &Session{ID: "b-" + id, name: "B", backend: "codex", resumeID: "thread-b-" + id, lastActivity: 1})
	}
	for i := 0; i < 3; i++ {
		if err := a.Save(sessionsA); err != nil {
			t.Fatal(err)
		}
		if err := b.Save(sessionsB); err != nil {
			t.Fatal(err)
		}
		if got := len(readRetentionEntries(t, filename)); got != 1200 {
			t.Fatalf("independent writers retained %d entries, want 1200", got)
		}
	}
}
