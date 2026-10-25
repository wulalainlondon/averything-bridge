package history

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestHistoryCacheMemoryHitAndMiss(t *testing.T) {
	c := NewMemoryCache()
	key := FileKey{Path: "/tmp/a.jsonl", MtimeNS: 10, Size: 20}
	msgs := []map[string]any{{"role": "user", "content": "hello"}}
	c.Save("claude:abc", key, msgs)

	got, ok := c.Load("claude:abc", key)
	if !ok || len(got) != 1 || got[0]["content"] != "hello" {
		t.Fatalf("cache hit failed ok=%v got=%+v", ok, got)
	}
	if _, ok := c.Load("claude:abc", FileKey{Path: "/tmp/a.jsonl", MtimeNS: 11, Size: 20}); ok {
		t.Fatal("cache must miss when file key changes")
	}
}

func TestHistoryCacheSQLitePersistsAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	key := FileKey{Path: "/tmp/a.jsonl", MtimeNS: 10, Size: 20}

	c1, err := NewSQLiteCache(path)
	if err != nil {
		t.Fatal(err)
	}
	c1.Save("claude:abc", key, []map[string]any{{"role": "assistant", "content": "cached"}})
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	c2, err := NewSQLiteCache(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	got, ok := c2.Load("claude:abc", key)
	if !ok || len(got) != 1 || got[0]["content"] != "cached" {
		t.Fatalf("sqlite cache miss ok=%v got=%+v", ok, got)
	}
}

func TestHistoryTailStatePersistsAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	state := TailState{Path: "/tmp/rollout.jsonl", Size: 123, MtimeNS: 456, HeadSHA256: "head", LineCount: 12, EndedWithNewline: true}
	c1, err := NewSQLiteCache(path)
	if err != nil {
		t.Fatal(err)
	}
	c1.SaveTailState(state)
	_ = c1.Close()
	c2, err := NewSQLiteCache(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	got, ok := c2.LoadTailState(state.Path)
	if !ok || got != state {
		t.Fatalf("tail state did not survive restart ok=%v got=%+v want=%+v", ok, got, state)
	}
}

func TestHistoryCacheEvictsLRUOverByteBudget(t *testing.T) {
	c := NewMemoryCache()
	c.maxBytes = 4096 // tiny budget so a few entries force eviction
	c.maxEntryBytes = 4096
	big := make([]map[string]any, 0, 40)
	for i := 0; i < 40; i++ {
		big = append(big, map[string]any{"role": "user", "content": "xxxxxxxxxxxxxxxxxxxx"})
	}
	key := func(p string) FileKey { return FileKey{Path: p, MtimeNS: 1, Size: 1} }
	c.Save("claude:a", key("/a"), big)
	c.Save("claude:b", key("/b"), big)
	c.Save("claude:c", key("/c"), big) // pushes total over budget -> oldest ("a") evicted

	c.mu.Lock()
	if c.curBytes > c.maxBytes {
		t.Fatalf("curBytes %d exceeds budget %d after eviction", c.curBytes, c.maxBytes)
	}
	_, aResident := c.mem["claude:a"]
	c.mu.Unlock()
	// Memory-only cache (db==nil): an evicted entry is simply gone.
	if aResident {
		t.Fatal("least-recently-used entry should have been evicted")
	}
	if _, ok := c.Load("claude:a", key("/a")); ok {
		t.Fatal("evicted entry must miss in a memory-only cache")
	}
}

func TestHistoryCacheOversizedEntrySkipsMemButHitsSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	c, err := NewSQLiteCache(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.maxEntryBytes = 32 // anything non-trivial exceeds this -> SQLite-only

	key := FileKey{Path: "/big.jsonl", MtimeNS: 7, Size: 9}
	c.Save("claude:big", key, []map[string]any{{"role": "assistant", "content": "well over thirty-two bytes once serialized"}})

	c.mu.Lock()
	_, resident := c.mem["claude:big"]
	c.mu.Unlock()
	if resident {
		t.Fatal("oversized entry must not be held in the mem tier")
	}
	// SQLite tier still serves it.
	if got, ok := c.Load("claude:big", key); !ok || len(got) != 1 {
		t.Fatalf("oversized entry should still load from SQLite ok=%v got=%+v", ok, got)
	}
}

func TestHistoryCacheCorruptSQLiteJSONMisses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.sqlite")
	c, err := NewSQLiteCache(path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(
		`INSERT INTO history_cache(cache_name,path,mtime_ns,size,messages_json,updated_at) VALUES(?,?,?,?,?,?)`,
		"claude:bad", "/tmp/a.jsonl", 1, 2, "{bad json", 123,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Load("claude:bad", FileKey{Path: "/tmp/a.jsonl", MtimeNS: 1, Size: 2}); ok {
		t.Fatal("corrupt cached JSON should miss")
	}
}
