package history

import (
	"container/list"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// In-memory cache sizing. The SQLite tier is the source of truth and is always
// written; the in-memory tier is only a hot-set accelerator and MUST be bounded
// or a long-lived process accumulates every session's full transcript on the
// heap (parsed []map[string]any blows up 5-10x over the JSON bytes, so an 80MB
// corpus became ~700MB-1.2GB RSS). Accounting is by serialized JSON bytes.
const (
	defaultMaxMemBytes   = 32 << 20 // total in-memory budget (JSON bytes)
	defaultMaxEntryBytes = 1 << 20  // entries larger than this stay SQLite-only
)

type FileKey struct {
	Path    string
	MtimeNS int64
	Size    int64
}

type cacheEntry struct {
	key      FileKey
	messages []map[string]any
	bytes    int // serialized JSON size, used for LRU accounting
}

type lruItem struct {
	name  string
	entry cacheEntry
}

type Cache struct {
	mu         sync.Mutex
	mem        map[string]*list.Element // cacheName -> *list.Element holding *lruItem
	tailStates map[string]TailState
	ll         *list.List // front = most recently used
	curBytes   int

	maxBytes      int
	maxEntryBytes int
	db            *sql.DB
}

var (
	defaultCacheOnce sync.Once
	defaultCache     *Cache
)

func DefaultCache() *Cache {
	defaultCacheOnce.Do(func() {
		path := defaultCachePath()
		cache, err := NewSQLiteCache(path)
		if err != nil {
			defaultCache = NewMemoryCache()
			return
		}
		defaultCache = cache
	})
	return defaultCache
}

func defaultCachePath() string {
	base, err := os.UserCacheDir()
	if err != nil || base == "" {
		base = os.TempDir()
	}
	return filepath.Join(base, "everything-go", "history_cache.sqlite")
}

func newCache(db *sql.DB) *Cache {
	return &Cache{
		mem:           map[string]*list.Element{},
		tailStates:    map[string]TailState{},
		ll:            list.New(),
		maxBytes:      envBytes("EVERYTHING_GO_HISTORY_CACHE_MAX_BYTES", defaultMaxMemBytes),
		maxEntryBytes: envBytes("EVERYTHING_GO_HISTORY_CACHE_MAX_ENTRY_BYTES", defaultMaxEntryBytes),
		db:            db,
	}
}

func NewMemoryCache() *Cache {
	return newCache(nil)
}

func NewSQLiteCache(path string) (*Cache, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	c := newCache(db)
	if err := c.initDB(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return c, nil
}

func (c *Cache) initDB() error {
	if c.db == nil {
		return nil
	}
	_, err := c.db.Exec(`
CREATE TABLE IF NOT EXISTS history_cache (
	cache_name TEXT PRIMARY KEY,
	path TEXT NOT NULL,
	mtime_ns INTEGER NOT NULL,
	size INTEGER NOT NULL,
	messages_json TEXT NOT NULL,
	updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS history_tail_state (
	path TEXT PRIMARY KEY,
	size INTEGER NOT NULL,
	mtime_ns INTEGER NOT NULL,
	head_sha256 TEXT NOT NULL,
	line_count INTEGER NOT NULL,
	ended_with_newline INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
)`)
	return err
}

func (c *Cache) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

func (c *Cache) Load(cacheName string, key FileKey) ([]map[string]any, bool) {
	if c == nil || cacheName == "" {
		return nil, false
	}
	c.mu.Lock()
	if el, ok := c.mem[cacheName]; ok {
		it := el.Value.(*lruItem)
		if sameFileKey(it.entry.key, key) {
			c.ll.MoveToFront(el)
			messages := cloneMessages(it.entry.messages)
			c.mu.Unlock()
			return messages, true
		}
		// Stale file key (transcript changed on disk) — drop it.
		c.removeElementLocked(el)
	}
	c.mu.Unlock()
	if c.db == nil {
		return nil, false
	}

	var raw string
	err := c.db.QueryRow(
		`SELECT messages_json FROM history_cache WHERE cache_name=? AND path=? AND mtime_ns=? AND size=?`,
		cacheName, key.Path, key.MtimeNS, key.Size,
	).Scan(&raw)
	if err != nil {
		return nil, false
	}
	var messages []map[string]any
	if json.Unmarshal([]byte(raw), &messages) != nil {
		return nil, false
	}
	c.mu.Lock()
	c.putLocked(cacheName, cacheEntry{key: key, messages: cloneMessages(messages), bytes: len(raw)})
	c.mu.Unlock()
	return messages, true
}

func (c *Cache) Save(cacheName string, key FileKey, messages []map[string]any) {
	c.store(cacheName, key, cloneMessages(messages))
}

func (c *Cache) SaveAsync(cacheName string, key FileKey, messages []map[string]any) {
	if c == nil {
		return
	}
	snapshot := cloneMessages(messages)
	go c.store(cacheName, key, snapshot)
}

// LoadTailState returns the persisted physical-line cursor used to avoid
// recounting an append-only rollout from byte zero on every history request.
func (c *Cache) LoadTailState(path string) (TailState, bool) {
	if c == nil || path == "" {
		return TailState{}, false
	}
	c.mu.Lock()
	if state, ok := c.tailStates[path]; ok {
		c.mu.Unlock()
		return state, true
	}
	c.mu.Unlock()
	if c.db == nil {
		return TailState{}, false
	}
	var state TailState
	var ended int
	err := c.db.QueryRow(
		`SELECT path,size,mtime_ns,head_sha256,line_count,ended_with_newline
		 FROM history_tail_state WHERE path=?`, path,
	).Scan(&state.Path, &state.Size, &state.MtimeNS, &state.HeadSHA256, &state.LineCount, &ended)
	if err != nil {
		return TailState{}, false
	}
	state.EndedWithNewline = ended != 0
	c.mu.Lock()
	c.tailStates[path] = state
	c.mu.Unlock()
	return state, true
}

// SaveTailState persists the line cursor synchronously. It is tiny and must be
// durable before a later request attempts an incremental count after restart.
func (c *Cache) SaveTailState(state TailState) {
	if c == nil || state.Path == "" {
		return
	}
	c.mu.Lock()
	c.tailStates[state.Path] = state
	c.mu.Unlock()
	if c.db == nil {
		return
	}
	ended := 0
	if state.EndedWithNewline {
		ended = 1
	}
	_, _ = c.db.Exec(
		`INSERT INTO history_tail_state(path,size,mtime_ns,head_sha256,line_count,ended_with_newline,updated_at)
		 VALUES(?,?,?,?,?,?,?)
		 ON CONFLICT(path) DO UPDATE SET
		  size=excluded.size,
		  mtime_ns=excluded.mtime_ns,
		  head_sha256=excluded.head_sha256,
		  line_count=excluded.line_count,
		  ended_with_newline=excluded.ended_with_newline,
		  updated_at=excluded.updated_at`,
		state.Path, state.Size, state.MtimeNS, state.HeadSHA256, state.LineCount, ended, time.Now().Unix(),
	)
}

// store takes ownership of stored — the caller must not mutate it afterwards.
// Both Save and SaveAsync hand it an exclusive clone, so we never clone again.
func (c *Cache) store(cacheName string, key FileKey, stored []map[string]any) {
	if c == nil || cacheName == "" {
		return
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		return
	}
	c.mu.Lock()
	c.putLocked(cacheName, cacheEntry{key: key, messages: stored, bytes: len(raw)})
	c.mu.Unlock()
	if c.db == nil {
		return
	}
	_, _ = c.db.Exec(
		`INSERT INTO history_cache(cache_name,path,mtime_ns,size,messages_json,updated_at)
VALUES(?,?,?,?,?,?)
ON CONFLICT(cache_name) DO UPDATE SET
	path=excluded.path,
	mtime_ns=excluded.mtime_ns,
	size=excluded.size,
	messages_json=excluded.messages_json,
	updated_at=excluded.updated_at`,
		cacheName, key.Path, key.MtimeNS, key.Size, string(raw), time.Now().Unix(),
	)
}

// putLocked inserts or replaces the in-memory entry for cacheName and evicts
// least-recently-used entries until the byte budget is satisfied. Entries that
// exceed maxEntryBytes are dropped from the mem tier entirely (SQLite-only):
// the few huge transcripts are exactly the worst heap offenders and rarely the
// hot set. Caller holds c.mu.
func (c *Cache) putLocked(cacheName string, entry cacheEntry) {
	if el, ok := c.mem[cacheName]; ok {
		c.removeElementLocked(el)
	}
	if c.maxEntryBytes > 0 && entry.bytes > c.maxEntryBytes {
		return
	}
	el := c.ll.PushFront(&lruItem{name: cacheName, entry: entry})
	c.mem[cacheName] = el
	c.curBytes += entry.bytes
	for c.maxBytes > 0 && c.curBytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil || back == el {
			break
		}
		c.removeElementLocked(back)
	}
}

func (c *Cache) removeElementLocked(el *list.Element) {
	it := el.Value.(*lruItem)
	delete(c.mem, it.name)
	c.curBytes -= it.entry.bytes
	c.ll.Remove(el)
}

func sameFileKey(a, b FileKey) bool {
	return a.Path == b.Path && a.MtimeNS == b.MtimeNS && a.Size == b.Size
}

func cloneMessages(messages []map[string]any) []map[string]any {
	if messages == nil {
		return nil
	}
	out := make([]map[string]any, len(messages))
	for i, m := range messages {
		cp := make(map[string]any, len(m))
		for k, v := range m {
			cp[k] = v
		}
		out[i] = cp
	}
	return out
}

func envBytes(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}
