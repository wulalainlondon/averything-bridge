package core

import (
	"errors"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"everything-go/internal/history"
)

// Anti-storm hardening: the mobile app half-disconnects and reconnects in bursts
// (WebRTC↔WS flap, backgrounding, flaky LAN/Tailscale), which otherwise produces
// a request storm — repeated hellos, repeated heavy scans (history/resumable/
// browse), and stale clients still enqueueing. The defenses here are:
//   1. latest-device-wins  : one live client per device (registerLatest/isCurrent)
//   3. current-client guard : Client.live() drops a replaced/dead client's results
//   4/6/7. coalesce+cache   : identical heavy requests share one execution + a TTL
//   9. semaphore            : a global cap on concurrent heavy work

// heavySlots caps concurrent heavy tasks (history load / resumable scan / browse
// enrichment). singleflight already collapses identical requests; this bounds
// distinct ones so a burst can't spawn unbounded work.
const heavySlots = 6

const (
	historyCacheTTL   = 2 * time.Second
	resumableCacheTTL = 5 * time.Second
)

type stormGuards struct {
	histSF   singleflight.Group
	resumeSF singleflight.Group

	histCache   *ttlCache
	resumeCache *ttlCache

	heavySem chan struct{}
}

func newStormGuards() *stormGuards {
	return &stormGuards{
		histCache:   newTTLCache(),
		resumeCache: newTTLCache(),
		heavySem:    make(chan struct{}, heavySlots),
	}
}

// --- latest-device-wins (#1) ------------------------------------------------

// registerLatest marks c as the newest client for its device and evicts the
// previous one. Called once, right after the hello sets c.deviceID.
func (h *Hub) registerLatest(c *Client) {
	if c.deviceID == "" || c.offlineDeviceGuard {
		return
	}
	h.latestMu.Lock()
	prev := h.latestByDevice[c.deviceID]
	h.latestByDevice[c.deviceID] = c
	h.latestMu.Unlock()

	if prev != nil && prev != c {
		log.Printf("[storm] device=%s: newer client %s replaces %s", c.deviceID, c.clientID, prev.clientID)
		h.releaseReplayLease(prev)
		prev.shutdown()                                                 // stop write pump + cancel heavy work
		go prev.conn.Close("replaced by newer client from same device") // unblock its read loop → serveConn cleanup
	}
}

// isCurrent reports whether c is still the latest client for its device. A
// device-less client (pre-hello) is treated as current.
func (h *Hub) isCurrent(c *Client) bool {
	if c.deviceID == "" {
		return true
	}
	h.latestMu.Lock()
	defer h.latestMu.Unlock()
	return h.latestByDevice[c.deviceID] == c
}

// --- coalesce + cache + semaphore (#4/#6/#7/#9) -----------------------------

// coalesce returns a cached result if fresh, else runs fn exactly once across
// all concurrent callers of the same key (singleflight), caching the result for
// ttl. The actual work holds a heavy-task slot (the global semaphore backstop).
func (h *Hub) coalesce(sf *singleflight.Group, cache *ttlCache, key string, ttl time.Duration, fn func() any) any {
	if v, ok := cache.get(key); ok {
		return v
	}
	v, _, _ := sf.Do(key, func() (any, error) {
		if cv, ok := cache.get(key); ok { // a racing caller may have filled it
			return cv, nil
		}
		h.storm.heavySem <- struct{}{} // bounded concurrency backstop
		defer func() { <-h.storm.heavySem }()
		res := fn()
		if _, failed := res.(error); res != nil && !failed {
			cache.set(key, res, ttl)
		}
		return res, nil
	})
	return v
}

// coalescedResumable runs the (heavy) all-providers resumable scan at most once
// per (limit) within the TTL, shared by get_resumable_sessions and browse_dir.
type resumableReadFailure struct {
	sessions []history.ResumableSession
	err      error
}

func (e *resumableReadFailure) Error() string { return e.err.Error() }
func (e *resumableReadFailure) Unwrap() error { return e.err }

func (h *Hub) coalescedResumable(limit int) []history.ResumableSession {
	sessions, _ := h.coalescedResumableChecked(limit)
	return sessions
}

func (h *Hub) coalescedResumableChecked(limit int) ([]history.ResumableSession, error) {
	hr, ok := h.exec.(historyRouter)
	if !ok {
		return nil, nil
	}
	key := "resumable:" + itoa(limit)
	v := h.coalesce(&h.storm.resumeSF, h.storm.resumeCache, key, resumableCacheTTL, func() any {
		all := make([]history.ResumableSession, 0)
		var failures []error
		for _, p := range hr.AllProviders() {
			list, err := p.ResumableSessions(limit)
			if err != nil {
				failures = append(failures, err)
				continue
			}
			for _, entry := range list {
				if h.cwdInScope(entry.Cwd) {
					all = append(all, entry)
				}
			}
		}
		if len(failures) > 0 {
			return &resumableReadFailure{sessions: all, err: errors.Join(failures...)}
		}
		return all
	})
	if v == nil {
		return nil, nil
	}
	if failure, ok := v.(*resumableReadFailure); ok {
		return failure.sessions, failure.err
	}
	return v.([]history.ResumableSession), nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// --- tiny TTL cache ---------------------------------------------------------

type ttlEntry struct {
	val any
	exp time.Time
	gen uint64
}

type ttlCache struct {
	mu  sync.Mutex
	m   map[string]ttlEntry
	gen uint64
}

func newTTLCache() *ttlCache { return &ttlCache{m: map[string]ttlEntry{}} }

func (c *ttlCache) get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok {
		return nil, false
	}
	if !time.Now().Before(e.exp) {
		delete(c.m, key)
		return nil, false
	}
	return e.val, true
}

func (c *ttlCache) set(key string, val any, ttl time.Duration) {
	c.mu.Lock()
	c.gen++
	gen := c.gen
	exp := time.Now().Add(ttl)
	c.m[key] = ttlEntry{val: val, exp: exp, gen: gen}
	c.mu.Unlock()

	// A lookup deletes an entry it observes as expired, but history cache keys
	// include the session and pagination cursor and are often never requested
	// again. Without an active expiry, every distinct history result remains in
	// the map forever even though callers see a two-second TTL. The timer only
	// captures the small key/generation pair, not the cached value.
	time.AfterFunc(ttl, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if e, ok := c.m[key]; ok && e.gen == gen && !time.Now().Before(e.exp) {
			delete(c.m, key)
		}
	})
}
