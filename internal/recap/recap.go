// Package recap owns a derived catch-up summary, never canonical chat history.
package recap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type Snapshot struct {
	ThreadID    string  `json:"thread_id"`
	SourceHash  string  `json:"source_hash"`
	Summary     string  `json:"summary"`
	NextAction  *string `json:"next_action"`
	GeneratedAt int64   `json:"generated_at_ms"`
}

type Result struct {
	Snapshot *Snapshot `json:"snapshot,omitempty"`
	Stale    bool      `json:"stale"`
}

type Generated struct {
	Summary    string  `json:"summary"`
	NextAction *string `json:"next_action"`
}

var errInvalidCache = errors.New("recap_cache_invalid")

func (g Generated) Valid() bool {
	return strings.TrimSpace(g.Summary) != "" && len([]rune(g.Summary)) <= 2000 &&
		(g.NextAction == nil || len([]rune(*g.NextAction)) <= 1000)
}

// Excerpt preserves the beginning and recent end, excludes tools/thinking and
// images, and bounds input before a model request. The hash covers all supplied
// messages (including omitted ones), so a cached summary cannot look current
// after a transcript edit or a new turn.
func Excerpt(messages []map[string]any) (string, string) {
	var turns []map[string]string
	for _, m := range messages {
		role, _ := m["role"].(string)
		text, _ := m["content"].(string)
		if (role == "user" || role == "assistant") && strings.TrimSpace(text) != "" {
			turns = append(turns, map[string]string{"role": role, "text": text})
		}
	}
	all, _ := json.Marshal(turns)
	hash := sha256.Sum256(all)
	var b strings.Builder
	for i, turn := range turns {
		if len(turns) > 40 && i == 8 {
			b.WriteString("\n[Earlier exchanges omitted]\n")
		}
		if len(turns) > 40 && i >= 8 && i < len(turns)-32 {
			continue
		}
		text := []rune(turn["text"])
		if len(text) > 1600 {
			text = append(text[:1600], []rune("\n[Message excerpted]")...)
		}
		b.WriteString("\n" + turn["role"] + ": " + string(text) + "\n")
	}
	return b.String(), hex.EncodeToString(hash[:])
}

type Store struct {
	Dir    string
	mu     sync.Mutex
	flight singleflight.Group
}

func (s *Store) path(sessionID string) string {
	hash := sha256.Sum256([]byte(sessionID))
	return filepath.Join(s.Dir, "session-recaps", hex.EncodeToString(hash[:])+".json")
}

func (s *Store) read(sessionID string) (*Snapshot, error) {
	f, err := os.Open(s.path(sessionID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 32*1024+1))
	if err != nil {
		return nil, err
	}
	var v Snapshot
	if len(b) > 32*1024 || json.Unmarshal(b, &v) != nil || v.ThreadID == "" || v.SourceHash == "" || v.GeneratedAt <= 0 || !(Generated{v.Summary, v.NextAction}).Valid() {
		return nil, errInvalidCache
	}
	return &v, nil
}

func (s *Store) Get(ctx context.Context, sessionID, threadID, sourceHash string, generate, force bool, makeRecap func(context.Context) (Generated, error)) (Result, error) {
	if s.Dir == "" {
		return Result{}, errors.New("recap_cache_unavailable")
	}
	s.mu.Lock()
	cached, err := s.read(sessionID)
	s.mu.Unlock()
	if err != nil && !(generate && errors.Is(err, errInvalidCache)) {
		return Result{}, errors.New("recap_cache_unavailable")
	}
	if cached != nil && cached.ThreadID != threadID {
		cached = nil
	}
	result := Result{Snapshot: cached, Stale: cached != nil && cached.SourceHash != sourceHash}
	if !generate || cached != nil && !result.Stale && !force {
		return result, nil
	}
	// One generation per conversation across all devices. A conflicting source
	// waits for the existing request, then reports its result as stale.
	ch := s.flight.DoChan(sessionID, func() (any, error) {
		// Recheck after joining singleflight, and absorb rapid sequential button
		// taps even when force is set. A refresh never becomes a retry storm.
		s.mu.Lock()
		latest, readErr := s.read(sessionID)
		s.mu.Unlock()
		if readErr == nil && latest != nil && latest.ThreadID == threadID && latest.SourceHash == sourceHash && (!force || time.Now().UnixMilli()-latest.GeneratedAt < 15_000) {
			return *latest, nil
		}
		generationCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		g, err := makeRecap(generationCtx)
		if err != nil {
			return nil, err
		}
		if !g.Valid() {
			return nil, errors.New("recap_invalid")
		}
		g.Summary = strings.TrimSpace(g.Summary)
		if g.NextAction != nil {
			next := strings.TrimSpace(*g.NextAction)
			g.NextAction = &next
			if next == "" {
				g.NextAction = nil
			}
		}
		v := Snapshot{ThreadID: threadID, SourceHash: sourceHash, Summary: g.Summary, NextAction: g.NextAction, GeneratedAt: time.Now().UnixMilli()}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.Dir == "" {
			return nil, errors.New("recap_cache_unavailable")
		}
		path := s.path(sessionID)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, errors.New("recap_cache_unavailable")
		}
		f, err := os.CreateTemp(filepath.Dir(path), ".recap-*")
		if err != nil {
			return nil, errors.New("recap_cache_unavailable")
		}
		defer os.Remove(f.Name())
		b, _ := json.Marshal(v)
		if _, err = f.Write(b); err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			err = os.Rename(f.Name(), path)
		}
		if err != nil {
			return nil, errors.New("recap_cache_unavailable")
		}
		return v, nil
	})
	select {
	case <-ctx.Done():
		return result, errors.New("recap_timeout")
	case completed := <-ch:
		if completed.Err != nil {
			return result, completed.Err
		}
		v := completed.Val.(Snapshot)
		if v.ThreadID != threadID {
			return result, errors.New("recap_source_changed")
		}
		return Result{Snapshot: &v, Stale: v.SourceHash != sourceHash}, nil
	}
}
