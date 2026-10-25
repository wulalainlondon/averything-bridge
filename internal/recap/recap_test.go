package recap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestExcerptIsBoundedAndRetainsGoalAndLatestProgress(t *testing.T) {
	var messages []map[string]any
	for i := 0; i < 200; i++ {
		messages = append(messages, map[string]any{"role": "assistant", "content": fmt.Sprintf("turn-%03d %s", i, strings.Repeat("字", 4000))})
	}
	messages = append(messages, map[string]any{"role": "tool", "content": "SECRET_TOOL_OUTPUT"})
	text, hash := Excerpt(messages)
	if !strings.Contains(text, "turn-000") || !strings.Contains(text, "turn-199") || strings.Contains(text, "turn-100") || strings.Contains(text, "SECRET_TOOL_OUTPUT") || len([]rune(text)) > 66_000 {
		t.Fatal("bad excerpt bounds or selection")
	}
	messages[100]["content"] = "changed omitted progress"
	_, changed := Excerpt(messages)
	if hash == changed {
		t.Fatal("omitted edits did not invalidate source hash")
	}
}

func TestStorePersistsCurrentAndStaleResultsWithoutModelOnRead(t *testing.T) {
	s := &Store{Dir: t.TempDir()}
	calls := 0
	generate := func(context.Context) (Generated, error) {
		calls++
		next := "Bring Note20 online"
		return Generated{Summary: "APK built; model picker validation is unfinished", NextAction: &next}, nil
	}
	r, err := s.Get(context.Background(), "s", "t", "hash-1", false, false, generate)
	if err != nil || r.Snapshot != nil || calls != 0 {
		t.Fatal(r, err, calls)
	}
	r, err = s.Get(context.Background(), "s", "t", "hash-1", true, false, generate)
	if err != nil || r.Snapshot == nil || r.Stale || calls != 1 {
		t.Fatal(r, err, calls)
	}
	// Recreate the owner to verify durable cache rather than in-memory reuse.
	s = &Store{Dir: s.Dir}
	r, err = s.Get(context.Background(), "s", "t", "hash-2", false, false, generate)
	if err != nil || !r.Stale || r.Snapshot == nil || calls != 1 {
		t.Fatal(r, err, calls)
	}
	r, err = s.Get(context.Background(), "s", "different-thread", "hash-1", false, false, generate)
	if err != nil || r.Snapshot != nil || calls != 1 {
		t.Fatal("cross-thread cache leaked", r, err)
	}
}

func TestStoreFailurePreservesOldSummaryAndRapidRefreshDoesNotRetry(t *testing.T) {
	s := &Store{Dir: t.TempDir()}
	good := func(context.Context) (Generated, error) {
		return Generated{Summary: "Implemented but not deployed"}, nil
	}
	s.Get(context.Background(), "s", "t", "old", true, false, good)
	fail := func(context.Context) (Generated, error) { return Generated{}, errors.New("recap_generation_failed") }
	r, err := s.Get(context.Background(), "s", "t", "new", true, true, fail)
	if err == nil || r.Snapshot == nil || r.Snapshot.Summary != "Implemented but not deployed" || !r.Stale {
		t.Fatal(r, err)
	}
	r, err = s.Get(context.Background(), "s", "t", "old", true, true, fail)
	if err != nil || r.Snapshot == nil {
		t.Fatal("rapid refresh retried model", r, err)
	}
}

func TestConcurrentRequestsCoalesceAndReportDifferentSourceAsStale(t *testing.T) {
	s := &Store{Dir: t.TempDir()}
	start, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	generate := func(context.Context) (Generated, error) {
		if calls.Add(1) == 1 {
			close(start)
		}
		<-finish
		return Generated{Summary: "Completed APK; phone validation pending"}, nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r, err := s.Get(context.Background(), "s", "t", "hash", true, false, generate)
		if err != nil || r.Stale {
			t.Error(r, err)
		}
	}()
	<-start
	// A read while a generation is in flight never waits or starts another call.
	r, err := s.Get(context.Background(), "s", "t", "hash", false, false, generate)
	if err != nil || r.Snapshot != nil {
		t.Fatal(r, err)
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Get(context.Background(), "s", "t", "hash", true, false, generate)
			if err != nil {
				t.Error(err)
			}
		}()
	}
	close(finish)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatal("duplicate generations", calls.Load())
	}
}

func TestInvalidGenerationDoesNotCreateCache(t *testing.T) {
	s := &Store{Dir: t.TempDir()}
	_, err := s.Get(context.Background(), "s", "t", "hash", true, false, func(context.Context) (Generated, error) { return Generated{Summary: strings.Repeat("字", 2001)}, nil })
	if err == nil {
		t.Fatal("accepted oversized output")
	}
	r, err := s.Get(context.Background(), "s", "t", "hash", false, false, nil)
	if err != nil || r.Snapshot != nil {
		t.Fatal(r, err)
	}
}
