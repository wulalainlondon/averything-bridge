package mediaservice

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testOrigin() backend.MediaOrigin {
	return backend.MediaOrigin{SessionID: "s", ThreadID: "t", TurnID: "turn", VoiceID: "voice", Generation: "gen"}
}
func testRequest() Request {
	return Request{ID: "vop_one_operation", Thread: "t", Method: "play_youtube", Input: backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}}
}
func TestKnownDispatchReceiptSurvivesCallerDeadlineWithoutReplay(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	r, err := s.Execute(ctx, testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
		cancel() // Device dispatch completed just as the caller's budget expired.
		return backend.VoiceMediaResult{CommandID: "original-command", State: "queued"}, nil
	})
	if err != nil || r.CommandID != "original-command" || r.Dispatched == nil || !*r.Dispatched || r.PlaybackConfirmed {
		t.Fatal(r, err)
	}
	r, err = s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
		t.Fatal("replayed after deadline")
		return backend.VoiceMediaResult{}, nil
	})
	if err != nil || r.CommandID != "original-command" || !r.Historical || r.PlaybackConfirmed {
		t.Fatal(r, err)
	}
}
func TestReceiptSurvivesRestartAndDoesNotReplayUnknown(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	var called atomic.Int32
	r, e := s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
		called.Add(1)
		return backend.VoiceMediaResult{}, errors.New("lost_after_send")
	})
	if e == nil || r.State != "unknown" || r.Dispatched != nil {
		t.Fatal(r, e)
	}
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r, e = s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
		called.Add(1)
		return backend.VoiceMediaResult{State: "playing"}, nil
	})
	if e != nil || r.State != "unknown" || called.Load() != 1 {
		t.Fatal(r, e, called.Load())
	}
}

func TestPreDispatchRefusalSurvivesRestartStatusAndNewVoice(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
		return backend.VoiceMediaResult{}, &backend.VoiceMediaNotDispatchedError{Cause: errors.New("voice_media_player_not_ready")}
	})
	if err != nil || r.State != "failed" || r.Dispatched == nil || *r.Dispatched || r.SuggestedAction != "music_unavailable_for_this_call" {
		t.Fatal(r, err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	q := testRequest()
	for _, status := range []bool{false, true} {
		if status {
			q.Method, q.Operation, q.ID, q.Input = "status", q.ID, "vop_status_query", backend.VoiceMediaRequest{}
		}
		o := testOrigin()
		o.Generation, o.VoiceID = "new-generation", "new-voice"
		r, err = s.Execute(context.Background(), o, q, func() (backend.VoiceMediaResult, error) {
			t.Fatal("refused operation must not run or observe a later device command")
			return backend.VoiceMediaResult{}, nil
		})
		if err != nil || r.State != "failed" || r.ErrorCode != "voice_media_player_not_ready" || r.Dispatched == nil || *r.Dispatched || !r.Historical {
			t.Fatal(r, err)
		}
	}
}

func TestUnmarkedOrConflictingRefusalRemainsUnknown(t *testing.T) {
	for _, markedWithCommand := range []bool{false, true} {
		t.Run(fmt.Sprint(markedWithCommand), func(t *testing.T) {
			s, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r, err := s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
				e := errors.New("voice_media_player_not_ready")
				if markedWithCommand {
					return backend.VoiceMediaResult{CommandID: "possibly-sent"}, &backend.VoiceMediaNotDispatchedError{Cause: e}
				}
				return backend.VoiceMediaResult{}, e
			})
			if err == nil || r.State != "unknown" || r.Dispatched != nil {
				t.Fatal(r, err)
			}
		})
	}
}
func TestConcurrentDuplicateIntentHasOneEffectAndConflictFails(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var count atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := s.Execute(context.Background(), testOrigin(), testRequest(), func() (backend.VoiceMediaResult, error) {
				count.Add(1)
				return backend.VoiceMediaResult{CommandID: "c", State: "queued"}, nil
			})
			if e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if count.Load() != 1 {
		t.Fatal(count.Load())
	}
	q := testRequest()
	q.Input.URL = "https://youtu.be/abcdefghijk"
	if _, e = s.Execute(context.Background(), testOrigin(), q, func() (backend.VoiceMediaResult, error) {
		t.Fatal("conflict effect")
		return backend.VoiceMediaResult{}, nil
	}); e == nil {
		t.Fatal("no conflict")
	}
}
func TestOperationStatusCannotRefreshFromAnotherCommandOrVoice(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	o := testOrigin()
	q := testRequest()
	_, e = s.Execute(context.Background(), o, q, func() (backend.VoiceMediaResult, error) {
		return backend.VoiceMediaResult{CommandID: "original", State: "playing", Observed: true, PlaybackConfirmed: true, LastObservedMS: time.Now().UnixMilli()}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	q.Method = "status"
	q.Operation = q.ID
	q.ID = "vop_status_query"
	q.Input = backend.VoiceMediaRequest{}
	r, e := s.Execute(context.Background(), o, q, func() (backend.VoiceMediaResult, error) {
		return backend.VoiceMediaResult{CommandID: "newer", State: "playing"}, nil
	})
	if e != nil || r.CommandID != "original" {
		t.Fatal(r, e)
	}
	o.Generation = "other-generation"
	r, e = s.Execute(context.Background(), o, q, func() (backend.VoiceMediaResult, error) {
		t.Fatal("old operation targeted new voice")
		return backend.VoiceMediaResult{}, nil
	})
	if e != nil || r.State != "expired" || r.PlaybackConfirmed {
		t.Fatal(r, e)
	}
}
