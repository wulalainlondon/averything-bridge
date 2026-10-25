package voicetransfer

import (
	"context"
	"sync"
	"testing"
	"time"
)

func sample() Record {
	return Record{ID: ID("authority", "request", "tool"), Authority: "authority", Device: "device", Request: "request", Turn: "turn", Tool: "tool", SourceVoice: "voice-a", Source: Target{Session: "a", Thread: "thread-a", Revision: 1}, Target: Target{Session: "b", Thread: "thread-b", Revision: 2}}
}
func TestDirectAuthorizationIsFreshAndCannotBeAddedToExistingIntent(t *testing.T) {
	now := time.Now()
	r := sample()
	r.Authorization = "voice_command"
	r.State = "confirmation_required"
	r.Revision = 1
	r.CreatedAt = now.Add(-5 * time.Second).UnixMilli()
	if !r.DirectAuthorized(now) {
		t.Fatal("fresh explicit voice intent refused")
	}
	r.CreatedAt = now.Add(-31 * time.Second).UnixMilli()
	if r.DirectAuthorized(now) {
		t.Fatal("expired intent authorized another effect")
	}
	r.CreatedAt = now.Add(time.Second).UnixMilli()
	if r.DirectAuthorized(now) {
		t.Fatal("future receipt accepted")
	}
	r.CreatedAt = now.UnixMilli()
	r.Authorization = ""
	if r.DirectAuthorized(now) {
		t.Fatal("legacy or restored receipt became direct authorization")
	}
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Create(context.Background(), sample()); err != nil {
		t.Fatal(err)
	}
	r = sample()
	r.Authorization = "voice_command"
	if _, err = s.Create(context.Background(), r); err == nil {
		t.Fatal("old intent retroactively authorized")
	}
}
func TestExactIntentDeduplicationAndCrossDeviceIsolation(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	r, err := s.Create(ctx, sample())
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.Create(ctx, sample())
	if err != nil || again.ID != r.ID || again.Revision != 1 {
		t.Fatal(again, err)
	}
	if _, err = s.Get(ctx, r.ID, "another-device"); err == nil {
		t.Fatal("cross-device receipt read")
	}
	other := sample()
	other.Target.Thread = "changed"
	if _, err = s.Create(ctx, other); err == nil {
		t.Fatal("same intent replaced")
	}
	if _, err = s.Advance(ctx, r, "completed", "voice-b"); err == nil {
		t.Fatal("accepted upgraded to completed")
	}
	confirmed, err := s.Advance(ctx, r, "confirmed", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Advance(ctx, r, "cancelled", ""); err == nil {
		t.Fatal("late old screen cancelled new stage")
	}
	if confirmed.Revision != 2 {
		t.Fatal(confirmed)
	}
}
func TestConcurrentIntentsCannotAcquireTwoDeviceTransfers(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, id := range []string{"first", "second"} {
		wg.Add(1)
		go func(id string) { defer wg.Done(); r := sample(); r.ID = id; _, err := s.Create(ctx, r); results <- err }(id)
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatal(success)
	}
}
func TestRestartRetainsCallerAndDoesNotReplayEffects(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Create(ctx, sample())
	if err != nil {
		t.Fatal(err)
	}
	r, err = s.Advance(ctx, r, "confirmed", "")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err = s.Get(ctx, r.ID, "device")
	if err != nil || r.State != "unknown_recovered" || r.Request != "request" || r.Tool != "tool" {
		t.Fatal(r, err)
	}
	if _, err = s.Advance(ctx, r, "starting_b", ""); err == nil {
		t.Fatal("restart replayed start")
	}
	other := sample()
	other.ID = "new-intent"
	if _, err = s.Create(ctx, other); err == nil {
		t.Fatal("unknown original silently replaced")
	}
	if _, err = s.Advance(ctx, r, "failed", ""); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
}
