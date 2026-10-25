package runtimejournal

import "testing"

func TestBlockingInteractionsSurviveRestartAndResumeOnlyAfterLastResolution(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("s1", "running", "turn", 4, "", "")
	initial := s.Snapshot("", []string{"s1"})[0]
	if _, changed := s.WaitForInteraction("s1", "first"); !changed {
		t.Fatal("did not wait")
	}
	s.WaitForInteraction("s1", "second")
	s = New(dir)
	if _, changed := s.ResolveInteraction("s1", "first"); changed {
		t.Fatal("resumed before final question")
	}
	if got := s.Snapshot("", []string{"s1"})[0]; got.Phase != "waiting" || got.ActiveRequestID != "turn" {
		t.Fatal(got)
	}
	if _, changed := s.ResolveInteraction("s1", "second"); !changed {
		t.Fatal("did not resume")
	}
	got := s.Snapshot("", []string{"s1"})[0]
	if got.Phase != "running" || got.ActiveRequestID != "turn" || got.QueueLength != 4 || got.ActiveStartedAt != initial.ActiveStartedAt {
		t.Fatal(got)
	}
	if _, changed := s.ResolveInteraction("s1", "second"); changed {
		t.Fatal("duplicate resolution changed runtime")
	}
}

func TestBlockingInteractionCannotCreateOrReviveWork(t *testing.T) {
	for _, phase := range []string{"idle", "queued", "completed", "failed", "interrupted", "closed", "stopping"} {
		t.Run(phase, func(t *testing.T) {
			s := New("")
			s.Update("s1", phase, "turn", 2, "", "")
			before := s.Snapshot("", []string{"s1"})[0]
			if _, changed := s.WaitForInteraction("s1", "question"); changed {
				t.Fatal("fabricated blocked turn")
			}
			if _, changed := s.ResolveInteraction("s1", "question"); changed {
				t.Fatal("fabricated running turn")
			}
			if after := s.Snapshot("", []string{"s1"})[0]; after != before {
				t.Fatal(after)
			}
		})
	}
}
