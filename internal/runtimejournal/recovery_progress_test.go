package runtimejournal

import "testing"

func TestNativeRetryNoticeSurvivesThinkingAndClearsOnNewProgress(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("s1", "running", "r1", 0, "", "")
	s.Progress("s1", "r1", "composing", "")
	notice := "模型端正在重試；Bridge 持續觀察原工作。"
	retrying, changed := s.Progress("s1", "r1", "thinking", notice)
	if !changed || retrying.StageMessage != notice || retrying.Phase != "running" || retrying.ActiveRequestID != "r1" {
		t.Fatalf("native retry status was lost: %+v", retrying)
	}
	s.Flush()
	if got := New(dir).Snapshot("phone", []string{"s1"}); len(got) != 1 || got[0].StageMessage != notice {
		t.Fatal("retry notice lost on reconnect")
	}
	resumed, changed := s.Progress("s1", "r1", "composing", "")
	if !changed || resumed.StageMessage != "" {
		t.Fatal("retry notice survived resumed streaming")
	}
}

func TestLateRetryProgressCannotRebindNextTurnOrTerminal(t *testing.T) {
	s := New(t.TempDir())
	s.Update("s1", "running", "old", 0, "", "")
	s.Update("s1", "completed", "old", 0, "completed", "")
	s.Update("s1", "running", "new", 0, "", "")
	s.Progress("s1", "new", "thinking", "")
	before := s.Snapshot("phone", []string{"s1"})[0]
	late, changed := s.Progress("s1", "old", "thinking", "模型端正在重試；Bridge 持續觀察原工作。")
	if changed || late.ActiveRequestID != "new" || late.Revision != before.Revision || late.StageMessage != "" {
		t.Fatalf("stale retry rebound runtime: %+v", late)
	}
	terminal, _ := s.Update("s1", "completed", "new", 0, "completed", "")
	late, changed = s.Progress("s1", "new", "thinking", "模型端正在重試；Bridge 持續觀察原工作。")
	if changed || late.Phase != "completed" || late.Stage != "completed" || late.Revision != terminal.Revision {
		t.Fatal("late retry revived terminal")
	}
}
