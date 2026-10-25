package main

import (
	"strings"
	"testing"
)

func TestDirtyPathQueueDeduplicatesAndSorts(t *testing.T) {
	queue := newDirtyPathQueue(4)
	queue.Add("/tmp/b.jsonl")
	queue.Add("/tmp/a.jsonl")
	queue.Add("/tmp/b.jsonl")
	paths, overflow := queue.Drain()
	if overflow {
		t.Fatal("deduplicated queue overflowed")
	}
	want := []string{"/tmp/a.jsonl", "/tmp/b.jsonl"}
	if len(paths) != len(want) || paths[0] != want[0] || paths[1] != want[1] {
		t.Fatalf("paths=%q want=%q", paths, want)
	}
}

func TestDirtyPathQueueOverflowRequiresFullReconcile(t *testing.T) {
	queue := newDirtyPathQueue(2)
	queue.Add("/tmp/a.jsonl")
	queue.Add("/tmp/b.jsonl")
	queue.Add("/tmp/c.jsonl")
	paths, overflow := queue.Drain()
	if !overflow || len(paths) != 2 {
		t.Fatalf("paths=%q overflow=%v", paths, overflow)
	}
}

func TestReadIndexPathsDeduplicatesAndBoundsInput(t *testing.T) {
	paths, err := readIndexPaths(strings.NewReader("/tmp/b\n/tmp/a\n/tmp/b\n"), 2)
	if err != nil || len(paths) != 2 || paths[0] != "/tmp/a" || paths[1] != "/tmp/b" {
		t.Fatalf("paths=%q err=%v", paths, err)
	}
	if _, err := readIndexPaths(strings.NewReader("/tmp/a\n/tmp/b\n/tmp/c\n"), 2); err == nil {
		t.Fatal("over-limit dirty path input was accepted")
	}
}

func TestMergeDirtyPathsDeduplicatesAcrossBatches(t *testing.T) {
	got := mergeDirtyPaths([]string{"/tmp/b", "/tmp/a"}, []string{"/tmp/c", "/tmp/a"})
	want := []string{"/tmp/a", "/tmp/b", "/tmp/c"}
	if len(got) != len(want) {
		t.Fatalf("merged=%q want=%q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("merged=%q want=%q", got, want)
		}
	}
}
