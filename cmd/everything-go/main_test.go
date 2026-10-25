package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"everything-go/internal/search"
)

func TestOccupiedPortDoesNotTouchDurableState(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOccupiedPortChild$")
	cmd.Env = append(os.Environ(),
		"EVERYTHING_GO_OCCUPIED_PORT_CHILD=1",
		"EVERYTHING_GO_OCCUPIED_PORT="+strconv.Itoa(listener.Addr().(*net.TCPAddr).Port),
		"EVERYTHING_GO_OCCUPIED_DATA_DIR="+dataDir,
	)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("occupied-port child timed out: %v", ctx.Err())
	}
	if err == nil || !strings.Contains(string(output), "before state initialization") {
		t.Fatalf("expected early listener rejection, err=%v output=%q", err, output)
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("port loser created durable state: %v", entries)
	}
}

func TestOccupiedPortChild(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_OCCUPIED_PORT_CHILD") != "1" {
		return
	}
	flag.CommandLine = flag.NewFlagSet("everything-go", flag.ExitOnError)
	os.Args = []string{"everything-go", "--port", os.Getenv("EVERYTHING_GO_OCCUPIED_PORT"),
		"--data-dir", os.Getenv("EVERYTHING_GO_OCCUPIED_DATA_DIR"), "--executor", "python"}
	main()
	t.Fatal("occupied-port bridge unexpectedly returned")
}

func TestIndexBackoff(t *testing.T) {
	min := time.Minute
	max := 15 * time.Minute
	wants := []time.Duration{time.Minute, time.Minute, 2 * time.Minute, 5 * time.Minute, 15 * time.Minute, 15 * time.Minute}
	for idle, want := range wants {
		if got := indexBackoff(min, max, idle); got != want {
			t.Fatalf("idle=%d delay=%s want=%s", idle, got, want)
		}
	}
	if got := indexBackoff(min, max, -1); got != min {
		t.Fatalf("negative idle delay=%s want=%s", got, min)
	}
}

func TestShouldRefreshSessionSummaries(t *testing.T) {
	if shouldRefreshSessionSummaries(search.IngestMetrics{MessagesAdded: 1}, false) {
		t.Fatal("invalid metrics must not broadcast")
	}
	if !shouldRefreshSessionSummaries(search.IngestMetrics{MessagesAdded: 1}, true) {
		t.Fatal("new preview messages must broadcast")
	}
	if !shouldRefreshSessionSummaries(search.IngestMetrics{MaintenanceRows: 2}, true) {
		t.Fatal("index rebuild maintenance must broadcast")
	}
	if shouldRefreshSessionSummaries(search.IngestMetrics{FilesChanged: 1}, true) {
		t.Fatal("metadata-only indexing must stay quiet")
	}
}

func TestParseIndexMetrics(t *testing.T) {
	output := "noise\n" + indexResultPrefix + `{"files_seen":5007,"files_changed":1,"files_queued":1,"messages_added":2,"bytes_read":4096,"db_bytes_delta":8192,"duration_ms":125}` + "\n"
	got, ok := parseIndexMetrics(output)
	if !ok || got.FilesSeen != 5007 || got.FilesChanged != 1 || got.MessagesAdded != 2 || got.BytesRead != 4096 {
		t.Fatalf("metrics=%+v ok=%v", got, ok)
	}
	if _, ok := parseIndexMetrics("missing"); ok {
		t.Fatal("missing metrics reported valid")
	}
}
