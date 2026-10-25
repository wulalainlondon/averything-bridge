package main

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestAuditedNonBlockingProjectionNeverHidesNativeOrOwnedWork(t *testing.T) {
	for _, scenario := range []string{"idle", "native-active", "bridge-streaming", "queued", "real-reply", "wrong-question", "wrong-thread"} {
		t.Run(scenario, func(t *testing.T) {
			f := testFixture(t)
			f.runtimes[1].Phase, f.runtimes[1].Request = "waiting", "question"
			// Production sessions_list projects waiting as is_streaming=true,
			// but request_status's actor count remains zero for these idle asks.
			f.sessions[1].Streaming = true
			actors := 0
			f.streamingOverride = &actors
			f.config.NonBlockingProjectionFile = filepath.Join(f.config.DataDir, "audit.json")
			if err := os.WriteFile(f.config.NonBlockingProjectionFile, []byte(`{"work":{"request_id":"question","thread_id":"work-thread"}}`), 0600); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "native-active":
				f.states["work-thread"] = "active"
			case "bridge-streaming":
				actors = 1
			case "queued":
				f.runtimes[1].Queue = 1
			case "wrong-question":
				f.runtimes[1].Request = "another-turn"
			case "wrong-thread":
				f.states["work-thread"] = "unknown"
			case "real-reply":
				db, err := sql.Open("sqlite", filepath.Join(f.config.DataDir, "message_queue.sqlite"))
				if err != nil {
					t.Fatal(err)
				}
				_, err = db.Exec("INSERT INTO queue_commands VALUES('work','question','completed')")
				db.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			busy, attention, err := inspect(context.Background(), f.config, map[string]tracked{})
			if scenario == "idle" {
				if err != nil || len(busy) != 0 || len(attention) != 0 {
					t.Fatal(busy, attention, err)
				}
			} else if err == nil && len(busy) == 0 && len(attention) == 0 {
				t.Fatal("unsafe quiet classification")
			}
		})
	}
}
