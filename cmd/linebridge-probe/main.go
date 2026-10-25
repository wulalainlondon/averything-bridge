// Explicit read-only integration probe. It creates no native AI thread and
// cannot send a message, schedule an action, pair an account or place a call.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"everything-go/internal/linebridge"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func main() {
	legacy := flag.String("readonly-legacy", "", "existing encrypted LINE directory (explicit opt-in)")
	helper := flag.String("helper", "", "existing Keychain helper")
	node := flag.String("node", "", "absolute Node executable")
	entry := flag.String("service", "", "compiled account service entry")
	shared := flag.String("shared", "", "reviewed voice/credential package")
	manifest := flag.String("manifest", "", "optional staged bundle integrity manifest")
	query := flag.String("find", "", "bounded optional contact lookup")
	openchats := flag.Bool("openchats", false, "read-only joined OpenChat lookup instead of friends")
	sampleJoined := flag.Bool("sample-first-joined", false, "bounded read-only sample of the first discovered joined room")
	poll := flag.Bool("poll", false, "one read-only incremental poll with an in-memory checkpoint sink")
	flag.Parse()
	if *legacy == "" || *helper == "" || *node == "" || *entry == "" || *shared == "" {
		os.Exit(64)
	}
	result := map[string]any{"transport": "local_go_to_node_readonly_probe", "newPairing": false, "messageSent": false, "callPlaced": false}
	run := func() error {
		root, err := os.MkdirTemp("", "line-readonly-probe-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(root)
		key := make([]byte, 32)
		if _, err = rand.Read(key); err != nil {
			return err
		}
		defer func() {
			for i := range key {
				key[i] = 0
			}
		}()
		store, err := linebridge.OpenStore(filepath.Join(root, "store"), "readonly-probe", key)
		if err != nil {
			return err
		}
		defer store.Close()
		sink := &probeSink{cursors: map[string]string{}}
		service := linebridge.NewService(store, linebridge.ExecFactory{Node: *node, Entry: *entry, Shared: *shared, Manifest: *manifest}, linebridge.Config{DataDir: root, KeychainHelper: *helper, LegacyDir: *legacy, EventSink: sink})
		defer service.Close()
		actor := linebridge.Caller{Subject: "osuid:" + strconv.Itoa(os.Getuid()), Admin: true}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		account, err := service.Create(ctx, actor, "Existing login read-only probe", true)
		if err != nil {
			return err
		}
		if _, err = service.Resume(ctx, actor, account.ID); err != nil {
			return err
		}
		result["existingLoginResumed"] = true
		status, err := service.Status(ctx, actor, account.ID)
		if err != nil {
			return err
		}
		result["runtimeState"] = status.RuntimeState
		if _, err = service.Execute(ctx, actor, linebridge.Request{Method: "health", Account: account.ID}); err != nil {
			return err
		}
		result["liveConnectivityVerified"] = true
		if *query != "" || *openchats {
			method := "find_targets"
			if *openchats {
				method = "find_openchats"
			}
			value, err := service.Execute(ctx, actor, linebridge.Request{Method: method, Account: account.ID, Query: *query, Limit: 10})
			if err != nil {
				return err
			}
			targets := value.([]linebridge.Target)
			if err != nil {
				return err
			}
			result["matchingTargets"] = len(targets)
			if len(targets) == 1 || (*openchats && *sampleJoined && len(targets) > 0) {
				messages, err := service.Read(ctx, actor, account.ID, targets[0].Ref, "", 3)
				if err != nil {
					return err
				}
				result["boundedMessagesRead"] = len(messages)
				if *poll {
					if _, err = service.Subscribe(ctx, actor, account.ID, targets[0].Ref, true); err != nil {
						return err
					}
					if err = service.PollTargetOnce(ctx, account.ID, targets[0].Ref); err != nil {
						return err
					}
					result["pollEvents"] = sink.events
					result["checkpointCommitted"] = len(sink.cursors) > 0
					result["syncGapReported"] = sink.gap
				}
			}
		}
		return nil
	}
	if err := run(); err != nil {
		result["ok"] = false
		result["code"] = linebridge.SafeCode(err)
	} else {
		result["ok"] = true
	}
	_ = json.NewEncoder(os.Stdout).Encode(result)
	if result["ok"] == false {
		os.Exit(1)
	}
}

type probeSink struct {
	cursors map[string]string
	events  int
	gap     bool
}

func (s *probeSink) Cursor(_ context.Context, a, stream string) (string, error) {
	return s.cursors[a+"/"+stream], nil
}
func (s *probeSink) Commit(_ context.Context, a, stream, previous string, events []linebridge.RuntimeEvent, next string) error {
	key := a + "/" + stream
	if s.cursors[key] != previous {
		return linebridge.Fail("line_cursor_conflict")
	}
	s.cursors[key] = next
	s.events += len(events)
	for _, e := range events {
		if e.Kind == "sync_gap" {
			s.gap = true
		}
	}
	return nil
}
