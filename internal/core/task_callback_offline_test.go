package core

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/fcm"
	"everything-go/internal/messagequeue"
)

// Exercise the actual public action after an enrolled recipient loses its
// socket, rather than assuming a push registration grants callback capability.
func TestCallbackPreviouslyEnrolledOfflineRecipient(t *testing.T) {
	for _, scenario := range []string{"set", "request_now", "revoked_pair", "new_credential", "withdrawn_push", "unsupported_push", "filtered_push", "wrong_inventory", "unknown_task", "connected_unsupported", "own_unsupported"} {
		t.Run(scenario, func(t *testing.T) {
			h, recipient, s, exec := callbackFixture(t)
			recipient.inventoryBinding.Store(&clientInventoryBinding{key: "inventory-original-ipad", deviceID: "phone-a", name: "Original iPad"})
			taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Completed)
			emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: "r_callbacktask", ThreadID: s.ResumeID(), TurnID: "callback-turn"})
			if err := h.armAdmittedCallback(context.Background(), recipient, s, "r_callbacktask"); err != nil {
				t.Fatal(err)
			}
			h.reconcileTaskCallbacks(context.Background(), s.ID, "r_callbacktask")
			rows, err := h.callbacks.List(context.Background())
			if err != nil || len(rows) != 1 || rows[0].Invite == nil {
				t.Fatal("missing original sealed callback", err)
			}
			before := rows[0]
			// No live connection remains for the original recipient.
			h.mu.Lock()
			delete(h.clients, recipient)
			h.mu.Unlock()
			sender := sharedReadClient(t, h, "phone-b")
			sender.callbackNative.Store(true)
			sender.inventoryBinding.Store(&clientInventoryBinding{key: "inventory-sender", deviceID: "phone-b"})
			n, err := fcm.NewFromBytes([]byte(`{"type":"service_account","project_id":"qa","client_email":"qa@example.invalid","private_key":"unused-in-this-no-network-test"}`), filepath.Join(t.TempDir(), "tokens.json"))
			if err != nil {
				t.Fatal(err)
			}
			n.SetToken("phone-a", "original-push-token", "ios")
			h.fcm = n
			in := callbackInput{Action: "request_now", Operation: "offline_" + scenario, Session: s.ID, Request: before.Request, Expected: before.Revision, Recipient: before.Recipient.Inventory, Enabled: true}
			switch scenario {
			case "set":
				in.Action = "set"
			case "revoked_pair", "new_credential":
				if err := h.pairing.Unclaim("qa-read-phone-a"); err != nil {
					t.Fatal(err)
				}
				if scenario == "new_credential" {
					h.pairing.OpenEnrollment(time.Second)
					if err := h.pairing.Claim("different-generation", "phone-a"); err != nil {
						t.Fatal(err)
					}
				}
			case "withdrawn_push":
				n.RemoveDevice("phone-a")
			case "unsupported_push":
				n.RemoveDevice("phone-a")
				n.SetToken("phone-a", "original-push-token", "")
			case "filtered_push":
				n.SetDeviceFilter(func(string) bool { return false })
			case "wrong_inventory":
				in.Recipient = "inventory-never-enrolled"
			case "unknown_task":
				in.Request = "r_unknown"
				in.Expected = 0
			case "connected_unsupported":
				recipient.callbackNative.Store(false)
				h.mu.Lock()
				h.clients[recipient] = struct{}{}
				h.mu.Unlock()
			case "own_unsupported":
				sender = recipient
				sender.callbackNative.Store(false)
			}
			got := callbackAction(t, h, sender, in)
			after, found, err := h.callbacks.Get(context.Background(), before.ID)
			if err != nil || !found {
				t.Fatal("lost original record", err)
			}
			if scenario == "set" || scenario == "request_now" {
				if got["accepted"] != true {
					t.Fatalf("previously enrolled paired push recipient was refused after socket loss: %+v", got)
				}
				if after.Recipient != before.Recipient || after.Report == nil || after.Report.Hash != before.Report.Hash || after.Invite == nil || after.Invite.ID == before.Invite.ID || after.Revision != before.Revision+1 {
					t.Fatal("offline recipient/source or exact new invitation lost")
				}
			} else {
				if got["accepted"] != false {
					t.Fatal("unverified offline recipient accepted", got)
				}
				a, _ := json.Marshal(after)
				b, _ := json.Marshal(before)
				if string(a) != string(b) || after.Recipient.Binding != before.Recipient.Binding {
					t.Fatal("rejection mutated original policy")
				}
				all, _ := h.callbacks.List(context.Background())
				if len(all) != 1 {
					t.Fatal("rejection created a policy")
				}
			}
			if exec.starts.Load() != 0 || exec.appends.Load() != 0 {
				t.Fatal("target resolution started voice or speech")
			}
		})
	}
}
