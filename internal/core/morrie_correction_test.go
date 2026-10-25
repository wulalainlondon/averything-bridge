package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

// These are isolated store/runtime fixtures, never model or remote E2E proof.
func correctionSeal(t *testing.T, h *Hub, r delegation.Record, wrongOwner bool) messagequeue.TaskSeal {
	t.Helper()
	thread, turn, known := h.messageQueue.NativeExecution(r.ChildSessionID, r.ChildRequestID)
	if !known {
		t.Fatal("fixture missing original native oracle")
	}
	request := r.ChildRequestID
	if wrongOwner {
		request = "synthetic-foreign-request"
	}
	anchor, _ := json.Marshal(map[string]any{"instance_id": h.cfg.InstanceID, "session_id": r.ChildSessionID, "request_id": request, "source_message_id": "synthetic-final", "execution": map[string]any{"backend": "codex", "native_thread_id": thread, "native_turn_id": turn}})
	hash := sha256.Sum256([]byte(r.Result))
	digest := hex.EncodeToString(hash[:])
	s, err := h.messageQueue.SealTask(context.Background(), messagequeue.TaskSeal{ID: "seal_" + digest, SessionID: r.ChildSessionID, RequestID: r.ChildRequestID, Hash: digest, Text: r.Result, Anchor: anchor})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestMorrieCanonicalQAStoreProjection(t *testing.T) {
	for _, tc := range []struct {
		name, verdict, want              string
		missing, wrongAnchor, wrongOwner bool
	}{
		{"missing_review", "PASS", "not_requested", true, false, false},
		{"bound_pending_review", "PASS", "pending", true, false, false},
		{"missing_qa_seal", "PASS", "unknown", false, false, false},
		{"passed", "PASS", "passed", false, false, false},
		{"failed", "FAIL", "failed", false, false, false},
		{"wrong_seal_owner", "PASS", "unknown", false, true, false},
		{"wrong_review_owner", "PASS", "unknown", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, p, _ := continuityFixture(t)
			_, owner := continuityDelegate(t, h, p, "synthetic-owner", "synthetic owner final")
			ownerSeal := correctionSeal(t, h, owner, false)
			ownerID := owner.ID
			if tc.wrongOwner {
				ownerID = "synthetic-foreign-round"
			}
			_, qa := continuityDelegate(t, h, p, "synthetic-independent-qa", fmt.Sprintf(`{"owner_round_id":%q,"verdict":%q}`, ownerID, tc.verdict))
			if tc.name != "missing_qa_seal" {
				correctionSeal(t, h, qa, tc.wrongAnchor)
			}
			if tc.name != "missing_review" {
				if _, err := h.BindDelegationQA(p, p.ActiveQueuedID(), "synthetic-bind", owner.ID, qa.ID); err != nil {
					t.Fatal(err)
				}
			}
			if !tc.missing {
				// Adversarial bad review exists only in this private store fixture;
				// production writes still go through RecordDelegationReview.
				if tc.wrongOwner {
					if err := h.delegations.RecordQA(context.Background(), owner.ID, qa.ID, tc.verdict); err != nil {
						t.Fatal(err)
					}
				} else if _, err := h.RecordDelegationReview(p, p.ActiveQueuedID(), "synthetic-record", owner.ID, qa.ID); err != nil {
					t.Fatal(err)
				}
			}
			input, _ := json.Marshal(map[string]any{"goal": "synthetic real-store projection", "acceptance": []any{}})
			meta, _ := json.Marshal(apiMetadata{Request: taskapi.Request{Input: input}, Caller: taskapi.VerifiedContext{BindingKind: "native_tool", SourceSessionID: p.ID}, SourceThread: p.ResumeID()})
			intent := taskapi.IntentRecord{TaskID: apiID(h.cfg.InstanceID, "delegation", owner.ID), Path: "delegation", NativeID: owner.ID, SessionID: owner.ChildSessionID, RequestID: owner.ChildRequestID, Metadata: meta}
			projected, err := h.apiTask(context.Background(), intent)
			if err != nil {
				t.Fatal(err)
			}
			axes := projected["axes"].(map[string]string)
			if axes["qa"] != tc.want || projected["qa"].(map[string]any)["status"] != tc.want {
				t.Fatalf("canonical review != TaskAPI projection: %s %+v", tc.want, projected)
			}
			if tc.want == "passed" {
				wrongTask := map[string]any{"seal_id": ownerSeal.ID, "result_anchor": map[string]any{"request_id": "wrong-owner-even-with-same-final-hash"}, "revision": uint64(1)}
				wrongAxes := map[string]string{"qa": "not_requested"}
				if err := h.projectCanonicalDelegationQA(context.Background(), owner.ID, wrongTask, wrongAxes); err != nil || wrongAxes["qa"] != "unknown" {
					t.Fatal("same hash authorized wrong selected owner", err, wrongAxes)
				}
			}
			if tc.want == "passed" || tc.want == "failed" {
				if projected["seal_id"] != ownerSeal.ID || len(projected["qa"].(map[string]any)["evidence_refs"].([]any)) != 1 {
					t.Fatal("review missing exact owner/QA seal proof")
				}
				// A subsequent unreviewed round cannot inherit this verdict.
				fresh := continuityContinue(t, h, p, "synthetic-next-owner", continuityInput(owner, p, h, "new owner final"))
				correctionSeal(t, h, fresh, false)
				// apiTask is an exact-request projection; select the new request
				// explicitly in this legacy-store fixture. Public formal reads are
				// covered through ExecuteTask by the public-contract regression.
				nextIntent := intent
				nextIntent.RequestID = fresh.ChildRequestID
				next, err := h.apiTask(context.Background(), nextIntent)
				if err != nil || next["axes"].(map[string]string)["qa"] != "pending" {
					t.Fatal("old verdict leaked to current round", next, err)
				}
				// Explicit selected-older projection remains independently verifiable.
				oldAxes := map[string]string{"qa": "not_requested"}
				var oldAnchor map[string]any
				if err := json.Unmarshal(ownerSeal.Anchor, &oldAnchor); err != nil {
					t.Fatal(err)
				}
				oldTask := map[string]any{"seal_id": ownerSeal.ID, "revision": uint64(1), "result_anchor": oldAnchor}
				if err := h.projectCanonicalDelegationQA(context.Background(), owner.ID, oldTask, oldAxes); err != nil || oldAxes["qa"] != tc.want {
					t.Fatal("selected older round lost immutable review", err, oldAxes)
				}
			}
		})
	}
}

func TestMorrieClaudeSourceFormalContinuationRefusal(t *testing.T) {
	for _, childBackend := range []string{backend.Codex, backend.Claude} {
		t.Run(childBackend, func(t *testing.T) {
			h, caller := capacityHub(t, 6)
			p := caller.Parent
			p.ApplyConfig(backend.Claude, "sonnet", "read-only")
			p.SetResumeID("synthetic-claude-source-thread")
			continuityParentTurn(t, p, "r_synthetic_claude_source")
			started := make(chan string, 1)
			f := &capacityScopeCapabilityFixture{fakeExec: &fakeExec{sink: h}}
			f.onSend = func(s *session.Session, request, content string) {
				s.SetResumeID("synthetic-bounded-thread")
				if s.Backend() == backend.Codex {
					emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: request, ThreadID: s.ResumeID(), TurnID: "synthetic-child-turn"})
				} else {
					h.Emit(backend.NativeProviderEvidence{SessionID: s.ID, RequestID: request, Backend: backend.Claude, ConversationID: s.ResumeID(), Token: "synthetic-message-uuid", TokenKind: "message_uuid", MessageID: "synthetic-message", Text: "synthetic final", Status: "succeeded"})
				}
				started <- request
				h.Emit(protocol.NewDone(s.ID, request))
			}
			h.SetExecutor(f)
			c := backend.TaskCaller{Session: p, RequestID: p.ActiveQueuedID(), ThreadID: p.ResumeID(), ProcessGeneration: "synthetic-original-process", ProviderVersion: "2.1.296", CallID: "synthetic-native-create", Validate: func() bool { return true }}
			input := map[string]any{"goal": "synthetic actual runtime boundary", "instruction": "read only", "new_worker": map[string]any{"name": "synthetic bounded", "profile": map[string]string{"backend": childBackend, "model": map[string]string{backend.Codex: "gpt-6.1-sol", backend.Claude: "sonnet"}[childBackend], "effort": "high"}}, "scope": map[string]any{"workspace_roots": []string{p.Cwd()}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "workspace": map[string]any{"cwd": p.Cwd(), "input_artifact_ids": []any{}}, "acceptance": []any{}, "dependencies": []any{}}
			raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-claude-create", "operation": "create_dispatch", "idempotency_key": "synthetic-claude-create", "input": input})
			response := h.ExecuteTask(context.Background(), c, raw)
			if !response.OK {
				t.Fatal(response.Error)
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("fixture worker missing")
			}
			r := capacityRecordFor(t, h, response.Result.(map[string]any)["task_ref"].(map[string]string)["native_record_id"])
			in := continuityInput(r, p, h, "same logical bounded task")
			if childBackend == backend.Claude {
				if _, _, known := h.messageQueue.NativeExecution(r.ChildSessionID, r.ChildRequestID); known {
					t.Fatal("Claude provider fixture fabricated Codex native tuple")
				}
			}
			_, directReason := h.ContinueDelegation(p, p.ActiveQueuedID(), "synthetic-direct-preflight", in)
			t.Logf("original core preflight backend=%s reason=%v", childBackend, directReason)

			command, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-claude-owner-command", "operation": "append", "idempotency_key": "synthetic-claude-owner-command", "input": map[string]any{"continuity": map[string]any{"action": "continue", "arguments": in}}})
			c.CallID = "synthetic-native-continue"
			refused := h.ExecuteTask(context.Background(), c, command)
			if refused.OK || refused.Error.Code != "unsupported" || refused.Error.Acceptance != "known_none" {
				t.Fatal("precise unsupported lost at Claude runtime boundary", refused)
			}
			view, err := h.DelegationRounds(p, r.ID, 0)
			if err != nil || view.(map[string]any)["continuation_availability"] != "unsupported" {
				t.Fatal(view, err)
			}
			roundsRequest, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-rounds-schema", "operation": "get", "input": map[string]any{"continuity": map[string]any{"action": "rounds", "arguments": map[string]any{"delegation_id": r.ID}}}})
			roundsResponse := h.ExecuteTask(context.Background(), c, roundsRequest)
			if !roundsResponse.OK {
				t.Fatal(roundsResponse.Error)
			}
			compiler, err := contract.New()
			if err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(roundsResponse)
			if err := compiler.Validate(encoded, "Response"); err != nil {
				t.Fatal("current_target changed closed rc1 schema", err)
			}
			c.Validate = func() bool { return false }
			if denied := h.ExecuteTask(context.Background(), c, command); denied.OK || denied.Error.Code != "caller_unbound" {
				t.Fatal("revoked native source accepted", denied)
			}
		})
	}
}
