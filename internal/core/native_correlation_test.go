package core

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"sync"
	"testing"
	"time"
)

type correlationHistoryFixture struct {
	mu     sync.Mutex
	Owners map[string]string
}

func TestNativeCorrelationActualFrozenListAndSnapshotRejectLegacyAlias(t *testing.T) {
	for _, op := range []string{"list", "snapshot"} {
		t.Run(op, func(t *testing.T) {
			h, fe := newTestHub(t)
			s := h.registry.Create("s", "task", t.TempDir(), backend.Codex, "", "", "root")
			useNativeCorrelationFixture(h, fe, "root", "old-real-request", "turn")
			ctx := context.Background()
			caller := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "isolated-fixture", BindingKind: "paired_human"}
			id := apiID(h.cfg.InstanceID, "ordinary", "synthetic-intent")
			c := taskapi.AuthorizedCommand{Caller: caller, Namespace: taskapi.Namespace{Authority: h.cfg.InstanceID, StableScopeID: caller.StableScopeID, Path: "ordinary", Operation: "create_dispatch"}, Locator: taskapi.Locator{Path: "ordinary"}, IntentHash: "fixture", Request: taskapi.Request{IdempotencyKey: "fixture-original"}}
			j := h.messageQueue.TaskJournal()
			err := j.Transaction(ctx, func(tx *sql.Tx) error {
				_, _, err := j.ClaimTx(tx, c, taskapi.IntentRecord{TaskID: id, ReceiptID: "fixture-receipt", NativeID: "synthetic-intent", SessionID: s.ID, RequestID: "new-request", Metadata: []byte(`{}`)})
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			views := []string{"self"}
			if op == "list" {
				views = []string{"inbox", "self", "results", "source_children", "manager_arranging"}
			}
			in := apiInput{SessionID: s.ID, Path: "ordinary", Limit: 10, Views: views}
			scope, err := h.apiReadScope(ctx, caller, in)
			if err != nil {
				t.Fatal(err)
			}
			freeze := taskapi.Freeze{ID: "fixture-old-freeze", ExpiresAt: time.Now().Add(time.Minute).UnixMilli(), Watermarks: []taskapi.Watermark{{Store: "ordinary", Sequence: 0}}}
			dto := map[string]any{"task_id": id, "target": map[string]any{"session_id": s.ID, "request_id": "new-request"}, "result_anchor": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "request_id": "new-request", "execution": map[string]any{"backend": "codex", "native_thread_id": "root", "native_turn_id": "turn"}}}
			if err = j.Freeze(ctx, scope, freeze, []taskapi.SnapshotRow{{Key: id, Value: dto}}); err != nil {
				t.Fatal(err)
			}
			key := bytes.Repeat([]byte{9}, 32)
			h.taskCursor, _ = taskapi.NewCursorCodec(key, nil)
			raw, _ := json.Marshal(map[string]any{"kind": "snapshot", "schema": contract.Hash(), "scope": scope, "freeze": freeze, "views": views})
			body := base64.RawURLEncoding.EncodeToString(raw)
			mac := hmac.New(sha256.New, key)
			mac.Write([]byte(body))
			in.Cursor = body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
			input, _ := json.Marshal(in)
			_, err = h.Read(ctx, taskapi.AuthorizedCommand{Caller: caller, Request: taskapi.Request{Operation: op, Input: input}, Revalidate: func(context.Context) error { return nil }})
			failure, ok := err.(*taskapi.APIError)
			if !ok || failure.Code != "cursor_expired" {
				t.Fatal("old frozen positive escaped actual route", err)
			}
		})
	}
}

func TestNativeCorrelationBackendSwitchCannotVerifyOldCodexTuple(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s", "task", t.TempDir(), backend.Codex, "", "", "root")
	useNativeCorrelationFixture(h, fe, "root", "request", "turn")
	anchor := map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "request_id": "request", "execution": map[string]any{"backend": "codex", "native_thread_id": "root", "native_turn_id": "turn"}}
	if !h.nativeSealKnown(s, anchor, "request", "root", "turn") {
		t.Fatal("original valid denied")
	}
	newConfig := s.Snapshot()
	newConfig.Backend = backend.Claude
	s.RestoreConfig(newConfig)
	if h.nativeSealKnown(s, anchor, "request", "root", "turn") || h.nativeCorrelationKnown(s, "root", "request", "turn") {
		t.Fatal("backend switch bypassed codex owner guard")
	}
}

func (p *correlationHistoryFixture) seedNativeFixture(proof backend.NativeTaskAccepted) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Owners[proof.ThreadID+"/"+proof.TurnID] = proof.RequestID
}

func (p *correlationHistoryFixture) LoadHistory(string, history.Opts) (*history.Result, error) {
	return &history.Result{Kind: "snapshot", Messages: []map[string]any{}}, nil
}
func (p *correlationHistoryFixture) ResumableSessions(int) ([]history.ResumableSession, error) {
	return nil, nil
}
func (p *correlationHistoryFixture) ValidateNativeAcceptance(s *session.Session, thread, request, turn string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if thread != s.ResumeID() || p.Owners[thread+"/"+turn] != request {
		return errors.New("provider owner mismatch")
	}
	return nil
}

type correlationFixtureExec struct {
	*fakeExec
	proof *correlationHistoryFixture
}

func (e *correlationFixtureExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.proof, true
}
func (e *correlationFixtureExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.proof}
}
func useNativeCorrelationFixture(h *Hub, fe *fakeExec, thread, request, turn string) *correlationHistoryFixture {
	p := &correlationHistoryFixture{Owners: map[string]string{thread + "/" + turn: request}}
	fe.nativeProofMu.Lock()
	fe.nativeProofs = p
	fe.nativeProofMu.Unlock()
	return p
}

// Existing positive hub fixtures explicitly attest the provider mapping as a
// separate test oracle; no production map/queue projection is copied to it.
func emitVerifiedNativeFixture(h *Hub, proof backend.NativeTaskAccepted) {
	if seed, ok := h.exec.(interface {
		seedNativeFixture(backend.NativeTaskAccepted)
	}); ok {
		seed.seedNativeFixture(proof)
	}
	if hr, ok := h.exec.(historyRouter); ok {
		if s, found := h.registry.Get(proof.SessionID); found {
			if p, found := hr.ProviderFor(s); found {
				if seed, ok := p.(interface {
					seedNativeFixture(backend.NativeTaskAccepted)
				}); ok {
					seed.seedNativeFixture(proof)
				}
			}
		}
	}
	h.Emit(proof)
}

func TestNativeCorrelationPreexistingAliasedQueueRowsNeverBecomeConsumedOrSealed(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s", "task", t.TempDir(), backend.Codex, "", "", "root")
	owner := sharedReadClient(t, h, "owner")
	old := taskEntry(t, h, s.ID, "old-request", "owner", messagequeue.Running)
	fresh := taskEntry(t, h, s.ID, "new-request", "owner", messagequeue.Uncertain)
	h.messageQueue.RecordNativeAcceptance(s.ID, old.RequestID, "root", "old-turn")
	h.messageQueue.RecordNativeAcceptance(s.ID, fresh.RequestID, "root", "old-turn")
	p := useNativeCorrelationFixture(h, fe, "root", old.RequestID, "old-turn")
	if h.nativeTaskTurn(s, old.RequestID) != "old-turn" || h.nativeTaskTurn(s, fresh.RequestID) != "" {
		t.Fatal("foreign alias verified")
	}
	before, _ := h.messageQueue.Snapshot(s.ID)
	rev := before.Revision
	h.reconcileQueueReceipts(owner, clientproto.Command{SessionID: s.ID, RequestID: "read", ExpectedRevision: &rev, TaskRequestIDs: []string{fresh.RequestID}})
	out := waitForType(t, owner, "queue_receipts_reconciled")
	fact := out["receipts"].([]any)[0].(map[string]any)
	if fact["delivery"] != "unverified" || fact["reason"] != "native_correlation_unverified" {
		t.Fatal(out)
	}
	finals := map[string]map[string]any{fresh.RequestID: {"source_message_id": "old-final", "source_turn_id": "old-turn", "content": "foreign"}}
	got := h.projectSessionTask(s, fresh, "owner", finals)
	if got.NativeTurnID != "" || got.APIAxes["execution"] != "unknown" || got.SourceMessageID != "" {
		t.Fatal(got)
	}
	anchor := map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "request_id": fresh.RequestID, "execution": map[string]any{"backend": "codex", "native_thread_id": "root", "native_turn_id": "old-turn"}}
	if h.nativeSealKnown(s, anchor, fresh.RequestID, "root", "old-turn") {
		t.Fatal("aliased seal verified")
	}
	p.Owners["root/old-turn"] = "different-real-request"
	if h.nativeSealKnown(s, anchor, old.RequestID, "root", "old-turn") {
		t.Fatal("changed owner verified")
	}
	after, _ := h.messageQueue.Snapshot(s.ID)
	current, _, _ := h.messageQueue.Get(s.ID, fresh.RequestID)
	if after.Revision != before.Revision || current.State != fresh.State || current.PayloadHash != fresh.PayloadHash {
		t.Fatal("pure validation changed original receipts")
	}
	native, found, err := h.messageQueue.ExactNativeAcceptance(s.ID, fresh.RequestID)
	if err != nil || !found || native.TurnID != "old-turn" {
		t.Fatal("old facts rewritten")
	}
}

func TestNativeCorrelationUnknownEventKeepsOriginalReceiptAndCannotCancelOrReplay(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s", "task", t.TempDir(), backend.Codex, "", "", "root")
	e := taskEntry(t, h, s.ID, "new-request", "owner", messagequeue.Running)
	fe.onSend = func(*session.Session, string, string) { t.Fatal("unknown replayed") }
	h.Emit(backend.NativeSubmissionUncertain{SessionID: s.ID, RequestID: e.RequestID, Reason: "native_acceptance_conflict"})
	current, found, err := h.messageQueue.Get(s.ID, e.RequestID)
	if err != nil || !found || current.State != messagequeue.Uncertain || current.PayloadHash != e.PayloadHash {
		t.Fatal(current, err)
	}
	h.resumeQueuedMessages()
	owner := sharedReadClient(t, h, "owner")
	h.cancelQueuedMessage(owner, clientproto.Command{SessionID: s.ID, RequestID: e.RequestID})
	if waitForType(t, owner, "queue_action_result")["status"] != "rejected" {
		t.Fatal("uncertain cancelled")
	}
	if _, changed, err := h.messageQueue.CancelWaitingAPI(s.ID, e.RequestID, e.PayloadHash, nil, ""); err == nil && changed {
		t.Fatal("unknown cancelled directly")
	}
}

func TestNativeCorrelationStoredSealRequiresWholeExecutionTuple(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s", "task", t.TempDir(), backend.Codex, "", "", "root")
	useNativeCorrelationFixture(h, fe, "root", "request", "turn")
	base := map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "request_id": "request", "execution": map[string]any{"backend": "codex", "native_thread_id": "root", "native_turn_id": "turn"}}
	if !h.nativeSealKnown(s, base, "request", "root", "turn") {
		t.Fatal("valid seal denied")
	}
	for _, field := range []string{"instance_id", "session_id", "request_id", "native_thread_id", "native_turn_id", "backend"} {
		raw, _ := json.Marshal(base)
		var saved map[string]any
		json.Unmarshal(raw, &saved)
		if field == "native_thread_id" || field == "native_turn_id" || field == "backend" {
			saved["execution"].(map[string]any)[field] = "other"
		} else {
			saved[field] = "other"
		}
		if h.nativeSealKnown(s, saved, "request", "root", "turn") {
			t.Fatal("foreign seal", field)
		}
	}
}

func TestNativeCorrelationLegacySteerTupleDoesNotInheritFinal(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s", "task", t.TempDir(), backend.Codex, "", "", "root")
	active := taskEntry(t, h, s.ID, "active", "owner", messagequeue.Completed)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: active.RequestID, ThreadID: "root", TurnID: "turn"})
	legacy := taskEntry(t, h, s.ID, "legacy", "owner", messagequeue.Queued)
	legacy, _, _ = h.messageQueue.Transition(s.ID, legacy.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Steered, "", active.RequestID, "turn")
	useNativeCorrelationFixture(h, fe, "root", active.RequestID, "turn")
	finals := map[string]map[string]any{active.RequestID: {"source_message_id": "final", "source_turn_id": "turn", "content": "result"}}
	got := h.projectSessionTask(s, legacy, "owner", finals)
	if got.State != "legacy_unknown" || got.NativeTurnID != "" || got.SourceMessageID != "" || got.APIAxes["execution"] != "unknown" {
		t.Fatal("legacy inherited positive", got)
	}
}

func TestNativeCorrelationFrozenListAndSnapshotGuardAnchorWithoutTargetTurn(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s", "task", t.TempDir(), backend.Codex, "", "", "root")
	useNativeCorrelationFixture(h, fe, "root", "old-real-request", "turn")
	for _, view := range []string{"list", "snapshot"} {
		t.Run(view, func(t *testing.T) {
			dto := map[string]any{"target": map[string]any{"session_id": s.ID, "request_id": "new-request"}, "result_anchor": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": s.ID, "request_id": "new-request", "execution": map[string]any{"backend": "codex", "native_thread_id": "root", "native_turn_id": "turn"}}}
			if h.frozenTaskNativeKnown(dto) {
				t.Fatal("alias frozen anchor verified")
			}
			dto["target"].(map[string]any)["request_id"] = "old-real-request"
			dto["result_anchor"].(map[string]any)["request_id"] = "old-real-request"
			if !h.frozenTaskNativeKnown(dto) {
				t.Fatal("genuine old owner denied")
			}
		})
	}
}
