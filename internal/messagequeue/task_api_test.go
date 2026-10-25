package messagequeue

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"testing"
	"time"
)

func apiFixture() taskapi.AuthorizedCommand {
	return taskapi.AuthorizedCommand{Caller: taskapi.VerifiedContext{StableScopeID: "scope", NamespaceGeneration: 7}, Request: taskapi.Request{Operation: "create_dispatch", IdempotencyKey: "fixture-key"}, Namespace: taskapi.Namespace{Authority: "instance", StableScopeID: "scope", Generation: 7, Path: "ordinary", Operation: "create_dispatch"}, Locator: taskapi.Locator{Authority: "instance", Path: "ordinary", Operation: "create_dispatch", Key: "fixture-key"}, IntentHash: "fixturehash"}
}
func TestTaskAPIEnqueueAtomicClaimReopenAndConflict(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	c := apiFixture()
	meta, _ := json.Marshal(map[string]string{"goal": "fixture"})
	payload := []byte(`{"owner_device":"fixture-device","message_purpose":"instruction","content":"fixture"}`)
	record := taskapi.IntentRecord{ReceiptID: "receipt1", TaskID: "task1", NativeID: "session/request", SessionID: "session", RequestID: "r_fixture111", Metadata: meta}
	entry := Entry{SessionID: "session", RequestID: "r_fixture111", Content: "fixture", Payload: payload, API: &APIAdmission{Command: c, Record: record}}
	if _, created, e := s.Enqueue(entry); e != nil || !created {
		t.Fatal(created, e)
	}
	entry.RequestID = "r_replacement111"
	entry.API.Record.RequestID = entry.RequestID
	if returned, created, e := s.Enqueue(entry); e != nil || created || returned.RequestID != "r_fixture111" {
		t.Fatal(returned, created, e)
	}
	entry.API.Command.IntentHash = "different"
	if _, _, e := s.Enqueue(entry); e == nil {
		t.Fatal("conflict accepted")
	}
	s.Close()
	s, e := Open(root)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r, e := s.TaskJournal().Lookup(context.Background(), c.Namespace, c.Request.IdempotencyKey)
	if e != nil || r.RequestID != "r_fixture111" {
		t.Fatal(r, e)
	}
	if _, found, e := s.TaskAdmission("session", "r_fixture111"); e != nil || !found {
		t.Fatal(found, e)
	}
}
func TestTaskAPINativeConflictsSurviveReopen(t *testing.T) {
	root := t.TempDir()
	s, _ := Open(root)
	if e := s.RecordNativeAcceptance("s", "r", "thread1", "turn1"); e != nil {
		t.Fatal(e)
	}
	if e := s.RecordNativeAcceptance("s", "r", "thread1", "turn1"); e != nil {
		t.Fatal(e)
	}
	if e := s.RecordNativeAcceptance("s", "r", "thread2", "turn2"); e == nil {
		t.Fatal("conflict swallowed")
	}
	s.Close()
	s, _ = Open(root)
	defer s.Close()
	if turn := s.NativeAcceptance("s", "r", "thread1"); turn != "" {
		t.Fatal("conflicted evidence surfaced", turn)
	}
	if flag, e := s.NativeConflict("s", "r"); e != nil || !flag {
		t.Fatal(flag, e)
	}
}
func TestTaskAPITombstoneRetentionUnknownNoReplay(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	c := apiFixture()
	record := taskapi.IntentRecord{ReceiptID: "receipt", TaskID: "task", NativeID: "native", SessionID: "s", RequestID: "r_fixture111", Metadata: []byte(`{}`)}
	entry := Entry{SessionID: "s", RequestID: record.RequestID, Content: "fixture", Payload: []byte(`{"content":"fixture"}`), API: &APIAdmission{Command: c, Record: record}}
	s.Enqueue(entry)
	if err := s.TaskJournal().Prune(context.Background(), time.Now().Add(100*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TaskJournal().Lookup(context.Background(), c.Namespace, c.Request.IdempotencyKey); err != nil {
		t.Fatal("pending intent erased", err)
	}
	stored, _ := s.TaskJournal().Lookup(context.Background(), c.Namespace, c.Request.IdempotencyKey)
	s.TaskJournal().Outbox(context.Background(), stored.Key, "resolved")
	s.TaskJournal().Prune(context.Background(), time.Now().Add(100*24*time.Hour))
	_, err := s.TaskJournal().Lookup(context.Background(), c.Namespace, c.Request.IdempotencyKey)
	var e *taskapi.APIError
	if !errors.As(err, &e) || e.Code != "key_expired" {
		t.Fatal(err)
	}
	if _, _, err := s.Enqueue(entry); err == nil {
		t.Fatal("expired intent re-executed")
	}
}
func TestTaskAPIProviderEvidenceNoCodexFabrication(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	proof := ProviderEvidence{SessionID: "s", RequestID: "r", Backend: "claude", ConversationID: "public-session", Token: "public-echo-uuid", TokenKind: "message_uuid"}
	if err := s.RecordProviderEvidence(proof); err != nil {
		t.Fatal(err)
	}
	proof.MessageID = "public-message-uuid"
	proof.Text = "final"
	proof.Status = "succeeded"
	s.RecordProviderEvidence(proof)
	if r, ok, e := s.ProviderEvidence("s", "r"); e != nil || !ok || r.Text != "final" {
		t.Fatal(r, ok, e)
	}
	proof.Token = "different"
	if err := s.RecordProviderEvidence(proof); err == nil {
		t.Fatal("provider conflict accepted")
	}
	if s.NativeAcceptance("s", "r", "public-session") != "" {
		t.Fatal("Claude UUID became Codex turn")
	}
}

func TestTaskAPIRevisionAndExactRecoverySurviveRetention(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.Enqueue(Entry{SessionID: "s", RequestID: "r_fixture", Content: "fixture", Payload: []byte(`{}`)})
	s.Transition("s", "r_fixture", []State{Queued}, Running, "", "", "")
	before, _ := s.TaskRevision(context.Background(), "s", "r_fixture")
	watermark, _ := s.TaskJournal().Watermark(context.Background())
	if e = s.Recover(); e != nil {
		t.Fatal(e)
	}
	changes, _, e := s.TaskJournal().Changes(context.Background(), watermark, 10)
	if e != nil {
		t.Fatal(e)
	}
	exact := false
	for _, c := range changes {
		if c.RequestID == "r_fixture" && c.Kind == "uncertain" {
			exact = true
		}
	}
	if !exact {
		t.Fatal("no exact recovery event")
	}
	recovered, _ := s.TaskRevision(context.Background(), "s", "r_fixture")
	if recovered <= before {
		t.Fatal("recovery revision did not advance")
	}
	if e = s.TaskJournal().Prune(context.Background(), time.Now().Add(100*24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	kept, _ := s.TaskRevision(context.Background(), "s", "r_fixture")
	if kept != recovered {
		t.Fatal("revision erased by event retention")
	}
	floor, _ := s.TaskJournal().Watermark(context.Background())
	if floor < recovered {
		t.Fatal("empty log watermark reset")
	}
}
func TestTaskAPIRefusedCancelOutcomeImmutableReopen(t *testing.T) {
	root := t.TempDir()
	s, _ := Open(root)
	c := apiFixture()
	c.Request.Operation = "cancel"
	c.Namespace.Operation = "cancel"
	c.Namespace.TaskID = "task"
	c.Locator.Operation = "cancel"
	c.Locator.TaskID = "task"
	var r taskapi.IntentRecord
	s.TaskJournal().Transaction(context.Background(), func(tx *sql.Tx) error {
		var e error
		r, _, e = s.TaskJournal().ClaimTx(tx, c, taskapi.IntentRecord{ReceiptID: "receipt", TaskID: "task", NativeID: "native", SessionID: "s", RequestID: "r_fixture", Metadata: []byte(`{}`)})
		return e
	})
	failure := taskapi.Failure("busy", "known_receipt", "lookup_original")
	failure.ReceiptID = r.ReceiptID
	if e := s.TaskJournal().SaveOutcome(context.Background(), r.Key, failure); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, _ = Open(root)
	defer s.Close()
	s.TaskJournal().SaveOutcome(context.Background(), r.Key, nil)
	stored, done, e := s.TaskJournal().Outcome(context.Background(), r.Key)
	if e != nil || !done || stored == nil || stored.Code != "busy" || stored.ReceiptID != r.ReceiptID {
		t.Fatal(stored, done, e)
	}
}
func TestTaskAPISealHashAndConflictQuarantine(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	ctx := context.Background()
	text := "exact final"
	hash := sha256.Sum256([]byte(text))
	seal := TaskSeal{ID: "seal", SessionID: "s", RequestID: "r", Hash: hex.EncodeToString(hash[:]), Text: text, Anchor: []byte(`{}`)}
	invalid := seal
	invalid.Text = "different"
	if _, e := s.SealTask(ctx, invalid); e == nil {
		t.Fatal("mismatched hash sealed")
	}
	if _, e := s.SealTask(ctx, seal); e != nil {
		t.Fatal(e)
	}
	proof := ProviderEvidence{SessionID: "s", RequestID: "r", Backend: "claude", ConversationID: "conversation", Token: "echo", TokenKind: "message_uuid", MessageID: "m1", Text: text, Status: "succeeded"}
	s.RecordProviderEvidence(proof)
	proof.MessageID = "m2"
	if e := s.RecordProviderEvidence(proof); e == nil {
		t.Fatal("different native final accepted")
	}
	if conflict, _ := s.NativeConflict("s", "r"); !conflict {
		t.Fatal("provider conflict not persisted")
	}
	if _, e := s.SealTask(ctx, seal); e == nil {
		t.Fatal("quarantined final surfaced")
	}
}
func TestTaskAPIFreezeConditionalWatermark(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	ctx := context.Background()
	scope := taskapi.ReadScope{StableScopeID: "scope", FilterHash: "filter"}
	f := taskapi.Freeze{ID: "f", ExpiresAt: time.Now().Add(time.Minute).UnixMilli(), Watermarks: []taskapi.Watermark{{Store: "ordinary", Sequence: 0}}}
	s.Enqueue(Entry{SessionID: "s", RequestID: "r", Payload: []byte(`{}`)})
	if e := s.TaskJournal().FreezeChecked(ctx, scope, f, []taskapi.SnapshotRow{}, 0); e == nil {
		t.Fatal("mutation between capture and materialization ignored")
	}
}
func TestTaskAPIUncertainCancelProvenanceRetained(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	s.Enqueue(Entry{SessionID: "s", RequestID: "r", Payload: []byte(`{}`)})
	s.Transition("s", "r", []State{Queued}, Uncertain, "", "", "")
	s.Transition("s", "r", []State{Uncertain}, Cancelled, "", "", "")
	if from, e := s.CancelOrigin("s", "r"); e != nil || from != Uncertain {
		t.Fatal(from, e)
	}
}

func TestTaskAPIBodyExpiryInvalidatesEventCursor(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	ctx := context.Background()
	c := apiFixture()
	record := taskapi.IntentRecord{ReceiptID: "receipt", TaskID: "task", NativeID: "n", SessionID: "s", RequestID: "r", Metadata: []byte(`{}`)}
	s.Enqueue(Entry{SessionID: "s", RequestID: "r", Payload: []byte(`{}`), API: &APIAdmission{Command: c, Record: record}})
	intent, _ := s.TaskJournal().Lookup(ctx, c.Namespace, c.Request.IdempotencyKey)
	s.TaskJournal().Outbox(ctx, intent.Key, "resolved")
	cursor, _ := s.TaskJournal().Watermark(ctx)
	if e := s.TaskJournal().Prune(ctx, time.Now().Add(100*24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if _, _, e := s.TaskJournal().Changes(ctx, cursor, 10); e == nil {
		t.Fatal("expired body invisible to existing consumer")
	}
}

func TestIntakeLateAdmissionGuardRollsBackAllFacts(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	c := apiFixture()
	c.Request.Operation = "intake"
	c.Namespace.Operation = "intake"
	c.Locator.Operation = "intake"
	record := taskapi.IntentRecord{ReceiptID: "receipt", TaskID: "task", NativeID: "s/r", SessionID: "s", RequestID: "r_original", Metadata: []byte(`{}`)}
	calls := 0
	_, _, e := s.Enqueue(Entry{SessionID: "s", RequestID: record.RequestID, Payload: []byte(`{}`), API: &APIAdmission{Command: c, Record: record, CanCommit: func() bool { calls++; return calls == 1 }}})
	if e == nil {
		t.Fatal("late revoked admission committed")
	}
	if _, found, _ := s.Get("s", record.RequestID); found {
		t.Fatal("queue row survived rollback")
	}
	if _, e := s.TaskJournal().Lookup(context.Background(), c.Namespace, c.Request.IdempotencyKey); e == nil {
		t.Fatal("intent survived rollback")
	}
}
func TestIntakeCannotAttachJournalToExistingOrdinaryRequest(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	c := apiFixture()
	c.Request.Operation = "intake"
	c.Namespace.Operation = "intake"
	c.Locator.Operation = "intake"
	payload := []byte(`{}`)
	s.Enqueue(Entry{SessionID: "s", RequestID: "r_same", Payload: payload})
	record := taskapi.IntentRecord{ReceiptID: "receipt", TaskID: "task", NativeID: "s/r_same", SessionID: "s", RequestID: "r_same", Metadata: []byte(`{}`)}
	_, _, e := s.Enqueue(Entry{SessionID: "s", RequestID: "r_same", Payload: payload, API: &APIAdmission{Command: c, Record: record}})
	if !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if _, e := s.TaskJournal().Lookup(context.Background(), c.Namespace, c.Request.IdempotencyKey); e == nil {
		t.Fatal("ordinaryrow backfilled formal")
	}
}

func TestIntakeRestartSameReceiptWithoutReexecution(t *testing.T) {
	root := t.TempDir()
	s, _ := Open(root)
	c := apiFixture()
	c.Request.Operation = "intake"
	c.Namespace.Operation = "intake"
	c.Locator.Operation = "intake"
	record := taskapi.IntentRecord{ReceiptID: "original-receipt", TaskID: "task", NativeID: "s/r", SessionID: "s", RequestID: "r_original", Metadata: []byte(`{"paired_intake":true}`)}
	entry := Entry{SessionID: "s", RequestID: record.RequestID, Payload: []byte(`{"content":"originalgoal"}`), API: &APIAdmission{Command: c, Record: record}}
	if _, created, e := s.Enqueue(entry); e != nil || !created {
		t.Fatal(e)
	}
	s.Transition("s", record.RequestID, []State{Queued}, Running, "", "", "")
	s.Close()
	s, e := Open(root)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.Recover()
	after, created, e := s.Enqueue(entry)
	if e != nil || created {
		t.Fatal("reexecutedacceptedintake", e, created)
	}
	if after.State != Uncertain {
		t.Fatal("restartfactfaked", after.State)
	}
	receipt, e := s.TaskJournal().Lookup(context.Background(), c.Namespace, c.Request.IdempotencyKey)
	if e != nil || receipt.ReceiptID != record.ReceiptID {
		t.Fatal("receiptchanged", e)
	}
}
