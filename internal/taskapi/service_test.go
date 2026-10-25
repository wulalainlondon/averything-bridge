package taskapi

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	taskcontract "everything-go/contracts/task-api/v1"
)

// All test authorities/gateways below are isolated fixtures, not native caller
// evidence or a production canonical store implementation.
type fixtureVerifier struct {
	context VerifiedContext
	revoked bool
}

func (f *fixtureVerifier) Verify(context.Context, Invocation) (VerifiedContext, error) {
	if f.revoked {
		return VerifiedContext{}, Failure("caller_unbound", "known_none", "refresh_identity")
	}
	return f.context, nil
}
func fixtureCaller(t *testing.T, generation string) (BoundCaller, *fixtureVerifier) {
	t.Helper()
	v := &fixtureVerifier{context: VerifiedContext{Authority: "instance-fixture", StableScopeID: "authorization-fixture", NamespaceGeneration: 7, InvocationGeneration: generation, SourceSessionID: "source-fixture", SourceRequestID: "r_fixture12345678", Transport: "authenticated_mcp", BindingKind: "mcp_process_binding"}}
	c, e := BindCaller(context.Background(), v, Invocation{Provider: "fixture", ProcessGeneration: generation})
	if e != nil {
		t.Fatal(e)
	}
	return c, v
}

type fixturePolicy struct {
	path   string
	denied bool
}

func (p fixturePolicy) Authorize(_ context.Context, c VerifiedContext, r Request) (Locator, error) {
	if p.denied {
		return Locator{}, Failure("permission", "known_none", "request_scope_change")
	}
	var input map[string]any
	json.Unmarshal(r.Input, &input)
	if r.Operation == "get" {
		if raw, ok := input["original_receipt"]; ok {
			bytes, _ := json.Marshal(raw)
			var locator Locator
			json.Unmarshal(bytes, &locator)
			if locator.Path != p.path {
				return Locator{}, Failure("permission", "known_none", "request_scope_change")
			}
			return locator, nil
		}
	}
	task, _ := input["task_id"].(string)
	if r.Operation == "create_dispatch" {
		task = ""
	}
	return Locator{c.Authority, p.path, r.Operation, r.IdempotencyKey, task}, nil
}

type fixtureGateway struct {
	mu       sync.Mutex
	receipts map[string]any
	hashes   map[string]string
	effects  int
	reads    int
	expire   bool
	unknown  bool
}

func (f *fixtureGateway) Mutate(_ context.Context, c AuthorizedCommand) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := c.Namespace.Key(c.Request.IdempotencyKey)
	if previous, exists := f.hashes[key]; exists {
		if previous != c.IntentHash {
			return nil, Failure("idempotency_conflict", "known_receipt", "lookup_original")
		}
		if f.expire {
			return nil, Failure("key_expired", "known_receipt", "lookup_original")
		}
		return f.receipts[key], nil
	}
	f.effects++
	if f.hashes == nil {
		f.hashes = map[string]string{}
		f.receipts = map[string]any{}
	}
	r := goldenResult("create_dispatch")
	r["operation"] = c.Request.Operation
	if c.Namespace.TaskID != "" {
		r["task_id"] = c.Namespace.TaskID
	}
	r["locator"] = c.Locator
	r["intent_hash"] = c.IntentHash
	r["task_ref"].(map[string]any)["path"] = c.Locator.Path
	f.hashes[key] = c.IntentHash
	f.receipts[key] = r
	if f.unknown {
		return nil, Failure("unknown_acceptance", "unknown", "lookup_original")
	}
	return r, nil
}
func (f *fixtureGateway) Read(_ context.Context, c AuthorizedCommand) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	var input struct {
		Lookup *Locator `json:"original_receipt"`
	}
	json.Unmarshal(c.Request.Input, &input)
	if input.Lookup != nil {
		n := c.Namespace
		r, ok := f.receipts[n.Key(c.Locator.Key)]
		if !ok {
			return nil, Failure("not_found", "known_none", "lookup_original")
		}
		return r, nil
	}
	return goldenResult(c.Request.Operation), nil
}
func goldenResult(op string) map[string]any {
	var v []struct {
		Name  string
		Value map[string]any
	}
	json.Unmarshal(taskcontract.Golden, &v)
	for _, x := range v {
		if x.Name == "positive-response-"+op {
			return x.Value["result"].(map[string]any)
		}
	}
	panic(op)
}
func goldenRequest(op string) []byte {
	var v []struct {
		Name  string
		Value json.RawMessage
	}
	json.Unmarshal(taskcontract.Golden, &v)
	for _, x := range v {
		if x.Name == "request-"+op {
			return x.Value
		}
	}
	panic(op)
}
func validateResponse(t *testing.T, r Response) {
	t.Helper()
	c, e := taskcontract.New()
	if e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Validate(raw, "Response"); e != nil {
		t.Fatalf("invalid response %s: %v", raw, e)
	}
}

func TestTypedUnknownOperationAndVersion(t *testing.T) {
	s, _ := NewService(nil, nil)
	c, _ := fixtureCaller(t, "process-1")
	for _, values := range [][2]string{{"future_op", taskcontract.Version}, {"cancel", "99.0"}} {
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": values[1], "correlation_id": "corr-fixture", "operation": values[0], "input": map[string]any{}})
		r := s.Execute(context.Background(), c, raw)
		if r.Error.Code != "unsupported" || *r.Operation != values[0] || *r.RequestVersion != values[1] || r.CorrelationID != "corr-fixture" {
			t.Fatal(r)
		}
		validateResponse(t, r)
	}
}
func TestInvalidAndUnboundHaveValidErrorEnvelope(t *testing.T) {
	s, _ := NewService(nil, nil)
	for _, raw := range [][]byte{[]byte(`{}`), []byte(`{} {}`), []byte(`{"operation":"x","operation":"y"}`), goldenRequest("get")} {
		r := s.Execute(context.Background(), BoundCaller{}, raw)
		validateResponse(t, r)
	}
}
func TestNamespaceTaskStoreAndReconnect(t *testing.T) {
	g := &fixtureGateway{}
	s, _ := NewService(fixturePolicy{path: "controller"}, g)
	c, _ := fixtureCaller(t, "process-1")
	raw := goldenRequest("append")
	r := s.Execute(context.Background(), c, raw)
	if !r.OK {
		t.Fatal(r)
	}
	validateResponse(t, r)
	c2, _ := fixtureCaller(t, "process-2")
	if r = s.Execute(context.Background(), c2, raw); !r.OK {
		t.Fatal(r)
	}
	if g.effects != 1 {
		t.Fatal(g.effects)
	}
	var x map[string]any
	json.Unmarshal(raw, &x)
	x["input"].(map[string]any)["content"] = "different"
	changed, _ := json.Marshal(x)
	if r = s.Execute(context.Background(), c2, changed); r.Error == nil || r.Error.Code != "idempotency_conflict" {
		t.Fatal(r)
	}
	x["input"].(map[string]any)["task_id"] = "another-task"
	other, _ := json.Marshal(x)
	if r = s.Execute(context.Background(), c2, other); !r.OK {
		t.Fatal(r)
	}
	if g.effects != 2 {
		t.Fatal(g.effects)
	}
	otherService, _ := NewService(fixturePolicy{path: "delegation"}, g)
	if r = otherService.Execute(context.Background(), c2, raw); !r.OK {
		t.Fatal(r)
	}
	if g.effects != 3 {
		t.Fatal(g.effects)
	}
	// Cancellation targets a command in one task; changing command under same
	// cancel key conflicts instead of silently selecting a second command.
	raw = goldenRequest("cancel")
	if r = s.Execute(context.Background(), c, raw); !r.OK {
		t.Fatal(r)
	}
	json.Unmarshal(raw, &x)
	x["input"].(map[string]any)["command_receipt_id"] = "another-command"
	other, _ = json.Marshal(x)
	r = s.Execute(context.Background(), c, other)
	if r.Error == nil || r.Error.Code != "idempotency_conflict" {
		t.Fatal(r)
	}
}
func TestConcurrentSameIntentOneFixtureEffect(t *testing.T) {
	g := &fixtureGateway{}
	s, _ := NewService(fixturePolicy{path: "delegation"}, g)
	c, _ := fixtureCaller(t, "process-1")
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if r := s.Execute(context.Background(), c, goldenRequest("create_dispatch")); !r.OK {
				t.Error(r)
			}
		})
	}
	wg.Wait()
	if g.effects != 1 {
		t.Fatal(g.effects)
	}
}
func TestExpiredBodyStillPreventsEffect(t *testing.T) {
	g := &fixtureGateway{}
	s, _ := NewService(fixturePolicy{path: "delegation"}, g)
	c, v := fixtureCaller(t, "process-1")
	raw := goldenRequest("create_dispatch")
	s.Execute(context.Background(), c, raw)
	g.expire = true
	r := s.Execute(context.Background(), c, raw)
	if r.Error.Code != "key_expired" || g.effects != 1 {
		t.Fatal(r, g.effects)
	}
	v.revoked = true
	r = s.Execute(context.Background(), c, raw)
	if r.Error.Code != "caller_unbound" || g.effects != 1 {
		t.Fatal(r, g.effects)
	}
}
func TestUnknownLookupOnlyNoReplay(t *testing.T) {
	g := &fixtureGateway{unknown: true}
	s, _ := NewService(fixturePolicy{path: "delegation"}, g)
	c, _ := fixtureCaller(t, "process-1")
	r := s.Execute(context.Background(), c, goldenRequest("append"))
	if r.Error.Code != "unknown_acceptance" {
		t.Fatal(r)
	}
	lookup, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": taskcontract.Version, "correlation_id": "lookup-fixture", "operation": "get", "input": map[string]any{"original_receipt": Locator{"instance-fixture", "delegation", "append", "key-fixture-12345678", "task-fixture"}}})
	r = s.Execute(context.Background(), c, lookup)
	if !r.OK || g.effects != 1 || g.reads != 1 {
		t.Fatal(r, g.effects, g.reads)
	}
}
func TestInternalDeliveryNeverModelCallable(t *testing.T) {
	g := &fixtureGateway{}
	s, _ := NewService(fixturePolicy{path: "controller"}, g)
	c, _ := fixtureCaller(t, "process-1")
	r := s.Execute(context.Background(), c, goldenRequest("deliver_result"))
	if r.Error.Code != "permission" || g.effects != 0 {
		t.Fatal(r, g.effects)
	}
	tools, err := taskcontract.ToolInputs()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tools["task_deliver_result"]; ok {
		t.Fatal("registered internal tool")
	}
}
func TestIntentNormalizationExcludesTransientIdentity(t *testing.T) {
	var r Request
	json.Unmarshal(goldenRequest("create_dispatch"), &r)
	h, _ := IntentHash(r)
	r.CorrelationID = "new-correlation"
	var in map[string]any
	json.Unmarshal(r.Input, &in)
	in["new_worker"].(map[string]any)["profile"] = map[string]any{"backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}
	r.Input, _ = json.Marshal(in)
	same, _ := IntentHash(r)
	if h != same {
		t.Fatal("default profile changed identity")
	}
	in["instruction"] = " Read isolated input artifact."
	r.Input, _ = json.Marshal(in)
	different, _ := IntentHash(r)
	if h == different {
		t.Fatal("instruction whitespace lost")
	}
}

type mismatchedLookupPolicy struct{}

func (mismatchedLookupPolicy) Authorize(_ context.Context, c VerifiedContext, r Request) (Locator, error) {
	return Locator{c.Authority, "controller", "append", "key-fixture-12345678", "task-fixture"}, nil
}
func TestLookupCanonicalAssertionMismatchBeforeGateway(t *testing.T) {
	c, _ := fixtureCaller(t, "process-1")
	g := &fixtureGateway{}
	s, _ := NewService(mismatchedLookupPolicy{}, g)
	r := s.Execute(context.Background(), c, goldenRequest("get"))
	if r.Error == nil || r.Error.Code != "permission" || g.reads != 0 {
		t.Fatal(r, g.reads)
	}
}
func TestLookupNamespaceIsolation(t *testing.T) {
	c, _ := fixtureCaller(t, "process-1")
	g := &fixtureGateway{}
	s, _ := NewService(fixturePolicy{path: "delegation"}, g)
	s.Execute(context.Background(), c, goldenRequest("append"))
	raw := goldenRequest("get")
	if r := s.Execute(context.Background(), c, raw); !r.OK {
		t.Fatal(r)
	}
	var request map[string]any
	json.Unmarshal(raw, &request)
	lookup := request["input"].(map[string]any)["original_receipt"].(map[string]any)
	lookup["task_id"] = "other-task"
	changed, _ := json.Marshal(request)
	r := s.Execute(context.Background(), c, changed)
	if r.Error == nil || r.Error.Code != "not_found" {
		t.Fatal(r)
	}
	lookup["task_id"] = "task-fixture"
	lookup["path"] = "controller"
	changed, _ = json.Marshal(request)
	before := g.reads
	r = s.Execute(context.Background(), c, changed)
	if r.Error == nil || r.Error.Code != "permission" || g.reads != before {
		t.Fatal(r, g.reads)
	}
	lookup["path"] = "delegation"
	lookup["authority_instance_id"] = "another-authority"
	changed, _ = json.Marshal(request)
	before = g.reads
	r = s.Execute(context.Background(), c, changed)
	if r.Error == nil || r.Error.Code != "wrong_authority" || g.reads != before {
		t.Fatal(r, g.reads)
	}
	c2, v2 := fixtureCaller(t, "process-2")
	v2.context.StableScopeID = "another-principal-scope"
	c2, _ = BindCaller(context.Background(), v2, Invocation{Provider: "fixture", ProcessGeneration: "process-2"})
	r = s.Execute(context.Background(), c2, raw)
	if r.Error == nil || r.Error.Code != "not_found" {
		t.Fatal(r)
	}
}
