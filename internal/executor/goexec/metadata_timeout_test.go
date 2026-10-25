package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/agentvisibility"
	"everything-go/internal/backend"
	"everything-go/internal/session"
)

type metadataFrame struct {
	ID     int
	Method string
	Params map[string]any
}
type metadataResult struct {
	raw json.RawMessage
	err error
}

func metadataFixture(t *testing.T) (*Codex, *session.Session, *rpcCaptureWriter) {
	t.Helper()
	c := NewCodex(&capSink{}, "forbidden-model-binary")
	c.SetDataDir(t.TempDir())
	c.appServerMode = "daemon"
	c.appServerSocket = filepath.Join(t.TempDir(), "absent.sock")
	c.RegisterExistingDaemonObservation()
	s := session.NewRegistry().Create("s1", "S", t.TempDir(), backend.Codex, "", "", "root")
	c.state(s.ID).threadID = "root"
	w := &rpcCaptureWriter{writes: make(chan []byte, 16)}
	c.rpc.setWriter(w)
	return c, s, w
}
func metadataNext(t *testing.T, w *rpcCaptureWriter) metadataFrame {
	t.Helper()
	select {
	case raw := <-w.writes:
		var f metadataFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatal(err)
		}
		return f
	case <-time.After(2 * time.Second):
		t.Fatal("bounded fixture: missing frame")
	}
	return metadataFrame{}
}
func metadataReadFrame(t *testing.T, w *rpcCaptureWriter) metadataFrame {
	t.Helper()
	f := metadataNext(t, w)
	if f.Method != "thread/read" || len(f.Params) != 2 || f.Params["includeTurns"] != false || f.Params["threadId"] != "root" {
		t.Fatalf("metadata widened RPC: %+v", f)
	}
	return f
}
func metadataReply(c *Codex, id int, status string) {
	c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"thread":{"id":"root","sessionId":"tree","status":{"type":%q},"turns":[]}}}`, id, status)))
}
func metadataAwait(t *testing.T, ch <-chan metadataResult) metadataResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("bounded fixture: missing result")
	}
	return metadataResult{}
}
func metadataRead(c *Codex, s *session.Session, ctx context.Context) <-chan metadataResult {
	done := make(chan metadataResult, 1)
	go func() {
		raw, err := (codexVisibilityRPC{c: c, s: s, authority: "authority"}).ReadThread(ctx, "root")
		done <- metadataResult{raw, err}
	}()
	return done
}
func metadataAssertQuiet(t *testing.T, c *Codex, w *rpcCaptureWriter, epoch uint64) {
	t.Helper()
	c.health.mu.Lock()
	changed := c.health.blocked || c.health.checking || !c.health.nextCheck.IsZero() || c.health.failures != 0 || c.health.method != "" || c.health.threadID != ""
	c.health.mu.Unlock()
	initialDiagnostics := map[string]any{"codex": map[string]any{"status": "not_checked"}}
	if changed || c.rpc.pendingCount() != 0 || c.rpc.transportEpoch.Load() != epoch || c.proc != nil || !reflect.DeepEqual(c.RuntimeDiagnostics(), initialDiagnostics) {
		t.Fatalf("metadata changed global health/transport or leaked pending: health=%v pending=%d epoch=%d want=%d diagnostics=%v", changed, c.rpc.pendingCount(), c.rpc.transportEpoch.Load(), epoch, c.RuntimeDiagnostics())
	}
	select {
	case raw := <-w.writes:
		t.Fatalf("unexpected retry/discovery/recovery RPC: %s", raw)
	default:
	}
}

// Real one-second response timeout, through the observation entry used by the Hub.
func TestMetadataTimeoutMustNotTriggerGlobalRecovery(t *testing.T) {
	c, s, w := metadataFixture(t)
	epoch := c.rpc.transportEpoch.Load()
	called, release, settled := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
	c.health.probe = func(string) codexProbeResult {
		called <- struct{}{}
		<-release
		close(settled)
		return codexProbeResult{Reads: 1}
	}
	c.health.recover = func() error { t.Error("metadata entered restart"); return nil }
	defer func() {
		close(release)
		select {
		case <-called:
			select {
			case <-settled:
			case <-time.After(time.Second):
				t.Error("hook did not settle")
			}
		default:
		}
	}()
	before := s.Snapshot()
	out := c.ObserveInternalAgents(context.Background(), s, "authority")
	metadataReadFrame(t, w)
	if out.Availability != "unknownstale" || out.ProbeError != "timeout" || out.ObservedAt != "" || len(out.Children) != 0 || out.Coverage != "partial_provider_child_cache" {
		t.Fatalf("timeout falsely promoted: %+v", out)
	}
	metadataAssertQuiet(t, c, w, epoch)
	if !reflect.DeepEqual(before, s.Snapshot()) || c.state(s.ID).turnActive {
		t.Fatal("native session changed")
	}
	select {
	case <-called:
		t.Fatal("metadata scheduled global health probe")
	default:
	}
}

func TestMetadataCancelAndDeadlineRetireOnlyOwnCorrelation(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			c, s, w := metadataFixture(t)
			epoch := c.rpc.transportEpoch.Load()
			ordinary := make(chan metadataResult, 1)
			go func() {
				raw, err := c.rpcCall("turn/start", map[string]any{"threadId": "ordinary"}, time.Second)
				ordinary <- metadataResult{raw, err}
			}()
			turn := metadataNext(t, w)
			ctx, cancel := context.WithCancel(context.Background())
			expected := error(context.Canceled)
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				expected = context.DeadlineExceeded
			}
			defer cancel()
			done := metadataRead(c, s, ctx)
			f := metadataReadFrame(t, w)
			if mode == "cancel" {
				cancel()
			}
			r := metadataAwait(t, done)
			if !errors.Is(r.err, expected) || r.raw != nil || c.rpc.pendingCount() != 1 {
				t.Fatalf("cancel leaked/failed other request: %v pending=%d", r.err, c.rpc.pendingCount())
			}
			metadataReply(c, f.ID, "active")
			select {
			case r := <-ordinary:
				t.Fatalf("late metadata settled ordinary turn: %v", r)
			default:
			}
			c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"turn":"original"}}`, turn.ID)))
			if r := metadataAwait(t, ordinary); r.err != nil || string(r.raw) != `{"turn":"original"}` {
				t.Fatal(r)
			}
			metadataAssertQuiet(t, c, w, epoch)
		})
	}
}

func TestMetadataTimeoutLateReplyCannotCompleteFreshOrOrdinaryRequest(t *testing.T) {
	c, s, w := metadataFixture(t)
	epoch := c.rpc.transportEpoch.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	first := metadataRead(c, s, ctx)
	old := metadataReadFrame(t, w)
	if r := metadataAwait(t, first); !errors.Is(r.err, context.DeadlineExceeded) || c.rpc.pendingCount() != 0 {
		t.Fatal(r)
	}
	ordinary := make(chan metadataResult, 1)
	go func() { raw, err := c.rpcCall("turn/start", nil, time.Second); ordinary <- metadataResult{raw, err} }()
	turn := metadataNext(t, w)
	next := make(chan agentvisibility.Snapshot, 1)
	go func() { next <- c.ObserveInternalAgents(context.Background(), s, "authority") }()
	fresh := metadataReadFrame(t, w)
	if old.ID == fresh.ID || old.ID == turn.ID || fresh.ID == turn.ID || c.rpc.pendingCount() != 2 {
		t.Fatal("correlation reused")
	}
	for i := 0; i < 2; i++ {
		metadataReply(c, old.ID, "active")
	}
	select {
	case r := <-ordinary:
		t.Fatalf("late reply settled turn: %v", r)
	case r := <-next:
		t.Fatalf("late reply settled metadata: %+v", r)
	case <-time.After(20 * time.Millisecond):
	}
	if c.rpc.pendingCount() != 2 {
		t.Fatal("late reply removed another pending")
	}
	c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{"turn":"original"}}`, turn.ID)))
	if r := metadataAwait(t, ordinary); r.err != nil {
		t.Fatal(r)
	}
	metadataReply(c, fresh.ID, "idle")
	select {
	case out := <-next:
		if out.Availability != "live" || out.RootState != "idle" || out.ObservedAt == "" || out.Coverage != "partial_provider_child_cache" {
			t.Fatal(out)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh exact reply missing")
	}
	metadataAssertQuiet(t, c, w, epoch)
}

func TestMetadataAbnormalRepliesStayUnverified(t *testing.T) {
	for _, mode := range []string{"malformed_result", "missing_identity", "wrong_root", "history", "rpc_error", "malformed_wire"} {
		t.Run(mode, func(t *testing.T) {
			c, s, w := metadataFixture(t)
			epoch := c.rpc.transportEpoch.Load()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan agentvisibility.Snapshot, 1)
			go func() { done <- c.ObserveInternalAgents(ctx, s, "authority") }()
			f := metadataReadFrame(t, w)
			payload := `{"thread": "invalid"}`
			switch mode {
			case "missing_identity":
				payload = `{}`
			case "wrong_root":
				payload = `{"thread":{"id":"foreign","sessionId":"tree","status":{"type":"idle"}}}`
			case "history":
				payload = `{"thread":{"id":"root","sessionId":"tree","status":{"type":"idle"},"turns":[{"private":"forbidden"}]}}`
			}
			if mode == "rpc_error" {
				c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"error":{"code":-1,"message":"rejected"}}`, f.ID)))
			} else if mode == "malformed_wire" {
				if c.rpc.dispatchResponse(json.RawMessage(`{bad`)) {
					t.Fatal("invalid wire accepted")
				}
				cancel()
			} else {
				c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":%s}`, f.ID, payload)))
			}
			select {
			case out := <-done:
				if out.Availability != "unknownstale" || out.ObservedAt != "" || len(out.Children) != 0 || out.Coverage == "complete" {
					t.Fatalf("abnormal promoted: %+v", out)
				}
			case <-time.After(time.Second):
				t.Fatal("abnormal did not settle")
			}
			metadataAssertQuiet(t, c, w, epoch)
		})
	}
}

func TestMetadataReadDoesNotEnterOrClearExistingHealthAdmission(t *testing.T) {
	c, s, w := metadataFixture(t)
	c.health.blocked = true
	c.health.nextCheck = time.Now().Add(-time.Hour)
	c.health.failures = 1
	c.health.method = "turn/start"
	c.health.threadID = "ordinary"
	var probes atomic.Int32
	c.health.probe = func(string) codexProbeResult { probes.Add(1); return codexProbeResult{Reads: 1} }
	before := c.health.nextCheck
	done := metadataRead(c, s, context.Background())
	f := metadataReadFrame(t, w)
	metadataReply(c, f.ID, "idle")
	if r := metadataAwait(t, done); r.err != nil {
		t.Fatal(r)
	}
	c.health.mu.Lock()
	changed := !c.health.blocked || c.health.checking || c.health.nextCheck != before || c.health.failures != 1 || c.health.method != "turn/start" || c.health.threadID != "ordinary"
	c.health.mu.Unlock()
	if changed || probes.Load() != 0 {
		t.Fatal("metadata touched pre-existing health circuit")
	}
	// The original admission still refuses ordinary thread/turn work.
	c.health.nextCheck = time.Now().Add(time.Hour)
	if _, err := c.rpcCall("turn/start", nil, time.Second); err == nil {
		t.Fatal("ordinary health admission removed")
	}
	if c.rpc.pendingCount() != 0 {
		t.Fatal("ordinary refusal leaked pending")
	}
	select {
	case raw := <-w.writes:
		t.Fatalf("blocked turn submitted: %s", raw)
	default:
	}
}

func TestMetadataSharedDisconnectStillFailsAllPendingAndChangesEpoch(t *testing.T) {
	c, s, w := metadataFixture(t)
	ordinary := make(chan metadataResult, 1)
	go func() { raw, err := c.rpcCall("turn/start", nil, time.Second); ordinary <- metadataResult{raw, err} }()
	turn := metadataNext(t, w)
	done := metadataRead(c, s, context.Background())
	old := metadataReadFrame(t, w)
	epoch := c.rpc.transportEpoch.Load()
	lost := errors.New("synthetic physical shared disconnect")
	// The transport owner, rather than a metadata deadline, retires all requests.
	c.rpc.failAll(lost)
	c.rpc.setWriter(nil)
	if r := metadataAwait(t, done); !errors.Is(r.err, lost) {
		t.Fatal(r)
	}
	if r := metadataAwait(t, ordinary); !errors.Is(r.err, lost) {
		t.Fatal(r)
	}
	if c.rpc.pendingCount() != 0 || c.rpc.transportEpoch.Load() == epoch {
		t.Fatal("physical failure policy suppressed")
	}
	c.rpc.setWriter(w) // fixture replacement only; no metadata reconnect.
	next := metadataRead(c, s, context.Background())
	fresh := metadataReadFrame(t, w)
	metadataReply(c, old.ID, "active")
	metadataReply(c, turn.ID, "active")
	if c.rpc.pendingCount() != 1 {
		t.Fatal("old transport reply polluted new pending")
	}
	metadataReply(c, fresh.ID, "idle")
	if r := metadataAwait(t, next); r.err != nil {
		t.Fatal(r)
	}
	metadataAssertQuiet(t, c, w, c.rpc.transportEpoch.Load())
}

type metadataFailWriter struct{}

func (metadataFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestMetadataWriterFailureAndEpochRefusalCleanOwnPending(t *testing.T) {
	c, s, w := metadataFixture(t)
	c.rpc.setWriter(metadataFailWriter{})
	if r := metadataAwait(t, metadataRead(c, s, context.Background())); !errors.Is(r.err, io.ErrClosedPipe) || c.rpc.pendingCount() != 0 {
		t.Fatal(r)
	}
	epoch := c.rpc.transportEpoch.Load()
	c.rpc.setWriter(w)
	_, err := c.rpc.requestAtEpochContext(context.Background(), "thread/read", nil, time.Second, &epoch)
	var refused *codexAcceptanceError
	if !errors.As(err, &refused) || refused.Reason != "native_source_changed" || c.rpc.pendingCount() != 0 {
		t.Fatalf("epoch bypass: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r := metadataAwait(t, metadataRead(c, s, ctx)); !errors.Is(r.err, context.Canceled) {
		t.Fatal(r)
	}
	metadataAssertQuiet(t, c, w, c.rpc.transportEpoch.Load())
}

func TestMetadataChildTimeoutKeepsPartialCoverageAndUnverifiedAxes(t *testing.T) {
	c, s, w := metadataFixture(t)
	epoch := c.rpc.transportEpoch.Load()
	c.state(s.ID).agents["qa"] = &codexAgent{id: "qa", parentID: "root"}
	done := make(chan agentvisibility.Snapshot, 1)
	go func() { done <- c.ObserveInternalAgents(context.Background(), s, "authority") }()
	root := metadataReadFrame(t, w)
	metadataReply(c, root.ID, "idle")
	child := metadataNext(t, w)
	if child.Method != "thread/read" || child.Params["threadId"] != "qa" || child.Params["includeTurns"] != false {
		t.Fatal(child)
	}
	select {
	case out := <-done:
		if out.Coverage != "partial_provider_child_cache" || len(out.Children) != 1 {
			t.Fatal(out)
		}
		ch := out.Children[0]
		if ch.State != "unknownstale" || ch.ProbeError != "timeout" || ch.Association != "unmatched" || ch.Binding != nil || ch.Report != nil || ch.Axes.NativeFinal != "unverified" || ch.Axes.Review != "unverified" || ch.Axes.Delivery != "unverified" {
			t.Fatalf("child timeout promoted verification: %+v", ch)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("child timeout did not settle")
	}
	metadataReply(c, child.ID, "active")
	metadataAssertQuiet(t, c, w, epoch)
}
