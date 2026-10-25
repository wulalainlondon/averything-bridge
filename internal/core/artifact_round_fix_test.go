package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	a3 "everything-go/contracts/task-api/artifacts-v3"
	a4 "everything-go/contracts/task-api/artifacts-v4"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func artifactRoundRead(t *testing.T, h *Hub, c backend.TaskCaller, version, task, owner, seal, hash string) taskapi.Response {
	t.Helper()
	input := map[string]any{"task_id": task, "seal_id": seal, "artifact_id": "result.txt", "expected_hash": hash, "offset": 0, "limit": 32768}
	if version == a4.Version {
		input["owner_request_id"] = owner
	}
	raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": version, "operation": "read_artifact_bytes", "correlation_id": version + owner + seal, "input": input})
	c.CallID = version + owner + seal
	return h.ExecuteTask(context.Background(), c, raw)
}
func artifactFollow(t *testing.T, h *Hub, p *session.Session, c backend.TaskCaller, root delegation.Record, previous, key, text string) delegation.Record {
	t.Helper()
	f := h.exec.(*extensionExec)
	f.final = text
	in := continuityInput(root, p, h, "original same-task follow")
	in.ExpectedPreviousID = previous
	request := r2OwnerRequest(t, key, in)
	raw, _ := json.Marshal(request)
	c.CallID = key
	x := h.ExecuteTask(context.Background(), c, raw)
	if !x.OK {
		t.Fatal(x.Error)
	}
	latest, e := h.delegations.LatestRound(context.Background(), root.ID)
	if e != nil {
		t.Fatal(e)
	}
	return continuityWait(t, h, latest.ID, "terminal")
}
func assertArtifactOwner(t *testing.T, x taskapi.Response, r delegation.Record, expectedText string, version string) {
	t.Helper()
	if !x.OK {
		t.Fatal(x.Error)
	}
	body := x.Result.(map[string]any)
	data, e := base64.StdEncoding.DecodeString(body["data_base64"].(string))
	if e != nil || string(data) != expectedText || body["owner_request_id"] != r.ChildRequestID {
		t.Fatal("wrong immutable owner/body", body, e)
	}
	if version == a4.Version && body["owner_round_id"] != r.ID {
		t.Fatal("wrong round", body)
	}
}
func TestArtifactRoundFixCurrentHistoricalAndSameContentOwners(t *testing.T) {
	h, p, c, intent, root := extensionFormal(t)
	ctx := context.Background()
	first := correctionSeal(t, h, root, false)
	second := artifactFollow(t, h, p, c, root, root.ID, "owned-round2", "second e\u0301 🧪")
	s2 := correctionSeal(t, h, second, false)
	for _, version := range []string{a3.Version, a4.Version} {
		assertArtifactOwner(t, artifactRoundRead(t, h, c, version, intent.TaskID, root.ChildRequestID, first.ID, first.Hash), root, root.Result, version)
		assertArtifactOwner(t, artifactRoundRead(t, h, c, version, intent.TaskID, second.ChildRequestID, s2.ID, s2.Hash), second, second.Result, version)
	}
	third := artifactFollow(t, h, p, c, root, second.ID, "owned-round3", second.Result)
	s3 := correctionSeal(t, h, third, false)
	if s3.ID != s2.ID || s3.Hash != s2.Hash {
		t.Fatal("identical content fixture missing")
	}
	x := artifactRoundRead(t, h, c, a3.Version, intent.TaskID, "", s2.ID, s2.Hash)
	if x.OK || x.Error.Code != "ambiguous_owner" {
		t.Fatal("rc3 guessed first/latest", x)
	}
	for _, round := range []delegation.Record{second, third} {
		assertArtifactOwner(t, artifactRoundRead(t, h, c, a4.Version, intent.TaskID, round.ChildRequestID, s2.ID, s2.Hash), round, round.Result, a4.Version)
	}
	assertArtifactOwner(t, artifactRoundRead(t, h, c, a3.Version, intent.TaskID, "", first.ID, first.Hash), root, root.Result, a3.Version)
	// Reopen original store; recover original command and continue once, never
	// synthesize another child/effect from artifact reads or receipt recovery.
	if e := h.delegations.Close(); e != nil {
		t.Fatal(e)
	}
	var e error
	h.delegations, e = delegation.Open(h.cfg.DataDir)
	if e != nil {
		t.Fatal(e)
	}
	assertArtifactOwner(t, artifactRoundRead(t, h, c, a4.Version, intent.TaskID, second.ChildRequestID, s2.ID, s2.Hash), second, second.Result, a4.Version)
	fourth := artifactFollow(t, h, p, c, root, third.ID, "round4-after-reopen", "fourth immutable")
	s4 := correctionSeal(t, h, fourth, false)
	assertArtifactOwner(t, artifactRoundRead(t, h, c, a4.Version, intent.TaskID, fourth.ChildRequestID, s4.ID, s4.Hash), fourth, fourth.Result, a4.Version)
	latest, e := h.delegations.LatestRound(ctx, root.ID)
	if e != nil || latest.Version != 4 || fourth.ChildSessionID != root.ChildSessionID {
		t.Fatal("new child/effect", latest, e)
	}
	// Receipt recovery through unchanged public owner command cannot create round5.
	request := r2OwnerRequest(t, "round4-after-reopen", func() backend.DelegationContinuation {
		in := continuityInput(root, p, h, "original same-task follow")
		in.ExpectedPreviousID = third.ID
		return in
	}())
	raw, _ := json.Marshal(request)
	c.CallID = "receipt-readback"
	if x := h.ExecuteTask(ctx, c, raw); !x.OK {
		t.Fatal(x.Error)
	}
	latest, _ = h.delegations.LatestRound(ctx, root.ID)
	if latest.Version != 4 {
		t.Fatal("receipt replay mutated")
	}
}
func TestArtifactRoundFixNewAndHistoricalConfiguredCompiledClient(t *testing.T) {
	h, p, c, intent, root := extensionFormal(t)
	first := correctionSeal(t, h, root, false)
	next := artifactFollow(t, h, p, c, root, root.ID, "client-round2", "new compiled-client result e\u0301 🧪")
	s2 := correctionSeal(t, h, next, false)
	for i, entry := range []struct {
		r        delegation.Record
		id, hash string
	}{{root, first.ID, first.Hash}, {next, s2.ID, s2.Hash}} {
		path, cfg := runtimeCaptureConfig(t, h, p, intent.TaskID, entry.hash, true)
		cfg.Sink = fmt.Sprint("round-client-", i)
		cfg.Verifier.SDK4Hash = captureHash(filepath.Join(filepath.Dir(cfg.Verifier.Script), "artifact-v4.mjs"))
		b, _ := json.Marshal(cfg)
		os.WriteFile(path, b, 0600)
		if e := h.ConfigureNativeResponseCapture(path); e != nil {
			t.Fatal(e)
		}
		input := map[string]any{"task_id": intent.TaskID, "owner_request_id": entry.r.ChildRequestID, "seal_id": entry.id, "artifact_id": "result.txt", "expected_hash": entry.hash, "offset": 0, "limit": 3}
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": a4.Version, "correlation_id": fmt.Sprint("round-client-", i), "operation": "read_artifact_bytes", "input": input})
		c.CallID = fmt.Sprint("round-client-", i)
		response := h.ExecuteTask(context.Background(), c, raw)
		if !response.OK {
			t.Fatal(response.Error)
		}
		if e := h.CaptureTaskResponse(context.Background(), c, raw, response); e != nil {
			t.Fatal(e)
		}
		sink := h.nativeCapture.Load().Directory
		files, _ := os.ReadDir(sink)
		proof := false
		for _, f := range files {
			raw, _ := os.ReadFile(filepath.Join(sink, f.Name()))
			var v map[string]any
			json.Unmarshal(raw, &v)
			if v["evidence_kind"] == "original_response_owner_artifact_sdk_verifier" {
				proof = v["owner_request_id"] == entry.r.ChildRequestID && v["full_hash"] == entry.hash && v["bytes"] == float64(len([]byte(entry.r.Result)))
			}
		}
		if !proof {
			t.Fatal("exact owner compiled verifier missing")
		}
	}
}
func TestArtifactRoundFixForeignRevokedAndStaleScope(t *testing.T) {
	for _, kind := range []string{"foreign-task", "foreign-request", "foreign-seal", "wrong-hash", "wrong-owner-seal", "revoked", "foreign-source", "stale-scope", "quarantined-anchor", "missing-child"} {
		t.Run(kind, func(t *testing.T) {
			h, p, c, intent, root := extensionFormal(t)
			first := correctionSeal(t, h, root, false)
			next := artifactFollow(t, h, p, c, root, root.ID, "negative-round2", "new result")
			s2 := correctionSeal(t, h, next, false)
			task, owner, seal, hash := intent.TaskID, next.ChildRequestID, s2.ID, s2.Hash
			switch kind {
			case "foreign-task":
				task = apiID("foreign", "delegation", root.ID)
			case "foreign-request":
				owner = "foreign"
			case "foreign-seal":
				seal = "foreign"
			case "wrong-hash":
				hash = fmt.Sprintf("%064d", 0)
			case "wrong-owner-seal":
				seal, hash = first.ID, first.Hash
			case "revoked":
				c.Validate = func() bool { return false }
			case "foreign-source":
				c.Session, _ = h.registry.Get(root.ChildSessionID)
				c.ThreadID = c.Session.ResumeID()
			case "stale-scope":
				child, _ := h.registry.Get(root.ChildSessionID)
				child.ApplyConfig(child.Backend(), child.SettingsSnapshot().Model, "workspace-write")
			case "quarantined-anchor":
				h.messageQueue.RecordNativeAcceptance(root.ChildSessionID, next.ChildRequestID, "foreign-thread", "foreign-turn")
			case "missing-child":
				h.registry.Get(root.ChildSessionID)
				child, _ := h.registry.Get(root.ChildSessionID)
				child.SetResumeID("foreign-thread")
			}
			x := artifactRoundRead(t, h, c, a4.Version, task, owner, seal, hash)
			if x.OK {
				t.Fatal("foreign/stale tuple read", kind, x)
			}
			latest, e := h.delegations.LatestRound(context.Background(), root.ID)
			if e != nil || latest.Version != 2 {
				t.Fatal("negative read mutated", latest, e)
			}
		})
	}
}

func TestArtifactRoundFixPendingAndUnsealedRefuse(t *testing.T) {
	for _, state := range []string{"pending", "unsealed"} {
		t.Run(state, func(t *testing.T) {
			h, p, c, intent, root := extensionFormal(t)
			f := h.exec.(*extensionExec)
			f.onSend = func(*session.Session, string, string) {}
			in := continuityInput(root, p, h, "owned pending read check")
			request := r2OwnerRequest(t, "pending-append", in)
			raw, _ := json.Marshal(request)
			c.CallID = "pending-append"
			x := h.ExecuteTask(context.Background(), c, raw)
			if !x.OK {
				t.Fatal(x.Error)
			}
			latest, e := h.delegations.LatestRound(context.Background(), root.ID)
			if e != nil || latest.Version != 2 {
				t.Fatal(e)
			}
			record, found, e := h.delegations.ByID(context.Background(), latest.ID)
			if e != nil || !found {
				t.Fatal(e)
			}
			if state == "unsealed" {
				h.delegations.MarkRunning(context.Background(), record.ID)
				if _, e := h.delegations.Complete(context.Background(), record.ChildSessionID, record.ChildRequestID, "completed", "owned-unsealed", "", nil); e != nil {
					t.Fatal(e)
				}
			}
			hash := fmt.Sprintf("%064d", 0)
			result := artifactRoundRead(t, h, c, a4.Version, intent.TaskID, record.ChildRequestID, "seal_"+hash, hash)
			if result.OK {
				t.Fatal("pending/unsealed body exposed")
			}
			latest, _ = h.delegations.LatestRound(context.Background(), root.ID)
			if latest.Version != 2 {
				t.Fatal("read recreated effect")
			}
		})
	}
}
