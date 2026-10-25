package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	artifact "everything-go/contracts/task-api/artifacts-v3"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func runtimeCaptureConfig(t *testing.T, h *Hub, p *session.Session, id, digest string, verify bool) (string, NativeCaptureConfig) {
	t.Helper()
	root, e := filepath.EvalSymlinks(h.cfg.DataDir)
	if e != nil {
		t.Fatal(e)
	}
	cfg := NativeCaptureConfig{Version: 1, Authority: h.cfg.InstanceID, Source: p.ID, Thread: p.ResumeID(), Revision: p.SettingsSnapshot().ConfigRevision, Sink: "owned-runtime-test", Operations: []string{"capabilities", "get", "read_result", "artifact_capabilities", "read_artifact_bytes"}, Tasks: map[string]string{id: digest}}
	if verify {
		node, e := exec.LookPath("node")
		if e != nil {
			t.Fatal(e)
		}
		node, e = filepath.EvalSymlinks(node)
		if e != nil {
			t.Fatal(e)
		}
		script := os.Getenv("OWNED_VERIFIER_SCRIPT")
		if script == "" {
			t.Fatal("compiled reviewed verifier required")
		}
		cfg.Verifier = &NativeArtifactVerifier{Node: node, NodeHash: captureHash(node), Script: script, ScriptHash: captureHash(script), SDKHash: captureHash(filepath.Join(filepath.Dir(script), "artifact-v3.mjs"))}
	}
	path := filepath.Join(root, "operator-capture.json")
	bytes, _ := json.Marshal(cfg)
	if e := os.WriteFile(path, bytes, 0600); e != nil {
		t.Fatal(e)
	}
	return path, cfg
}
func TestRuntimeBootstrapOriginalOwnerCompiledVerifier(t *testing.T) {
	h, p, c, intent, r := extensionFormal(t)
	seal := correctionSeal(t, h, r, false)
	ctx := context.Background()
	path, cfg := runtimeCaptureConfig(t, h, p, intent.TaskID, seal.Hash, true)
	if e := h.ConfigureNativeResponseCapture(path); e != nil {
		t.Fatal(e)
	}
	defer h.DisableNativeResponseCapture()
	raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": artifact.Version, "correlation_id": "original-correlated-range", "operation": "read_artifact_bytes", "input": map[string]any{"task_id": intent.TaskID, "seal_id": seal.ID, "artifact_id": "result.txt", "expected_hash": seal.Hash, "offset": 0, "limit": 3}})
	c.CallID = "actual-fixture-native-call"
	response := h.ExecuteTask(ctx, c, raw)
	if !response.OK {
		t.Fatal(response.Error)
	}
	// This is the same hook called by both actual native handlers; a separate
	// actual-MCP test uses production config with the same owner below.
	if e := h.CaptureTaskResponse(ctx, c, raw, response); e != nil {
		t.Fatal(e)
	}
	sink := h.nativeCapture.Load().Directory
	files, e := os.ReadDir(sink)
	if e != nil {
		t.Fatal(e)
	}
	verified := false
	bytesSeen := false
	for _, f := range files {
		b, _ := os.ReadFile(filepath.Join(sink, f.Name()))
		var value map[string]any
		json.Unmarshal(b, &value)
		if value["raw_wire"] != false || value["native_call_id"] != c.CallID || value["source_session_id"] != p.ID {
			t.Fatal("origin lost", value)
		}
		if value["evidence_kind"] == "original_response_owner_artifact_sdk_verifier" {
			verified = value["full_hash"] == seal.Hash && value["bytes"] == float64(len([]byte(seal.Text)))
		}
		if public, ok := value["public_result"].(map[string]any); ok && public["data_base64"] != nil {
			bytesSeen = true
		}
	}
	if !verified || !bytesSeen {
		t.Fatal("compiled client route missing")
	}
	// Removal is explicit disable; no further source reads/sink writes.
	os.Remove(path)
	c.CallID = "after-disable"
	if e := h.CaptureTaskResponse(ctx, c, raw, response); e == nil {
		t.Fatal("removed operator config accepted")
	}
	if h.nativeCapture.Load() != nil {
		t.Fatal("disable lifecycle failed")
	}
	_ = cfg
}
func TestRuntimeBootstrapCaptureConfigGuards(t *testing.T) {
	for _, kind := range []string{"no-config", "foreign-authority", "foreign-task", "stale-thread", "stale-config", "malformed", "world-readable", "symlink", "revoked", "disable", "changed-file"} {
		t.Run(kind, func(t *testing.T) {
			h, p, c, intent, r := extensionFormal(t)
			seal := correctionSeal(t, h, r, false)
			path, cfg := runtimeCaptureConfig(t, h, p, intent.TaskID, seal.Hash, false)
			if kind == "no-config" {
				if e := h.ConfigureNativeResponseCapture(""); e != nil || h.nativeCapture.Load() != nil {
					t.Fatal(e)
				}
				return
			}
			switch kind {
			case "foreign-authority":
				cfg.Authority = "foreign"
			case "foreign-task":
				cfg.Tasks = map[string]string{"ta1:foreign": seal.Hash}
			case "stale-thread":
				cfg.Thread = "foreign"
			case "stale-config":
				cfg.Revision++
			case "malformed":
				os.WriteFile(path, []byte(`{"version":1,"version":1}`), 0600)
			case "world-readable":
				os.Chmod(path, 0644)
			case "symlink":
				old := path + "-original"
				os.Rename(path, old)
				os.Symlink(old, path)
			}
			if kind == "foreign-authority" || kind == "foreign-task" || kind == "stale-thread" || kind == "stale-config" {
				b, _ := json.Marshal(cfg)
				os.WriteFile(path, b, 0600)
			}
			e := h.ConfigureNativeResponseCapture(path)
			positive := kind == "revoked" || kind == "disable" || kind == "changed-file"
			if !positive {
				if e == nil || h.nativeCapture.Load() != nil {
					t.Fatal("bad config enabled")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			raw := []byte(`{"operation":"capabilities","correlation_id":"guard"}`)
			op := "capabilities"
			response := h.ExecuteTask(context.Background(), c, []byte(`{"type":"task_api_request","api_version":"1.0.0-rc.1","correlation_id":"guard","operation":"capabilities","input":{}}`))
			response.Operation = &op
			sink := h.nativeCapture.Load().Directory
			if kind == "revoked" {
				c.Validate = func() bool { return false }
			}
			if kind == "disable" {
				h.DisableNativeResponseCapture()
			}
			if kind == "changed-file" {
				os.WriteFile(path, []byte(`{}`), 0600)
			}
			h.CaptureTaskResponse(context.Background(), c, raw, response)
			files, _ := os.ReadDir(sink)
			if len(files) != 0 {
				t.Fatal("invalid capture wrote")
			}
		})
	}
}
func TestRuntimeVerifierRejectsChangedTupleAndPinnedCode(t *testing.T) {
	h, p, c, intent, r := extensionFormal(t)
	seal := correctionSeal(t, h, r, false)
	path, _ := runtimeCaptureConfig(t, h, p, intent.TaskID, seal.Hash, true)
	if e := h.ConfigureNativeResponseCapture(path); e != nil {
		t.Fatal(e)
	}
	config := h.nativeCapture.Load()
	for _, kind := range []string{"task", "seal", "hash", "artifact", "revoked"} {
		input := map[string]any{"task_id": intent.TaskID, "seal_id": seal.ID, "artifact_id": "result.txt", "expected_hash": seal.Hash, "offset": 0}
		switch kind {
		case "task":
			input["task_id"] = "foreign"
		case "seal":
			input["seal_id"] = "foreign"
		case "hash":
			input["expected_hash"] = hex.EncodeToString(make([]byte, sha256.Size))
		case "artifact":
			input["artifact_id"] = "arbitrary"
		case "revoked":
			c.Validate = func() bool { return false }
		}
		raw, _ := json.Marshal(input)
		request := taskapi.Request{Type: "task_api_request", Version: artifact.Version, Operation: "read_artifact_bytes", CorrelationID: "negative", Input: raw}
		if e := h.verifyOriginalArtifact(context.Background(), c, request, config); e == nil {
			t.Fatal("foreign/revoked tuple verified", kind)
		}
	}
	copy := *config.Verifier
	copy.ScriptHash = "wrong"
	if verifierPinned(&copy) {
		t.Fatal("changed script accepted")
	}
}
