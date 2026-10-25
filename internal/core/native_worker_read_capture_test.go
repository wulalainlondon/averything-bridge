package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSelectedWorkerReadCaptureUsesActualNativeResponses(t *testing.T) {
	h, p, parent, intent, root := extensionFormal(t)
	ctx := context.Background()
	inputText := "owned input 中文 🧪 e\u0301\n"
	inputPath := "owned-native-input.txt"
	if err := os.WriteFile(filepath.Join(p.Cwd(), inputPath), []byte(inputText), 0600); err != nil {
		t.Fatal(err)
	}
	seal := correctionSeal(t, h, root, false)
	cfgPath, cfg := runtimeCaptureConfig(t, h, p, intent.TaskID, seal.Hash, false)
	cfg.Version = 3
	cfg.WorkerReadProofs = true
	encoded, _ := json.Marshal(cfg)
	os.WriteFile(cfgPath, encoded, 0600)
	if err := h.ConfigureNativeResponseCapture(cfgPath); err != nil {
		t.Fatal(err)
	}
	started := make(chan backend.TaskCaller, 1)
	f := h.exec.(*extensionExec)
	f.onSend = func(s *session.Session, req, _ string) {
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-capture-tools"})
		started <- backend.TaskCaller{Session: s, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-capture-tools", ProviderVersion: "0.162.1", Validate: func() bool { return true }}
	}
	raw, _ := json.Marshal(r2OwnerRequest(t, "synthetic-capture-worker-round", continuityInput(root, p, h, "native read proof")))
	parent.CallID = "synthetic-capture-worker-round"
	if response := h.ExecuteTask(ctx, parent, raw); !response.OK {
		t.Fatal(response.Error)
	}
	var caller backend.TaskCaller
	select {
	case caller = <-started:
	case <-time.After(time.Second):
		t.Fatal("worker not started")
	}
	invoke := func(op, path, call string) (json.RawMessage, taskapi.Response) {
		t.Helper()
		in := map[string]any{}
		if op == "read_input" {
			in["relative_path"] = path
		}
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "operation": op, "correlation_id": call, "input": in})
		caller.CallID = call
		response := h.ExecuteTask(ctx, caller, raw)
		return raw, response
	}
	for _, test := range []struct {
		op, path, call string
		ok             bool
	}{{"capabilities", "", "synthetic-cap", true}, {"read_input", "../owned-outside-probe.txt", "synthetic-denial", false}, {"read_input", inputPath, "synthetic-input", true}} {
		raw, response := invoke(test.op, test.path, test.call)
		if response.OK != test.ok || !test.ok && response.Error.Code != "permission" {
			t.Fatal("fixture response", response)
		}
		if err := h.CaptureTaskResponse(ctx, caller, raw, response); err != nil {
			t.Fatal(err)
		}
	}
	files, err := os.ReadDir(h.nativeCapture.Load().Directory)
	if err != nil || len(files) != 3 {
		t.Fatal("missing actual worker receipts", len(files), err)
	}
	sum := sha256.Sum256([]byte(inputText))
	wanted := hex.EncodeToString(sum[:])
	seenInput := false
	seenDenial := false
	for _, file := range files {
		data, err := os.ReadFile(filepath.Join(h.nativeCapture.Load().Directory, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), inputText) || strings.Contains(string(data), inputPath) || strings.Contains(string(data), "../owned-outside-probe.txt") {
			t.Fatal("input or path leaked")
		}
		var record map[string]any
		json.Unmarshal(data, &record)
		body := record["public_result"].(map[string]any)
		if record["evidence_kind"] != "original_native_worker_read_public_projection" || record["source_request_id"] != caller.RequestID || record["source_native_thread_id"] != caller.ThreadID || body["task_id"] != intent.TaskID || body["owner_request_id"] != caller.RequestID {
			t.Fatal("borrowed native/task identity", record)
		}
		if body["input_sha256"] == wanted && body["input_bytes"] == float64(len([]byte(inputText))) {
			seenInput = true
		}
		if body["ok"] == false && body["error_code"] == "permission" {
			seenDenial = true
		}
	}
	if !seenInput || !seenDenial {
		t.Fatal("missing input/hash or denial proof")
	}
	// A valid response cannot be reused after caller/config/owner scope changes.
	for _, variant := range []string{"v2", "disabled", "revoked", "wrong-request", "wrong-thread", "unselected", "parent-drift", "context-cancelled", "wrong-response-correlation"} {
		t.Run(variant, func(t *testing.T) {
			raw, response := invoke("capabilities", "", "synthetic-guard-"+variant)
			if !response.OK {
				t.Fatal(response.Error)
			}
			original := h.nativeCapture.Load()
			copyConfig := *original
			local := caller
			checkCtx := ctx
			switch variant {
			case "v2":
				copyConfig.Version = 2
			case "disabled":
				copyConfig.WorkerReadProofs = false
			case "revoked":
				local.Validate = func() bool { return false }
			case "wrong-request":
				local.RequestID = root.ChildRequestID
			case "wrong-thread":
				local.ThreadID = "foreign-native"
			case "unselected":
				copyConfig.Tasks = map[string]string{}
			case "parent-drift":
				copyConfig.SourceThread = "foreign-parent"
			case "context-cancelled":
				var cancel context.CancelFunc
				checkCtx, cancel = context.WithCancel(ctx)
				cancel()
			case "wrong-response-correlation":
				response.CorrelationID = "foreign-call"
			}
			h.nativeCapture.Store(&copyConfig)
			defer h.nativeCapture.Store(original)
			if err := h.CaptureTaskResponse(checkCtx, local, raw, response); err != nil {
				t.Fatal(err)
			}
			after, _ := os.ReadDir(original.Directory)
			if len(after) != len(files) {
				t.Fatal("ineligible caller captured", variant)
			}
		})
	}
}

func TestWorkerReadCaptureOptInCannotEnableOnOldConfig(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			h, p, _, intent, root := extensionFormal(t)
			seal := correctionSeal(t, h, root, false)
			path, cfg := runtimeCaptureConfig(t, h, p, intent.TaskID, seal.Hash, false)
			cfg.Version = version
			data, _ := json.Marshal(cfg)
			var fields map[string]any
			json.Unmarshal(data, &fields)
			fields["worker_read_proofs"] = false // even explicit false must not extend v1/v2
			data, _ = json.Marshal(fields)
			os.WriteFile(path, data, 0600)
			if err := h.ConfigureNativeResponseCapture(path); err == nil || h.nativeCapture.Load() != nil {
				t.Fatal("old config silently accepted new proof field")
			}
		})
	}
}
