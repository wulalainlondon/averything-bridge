package core

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	artifact "everything-go/contracts/task-api/artifacts-v3"
	artifact4 "everything-go/contracts/task-api/artifacts-v4"
	"everything-go/internal/backend"
	"everything-go/internal/taskapi"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func verifierPinned(v *NativeArtifactVerifier) bool {
	for _, pin := range []struct{ path, hash string }{{v.Node, v.NodeHash}, {v.Script, v.ScriptHash}, {filepath.Join(filepath.Dir(v.Script), "artifact-v3.mjs"), v.SDKHash}} {
		if !filepath.IsAbs(pin.path) || len(pin.hash) != 64 || captureHash(pin.path) != pin.hash {
			return false
		}
	}
	if v.SDK4Hash != "" && captureHash(filepath.Join(filepath.Dir(v.Script), "artifact-v4.mjs")) != v.SDK4Hash {
		return false
	}
	return true
}

// Subreads use the SAME actual native invocation in the original response
// owner. The SDK child receives public JSON only, no lease/auth/session object.
func (h *Hub) verifyOriginalArtifact(ctx context.Context, c backend.TaskCaller, initial taskapi.Request, p *NativeResponseCapture) error {
	fail := errors.New("native_artifact_verifier_failed")
	if !captureCallerCurrent(p, c) || !h.captureConfigCurrent(p) || !verifierPinned(p.Verifier) {
		return fail
	}
	tools, e := h.TaskTools(c.Session)
	if e != nil {
		return fail
	}
	registered := map[string]bool{}
	for _, t := range tools {
		if name, ok := t["name"].(string); ok {
			registered[name] = true
		}
	}
	if !registered["task_artifact_capabilities"] || !registered["task_read_artifact_bytes"] {
		return fail
	}
	var tuple struct {
		Task         string `json:"task_id"`
		Seal         string `json:"seal_id"`
		Artifact     string `json:"artifact_id"`
		Hash         string `json:"expected_hash"`
		OwnerRequest string `json:"owner_request_id"`
		Offset       int    `json:"offset"`
	}
	if json.Unmarshal(initial.Input, &tuple) != nil || tuple.Offset != 0 || !p.selectedHash(tuple.Task, tuple.Hash) || tuple.Seal != "seal_"+tuple.Hash || tuple.Artifact != "result.txt" {
		return fail
	}
	validate := artifact.Validate
	v4 := initial.Version == artifact4.Version
	if initial.Version != artifact.Version && !v4 {
		return fail
	}
	if v4 {
		validate = artifact4.Validate
		if tuple.OwnerRequest == "" || len(p.Verifier.SDK4Hash) != 64 {
			return fail
		}
	}
	runctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(runctx, p.Verifier.Node, p.Verifier.Script)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=en_US.UTF-8"}
	cmd.Stderr = io.Discard
	stdin, e := cmd.StdinPipe()
	if e != nil {
		return fail
	}
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		return fail
	}
	if cmd.Start() != nil {
		return fail
	}
	defer func() {
		stdin.Close()
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	enc := json.NewEncoder(stdin)
	input := map[string]any{"task_id": tuple.Task, "seal_id": tuple.Seal, "artifact_id": tuple.Artifact, "expected_hash": tuple.Hash, "offset": 0, "limit": 32768}
	if v4 {
		input["owner_request_id"] = tuple.OwnerRequest
	}
	if enc.Encode(map[string]any{"kind": "verify", "version": 1, "api_version": initial.Version, "input": input}) != nil {
		return fail
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), 128*1024)
	reads := 0
	for scanner.Scan() {
		if ctx.Err() != nil || !captureCallerCurrent(p, c) || !h.captureConfigCurrent(p) {
			return fail
		}
		var frame struct {
			Version      int             `json:"version"`
			Kind         string          `json:"kind"`
			Request      json.RawMessage `json:"request"`
			OwnerRequest string          `json:"owner_request_id"`
			Hash         string          `json:"full_hash"`
			Bytes        int             `json:"bytes"`
		}
		if json.Unmarshal(scanner.Bytes(), &frame) != nil || frame.Version != 1 {
			return fail
		}
		if frame.Kind == "verified" {
			if (v4 && frame.OwnerRequest != tuple.OwnerRequest) || frame.Hash != tuple.Hash || frame.Bytes < 0 || frame.Bytes > 8*1024*1024 || reads < 2 {
				return fail
			}
			stdin.Close()
			if cmd.Wait() != nil || !captureCallerCurrent(p, c) || !h.captureConfigCurrent(p) {
				return fail
			}
			proof := map[string]any{"evidence_kind": "original_response_owner_artifact_sdk_verifier", "raw_wire": false, "source_session_id": c.Session.ID, "source_request_id": c.RequestID, "native_call_id": c.CallID, "correlation_id": initial.CorrelationID, "task_id": tuple.Task, "seal_id": tuple.Seal, "artifact_id": tuple.Artifact, "full_hash": frame.Hash, "bytes": frame.Bytes, "owner_subreads": reads, "subreads_are_separate_provider_native_calls": false}
			if v4 {
				proof["owner_request_id"] = tuple.OwnerRequest
				proof["api_version"] = initial.Version
			}
			b, _ := json.Marshal(proof)
			key := sha256.Sum256([]byte(c.RequestID + "\x00" + c.CallID + "\x00" + initial.CorrelationID))
			f, e := os.OpenFile(filepath.Join(p.Directory, hex.EncodeToString(key[:])+"-verified.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if e != nil {
				return e
			}
			defer f.Close()
			_, e = f.Write(append(b, '\n'))
			return e
		}
		if frame.Kind != "original-owner-read" || reads >= 1024 || validate(frame.Request, "Request") != nil {
			return fail
		}
		var req taskapi.Request
		if json.Unmarshal(frame.Request, &req) != nil || req.Version != initial.Version {
			return fail
		}
		if req.Operation != "artifact_capabilities" && req.Operation != "read_artifact_bytes" {
			return fail
		}
		if req.Operation == "read_artifact_bytes" {
			var t struct {
				Task         string `json:"task_id"`
				Seal         string `json:"seal_id"`
				Artifact     string `json:"artifact_id"`
				Hash         string `json:"expected_hash"`
				OwnerRequest string `json:"owner_request_id"`
			}
			if json.Unmarshal(req.Input, &t) != nil || t.Task != tuple.Task || t.Seal != tuple.Seal || t.Artifact != tuple.Artifact || t.Hash != tuple.Hash || v4 && t.OwnerRequest != tuple.OwnerRequest {
				return fail
			}
		}
		response := h.ExecuteTask(runctx, c, frame.Request)
		if !response.OK {
			return fail
		}
		if e := h.capturePublicResponse(runctx, c, frame.Request, response, false); e != nil {
			return fail
		}
		raw, e := json.Marshal(response)
		if e != nil || enc.Encode(map[string]any{"kind": "original-owner-response", "version": 1, "raw": string(raw)}) != nil {
			return fail
		}
		reads++
	}
	return fail
}
