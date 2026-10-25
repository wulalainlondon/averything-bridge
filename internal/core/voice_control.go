package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/voicecontrol"
	"everything-go/internal/voicetransfer"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type voiceControlResolver interface {
	ResolveVoiceControlCaller(string, string, string) (backend.SessionControlCaller, error)
}

func (h *Hub) StartVoiceControl(resolver voiceControlResolver) (func(), error) {
	if resolver == nil {
		return nil, errors.New("voice_control_configuration_invalid")
	}
	home, e := os.UserHomeDir()
	if e != nil {
		return nil, e
	}
	launcher := filepath.Join(home, ".local", "bin", "bridge-voice")
	server, e := voicecontrol.OpenServer(h.cfg.DataDir, h.cfg.InstanceID, voicecontrol.DefaultManifest(), func(ctx context.Context, q voicecontrol.Request) (voicecontrol.Result, error) {
		return h.executeVoiceControl(ctx, resolver, q)
	})
	if e != nil {
		return nil, e
	}
	binary, e := os.Executable()
	if e != nil {
		server.Close()
		return nil, e
	}
	const marker = "# Everything Go managed voice entry v1\n"
	if info, err := os.Lstat(launcher); err == nil {
		old, read := os.ReadFile(launcher)
		if !info.Mode().IsRegular() || read != nil || !strings.HasPrefix(string(old), "#!/bin/sh\n"+marker) {
			server.Close()
			return nil, errors.New("voice_control_launcher_conflict")
		}
	} else if !os.IsNotExist(err) {
		server.Close()
		return nil, err
	}
	if e = os.MkdirAll(filepath.Dir(launcher), 0700); e != nil {
		server.Close()
		return nil, e
	}
	temp, e := os.CreateTemp(filepath.Dir(launcher), ".bridge-voice-*")
	if e != nil {
		server.Close()
		return nil, e
	}
	name := temp.Name()
	defer os.Remove(name)
	script := "#!/bin/sh\n" + marker + "exec " + shellArgument(binary) + " voice-control \"$@\"\n"
	e = temp.Chmod(0700)
	if e == nil {
		_, e = temp.WriteString(script)
	}
	if e == nil {
		e = temp.Sync()
	}
	temp.Close()
	if e == nil {
		e = os.Rename(name, launcher)
	}
	if e != nil {
		server.Close()
		return nil, e
	}
	return func() { server.Close() }, nil
}
func (h *Hub) executeVoiceControl(ctx context.Context, resolver voiceControlResolver, q voicecontrol.Request) (voicecontrol.Result, error) {
	var caller backend.SessionControlCaller
	var err error
	deadline, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		caller, err = resolver.ResolveVoiceControlCaller(q.Thread, q.ID, q.Fingerprint())
		if err == nil {
			break
		}
		select {
		case <-deadline.Done():
			return voicecontrol.Result{}, errors.New("voice_control_origin_unavailable")
		case <-ticker.C:
		}
	}
	if caller.Parent == nil || caller.Parent.ResumeID() != q.Thread || caller.VoiceID == "" {
		return voicecontrol.Result{}, errors.New("voice_control_live_caller_required")
	}
	if _, err = h.transferClient(caller); err != nil {
		return voicecontrol.Result{}, err
	}
	var input backend.SessionControlRequest
	decoder := json.NewDecoder(bytes.NewReader(q.Params))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.Action != "" || input.Content != "" || input.Mode != "" {
		return voicecontrol.Result{}, errors.New("voice_control_arguments_invalid")
	}
	input.Action = q.Method
	if q.Method == "voice_transfer" {
		if h.transfers == nil {
			return voicecontrol.Result{}, errors.New("voice_transfer_unavailable")
		}
		// CLI intent identity survives uncertain shell replies. Re-observation can only
		// return the original record; another tool call cannot recreate an effect.
		id := voicetransfer.ID(h.cfg.InstanceID, q.Thread, q.ID)
		c, _ := h.transferClient(caller)
		if old, e := h.transfers.Get(ctx, id, h.pairedTaskDevice(c)); e == nil {
			if old.Source.Session != caller.Parent.ID || old.SourceVoice != caller.VoiceID || old.Target.Session != input.SessionID || old.Target.Thread != input.ExpectedThreadID || old.Target.Revision != input.ExpectedConfigRevision {
				return voicecontrol.Result{}, errors.New("voice_transfer_intent_conflict")
			}
			return voicecontrol.Result{OK: true, Result: old}, nil
		}
		result, e := h.requestVoiceTransferWithID(ctx, caller, input, id)
		return voicecontrol.Result{OK: e == nil, Result: result}, e
	}
	result, e := h.ControlSession(ctx, caller, input)
	return voicecontrol.Result{OK: e == nil, Result: result}, e
}
func voiceControlInstructions() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return fmt.Sprintf(`
User-requested voice transfer on this original Android call is installed at %s through the existing exec_command tool, including older native threads. Never rely on a late dynamic-tool update. Use one literal command: --id vtr_UNIQUE_QUERY --method list_sessions --params '{"query":"NAME","limit":50}'. Page the authorized catalog; names are not routing keys and ambiguous names require human choice. For an explicitly requested target use --id vtr_UNIQUE_INTENT --method voice_transfer --params '{"session_id":"EXACT_CATALOG_ID","expected_thread_id":"EXACT_THREAD","expected_config_revision":REVISION}'. No caller/thread/device override. An explicit user voice request authorizes direct transfer on capable Android clients without a second tap. The device must release A before starting B and confirm B actually connected. Older clients may still require a tap. No text dispatch. Existing controller grants remain required; never grant yourself access. Record transfer_id; on uncertainty query --id vtr_UNIQUE_QUERY --method get_voice_transfer --params '{"dispatch_id":"ORIGINAL_TRANSFER_ID"}' without repeating the effect. Before a transfer_id was received the exact original --id may be observed again only with identical arguments, never a new ID. Report actual state; acceptance or confirmation_required is not connected. Unknown/recovered operations are observation-only, never automatic redial. Do not promise seamless audio.
`, shellArgument(filepath.Join(home, ".local/bin/bridge-voice")))
}
