package goexec

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"io"
	"os"
	"strings"
	"time"
)

// Only the original thread's provider-indexed file is considered. No latest
// assistant message, task replay or replacement conversation is used.
func (c *Codex) ReadRealtimeVoiceSpeech(ctx context.Context, s *session.Session, voice string, since int64) (backend.RealtimeSpeechEvidence, error) {
	if s.ResumeID() == "" || voice == "" {
		return backend.RealtimeSpeechEvidence{}, errors.New("voice_readback_source_required")
	}
	path, e := c.findCodexSessionFileChecked(s.ResumeID())
	if e != nil {
		return backend.RealtimeSpeechEvidence{}, e
	}
	if path == "" || strings.HasSuffix(path, ".gz") {
		return backend.RealtimeSpeechEvidence{}, errors.New("voice_readback_live_source_unavailable")
	}
	return readVoiceRolloutSince(ctx, path, s.ResumeID(), voice, since)
}
func readVoiceRollout(ctx context.Context, path, thread, voice string) (backend.RealtimeSpeechEvidence, error) {
	return readVoiceRolloutSince(ctx, path, thread, voice, 0)
}
func readVoiceRolloutSince(ctx context.Context, path, thread, voice string, since int64) (backend.RealtimeSpeechEvidence, error) {
	proof := backend.RealtimeSpeechEvidence{}
	before, e := os.Lstat(path)
	if e != nil {
		return proof, e
	}
	if !before.Mode().IsRegular() {
		return proof, errors.New("voice_readback_regular_file_required")
	}
	f, e := os.Open(path)
	if e != nil {
		return proof, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || !os.SameFile(before, info) {
		return proof, errors.New("voice_readback_source_replaced")
	}
	header, e := voiceReadbackHeader(f, thread)
	if e != nil {
		return proof, e
	}
	const window int64 = 8 << 20
	start := info.Size() - window
	if start < int64(header.size) {
		start = int64(header.size)
	}
	reader := bufio.NewReaderSize(io.NewSectionReader(f, start, info.Size()-start), 256<<10)
	pos := start
	if start > int64(header.size) {
		b, e := reader.ReadBytes('\n')
		pos += int64(len(b))
		if e != nil {
			return proof, errors.New("voice_readback_boundary_unavailable")
		}
	}
	pins := []*exactFinalPin{&header}
	started := false
	userBoundary := false
	var text strings.Builder
	for {
		if e := ctx.Err(); e != nil {
			return proof, e
		}
		at := pos
		line, err := reader.ReadSlice('\n')
		pos += int64(len(line))
		if errors.Is(err, bufio.ErrBufferFull) {
			return proof, errors.New("voice_readback_line_exceeds_bound")
		}
		var row struct {
			Timestamp string `json:"timestamp"`
			Type      string `json:"type"`
			Payload   struct {
				Type  string `json:"type"`
				Voice string `json:"realtime_session_id"`
				Role  string `json:"role"`
				Text  string `json:"text"`
			} `json:"payload"`
		}
		if len(line) > 0 && line[len(line)-1] == '\n' && json.Unmarshal(line, &row) == nil && row.Type == "realtime_item" && row.Payload.Voice == voice {
			if !unambiguousPublicJSON(line) {
				return proof, errors.New("voice_readback_ambiguous_record")
			}
			switch row.Payload.Type {
			case "realtime_session_started":
				if started {
					return proof, errors.New("voice_readback_duplicate_generation")
				}
				started = true
				pins = append(pins, pin(at, line))
			case "transcript_segment":
				atTime, parseErr := time.Parse(time.RFC3339Nano, row.Timestamp)
				if since > 0 && (parseErr != nil || atTime.UnixMilli() < since) {
					continue
				}
				if started && !userBoundary && row.Payload.Role == "user" && text.Len() > 0 {
					userBoundary = true
					pins = append(pins, pin(at, line))
				}
				if started && row.Payload.Role == "assistant" && !userBoundary {
					if text.Len()+len(row.Payload.Text) > 24000 || len(pins) >= 128 {
						return proof, errors.New("voice_readback_text_exceeds_bound")
					}
					text.WriteString(row.Payload.Text)
					pins = append(pins, pin(at, line))
				}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return proof, err
			}
			break
		}
	}
	after, e := os.Stat(path)
	if e != nil || !os.SameFile(info, after) {
		return proof, errors.New("voice_readback_source_replaced")
	}
	for _, p := range pins {
		if !checkPin(f, p) {
			return proof, errors.New("voice_readback_evidence_changed")
		}
	}
	if !started || text.Len() == 0 {
		return proof, nil
	}
	return backend.RealtimeSpeechEvidence{ThreadID: thread, VoiceID: voice, Text: text.String()}, nil
}

// Native session metadata includes startup instructions and can exceed the
// transcript line limit. Pin only its public identity, with a separate 1 MiB cap.
func voiceReadbackHeader(f *os.File, thread string) (exactFinalPin, error) {
	const maxHeader = 1 << 20
	b := bufio.NewReaderSize(io.NewSectionReader(f, 0, maxHeader+1), maxHeader+1)
	line, e := b.ReadSlice('\n')
	if e != nil || len(line) > maxHeader {
		return exactFinalPin{}, errors.New("voice_readback_metadata_bound")
	}
	var meta struct {
		Type    string `json:"type"`
		Payload struct {
			ID string `json:"id"`
		} `json:"payload"`
	}
	if !unambiguousPublicJSON(line) || json.Unmarshal(line, &meta) != nil || meta.Type != "session_meta" || meta.Payload.ID != thread {
		return exactFinalPin{}, errors.New("voice_readback_thread_mismatch")
	}
	return *pin(0, line), nil
}
