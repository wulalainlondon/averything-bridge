package nativewatch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

type TurnActivity struct {
	Session NativeSession
	TurnID  string
	Phase   string
}

type turnFile struct {
	session        NativeSession
	size, modified int64
	initialized    bool
	turnID, phase  string
}

// TurnWatcher stats known transcripts only (no recursive scan or persistent
// per-file descriptors). Text, timestamps and mtime never imply execution:
// only explicit native task_started/task_complete/turn_aborted records do.
type TurnWatcher struct {
	mu    sync.Mutex
	files map[string]*turnFile
}

func NewTurnWatcher() *TurnWatcher { return &TurnWatcher{files: map[string]*turnFile{}} }

func (w *TurnWatcher) Track(s NativeSession) {
	if s.Backend != BackendCodex || !strings.HasSuffix(s.Path, ".jsonl") {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.files[s.Path]; !exists {
		w.files[s.Path] = &turnFile{session: s}
	}
}

func (w *TurnWatcher) Run(ctx context.Context, emit func(TurnActivity)) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.poll(emit)
		}
	}
}

func (w *TurnWatcher) poll(emit func(TurnActivity)) {
	w.mu.Lock()
	files := make([]*turnFile, 0, len(w.files))
	for _, f := range w.files {
		files = append(files, f)
	}
	w.mu.Unlock()
	for _, f := range files {
		info, err := os.Stat(f.session.Path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		if f.initialized && f.size == info.Size() && f.modified == info.ModTime().UnixNano() {
			continue
		}
		initial := !f.initialized
		f.initialized, f.size, f.modified = true, info.Size(), info.ModTime().UnixNano()
		// Inactive old files enter the stat cache without reading their history.
		if initial && time.Since(info.ModTime()) > 24*time.Hour {
			continue
		}
		turnID, phase := latestNativeTurn(f.session.Path, info.Size())
		if turnID == "" || (turnID == f.turnID && phase == f.phase) {
			continue
		}
		f.turnID, f.phase = turnID, phase
		// A short turn may start and finish between polls. Forward its committed
		// terminal even when the start was missed; the executor checks ownership
		// and the exact turn ID. Never replay old completions on cold startup.
		if phase == "running" || !initial {
			emit(TurnActivity{Session: f.session, TurnID: turnID, Phase: phase})
		}
	}
}

func latestNativeTurn(path string, size int64) (string, string) {
	f, err := os.Open(path)
	if err != nil {
		return "", ""
	}
	defer f.Close()
	// Usually the latest lifecycle is in the last 64 KiB. Widen only when a
	// long tool result separates it from the tail, with a finite 32 MiB cap.
	for window := int64(64 * 1024); ; window = min(window*4, 32*1024*1024) {
		start := max(int64(0), size-window)
		r := bufio.NewReaderSize(io.NewSectionReader(f, start, size-start), 64*1024)
		skip := start > 0
		turnID, phase := "", ""
		for {
			line, readErr := r.ReadSlice('\n')
			if readErr == bufio.ErrBufferFull {
				skip = true
				continue
			}
			if readErr != nil {
				break
			} // Partial final records are not committed.
			if skip {
				skip = false
				continue
			}
			if !bytes.Contains(line, []byte(`"task_started"`)) && !bytes.Contains(line, []byte(`"task_complete"`)) && !bytes.Contains(line, []byte(`"turn_aborted"`)) {
				continue
			}
			var row struct {
				Type    string `json:"type"`
				Payload struct {
					Type   string          `json:"type"`
					TurnID string          `json:"turn_id"`
					Error  json.RawMessage `json:"error"`
				} `json:"payload"`
			}
			if json.Unmarshal(line, &row) != nil || row.Type != "event_msg" || row.Payload.TurnID == "" {
				continue
			}
			switch row.Payload.Type {
			case "task_started":
				turnID, phase = row.Payload.TurnID, "running"
			case "task_complete":
				turnID, phase = row.Payload.TurnID, "completed"
				if len(row.Payload.Error) > 0 && string(row.Payload.Error) != "null" {
					phase = "failed"
				}
			case "turn_aborted":
				turnID, phase = row.Payload.TurnID, "interrupted"
			}
		}
		if turnID != "" || start == 0 || window >= 32*1024*1024 {
			return turnID, phase
		}
	}
}
