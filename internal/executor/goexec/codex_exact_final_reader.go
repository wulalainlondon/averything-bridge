package goexec

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

// These are read projections, not a provider/business index or durable ledger.
const exactFinalWindow int64 = 64 << 20
const exactFinalStep int64 = 8 << 20
const exactFinalLine = 256 << 10
const exactFinalText = 64 << 10
const exactFinalCache = 32

type exactFinalError string

func (e exactFinalError) Error() string            { return string(e) }
func (e exactFinalError) QueueFinalReason() string { return string(e) }

type exactFinalPin struct {
	at   int64
	size int
	sum  [32]byte
}
type exactFinalScan struct {
	info                   os.FileInfo
	root, path             string
	pos, lineAt            int64
	prefix                 []byte
	metadata               boundedJSONMetadata
	oversized, cut         bool
	turn, text             string
	start, final, complete *exactFinalPin
	terminal               bool
	conflict               bool
	header                 exactFinalPin
	boundary               *exactFinalPin
	touched                uint64
	bytesRead              int64
}

func pin(at int64, b []byte) *exactFinalPin {
	return &exactFinalPin{at: at, size: len(b), sum: sha256.Sum256(b)}
}
func checkPin(f *os.File, p *exactFinalPin) bool {
	if p == nil {
		return true
	}
	b := make([]byte, p.size)
	n, e := f.ReadAt(b, p.at)
	return n == len(b) && (e == nil || errors.Is(e, io.EOF)) && sha256.Sum256(b) == p.sum
}
func fileRevision(st os.FileInfo) string {
	v := reflect.ValueOf(st.Sys())
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	for _, name := range []string{"Ctimespec", "Ctim"} {
		f := v.FieldByName(name)
		if f.IsValid() && f.CanInterface() {
			return fmt.Sprint(f.Interface())
		}
	}
	return ""
}
func sameStamp(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime() == b.ModTime() && fileRevision(a) != "" && fileRevision(a) == fileRevision(b)
}

func (c *Codex) mappedExactTurn(thread, request, turn string) error {
	c.historyRequestMu.Lock()
	r, e := c.loadTurnRequests(thread)
	c.historyRequestMu.Unlock()
	if e != nil {
		return exactFinalError("final_mapping_unavailable")
	}
	found := ""
	for native, id := range r.Requests {
		if id == request {
			if found != "" && native != found {
				return exactFinalError("final_mapping_ambiguous")
			}
			found = native
		}
	}
	if found == "" {
		return exactFinalError("final_mapping_missing")
	}
	if found != turn {
		return exactFinalError("final_mapping_mismatch")
	}
	return nil
}
func (c *Codex) exactIndexedPath(thread string) (string, string, error) {
	c.rolloutMu.Lock()
	defer c.rolloutMu.Unlock()
	if c.rolloutRoot != c.sessionsRoot {
		return "", "", exactFinalError("final_scope_changed")
	}
	path := c.rolloutByID[thread]
	if path == "" {
		return "", "", exactFinalError("final_index_unavailable")
	}
	return c.sessionsRoot, path, nil
}
func openExactIndexed(root, path, thread string) (*os.Root, *os.File, string, os.FileInfo, error) {
	fail := func(code string) (*os.Root, *os.File, string, os.FileInfo, error) {
		return nil, nil, "", nil, exactFinalError(code)
	}
	if strings.HasSuffix(path, ".gz") {
		return fail("final_compressed_unsupported")
	}
	rel, e := filepath.Rel(root, path)
	if e != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) || codexRolloutUID(filepath.Base(path)) != thread {
		return fail("final_source_scope_mismatch")
	}
	realRoot, e := filepath.EvalSymlinks(root)
	if e != nil {
		return fail("final_source_unavailable")
	}
	confined, e := os.OpenRoot(realRoot)
	if e != nil {
		return fail("final_source_unavailable")
	}
	rootInfo, e := confined.Stat(".")
	if e != nil {
		confined.Close()
		return fail("final_source_unavailable")
	}
	l, e := confined.Lstat(rel)
	if e != nil || !l.Mode().IsRegular() {
		confined.Close()
		return fail("final_source_scope_mismatch")
	}
	f, e := openExactRegular(confined, rel)
	if e != nil {
		confined.Close()
		var classified exactFinalError
		if errors.As(e, &classified) {
			return nil, nil, "", nil, classified
		}
		return fail("final_source_scope_mismatch")
	}
	st, e := f.Stat()
	if e != nil || !os.SameFile(st, l) {
		f.Close()
		confined.Close()
		return fail("final_source_changed")
	}
	return confined, f, realRoot, rootInfo, nil
}

func exactHeader(f *os.File, thread string) (exactFinalPin, error) {
	// One bounded public session_meta record; no transcript discovery or body dump.
	b := bufio.NewReaderSize(io.NewSectionReader(f, 0, exactFinalLine+1), exactFinalLine+1)
	line, e := b.ReadSlice('\n')
	if e != nil || len(line) > exactFinalLine {
		return exactFinalPin{}, exactFinalError("final_metadata_bound")
	}
	var meta struct {
		Type    string `json:"type"`
		Payload struct {
			ID string `json:"id"`
		} `json:"payload"`
	}
	if !unambiguousPublicJSON(line) || json.Unmarshal(line, &meta) != nil || meta.Type != "session_meta" || meta.Payload.ID != thread {
		return exactFinalPin{}, exactFinalError("final_thread_mismatch")
	}
	return *pin(0, line), nil
}

// Only classify public protocol headers from a bounded prefix. Oversized body
// bytes (including reasoning/tool output) are discarded, never decoded/exported.
func (st *exactFinalScan) record(line []byte, at int64, target string) {
	if st.oversized {
		row, kind, native, phase, known := st.metadata.publicHeader()
		control := row == "turn_context" || row == "event_msg" && (kind == "task_started" || kind == "task_complete" || kind == "turn_aborted")
		if known && control && native == target {
			st.conflict = true
			st.terminal = false
			return
		}
		if known && control && native != "" && native != target {
			// Even a discarded foreign control is a turn boundary. Its full raw
			// bytes cannot be pinned within the line budget, so relinquish the
			// active association rather than let a later final inherit target.
			// Already closed target start/final/complete pins remain historical
			// proof; unfinished target cannot complete across this boundary.
			st.turn = ""
			st.boundary = nil
			return
		}
		safe := known && (row == "compacted" || row == "response_item" && (kind != "message" || phase != "final_answer" || st.terminal && st.turn != target) || row == "event_msg" && (kind == "item_completed" || kind == "token_count" || kind == "agent_message" || kind == "agent_reasoning" || kind == "agent_reasoning_raw_content" || kind == "user_message"))
		if !safe {
			st.turn = ""
			st.text = ""
			st.start = nil
			st.final = nil
			st.complete = nil
			st.terminal = false
			st.conflict = true
		}
		return
	}

	var row struct {
		Type    string `json:"type"`
		Payload struct {
			Type, Role, Phase string
			TurnID            string          `json:"turn_id"`
			Content           json.RawMessage `json:"content"`
			Last              string          `json:"last_agent_message"`
		} `json:"payload"`
	}
	if !unambiguousPublicJSON(line) || json.Unmarshal(line, &row) != nil {
		if st.terminal {
			st.conflict = true
			st.terminal = false
			return
		}
		if st.terminal {
			return
		}
		st.turn = ""
		st.text = ""
		st.final = nil
		st.terminal = false
		return
	}
	e := row.Payload
	if row.Type == "turn_context" || row.Type == "event_msg" && e.Type == "task_started" {
		st.turn = e.TurnID
		st.boundary = pin(at, line)
		if st.turn == target {
			if st.terminal {
				st.conflict = true
			}
			if st.start == nil && !st.terminal {
				st.conflict = false
			}
			st.start = st.boundary
			st.final = nil
			st.complete = nil
			st.text = ""
			st.terminal = false
		}
		return
	}
	if row.Type == "event_msg" && (e.Type == "task_complete" || e.Type == "turn_aborted") {
		if e.TurnID == target {
			if e.Type == "turn_aborted" || st.turn != target || st.final == nil || st.text == "" || e.Last != "" && strings.TrimSpace(e.Last) != st.text {
				st.conflict = true
				st.terminal = false
			} else {
				st.complete = pin(at, line)
				st.terminal = true
			}
		}
		st.turn = ""
		st.boundary = pin(at, line)
		return
	}
	if row.Type == "response_item" && e.Type == "message" && e.Role == "assistant" && e.Phase == "final_answer" && st.turn == target {
		text := extractCodexText(e.Content)
		if len(text) > exactFinalText || len(text) == 0 {
			st.conflict = true
			return
		}
		if st.final != nil && st.text != text {
			st.conflict = true
			return
		}
		st.text = text
		st.final = pin(at, line)
	}
}

// The trusted caller supplies the canonical immutable payload hash for cache
// isolation. The hash never reconstructs a native mapping or grants authority.
func (c *Codex) ExactQueueFinalForReceipt(ctx context.Context, thread, request, turn, payloadHash string) (bool, error) {
	if ctx.Err() != nil {
		return false, exactFinalError("final_scan_pending")
	}
	if thread == "" || request == "" || turn == "" {
		return false, exactFinalError("final_mapping_missing")
	}
	if payloadHash != "" {
		if b, e := hex.DecodeString(payloadHash); e != nil || len(b) != 32 {
			return false, exactFinalError("final_payload_identity_invalid")
		}
	}
	if err := c.mappedExactTurn(thread, request, turn); err != nil {
		return false, err
	}
	root, path, err := c.exactIndexedPath(thread)
	if err != nil {
		return false, err
	}
	confined, f, realRoot, rootInfo, err := openExactIndexed(root, path, thread)
	if err != nil {
		return false, err
	}
	defer f.Close()
	defer confined.Close()
	info, err := f.Stat()
	if err != nil {
		return false, exactFinalError("final_source_unavailable")
	}
	if fileRevision(info) == "" {
		return false, exactFinalError("final_revision_unsupported")
	}
	header, err := exactHeader(f, thread)
	if err != nil {
		return false, err
	}
	if ctx.Err() != nil {
		return false, exactFinalError("final_scan_pending")
	}
	if !c.exactFinalMu.TryLock() {
		return false, exactFinalError("final_scan_busy")
	}
	defer c.exactFinalMu.Unlock()
	key := root + "\x00" + thread + "\x00" + request + "\x00" + turn + "\x00" + payloadHash
	if c.exactFinalScans == nil {
		c.exactFinalScans = map[string]*exactFinalScan{}
	}
	st := c.exactFinalScans[key]
	if st != nil && (!os.SameFile(st.info, info) || st.root != realRoot || st.path != path || info.Size() < st.info.Size() || st.header.sum != header.sum || !checkPin(f, st.boundary) || !checkPin(f, st.start) || !checkPin(f, st.final) || !checkPin(f, st.complete) || !sameStamp(st.info, info)) {
		delete(c.exactFinalScans, key)
		return false, exactFinalError("final_source_changed")
	}
	if st == nil {
		if len(c.exactFinalScans) >= exactFinalCache {
			old := ""
			age := ^uint64(0)
			for k, v := range c.exactFinalScans {
				if v.touched < age {
					old, age = k, v.touched
				}
			}
			delete(c.exactFinalScans, old)
		}
		start := info.Size() - exactFinalWindow
		if start < 0 {
			start = 0
		}
		st = &exactFinalScan{info: info, root: realRoot, path: path, pos: start, lineAt: start, cut: start > 0, header: header}
		c.exactFinalScans[key] = st
	}
	c.exactFinalClock++
	st.touched = c.exactFinalClock
	// A call consumes <=8MiB in <=64KiB read chunks and carries only bounded
	// line/pins/final. Following polls resume, never rescan a gigabyte head.
	remaining := info.Size() - st.pos
	if remaining > exactFinalStep {
		remaining = exactFinalStep
	}
	if remaining > exactFinalWindow-st.bytesRead {
		remaining = exactFinalWindow - st.bytesRead
	}
	if remaining < 0 {
		remaining = 0
	}
	reader := bufio.NewReaderSize(io.NewSectionReader(f, st.pos, remaining), 64<<10)
	used := int64(0)
	deadline := time.Now().Add(100 * time.Millisecond)
	for st.pos < info.Size() && used < exactFinalStep && ctx.Err() == nil && time.Now().Before(deadline) {
		chunk, e := reader.ReadSlice('\n')
		st.pos += int64(len(chunk))
		used += int64(len(chunk))
		st.bytesRead += int64(len(chunk))
		if !st.cut {
			st.metadata.add(chunk)
			if !st.oversized && len(st.prefix)+len(chunk) <= exactFinalLine {
				st.prefix = append(st.prefix, chunk...)
			} else {
				st.oversized = true
				if len(st.prefix) > 8192 {
					st.prefix = st.prefix[:8192]
				}
				if len(st.prefix) < 8192 {
					take := 8192 - len(st.prefix)
					if take > len(chunk) {
						take = len(chunk)
					}
					st.prefix = append(st.prefix, chunk[:take]...)
				}
			}
		}
		if len(chunk) > 0 && chunk[len(chunk)-1] == '\n' {
			if !st.cut {
				st.record(st.prefix, st.lineAt, turn)
			}
			st.cut = false
			st.prefix = nil
			st.metadata = boundedJSONMetadata{}
			st.oversized = false
			st.lineAt = st.pos
		}
		if e != nil && !errors.Is(e, bufio.ErrBufferFull) && !errors.Is(e, io.EOF) {
			delete(c.exactFinalScans, key)
			return false, exactFinalError("final_source_unavailable")
		}
		if errors.Is(e, io.EOF) {
			break
		}
	}
	after, e := f.Stat()
	resolved, e2 := filepath.EvalSymlinks(root)
	rel, _ := filepath.Rel(root, path)
	current, e3 := confined.Lstat(rel)
	currentRoot, e4 := os.Stat(realRoot)
	reopened, e5 := openExactRegular(confined, rel)
	sameCurrent := false
	if e5 == nil {
		present, e := reopened.Stat()
		sameCurrent = e == nil && os.SameFile(present, info)
		reopened.Close()
	}
	newRoot, newPath, indexErr := c.exactIndexedPath(thread)
	if e != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || !sameCurrent || !os.SameFile(currentRoot, rootInfo) || !sameStamp(info, after) || !os.SameFile(current, info) || resolved != realRoot || indexErr != nil || newRoot != root || newPath != path || !checkPin(f, &header) || !checkPin(f, st.start) || !checkPin(f, st.final) || !checkPin(f, st.complete) {
		delete(c.exactFinalScans, key)
		return false, exactFinalError("final_source_changed")
	}
	if e = c.mappedExactTurn(thread, request, turn); e != nil {
		delete(c.exactFinalScans, key)
		return false, e
	}
	st.info = after
	if st.bytesRead >= exactFinalWindow && st.pos < info.Size() {
		return false, exactFinalError("final_window_exhausted")
	}
	if st.pos < info.Size() || len(st.prefix) > 0 || st.cut {
		return false, exactFinalError("final_scan_pending")
	}
	if st.conflict {
		return false, exactFinalError("final_boundary_conflict")
	}
	if st.terminal && st.start != nil && st.final != nil && st.complete != nil {
		return true, nil
	}
	if st.start == nil && info.Size() > exactFinalWindow {
		return false, exactFinalError("final_outside_window")
	}
	if st.final != nil {
		return false, exactFinalError("final_terminal_missing")
	}
	return false, nil
}
