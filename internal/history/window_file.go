package history

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"syscall"
)

// Sparse offsets and native control markers contain no conversation body.
// They make an old physical-line cursor seekable without parsing the whole log.
type linePoint struct {
	line   int
	offset int64
}
type fileWindowIndex struct {
	size, mtime          int64
	identity, generation string
	ended                bool
	lines                int
	points               []linePoint
	markers              []TailLine
	groups               []linePoint
	pendingLine          int
	pendingOffset        int64
}

var windowFiles = struct {
	sync.Mutex
	values map[string]*fileWindowIndex
}{values: map[string]*fileWindowIndex{}}

type FileWindow struct {
	Lines                 []TailLine
	MoreBefore, MoreAfter bool
	Generation            string
	StartLine, EndLine    int
}

func indexHistoryFile(f *os.File, path string, info os.FileInfo) (*fileWindowIndex, error) {
	head := make([]byte, 8192)
	n, err := f.ReadAt(head, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	head = head[:n]
	if at := bytes.IndexByte(head, '\n'); at >= 0 {
		head = head[:at]
	}
	sum := sha256.Sum256(head)
	identity := hex.EncodeToString(sum[:])
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		identity = fmt.Sprintf("%d:%d:%s", stat.Dev, stat.Ino, identity)
	}
	windowFiles.Lock()
	defer windowFiles.Unlock()
	prior := windowFiles.values[path]
	if prior != nil && prior.identity == identity && prior.size == info.Size() && prior.mtime == info.ModTime().UnixNano() {
		return prior, nil
	}
	generation, e := retainedFileGeneration(f, path, identity, info)
	if e != nil {
		return nil, e
	}
	idx := &fileWindowIndex{identity: identity, generation: generation, points: []linePoint{{1, 0}}}
	start := int64(0)
	line := 1
	if prior != nil && prior.identity == identity && prior.generation == generation && info.Size() > prior.size && prior.ended {
		*idx = *prior
		idx.points = append([]linePoint(nil), prior.points...)
		idx.markers = append([]TailLine(nil), prior.markers...)
		idx.groups = append([]linePoint(nil), prior.groups...)
		start = prior.size
		line = prior.lines + 1

	}
	buf := make([]byte, 1<<20)
	prefix := make([]byte, 0, 8192)
	oversized := false
	lineStart := start
	marker := func() {
		if len(prefix) == 0 {
			return
		}
		row, complete := nativeWindowHeader(prefix)
		if row.Type == "response_item" {
			payload := row.Payload
			if payload.Type == "message" {
				if payload.Role == "assistant" && (payload.Phase == "commentary" || !complete && payload.Phase == "") {
					if idx.pendingLine == 0 {
						idx.pendingLine = line
						idx.pendingOffset = lineStart
					}
					idx.groups = append(idx.groups, linePoint{line, idx.pendingOffset})
				} else if payload.Role == "user" {
					idx.groups = append(idx.groups, linePoint{line, lineStart})
					// A filtered bootstrap user must not erase pending commentary.
				} else if payload.Role == "assistant" {
					offset := lineStart
					if idx.pendingLine != 0 {
						offset = idx.pendingOffset
					}
					idx.groups = append(idx.groups, linePoint{line, offset})
					idx.pendingLine = 0
				}
			} else if payload.Type == "function_call" || payload.Type == "custom_tool_call" {
				offset := lineStart
				if idx.pendingLine != 0 {
					offset = idx.pendingOffset
				}
				idx.groups = append(idx.groups, linePoint{line, offset})
				if payload.Name != "update_plan" && (complete || payload.Name != "") {
					idx.pendingLine = 0
				}
			}
		}
		if !complete || oversized {
			return
		}
		if row.Type != "turn_context" && (row.Type != "event_msg" || (row.Payload.Type != "task_started" && row.Payload.Type != "task_complete" && row.Payload.Type != "turn_aborted")) {
			return
		}
		data, _ := json.Marshal(row)
		idx.markers = append(idx.markers, TailLine{LineNo: line, Data: data})
		idx.pendingLine = 0
	}

	for offset := start; offset < info.Size(); {
		want := int64(len(buf))
		if info.Size()-offset < want {
			want = info.Size() - offset
		}
		n, e := f.ReadAt(buf[:want], offset)
		if e != nil && e != io.EOF {
			return nil, e
		}
		if n == 0 {
			break
		}
		part := buf[:n]
		base := offset
		for len(part) > 0 {
			at := bytes.IndexByte(part, '\n')
			take := len(part)
			if at >= 0 {
				take = at
			}
			if !oversized {
				if len(prefix)+take <= 65536 {
					prefix = append(prefix, part[:take]...)
				} else {
					room := 65536 - len(prefix)
					if room > 0 {
						prefix = append(prefix, part[:room]...)
					}
					oversized = true
				}
			}
			if at < 0 {
				break
			}
			marker()
			line++
			lineStart = base + int64(at) + 1
			if (line-1)%256 == 0 {
				idx.points = append(idx.points, linePoint{line, lineStart})
			}
			prefix = prefix[:0]
			oversized = false
			part = part[at+1:]
			base = lineStart
		}
		offset += int64(n)
	}
	if len(prefix) > 0 {
		marker()
	}
	idx.size = info.Size()
	idx.mtime = info.ModTime().UnixNano()
	idx.ended = idx.size == 0 || lineStart == idx.size
	idx.lines = line - 1
	if !idx.ended {
		idx.lines++
	}
	// Do not cache an unterminated marker; it is only authoritative when complete.
	if len(windowFiles.values) >= 64 {
		windowFiles.values = map[string]*fileWindowIndex{}
	}
	windowFiles.values[path] = idx
	return idx, nil
}
func windowOffsetForLine(f *os.File, idx *fileWindowIndex, target int) (int64, error) {
	if target < 1 || target > idx.lines+1 {
		return 0, fmt.Errorf("history cursor line out of range")
	}
	if target == idx.lines+1 {
		return idx.size, nil
	}
	at := sort.Search(len(idx.points), func(i int) bool { return idx.points[i].line > target }) - 1
	if at < 0 {
		at = 0
	}
	point := idx.points[at]
	remaining := target - point.line
	if remaining == 0 {
		return point.offset, nil
	}
	buf := make([]byte, 1<<20)
	for offset := point.offset; offset < idx.size; {
		want := int64(len(buf))
		if idx.size-offset < want {
			want = idx.size - offset
		}
		n, e := f.ReadAt(buf[:want], offset)
		if e != nil && e != io.EOF {
			return 0, e
		}
		if n == 0 {
			break
		}
		part := buf[:n]
		base := offset
		for remaining > 0 {
			at := bytes.IndexByte(part, '\n')
			if at < 0 {
				break
			}
			remaining--
			base += int64(at) + 1
			if remaining == 0 {
				return base, nil
			}
			part = part[at+1:]
		}
		offset += int64(n)
	}
	return 0, fmt.Errorf("history cursor missing")
}

// ReadFileWindow keeps the cursor and look-ahead in the parse window. The
// parser can therefore attach commentary to its actual following row instead
// of fabricating a standalone row at every page boundary.
func ReadFileWindow(path string, cursorLine, budget int) (FileWindow, error) {
	f, err := os.Open(path)
	if err != nil {
		return FileWindow{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return FileWindow{}, err
	}
	if !info.Mode().IsRegular() {
		return FileWindow{}, fmt.Errorf("history source is not regular")
	}
	idx, err := indexHistoryFile(f, path, info)
	if err != nil {
		return FileWindow{}, err
	}
	if budget <= 0 {
		budget = defaultLoadMaxBytes
	}
	generation := sha256.Sum256([]byte(idx.generation))
	if idx.lines == 0 {
		return FileWindow{Generation: hex.EncodeToString(generation[:])}, nil
	}
	if cursorLine == 0 {
		cursorLine = idx.lines
	}
	cursor, err := windowOffsetForLine(f, idx, cursorLine)
	if err != nil {
		return FileWindow{}, err
	}
	next, err := windowOffsetForLine(f, idx, cursorLine+1)
	if err != nil {
		return FileWindow{}, err
	}
	end := cursor + int64(budget/4)
	if end >= idx.size {
		end = idx.size
	} else {
		// Include the entire look-ahead row, especially a tool result whose
		// output must remain attached to the cursor's original tool call.
		var found bool
		end, found, err = findNextLineStart(f, end, idx.size)
		if err != nil {
			return FileWindow{}, err
		}
		if !found {
			end = idx.size
		}
	}
	if end < next {
		end = next
	}
	start := cursor - int64(3*budget/4)
	if start < 0 {
		start = 0
	} else {
		start, _, err = findNextLineStart(f, start, idx.size)
		if err != nil {
			return FileWindow{}, err
		}
	}
	if end-start > int64(budget)*2 {
		start = end - int64(budget)*2
		if start < 0 {
			start = 0
		} else {
			var found bool
			start, found, err = findNextLineStart(f, start, idx.size)
			if err != nil {
				return FileWindow{}, err
			}
			if !found {
				return FileWindow{}, fmt.Errorf("history row has no bounded start")
			}
		}
	}
	if start > cursor || end-start > int64(budget)*2 {
		return FileWindow{}, fmt.Errorf("history cursor row exceeds bounded page budget")
	}
	// Retain the complete leading commentary/tool group. A physical byte cut
	// must not change an existing source row's body or invent a comment row.
	atStart := sort.Search(len(idx.points), func(i int) bool { return idx.points[i].offset > start }) - 1
	if atStart < 0 {
		atStart = 0
	}
	counted, e := countNewlines(f, idx.points[atStart].offset, start)
	if e != nil {
		return FileWindow{}, e
	}
	firstLine := idx.points[atStart].line + int(counted)
	group := sort.Search(len(idx.groups), func(i int) bool { return idx.groups[i].line >= firstLine })
	if group < len(idx.groups) && idx.groups[group].line <= cursorLine && idx.groups[group].offset < start {
		start = idx.groups[group].offset
	}
	if end-start > int64(budget)*2 {
		// Drop an incomplete leading logical group instead of returning a
		// partial body. It remains available to the next earlier page.
		for group < len(idx.groups) && idx.groups[group].line <= cursorLine && end-start > int64(budget)*2 {
			group++
			if group < len(idx.groups) && idx.groups[group].offset >= end-int64(budget)*2 {
				start = idx.groups[group].offset
			}
		}
		if start > cursor || end-start > int64(budget)*2 {
			return FileWindow{}, fmt.Errorf("history logical row exceeds bounded page budget")
		}
	}
	pointAt := sort.Search(len(idx.points), func(i int) bool { return idx.points[i].offset > start }) - 1
	if pointAt < 0 {
		pointAt = 0
	}
	point := idx.points[pointAt]
	count, err := countNewlines(f, point.offset, start)
	if err != nil {
		return FileWindow{}, err
	}
	base := point.line + int(count) - 1
	raw, err := io.ReadAll(io.NewSectionReader(f, start, end-start))
	if err != nil {
		return FileWindow{}, err
	}
	lines, cut, err := StreamTailLines(bytes.NewReader(raw), budget*2)
	if err != nil {
		return FileWindow{}, err
	}
	for i := range lines {
		lines[i].LineNo += base
	}
	if len(lines) > 0 {
		m := sort.Search(len(idx.markers), func(i int) bool { return idx.markers[i].LineNo >= lines[0].LineNo }) - 1
		if m >= 0 {
			lines = append([]TailLine{idx.markers[m]}, lines...)
		}
	}
	return FileWindow{Lines: lines, MoreBefore: start > 0 || cut, MoreAfter: end < idx.size, Generation: hex.EncodeToString(generation[:]), StartLine: base + 1, EndLine: base + int(physicalLineCount(raw))}, nil
}

func FileWindowGeneration(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return "", e
	}
	idx, e := indexHistoryFile(f, path, info)
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256([]byte(idx.generation))
	return hex.EncodeToString(sum[:]), nil
}
func ImmutableFileGeneration(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return "", e
	}
	head, e := fileHeadSHA(f, info.Size())
	if e != nil {
		return "", e
	}
	identity := fmt.Sprintf("%s:%d:%d", head, info.Size(), info.ModTime().UnixNano())
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		identity = fmt.Sprintf("%d:%d:%s", st.Dev, st.Ino, identity)
	}
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:]), nil
}
