package history

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

// Archives are reopened for a metadata pass and a bounded body pass. Never
// expand a compressed transcript to disk or retain its whole decompressed body.
// This preserves complete commentary/tool groups exactly as seekable files do.
func ReadStreamWindow(open func() (io.Reader, func(), error), cursorLine, budget int) (FileWindow, error) {
	if budget <= 0 {
		budget = defaultLoadMaxBytes
	}
	reader, closeFn, err := open()
	if err != nil {
		return FileWindow{}, err
	}
	br := bufio.NewReaderSize(reader, 1<<20)
	points := []linePoint{{1, 0}}
	var groups []linePoint
	var markers []TailLine
	var total int64
	lineNo := 0
	pendingLine := 0
	var pendingOffset int64
	for {
		start := total
		prefix := make([]byte, 0, 8192)
		size := 0
		var e error
		for {
			var part []byte
			part, e = br.ReadSlice('\n')
			size += len(part)
			total += int64(len(part))
			room := 65536 - len(prefix)
			if room > len(part) {
				room = len(part)
			}
			if room > 0 {
				prefix = append(prefix, part[:room]...)
			}
			if e != bufio.ErrBufferFull {
				break
			}
		}
		if size > 0 {
			lineNo++
			row, complete := nativeWindowHeader(bytes.TrimSpace(prefix))
			if row.Type == "response_item" {
				p := row.Payload
				if p.Type == "message" {
					if p.Role == "assistant" && (p.Phase == "commentary" || !complete && p.Phase == "") {
						if pendingLine == 0 {
							pendingLine = lineNo
							pendingOffset = start
						}
						groups = append(groups, linePoint{lineNo, pendingOffset})
					} else if p.Role == "user" {
						groups = append(groups, linePoint{lineNo, start})
					} else if p.Role == "assistant" {
						at := start
						if pendingLine != 0 {
							at = pendingOffset
						}
						groups = append(groups, linePoint{lineNo, at})
						pendingLine = 0
					}
				} else if p.Type == "function_call" || p.Type == "custom_tool_call" {
					at := start
					if pendingLine != 0 {
						at = pendingOffset
					}
					groups = append(groups, linePoint{lineNo, at})
					if p.Name != "update_plan" && (complete || p.Name != "") {
						pendingLine = 0
					}
				}
			}
			if complete {
				if marker, ok := streamControlMarker(TailLine{lineNo, bytes.TrimSpace(prefix)}); ok {
					markers = append(markers, marker)
				}
			}
			points = append(points, linePoint{lineNo + 1, total})
		}
		if e != nil {
			closeFn()
			if e != io.EOF {
				return FileWindow{}, e
			}
			break
		}
	}
	if lineNo == 0 {
		return FileWindow{}, nil
	}
	if cursorLine == 0 {
		cursorLine = lineNo
	}
	if cursorLine < 1 || cursorLine > lineNo {
		return FileWindow{}, fmt.Errorf("history archive cursor line missing")
	}
	cursor := points[cursorLine-1].offset
	end := cursor + int64(budget/4)
	if end >= total {
		end = total
	} else {
		at := sort.Search(len(points), func(i int) bool { return points[i].offset > end })
		if at < len(points) {
			end = points[at].offset
		} else {
			end = total
		}
	}
	if end < points[cursorLine].offset {
		end = points[cursorLine].offset
	}
	start := cursor - int64(3*budget/4)
	if start < 0 {
		start = 0
	} else {
		at := sort.Search(len(points), func(i int) bool { return points[i].offset > start })
		if at < len(points) {
			start = points[at].offset
		}
	}
	if end-start > int64(2*budget) {
		at := sort.Search(len(points), func(i int) bool { return points[i].offset >= end-int64(2*budget) })
		start = points[at].offset
	}
	first := sort.Search(len(points), func(i int) bool { return points[i].offset >= start })
	group := sort.Search(len(groups), func(i int) bool { return groups[i].line >= first+1 })
	if group < len(groups) && groups[group].line <= cursorLine && groups[group].offset < start {
		start = groups[group].offset
	}
	for end-start > int64(2*budget) && group < len(groups) && groups[group].line <= cursorLine {
		group++
		if group < len(groups) {
			start = groups[group].offset
		}
	}
	if start > cursor || end-start > int64(2*budget) {
		return FileWindow{}, fmt.Errorf("history archive logical group exceeds bounded page budget")
	}
	first = sort.Search(len(points), func(i int) bool { return points[i].offset >= start })
	last := sort.Search(len(points), func(i int) bool { return points[i].offset >= end })
	reader, closeFn, err = open()
	if err != nil {
		return FileWindow{}, err
	}
	defer closeFn()
	if _, err = io.CopyN(io.Discard, reader, start); err != nil {
		return FileWindow{}, err
	}
	raw, err := io.ReadAll(io.LimitReader(reader, end-start))
	if err != nil {
		return FileWindow{}, err
	}
	if int64(len(raw)) != end-start {
		return FileWindow{}, io.ErrUnexpectedEOF
	}
	var lines []TailLine
	for n, line := range bytes.Split(raw, []byte{'\n'}) {
		if trimmed := bytes.TrimSpace(line); len(trimmed) > 0 {
			lines = append(lines, TailLine{first + n + 1, trimmed})
		}
	}
	if first > 0 {
		at := sort.Search(len(markers), func(i int) bool { return markers[i].LineNo >= first+1 })
		if at > 0 {
			lines = append([]TailLine{markers[at-1]}, lines...)
		}
	}
	return FileWindow{Lines: lines, MoreBefore: start > 0, MoreAfter: end < total, StartLine: first + 1, EndLine: last}, nil
}
func streamControlMarker(line TailLine) (TailLine, bool) {
	if len(line.Data) > 65536 || !bytes.Contains(line.Data, []byte("turn")) {
		return TailLine{}, false
	}
	var row struct {
		Type    string `json:"type"`
		Payload struct {
			Type   string          `json:"type"`
			TurnID string          `json:"turn_id"`
			Error  json.RawMessage `json:"error"`
		} `json:"payload"`
	}
	if json.Unmarshal(line.Data, &row) != nil {
		return TailLine{}, false
	}
	if row.Type != "turn_context" && (row.Type != "event_msg" || (row.Payload.Type != "task_started" && row.Payload.Type != "task_complete" && row.Payload.Type != "turn_aborted")) {
		return TailLine{}, false
	}
	data, _ := json.Marshal(row)
	return TailLine{LineNo: line.LineNo, Data: data}, true
}
