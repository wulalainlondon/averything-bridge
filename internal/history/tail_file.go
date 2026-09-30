package history

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

// TailState is the durable physical-line cursor for one append-only rollout.
// It deliberately stores no message content; parsed history remains in Cache.
type TailState struct {
	Path             string
	Size             int64
	MtimeNS          int64
	HeadSHA256       string
	LineCount        int64
	EndedWithNewline bool
}

// ReadTailFileLines reads only the retained tail of a regular JSONL file. A
// prior state lets append-only growth update the absolute line count by reading
// only the appended bytes. On first use or rotation it performs a cheap newline
// count over the file, then parses only the final budget rather than JSON
// decoding the complete rollout.
func ReadTailFileLines(path string, budget int, prior TailState, hasPrior bool) ([]TailLine, bool, TailState, error) {
	if budget <= 0 {
		budget = defaultLoadMaxBytes
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, false, TailState{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, false, TailState{}, err
	}
	size := info.Size()
	head, err := fileHeadSHA(f, size)
	if err != nil {
		return nil, false, TailState{}, err
	}

	lineCount, ended, err := updatePhysicalLineCount(f, size, head, prior, hasPrior)
	if err != nil {
		return nil, false, TailState{}, err
	}
	state := TailState{
		Path: path, Size: size, MtimeNS: info.ModTime().UnixNano(), HeadSHA256: head,
		LineCount: lineCount, EndedWithNewline: ended,
	}
	if size == 0 {
		return nil, false, state, nil
	}

	start := int64(0)
	if size > int64(budget) {
		candidate := size - int64(budget)
		if next, ok, findErr := findNextLineStart(f, candidate, size); findErr != nil {
			return nil, false, TailState{}, findErr
		} else if ok && next < size {
			start = next
		} else if previous, prevErr := findPreviousLineStart(f, candidate); prevErr != nil {
			return nil, false, TailState{}, prevErr
		} else {
			start = previous
		}
	}

	raw, err := io.ReadAll(io.NewSectionReader(f, start, size-start))
	if err != nil {
		return nil, false, TailState{}, err
	}
	lines, locallyTruncated, err := StreamTailLines(bytes.NewReader(raw), budget)
	if err != nil {
		return nil, false, TailState{}, err
	}
	sectionLines := physicalLineCount(raw)
	base := lineCount - sectionLines
	for i := range lines {
		lines[i].LineNo += int(base)
	}
	return lines, start > 0 || locallyTruncated, state, nil
}

func fileHeadSHA(f *os.File, size int64) (string, error) {
	n := int64(4096)
	if size < n {
		n = size
	}
	buf := make([]byte, n)
	if n > 0 {
		if _, err := f.ReadAt(buf, 0); err != nil && err != io.EOF {
			return "", err
		}
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:]), nil
}

func updatePhysicalLineCount(f *os.File, size int64, head string, prior TailState, hasPrior bool) (int64, bool, error) {
	ended, err := fileEndsWithNewline(f, size)
	if err != nil {
		return 0, false, err
	}
	appendOnly := hasPrior && prior.Path != "" && prior.HeadSHA256 == head && size >= prior.Size
	if !appendOnly {
		newlines, err := countNewlines(f, 0, size)
		if err != nil {
			return 0, false, err
		}
		count := newlines
		if size > 0 && !ended {
			count++
		}
		return count, ended, nil
	}
	if size == prior.Size {
		return prior.LineCount, ended, nil
	}
	newlines, err := countNewlines(f, prior.Size, size)
	if err != nil {
		return 0, false, err
	}
	count := prior.LineCount + newlines
	if size > 0 && !ended {
		count++
	}
	if prior.Size > 0 && !prior.EndedWithNewline {
		count--
	}
	return count, ended, nil
}

func countNewlines(f *os.File, start, end int64) (int64, error) {
	if end <= start {
		return 0, nil
	}
	buf := make([]byte, 4<<20)
	var total int64
	for offset := start; offset < end; {
		want := int64(len(buf))
		if remaining := end - offset; remaining < want {
			want = remaining
		}
		n, err := f.ReadAt(buf[:want], offset)
		if n > 0 {
			total += int64(bytes.Count(buf[:n], []byte{'\n'}))
			offset += int64(n)
		}
		if err != nil && err != io.EOF {
			return 0, err
		}
		if n == 0 {
			break
		}
	}
	return total, nil
}

func fileEndsWithNewline(f *os.File, size int64) (bool, error) {
	if size == 0 {
		return false, nil
	}
	var last [1]byte
	_, err := f.ReadAt(last[:], size-1)
	if err != nil && err != io.EOF {
		return false, err
	}
	return last[0] == '\n', nil
}

func physicalLineCount(raw []byte) int64 {
	if len(raw) == 0 {
		return 0
	}
	count := int64(bytes.Count(raw, []byte{'\n'}))
	if raw[len(raw)-1] != '\n' {
		count++
	}
	return count
}

func findNextLineStart(f *os.File, start, end int64) (int64, bool, error) {
	buf := make([]byte, 1<<20)
	for offset := start; offset < end; {
		want := int64(len(buf))
		if remaining := end - offset; remaining < want {
			want = remaining
		}
		n, err := f.ReadAt(buf[:want], offset)
		if n > 0 {
			if i := bytes.IndexByte(buf[:n], '\n'); i >= 0 {
				return offset + int64(i) + 1, true, nil
			}
			offset += int64(n)
		}
		if err != nil && err != io.EOF {
			return 0, false, err
		}
		if n == 0 {
			break
		}
	}
	return 0, false, nil
}

func findPreviousLineStart(f *os.File, before int64) (int64, error) {
	buf := make([]byte, 1<<20)
	for end := before; end > 0; {
		start := end - int64(len(buf))
		if start < 0 {
			start = 0
		}
		n, err := f.ReadAt(buf[:end-start], start)
		if n > 0 {
			if i := bytes.LastIndexByte(buf[:n], '\n'); i >= 0 {
				return start + int64(i) + 1, nil
			}
		}
		if err != nil && err != io.EOF {
			return 0, err
		}
		end = start
	}
	return 0, nil
}
