package goexec

import (
	"bytes"
	"encoding/json"
	"io"
	"unicode"
	"unicode/utf8"
)

// Reduce long JSON strings without decoding/exporting their content. Syntax,
// short public IDs and property order are retained. Output and each string use
// fixed budgets; oversized arrays/invalid syntax stay unclassified/unknown.
type boundedJSONMetadata struct {
	out, word                    []byte
	quoted, escape, long, failed bool
	unicodeRemaining             int
}

func (s *boundedJSONMetadata) add(b []byte) {
	if s.failed {
		return
	}
	for _, v := range b {
		if s.quoted {
			if s.unicodeRemaining > 0 {
				if !(v >= '0' && v <= '9' || v >= 'a' && v <= 'f' || v >= 'A' && v <= 'F') {
					s.failed = true
					return
				}
				s.unicodeRemaining--
				if !s.long {
					s.word = append(s.word, v)
				}
			} else if s.escape {
				if !bytes.ContainsRune([]byte(`"\/bfnrtu`), rune(v)) {
					s.failed = true
					return
				}
				if v == 'u' {
					s.unicodeRemaining = 4
				}
				s.escape = false
				if !s.long {
					s.word = append(s.word, v)
				}
			} else if v == '\\' {
				s.escape = true
				if !s.long {
					s.word = append(s.word, v)
				}
			} else if v == '"' {
				s.quoted = false
				if s.long {
					s.out = append(s.out, []byte(`"__bounded_opaque__"`)...)
				} else {
					s.out = append(s.out, '"')
					s.out = append(s.out, s.word...)
					s.out = append(s.out, '"')
				}
				s.word = nil
				s.long = false
			} else {
				if v < 32 {
					s.failed = true
					return
				}
				if !s.long {
					s.word = append(s.word, v)
				}
			}
			if len(s.word) > 256 {
				s.word = nil
				s.long = true
			}
		} else if v == '"' {
			s.quoted = true
			s.word = nil
		} else {
			s.out = append(s.out, v)
		}
		if len(s.out) > exactFinalLine {
			s.out = nil
			s.word = nil
			s.failed = true
			return
		}
	}
}
func (s *boundedJSONMetadata) publicHeader() (row, kind, turn, phase string, ok bool) {
	if s.failed || s.quoted || s.escape || s.unicodeRemaining != 0 || !unambiguousPublicJSON(s.out) {
		return
	}
	var meta struct {
		Type    string `json:"type"`
		Payload struct {
			Type  string `json:"type"`
			Turn  string `json:"turn_id"`
			Phase string `json:"phase"`
		} `json:"payload"`
	}
	if json.Unmarshal(s.out, &meta) != nil {
		return
	}
	return meta.Type, meta.Payload.Type, meta.Payload.Turn, meta.Payload.Phase, true
}

func unambiguousPublicJSON(data []byte) bool {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var walk func(int) bool
	walk = func(depth int) bool {
		if depth > 64 {
			return false
		}
		tok, e := d.Token()
		if e != nil {
			return false
		}
		delim, container := tok.(json.Delim)
		if !container {
			return true
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				key, ok := k.(string)
				if e != nil || !ok {
					return false
				}
				key = decoderFoldName(key)
				if seen[key] {
					return false
				}
				seen[key] = true
				if !walk(depth + 1) {
					return false
				}
			}
			end, e := d.Token()
			return e == nil && end == json.Delim('}')
		case '[':
			for d.More() {
				if !walk(depth + 1) {
					return false
				}
			}
			end, e := d.Token()
			return e == nil && end == json.Delim(']')
		}
		return false
	}
	if !walk(0) {
		return false
	}
	_, e := d.Token()
	return e == io.EOF
}

func decoderFoldName(key string) string {
	out := make([]byte, 0, len(key))
	for len(key) > 0 {
		r, n := utf8.DecodeRuneInString(key)
		key = key[n:]
		if r < 128 {
			if r >= 'a' && r <= 'z' {
				r -= 'a' - 'A'
			}
		} else {
			for {
				next := unicode.SimpleFold(r)
				if next <= r {
					r = next
					break
				}
				r = next
			}
		}
		out = utf8.AppendRune(out, r)
	}
	return string(out)
}
