package voicecontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"time"
)

type Request struct {
	ID     string          `json:"id"`
	Thread string          `json:"thread"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}
type Result struct {
	OK     bool   `json:"ok"`
	Result any    `json:"result,omitempty"`
	Code   string `json:"code,omitempty"`
}

var methods = map[string]bool{"list_sessions": true, "voice_transfer": true, "get_voice_transfer": true}

func canonicalParams(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var parse func(int) (any, error)
	parse = func(depth int) (any, error) {
		if depth > 32 {
			return nil, errors.New("voice_control_request_invalid")
		}
		token, e := decoder.Token()
		if e != nil {
			return nil, e
		}
		switch t := token.(type) {
		case json.Delim:
			if t == '{' {
				value := map[string]any{}
				for decoder.More() {
					k, e := decoder.Token()
					if e != nil {
						return nil, e
					}
					key, ok := k.(string)
					if !ok {
						return nil, errors.New("voice_control_request_invalid")
					}
					if _, duplicate := value[key]; duplicate {
						return nil, errors.New("voice_control_request_ambiguous")
					}
					item, e := parse(depth + 1)
					if e != nil {
						return nil, e
					}
					value[key] = item
				}
				end, e := decoder.Token()
				if e != nil || end != json.Delim('}') {
					return nil, errors.New("voice_control_request_invalid")
				}
				return value, nil
			}
			if t == '[' {
				value := []any{}
				for decoder.More() {
					item, e := parse(depth + 1)
					if e != nil {
						return nil, e
					}
					value = append(value, item)
				}
				end, e := decoder.Token()
				if e != nil || end != json.Delim(']') {
					return nil, errors.New("voice_control_request_invalid")
				}
				return value, nil
			}
			return nil, errors.New("voice_control_request_invalid")
		default:
			return token, nil
		}
	}
	value, e := parse(0)
	if e != nil {
		return nil, e
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, errors.New("voice_control_request_invalid")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("voice_control_request_invalid")
	}
	return json.Marshal(value)
}
func (q Request) Valid() bool {
	if !methods[q.Method] || len(q.Params) > 60000 {
		return false
	}
	value, e := canonicalParams(q.Params)
	return e == nil && len(value) <= 60000
}
func (q Request) Fingerprint() string {
	value, e := canonicalParams(q.Params)
	if e != nil {
		return ""
	}
	sum := sha256.Sum256(append([]byte(q.Method+"\x00"), value...))
	return hex.EncodeToString(sum[:])
}
func ParseInvocation(argv []string) (Request, error) {
	f := flag.NewFlagSet("bridge-voice", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	id := f.String("id", "", "unique original voice-transfer intent/query ID")
	method := f.String("method", "list_sessions", "voice-transfer action")
	params := f.String("params", "{}", "JSON arguments with exact catalog session/thread/revision")
	seen := map[string]bool{}
	for i := 0; i < len(argv); i += 2 {
		if i+1 >= len(argv) || (argv[i] != "--id" && argv[i] != "--method" && argv[i] != "--params") || seen[argv[i]] {
			return Request{}, errors.New("voice_control_request_invalid")
		}
		seen[argv[i]] = true
	}
	if f.Parse(argv) != nil || f.NArg() != 0 {
		return Request{}, errors.New("voice_control_request_invalid")
	}
	q := Request{ID: *id, Method: *method, Params: json.RawMessage(*params)}
	if !IntentID.MatchString(q.ID) || !q.Valid() {
		return q, errors.New("voice_control_request_invalid")
	}
	return q, nil
}
func RunCLI(args []string, out, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		io.WriteString(stderr, "Usage: bridge-voice --id vtr_UNIQUE_INTENT --method list_sessions|voice_transfer|get_voice_transfer --params JSON\nOriginal provider-observed command, live Android call and existing controller grants are required. An explicit voice request authorizes capable Android clients to transfer without another tap; receipt acceptance is not connected. No text dispatch.\n")
		return 0
	}
	q, e := ParseInvocation(args)
	fail := func(e error) int { json.NewEncoder(out).Encode(Result{Code: e.Error()}); return 1 }
	if e != nil {
		return fail(e)
	}
	q.Thread = os.Getenv("CODEX_THREAD_ID")
	if q.Thread == "" || (os.Getenv("CODEX_SESSION_ID") != "" && os.Getenv("CODEX_SESSION_ID") != q.Thread) {
		return fail(errors.New("voice_control_origin_unavailable"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	r, e := Send(ctx, DefaultManifest(), q)
	if e != nil {
		return fail(e)
	}
	json.NewEncoder(out).Encode(r)
	if !r.OK {
		return 1
	}
	return 0
}
