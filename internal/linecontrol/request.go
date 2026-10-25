package linecontrol

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

var methods = map[string]bool{"handoff_call": true, "get_call": true, "cancel_call": true, "call_intent_status": true, "get_message_page": true, "set_target_alias": true, "query_operations": true, "get_operation_detail": true, "update_scheduled_message": true, "resolve_schedule_time": true, "access": true, "list_subscriptions": true, "accounts": true, "intent_status": true, "status": true, "health": true, "find_targets": true, "find_openchats": true, "get_messages": true, "send_message": true, "schedule_message": true, "get_operation": true, "list_operations": true, "reschedule_operation": true, "cancel_operation": true, "subscribe": true, "list_attachments": true, "import_resource": true, "download_resource": true}

func canonicalParams(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var parse func(int) (any, error)
	parse = func(depth int) (any, error) {
		if depth > 32 {
			return nil, errors.New("line_request_invalid")
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
						return nil, errors.New("line_request_invalid")
					}
					if _, duplicate := value[key]; duplicate {
						return nil, errors.New("line_request_ambiguous")
					}
					item, e := parse(depth + 1)
					if e != nil {
						return nil, e
					}
					value[key] = item
				}
				end, e := decoder.Token()
				if e != nil || end != json.Delim('}') {
					return nil, errors.New("line_request_invalid")
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
					return nil, errors.New("line_request_invalid")
				}
				return value, nil
			}
			return nil, errors.New("line_request_invalid")
		default:
			return token, nil
		}
	}
	value, e := parse(0)
	if e != nil {
		return nil, e
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, errors.New("line_request_invalid")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("line_request_invalid")
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
	f := flag.NewFlagSet("bridge-line", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	id := f.String("id", "", "unique original LINE intent/query ID")
	method := f.String("method", "accounts", "LINE action")
	params := f.String("params", "{}", "JSON arguments with original account/target references")
	if f.Parse(argv) != nil || f.NArg() != 0 {
		return Request{}, errors.New("line_request_invalid")
	}
	q := Request{ID: *id, Method: *method, Params: json.RawMessage(*params)}
	if !IntentID.MatchString(q.ID) || !q.Valid() {
		return q, errors.New("line_request_invalid")
	}
	return q, nil
}
func RunCLI(args []string, out, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		io.WriteString(stderr, "Usage: bridge-line --id lop_UNIQUE_INTENT --method accounts|status|health|find_targets|get_messages|send_message|get_operation|intent_status --params JSON\nThe service requires the actual original provider-observed command and existing account grants; no thread/device override or grant management; handoff requires the exact active authorized voice.\n")
		return 0
	}
	q, e := ParseInvocation(args)
	fail := func(e error) int { json.NewEncoder(out).Encode(Result{Code: e.Error()}); return 1 }
	if e != nil {
		return fail(e)
	}
	q.Thread = os.Getenv("CODEX_THREAD_ID")
	if q.Thread == "" || (os.Getenv("CODEX_SESSION_ID") != "" && os.Getenv("CODEX_SESSION_ID") != q.Thread) {
		return fail(errors.New("line_origin_unavailable"))
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
