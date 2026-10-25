package linebridge

import (
	"context"
	"encoding/base64"
	"strings"
)

type MessagePage struct {
	Messages        []Message `json:"messages"`
	Next            string    `json:"next_cursor,omitempty"`
	Scanned         int       `json:"scanned"`
	Coverage        string    `json:"coverage"`
	CompleteHistory bool      `json:"complete_history"`
	Failures        int       `json:"decryption_failures"`
}
type historyCursor struct {
	Subject, Account, Target, Query, Remote string
	Binding                                 uint64
	Since, Until                            int64
}

// ReadPage is bounded per invocation. An exhausted server page does not imply
// that LINE has retained a complete lifetime history for this account.
func (s *Service) ReadPage(ctx context.Context, c Caller, r Request) (MessagePage, error) {
	if r.Limit == 0 {
		r.Limit = 30
	}
	if r.Limit < 1 || r.Limit > 100 || len(r.Query) > 512 || r.Since < 0 || r.Until < 0 || (r.Until > 0 && r.Since >= r.Until) {
		return MessagePage{}, Fail("line_history_filter_invalid")
	}
	a, d, e := s.driver(ctx, c, r.Account, "messages.read")
	if e != nil {
		return MessagePage{}, e
	}
	t, e := s.store.Target(ctx, r.Account, r.Target)
	if e != nil {
		return MessagePage{}, e
	}
	if t.Kind != "user" {
		return MessagePage{}, Fail("line_history_target_unsupported")
	}
	cur := historyCursor{Subject: c.Subject, Account: r.Account, Target: r.Target, Query: r.Query, Binding: a.Revision, Since: r.Since, Until: r.Until}
	if r.Cursor != "" {
		b, err := base64.RawURLEncoding.DecodeString(r.Cursor)
		var old historyCursor
		if len(r.Cursor) > 4096 || err != nil || s.store.unseal(r.Account, b, &old) != nil || old.Subject != cur.Subject || old.Account != cur.Account || old.Target != cur.Target || old.Query != cur.Query || old.Binding != cur.Binding || old.Since != cur.Since || old.Until != cur.Until {
			return MessagePage{}, Fail("line_cursor_invalid")
		}
		cur.Remote = old.Remote
	}
	var wire struct {
		Messages []Message `json:"messages"`
		Next     string    `json:"next_cursor"`
		Scanned  int       `json:"scanned"`
		Failures int       `json:"decryption_failures"`
		Coverage string    `json:"coverage"`
	}
	if e = d.Invoke(ctx, "get_message_page", map[string]any{"target": t.ExternalID, "limit": r.Limit, "cursor": cur.Remote}, &wire); e != nil {
		return MessagePage{}, e
	}
	if len(wire.Messages) > r.Limit || wire.Scanned < 0 || wire.Scanned > r.Limit+1 || len(wire.Next) > 2048 || wire.Next != "" && wire.Next == cur.Remote {
		return MessagePage{}, Fail("line_runtime_result_invalid")
	}
	p := MessagePage{Messages: []Message{}, Scanned: wire.Scanned, Failures: wire.Failures, Coverage: wire.Coverage}
	query := strings.ToLower(r.Query)
	for _, m := range wire.Messages {
		if m.Timestamp < r.Since || (r.Until > 0 && m.Timestamp >= r.Until) || query != "" && !strings.Contains(strings.ToLower(m.Text), query) {
			continue
		}
		m.Target = r.Target
		m.ID, e = s.store.MessageRef(ctx, r.Account, r.Target, m.ID)
		if e != nil {
			return p, e
		}
		p.Messages = append(p.Messages, m)
	}
	if wire.Next != "" {
		cur.Remote = wire.Next
		b, err := s.store.seal(r.Account, cur)
		if err != nil {
			return p, err
		}
		p.Next = base64.RawURLEncoding.EncodeToString(b)
	}
	return p, nil
}
