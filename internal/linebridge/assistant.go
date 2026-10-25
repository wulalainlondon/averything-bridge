package linebridge

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Assistant-only views expose content only after rechecking the original caller.
// The existing receipt DTO remains metadata-only for management and logs.
type OperationDetail struct {
	ContentTruncated bool `json:"content_truncated,omitempty"`
	Operation
	Message      Content `json:"content"`
	TargetName   string  `json:"target_name"`
	AllowedDelay int64   `json:"late_window_ms"`
}
type OperationPage struct {
	Items []OperationDetail `json:"items"`
	Next  string            `json:"next_cursor,omitempty"`
}
type pageCursor struct {
	Subject string
	Account string
	Binding uint64
	Target  string
	State   string
	At      int64
	ID      string
}

func (s *Service) OperationDetail(ctx context.Context, c Caller, id string) (OperationDetail, error) {
	o, err := s.Receipt(ctx, c, id)
	if err != nil {
		return OperationDetail{}, err
	}
	t, err := s.store.Target(ctx, o.Account, o.Target)
	return OperationDetail{Operation: o, Message: o.Content, TargetName: t.Name, AllowedDelay: o.LateWindow}, err
}

// QueryOperations uses a sealed keyset cursor, bound to account, caller and filters.
func (s *Service) QueryOperations(ctx context.Context, c Caller, r Request) (OperationPage, error) {
	a, err := s.account(ctx, c, r.Account, "status")
	if err != nil {
		return OperationPage{}, err
	}
	if r.Limit == 0 {
		r.Limit = 20
	}
	if r.Limit < 1 || r.Limit > 50 {
		return OperationPage{}, Fail("line_limit_invalid")
	}
	states := map[string]bool{"": true, "pending": true, "scheduled": true, "queued": true, "held_late": true, "held_binding": true, "held_unavailable": true, "failed_before_effect": true, "dispatching": true, "accepted_by_line": true, "unknown": true, "cancelled": true}
	if !states[r.State] {
		return OperationPage{}, Fail("line_state_invalid")
	}
	if r.Target != "" {
		if _, err = s.store.Target(ctx, r.Account, r.Target); err != nil {
			return OperationPage{}, err
		}
	}
	q := "SELECT * FROM line_ops WHERE account=? AND subject=?"
	args := []any{r.Account, c.Subject}
	if r.Target != "" {
		q += " AND target=?"
		args = append(args, r.Target)
	}
	if r.State == "pending" {
		q += " AND state IN ('queued','scheduled','held_late','held_binding','held_unavailable')"
	} else if r.State != "" {
		q += " AND state=?"
		args = append(args, r.State)
	}
	if r.Cursor != "" {
		var cur pageCursor
		b, e := base64.RawURLEncoding.DecodeString(r.Cursor)
		if len(r.Cursor) > 4096 || e != nil || s.store.unseal(r.Account, b, &cur) != nil || cur.Subject != c.Subject || cur.Account != r.Account || cur.Binding != a.Revision || cur.Target != r.Target || cur.State != r.State {
			return OperationPage{}, Fail("line_cursor_invalid")
		}
		q += " AND (created_at<? OR (created_at=? AND id<?))"
		args = append(args, cur.At, cur.At, cur.ID)
	}
	q += " ORDER BY created_at DESC,id DESC LIMIT ?"
	args = append(args, r.Limit+1)
	rows, err := s.store.db.QueryContext(ctx, q, args...)
	if err != nil {
		return OperationPage{}, err
	}
	ops := []Operation{}
	for rows.Next() {
		o, e := scanOp(rows, s.store)
		if e != nil {
			rows.Close()
			return OperationPage{}, e
		}
		ops = append(ops, o)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return OperationPage{}, err
	}
	page := OperationPage{Items: []OperationDetail{}}
	if len(ops) > r.Limit {
		ops = ops[:r.Limit]
		last := ops[len(ops)-1]
		b, e := s.store.seal(r.Account, pageCursor{Subject: c.Subject, Account: r.Account, Binding: a.Revision, Target: r.Target, State: r.State, At: last.CreatedAt, ID: last.ID})
		if e != nil {
			return page, e
		}
		page.Next = base64.RawURLEncoding.EncodeToString(b)
	}
	for _, o := range ops {
		t, e := s.store.Target(ctx, o.Account, o.Target)
		if e != nil {
			return page, e
		}
		detail := OperationDetail{Operation: o, Message: o.Content, TargetName: t.Name, AllowedDelay: o.LateWindow}
		runes := []rune(detail.Message.Text)
		if len(runes) > 300 {
			detail.Message.Text = string(runes[:300])
			detail.ContentTruncated = true
		}
		page.Items = append(page.Items, detail)
	}
	return page, nil
}

// Alias is a user-chosen label, never an authorization or an inferred identity.
func normalizedAlias(value string) (string, error) {
	v := strings.ToLower(norm.NFKC.String(strings.TrimSpace(value)))
	if v == "" || !utf8.ValidString(v) || utf8.RuneCountInString(v) > 80 || strings.ContainsAny(v, "\x00\r\n") {
		return "", Fail("line_alias_invalid")
	}
	return v, nil
}
func (s *Service) SetTargetAlias(ctx context.Context, c Caller, r Request) (Target, error) {
	if _, e := s.account(ctx, c, r.Account, "directory"); e != nil {
		return Target{}, e
	}
	name, e := normalizedAlias(r.Alias)
	if e != nil {
		return Target{}, e
	}
	t, e := s.store.Target(ctx, r.Account, r.Target)
	if e != nil {
		return t, e
	}
	key := s.store.digest(r.Account + "\x00" + c.Subject + "\x00" + name)
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	var old string
	e = s.store.db.QueryRowContext(ctx, "SELECT target FROM line_target_aliases WHERE account=? AND subject=? AND alias_key=?", r.Account, c.Subject, key).Scan(&old)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return t, e
	}
	if old != "" && old != r.Target {
		return t, Fail("line_alias_conflict")
	}
	_, e = s.store.db.ExecContext(ctx, "INSERT OR IGNORE INTO line_target_aliases(account,subject,alias_key,target) VALUES(?,?,?,?)", r.Account, c.Subject, key, r.Target)
	t.Alias = strings.TrimSpace(r.Alias)
	return t, e
}
func (s *Service) aliasTarget(ctx context.Context, c Caller, account, query string) (*Target, error) {
	name, e := normalizedAlias(query)
	if e != nil {
		return nil, nil
	}
	key := s.store.digest(account + "\x00" + c.Subject + "\x00" + name)
	var ref string
	e = s.store.db.QueryRowContext(ctx, "SELECT target FROM line_target_aliases WHERE account=? AND subject=? AND alias_key=?", account, c.Subject, key).Scan(&ref)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	t, e := s.store.Target(ctx, account, ref)
	t.Alias = strings.TrimSpace(query)
	return &t, e
}

// Editing a scheduled payload is CAS-protected and cannot replay uncertain sends.
func (s *Service) UpdateScheduledMessage(ctx context.Context, c Caller, r Request) (OperationDetail, error) {
	o, err := s.Receipt(ctx, c, r.Operation)
	if err != nil {
		return OperationDetail{}, err
	}
	if o.DueAt <= 0 || o.Subject != c.Subject {
		return OperationDetail{}, Fail("line_schedule_invalid")
	}
	if _, err = s.account(ctx, c, o.Account, "schedule"); err != nil {
		return OperationDetail{}, err
	}
	if _, err = s.account(ctx, c, o.Account, "messages.send"); err != nil {
		return OperationDetail{}, err
	}
	if strings.TrimSpace(r.Content.Text) == "" || len(r.Content.Text) > 20000 || r.Content.ResourceRef != "" || r.Content.ReplyTo != "" {
		return OperationDetail{}, Fail("line_schedule_text_required")
	}
	b, err := s.store.seal(o.Account, r.Content)
	if err != nil {
		return OperationDetail{}, err
	}
	result, err := s.store.db.ExecContext(ctx, "UPDATE line_ops SET content=?,updated_at=?,revision=revision+1 WHERE id=? AND revision=? AND state IN ('scheduled','held_late','held_binding','held_unavailable')", b, nowMS(), o.ID, r.Revision)
	if err != nil {
		return OperationDetail{}, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return OperationDetail{}, err
	}
	if n != 1 {
		return OperationDetail{}, Fail("line_operation_revision_conflict")
	}
	return s.OperationDetail(ctx, c, o.ID)
}

// ResolveScheduleTime requires an explicit local timestamp and IANA zone. It
// rejects missing/repeated DST wall times instead of silently picking an offset.
func ResolveScheduleTime(local, zone string) (any, error) {
	loc, e := time.LoadLocation(zone)
	if e != nil || zone == "" || zone == "Local" {
		return nil, Fail("line_timezone_invalid")
	}
	t, e := time.ParseInLocation("2006-01-02 15:04:05", local, loc)
	if e != nil || t.Format("2006-01-02 15:04:05") != local {
		return nil, Fail("line_local_time_invalid")
	}
	for _, delta := range []time.Duration{-2 * time.Hour, -time.Hour, -30 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour} {
		if t.Add(delta).Format("2006-01-02 15:04:05") == local {
			return nil, Fail("line_local_time_ambiguous")
		}
	}
	if t.UnixMilli() <= nowMS() {
		return nil, Fail("line_schedule_invalid")
	}
	return map[string]any{"due_at": t.UnixMilli(), "timezone": zone, "local_time": local, "iso_time": t.Format(time.RFC3339)}, nil
}
