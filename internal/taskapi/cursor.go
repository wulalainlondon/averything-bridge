package taskapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	taskcontract "everything-go/contracts/task-api/v1"
)

type Watermark struct {
	Store    string `json:"store"`
	Sequence uint64 `json:"sequence"`
}
type ReadScope struct {
	Authority, StableScopeID, FilterHash string
	NamespaceGeneration                  uint64
}
type Freeze struct {
	ID         string      `json:"freeze_id"`
	ExpiresAt  int64       `json:"expires_at_ms"`
	Watermarks []Watermark `json:"watermarks"`
}
type SnapshotRow struct {
	Key   string
	Value any
}

// Capture must freeze membership and each row's payload/revision at the same
// per-store watermarks. Page MUST NOT read mutable latest rows. It uses the
// owner's original-store MVCC/materialized snapshot, not a second task ledger.
// Lost/restarted/expired freezes return cursor_expired, never a new watermark.
type SnapshotSource interface {
	Capture(context.Context, ReadScope) (Freeze, error)
	Page(context.Context, ReadScope, Freeze, string, int) ([]SnapshotRow, bool, error)
}
type cursorPayload struct {
	Kind   string    `json:"kind"`
	Schema string    `json:"schema"`
	Scope  ReadScope `json:"scope"`
	Freeze Freeze    `json:"freeze"`
	After  string    `json:"after,omitempty"`
	Views  []string  `json:"views,omitempty"`
}
type CursorCodec struct {
	secret []byte
	now    func() time.Time
}

func NewCursorCodec(secret []byte, now func() time.Time) (*CursorCodec, error) {
	if len(secret) < 32 {
		return nil, errors.New("cursor signing key too short")
	}
	if now == nil {
		now = time.Now
	}
	return &CursorCodec{append([]byte(nil), secret...), now}, nil
}
func (c *CursorCodec) encode(p cursorPayload) (string, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, c.secret)
	mac.Write([]byte(body))
	token := body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if len(token) > 16000 {
		return "", Failure("invalid_argument", "known_none", "correct_input")
	}
	return token, nil
}
func (c *CursorCodec) decode(token string, scope ReadScope, kind string) (cursorPayload, error) {
	bad := Failure("cursor_expired", "known_none", "refresh_snapshot")
	if len(token) > 16000 {
		return cursorPayload{}, bad
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return cursorPayload{}, bad
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return cursorPayload{}, bad
	}
	mac := hmac.New(sha256.New, c.secret)
	mac.Write([]byte(parts[0]))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return cursorPayload{}, bad
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return cursorPayload{}, bad
	}
	if _, err = taskcontract.Parse(raw); err != nil {
		return cursorPayload{}, bad
	}
	var p cursorPayload
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&p); err != nil {
		return cursorPayload{}, bad
	}
	if p.Kind != kind || p.Schema != taskcontract.Hash() || p.Scope != scope || p.Freeze.ID == "" || p.Freeze.ExpiresAt <= c.now().UnixMilli() {
		return cursorPayload{}, bad
	}
	return p, nil
}
func FilterHash(filter any) (string, error) {
	raw, err := json.Marshal(filter)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

type SnapshotPage struct {
	SourceRelationCoverage string      `json:"source_relation_coverage,omitempty"`
	Items                  []any       `json:"items"`
	Views                  []string    `json:"views"`
	Watermarks             []Watermark `json:"watermarks"`
	EventsCursor           string      `json:"events_cursor"`
	NextPageCursor         string      `json:"next_page_cursor,omitempty"`
	HasMore                bool        `json:"has_more"`
	Partial                bool        `json:"partial"`
	Consistency            string      `json:"consistency"`
	FreezeID               string      `json:"freeze_id"`
	ExpiresAt              int64       `json:"expires_at_ms"`
	PageOrder              string      `json:"page_order"`
}
type SnapshotEngine struct {
	Codec  *CursorCodec
	Source SnapshotSource
}

func (s SnapshotEngine) Page(ctx context.Context, scope ReadScope, views []string, token string, limit int) (SnapshotPage, error) {
	var result SnapshotPage
	if s.Codec == nil || s.Source == nil {
		return result, Failure("unsupported", "known_none", "read_capabilities")
	}
	if limit < 1 || limit > 100 || scope.Authority == "" || scope.StableScopeID == "" || scope.FilterHash == "" {
		return result, Failure("invalid_argument", "known_none", "correct_input")
	}
	var p cursorPayload
	var err error
	if token == "" {
		p = cursorPayload{Kind: "snapshot", Schema: taskcontract.Hash(), Scope: scope, Views: append([]string{}, views...)}
		p.Freeze, err = s.Source.Capture(ctx, scope)
		if err != nil {
			return result, err
		}
	} else {
		p, err = s.Codec.decode(token, scope, "snapshot")
		if err != nil {
			return result, err
		}
		if strings.Join(p.Views, "\x00") != strings.Join(views, "\x00") {
			return result, Failure("cursor_expired", "known_none", "refresh_snapshot")
		}
	}
	if p.Freeze.ID == "" || p.Freeze.ExpiresAt <= s.Codec.now().UnixMilli() {
		return result, Failure("cursor_expired", "known_none", "refresh_snapshot")
	}
	seenStores := map[string]bool{}
	for _, w := range p.Freeze.Watermarks {
		if w.Store == "" || seenStores[w.Store] {
			return result, Failure("unknown_acceptance", "unknown", "refresh_snapshot")
		}
		seenStores[w.Store] = true
	}
	rows, more, err := s.Source.Page(ctx, scope, p.Freeze, p.After, limit)
	if err != nil {
		return result, err
	}
	if len(rows) > limit || (more && len(rows) == 0) {
		return result, Failure("unknown_acceptance", "unknown", "refresh_snapshot")
	}
	coverage := "not_requested"
	partial := false
	for _, view := range p.Views {
		if view == "source_children" {
			coverage = "trusted_relations_only"
			partial = true
		}
	}
	result = SnapshotPage{SourceRelationCoverage: coverage, Partial: partial, Items: []any{}, Views: append([]string{}, p.Views...), Watermarks: append([]Watermark{}, p.Freeze.Watermarks...), HasMore: more, Consistency: "per_store_snapshot_vector", FreezeID: p.Freeze.ID, ExpiresAt: p.Freeze.ExpiresAt, PageOrder: "task_id"}
	last := p.After
	for _, row := range rows {
		if row.Key == "" || row.Key <= last {
			return SnapshotPage{}, Failure("unknown_acceptance", "unknown", "refresh_snapshot")
		}
		last = row.Key
		result.Items = append(result.Items, row.Value)
	}
	// Events always resume at the ORIGINAL vector, on every page.
	result.EventsCursor, err = s.Codec.encode(cursorPayload{Kind: "events", Schema: p.Schema, Scope: scope, Freeze: p.Freeze})
	if err != nil {
		return SnapshotPage{}, err
	}
	if more {
		p.After = last
		result.NextPageCursor, err = s.Codec.encode(p)
	}
	return result, err
}
func (c *CursorCodec) EventWatermarks(token string, scope ReadScope) ([]Watermark, error) {
	p, err := c.decode(token, scope, "events")
	return p.Freeze.Watermarks, err
}
func (c *CursorCodec) EventsFromWatermarks(scope ReadScope, marks []Watermark, expires time.Time) (string, error) {
	return c.encode(cursorPayload{Kind: "events", Schema: taskcontract.Hash(), Scope: scope, Freeze: Freeze{ID: "event-stream", ExpiresAt: expires.UnixMilli(), Watermarks: marks}})
}
