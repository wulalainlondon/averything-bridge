package linebridge

import (
	"context"
	"time"
)

type Subscription struct {
	ID      string `json:"subscription_id"`
	Account string `json:"account_ref"`
	Target  string `json:"target_ref"`
	Enabled bool   `json:"enabled"`
	Subject string `json:"-"`
}

func (s *Service) Subscribe(ctx context.Context, c Caller, account, target string, enabled bool) (Subscription, error) {
	if _, err := s.account(ctx, c, account, "events"); err != nil {
		return Subscription{}, err
	}
	if !s.store.Allowed(ctx, c, account, "messages.read") {
		return Subscription{}, Fail("line_forbidden")
	}
	if _, err := s.store.Target(ctx, account, target); err != nil {
		return Subscription{}, err
	}
	if s.config.EventSink == nil {
		return Subscription{}, Fail("line_event_store_unavailable")
	}
	ref := "ls_" + s.store.digest(account + "\x00" + target + "\x00" + c.Subject)[:32]
	_, err := s.store.db.ExecContext(ctx, "INSERT INTO line_subscriptions VALUES(?,?,?,?,?) ON CONFLICT(account,target,subject) DO UPDATE SET enabled=excluded.enabled", ref, account, target, c.Subject, enabled)
	return Subscription{ID: ref, Account: account, Target: target, Subject: c.Subject, Enabled: enabled}, err
}
func (s *Store) subscriptions(ctx context.Context, account string) ([]Subscription, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT id,account,target,subject,enabled FROM line_subscriptions WHERE account=? AND enabled=1", account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Subscription{}
	for rows.Next() {
		var sub Subscription
		if err = rows.Scan(&sub.ID, &sub.Account, &sub.Target, &sub.Subject, &sub.Enabled); err != nil {
			return nil, err
		}
		items = append(items, sub)
	}
	return items, rows.Err()
}
func (s *Service) pollTargets(ctx context.Context, account string) ([]Target, error) {
	subs, err := s.store.subscriptions(ctx, account)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	targets := []Target{}
	for _, sub := range subs {
		c := Caller{Subject: sub.Subject}
		if !s.store.Allowed(ctx, c, account, "events") || !s.store.Allowed(ctx, c, account, "messages.read") {
			continue
		}
		if s.config.SubjectAllowed != nil && !s.config.SubjectAllowed(sub.Subject) {
			continue
		}
		if seen[sub.Target] {
			continue
		}
		seen[sub.Target] = true
		t, err := s.store.Target(ctx, account, sub.Target)
		if err != nil {
			return nil, err
		}
		targets = append(targets, t)
	}
	if len(targets) > 100 {
		return nil, Fail("line_subscription_limit")
	}
	return targets, nil
}
func (s *Service) startPollers(ctx context.Context) {
	if s.config.EventSink == nil || s.config.BlockedScopes["events"] {
		return
	}
	rows, err := s.store.db.QueryContext(ctx, "SELECT DISTINCT s.account FROM line_subscriptions s JOIN line_accounts a ON a.id=s.account WHERE s.enabled=1 AND a.enabled=1")
	if err != nil {
		return
	}
	accounts := []string{}
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			accounts = append(accounts, id)
		}
	}
	rows.Close()
	for _, account := range accounts {
		targets, err := s.pollTargets(ctx, account)
		if err != nil {
			continue
		}
		talk := false
		for _, target := range targets {
			if target.Kind == "openchat" {
				s.startPoller(account, "square:"+target.Ref)
			} else {
				talk = true
			}
		}
		if talk {
			s.startPoller(account, "talk")
		}
	}
}
func (s *Service) startPoller(account, stream string) {
	key := account + "\x00" + stream
	s.mu.Lock()
	if s.ctx.Err() != nil || s.polling[key] {
		s.mu.Unlock()
		return
	}
	s.polling[key] = true
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		defer func() { s.mu.Lock(); delete(s.polling, key); s.mu.Unlock() }()
		for s.ctx.Err() == nil {
			ctx, cancel := context.WithTimeout(s.ctx, 35*time.Second)
			err := s.pollStream(ctx, account, stream)
			cancel()
			if s.ctx.Err() != nil {
				return
			}
			code := SafeCode(err)
			if code == "line_no_subscriptions" || code == "line_account_disabled" {
				return
			}
			if err != nil {
				s.mu.Lock()
				s.pollState[account] = "degraded:" + code
				s.mu.Unlock()
			}
			delay := time.Second
			if err != nil {
				delay = 10 * time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}
func (s *Service) PollOnce(ctx context.Context, account string) error {
	return s.pollStream(ctx, account, "talk")
}
func (s *Service) pollStream(ctx context.Context, account, stream string) error {
	if s.config.EventSink == nil {
		return Fail("line_event_store_unavailable")
	}
	a, err := s.store.Account(ctx, account)
	if err != nil {
		return err
	}
	if !a.Enabled {
		return Fail("line_account_disabled")
	}
	targets, err := s.pollTargets(ctx, account)
	if err != nil {
		return err
	}
	filtered := []Target{}
	for _, t := range targets {
		if (stream == "talk" && t.Kind != "openchat") || (stream == "square:"+t.Ref && t.Kind == "openchat") {
			filtered = append(filtered, t)
		}
	}
	targets = filtered
	if len(targets) == 0 {
		return Fail("line_no_subscriptions")
	}
	_, d, err := s.driver(ctx, Caller{Subject: a.Owner}, account, "events")
	if err != nil {
		return err
	}
	previous, err := s.config.EventSink.Cursor(ctx, account, stream)
	if err != nil {
		return err
	}
	externals := []string{}
	byExternal := map[string]Target{}
	for _, t := range targets {
		externals = append(externals, t.ExternalID)
		byExternal[t.ExternalID] = t
	}
	var batch struct {
		Events []RuntimeEvent `json:"events"`
		Cursor string         `json:"cursor"`
		Gap    bool           `json:"gap"`
	}
	method := "poll_events"
	args := map[string]any{"targets": externals, "cursor": previous}
	if stream != "talk" {
		method = "poll_openchat"
		args = map[string]any{"target": targets[0].ExternalID, "cursor": previous}
	}
	if err = d.Invoke(ctx, method, args, &batch); err != nil {
		return err
	}
	if len(batch.Events) > 100 || len(batch.Cursor) > 4096 || batch.Cursor == "" {
		return Fail("line_event_batch_invalid")
	}
	current, err := s.store.Account(ctx, account)
	if err != nil {
		return err
	}
	if !current.Enabled || current.Revision != a.Revision {
		return Fail("line_binding_changed")
	}
	allowed, err := s.pollTargets(ctx, account)
	if err != nil {
		return err
	}
	active := map[string]bool{}
	for _, t := range allowed {
		active[t.Ref] = true
	}
	events := []RuntimeEvent{}
	for _, event := range batch.Events {
		t, ok := byExternal[event.Target]
		if !ok || !active[t.Ref] {
			continue
		}
		event.Target = t.Ref
		event.Key = s.store.EventKey(account, event.Key)
		if event.Message != nil {
			event.Message.Target = t.Ref
			event.Message.ID, err = s.store.MessageRef(ctx, account, t.Ref, event.Message.ID)
			if err != nil {
				return err
			}
		}
		events = append(events, event)
	}
	if batch.Gap {
		events = append(events, RuntimeEvent{Kind: "sync_gap", Key: s.store.EventKey(account, stream+":gap:"+batch.Cursor), Code: "line_sync_gap"})
	}
	if err = s.config.EventSink.Commit(ctx, account, stream, previous, events, batch.Cursor); err != nil {
		return err
	}
	s.mu.Lock()
	if batch.Gap {
		s.pollState[account] = "live_with_gap"
	} else if s.pollState[account] != "live_with_gap" {
		s.pollState[account] = "live"
	}
	s.mu.Unlock()
	return nil
}

func (s *Service) PollTargetOnce(ctx context.Context, account, ref string) error {
	t, err := s.store.Target(ctx, account, ref)
	if err != nil {
		return err
	}
	stream := "talk"
	if t.Kind == "openchat" {
		stream = "square:" + t.Ref
	}
	return s.pollStream(ctx, account, stream)
}
