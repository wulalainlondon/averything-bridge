package linebridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Config struct {
	DataDir, KeychainHelper, LegacyDir string
	SubjectAllowed                     func(string) bool
	EventSink                          EventSink
	Resources                          ResourceResolver
	BlockedScopes                      map[string]bool
	Subjects                           func() []Subject
	VoiceDispatchAccounts              []VoiceDispatchAccount
	VoiceDispatchSubjectAllowed        func(string) bool
}
type runtimeEntry struct {
	epoch   string
	stop    context.CancelFunc
	retryAt time.Time
	ready   chan struct{}
	driver  Driver
	err     error
}
type Service struct {
	store     *Store
	factory   Factory
	config    Config
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	runtimes  map[string]*runtimeEntry
	busy      map[string]bool
	polling   map[string]bool
	pollState map[string]string
	wg        sync.WaitGroup
	calls     map[string]*callOwner
}

func NewService(store *Store, factory Factory, config Config) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{store: store, factory: factory, config: config, ctx: ctx, cancel: cancel, runtimes: map[string]*runtimeEntry{}, busy: map[string]bool{}, polling: map[string]bool{}, pollState: map[string]string{}, calls: map[string]*callOwner{}}
	store.voiceDefaultAllowed = s.voiceDispatchAllowed
	return s
}
func (s *Service) Store() *Store { return s.store }
func (s *Service) Close() {
	s.cancel()
	s.mu.Lock()
	callOwners := []*callOwner{}
	for _, owner := range s.calls {
		callOwners = append(callOwners, owner)
	}
	entries := make([]*runtimeEntry, 0, len(s.runtimes))
	for _, e := range s.runtimes {
		entries = append(entries, e)
	}
	s.mu.Unlock()
	for _, owner := range callOwners {
		owner.cancel()
	}
	for _, e := range entries {
		<-e.ready
		if e.driver != nil {
			_ = e.driver.Close()
		}
	}
	s.wg.Wait()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for _, owner := range callOwners {
		select {
		case <-owner.done:
		case <-deadline.C:
			return
		}
	}
}
func (s *Service) Create(ctx context.Context, c Caller, label string, legacy bool) (Account, error) {
	if !c.Admin || c.Subject == "" {
		return Account{}, Fail("line_admin_required")
	}
	dir := filepath.Join(s.config.DataDir, "accounts", newID("profile_"))
	if legacy {
		if s.config.LegacyDir == "" {
			return Account{}, Fail("line_legacy_not_configured")
		}
		dir = s.config.LegacyDir
		if _, err := os.Stat(filepath.Join(dir, "session.enc")); err != nil {
			return Account{}, Fail("line_legacy_login_missing")
		}
		accounts, err := s.store.Accounts(ctx, c)
		if err != nil {
			return Account{}, err
		}
		for _, a := range accounts {
			if a.DataDir == dir {
				return a, nil
			}
		}
	}
	a, err := s.store.CreateAccount(ctx, label, c.Subject, dir, s.config.KeychainHelper)
	if err == nil && legacy {
		s.store.SetAuth(ctx, a.ID, "stored_login")
		a.AuthState = "stored_login"
	}
	return a, err
}
func (s *Service) account(ctx context.Context, c Caller, id, scope string) (Account, error) {
	if s.config.BlockedScopes[scope] {
		return Account{}, Fail("line_capability_disabled")
	}
	if s.config.SubjectAllowed != nil && !s.config.SubjectAllowed(c.Subject) {
		return Account{}, Fail("line_caller_revoked")
	}
	if !s.store.Allowed(ctx, c, id, scope) {
		return Account{}, Fail("line_forbidden")
	}
	a, err := s.store.Account(ctx, id)
	if err != nil {
		return a, err
	}
	if !a.Enabled {
		return a, Fail("line_account_disabled")
	}
	return a, nil
}
func (s *Service) ensure(ctx context.Context, a Account) (Driver, error) {
	if s.ctx.Err() != nil {
		return nil, Fail("line_service_closed")
	}
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil, Fail("line_service_closed")
	}
	entry := s.runtimes[a.ID]
	if entry != nil {
		select {
		case <-entry.ready:
			failed := entry.err != nil
			if alive, ok := entry.driver.(interface{ Alive() bool }); ok && !alive.Alive() {
				failed = true
			}
			if failed && time.Now().After(entry.retryAt) {
				entry.stop()
				delete(s.runtimes, a.ID)
				entry = nil
			}
		default:
		}
	}
	if entry == nil {
		runtimeCtx, stop := context.WithCancel(s.ctx)
		entry = &runtimeEntry{epoch: newID("epoch_"), ready: make(chan struct{}), stop: stop, retryAt: time.Now().Add(5 * time.Second)}
		s.runtimes[a.ID] = entry
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer close(entry.ready)
			driver, err := s.factory.Start(runtimeCtx, a, entry.epoch, func(event RuntimeEvent) { s.onEvent(a.ID, entry.epoch, event) })
			entry.driver, entry.err = driver, err
		}()
	}
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-entry.ready:
	}
	return entry.driver, entry.err
}
func (s *Service) Resume(ctx context.Context, c Caller, id string) (Identity, error) {
	a, err := s.account(ctx, c, id, "status")
	if err != nil {
		return Identity{}, err
	}
	d, err := s.ensure(ctx, a)
	if err != nil {
		return Identity{}, err
	}
	var identity Identity
	if err = d.Invoke(ctx, "resume", nil, &identity); err != nil {
		s.store.SetAuth(context.Background(), id, SafeCode(err))
		return identity, err
	}
	if err = s.store.BindIdentity(ctx, id, identity); err != nil {
		_ = d.Close()
		return Identity{}, err
	}
	return Identity{Name: identity.Name}, nil
}
func (s *Service) driver(ctx context.Context, c Caller, id, scope string) (Account, Driver, error) {
	a, err := s.account(ctx, c, id, scope)
	if err != nil {
		return a, nil, err
	}
	d, err := s.ensure(ctx, a)
	if err != nil {
		return a, nil, err
	}
	var identity Identity
	if err = d.Invoke(ctx, "resume", nil, &identity); err != nil {
		return a, nil, err
	}
	if err = s.store.BindIdentity(ctx, id, identity); err != nil {
		_ = d.Close()
		return a, nil, err
	}
	a, err = s.store.Account(ctx, id)
	return a, d, err
}
func (s *Service) Status(ctx context.Context, c Caller, id string) (Status, error) {
	a, err := s.account(ctx, c, id, "status")
	if err != nil {
		return Status{}, err
	}
	status := Status{Account: a, RuntimeState: "stopped", CheckedAt: nowMS(), Connectivity: "not_checked", Capabilities: map[string]string{"messages": "available_after_login", "events": "experimental", "voice": "pending_live_validation", "openchat": "joined_rooms_only"}}
	status.LastHealth, err = s.store.healthObservation(ctx, id)
	if err != nil {
		return Status{}, err
	}
	for _, scope := range []string{"messages.send", "schedule", "events", "resources", "call"} {
		if s.config.BlockedScopes[scope] {
			status.Capabilities[scope] = "disabled"
		} else {
			status.Capabilities[scope] = "available_after_login"
		}
	}
	s.mu.Lock()
	e := s.runtimes[id]
	if state := s.pollState[id]; state != "" && !s.config.BlockedScopes["events"] {
		status.Capabilities["events"] = state
	}
	s.mu.Unlock()
	if e != nil {
		status.Epoch = e.epoch
		select {
		case <-e.ready:
			if e.err == nil {
				status.RuntimeState = "running"
			} else {
				status.RuntimeState = "failed"
			}
			if alive, ok := e.driver.(interface{ Alive() bool }); ok && !alive.Alive() {
				status.RuntimeState = "failed"
			}
		default:
			status.RuntimeState = "starting"
		}
	}
	return status, nil
}
func (s *Service) SetEnabled(ctx context.Context, c Caller, id string, enabled bool) error {
	if err := s.store.SetEnabled(ctx, c, id, enabled); err != nil {
		return err
	}
	if !enabled {
		s.stopAccountCalls(id)
		s.mu.Lock()
		entry := s.runtimes[id]
		delete(s.runtimes, id)
		s.mu.Unlock()
		if entry != nil {
			entry.stop()
			select {
			case <-entry.ready:
				if entry.driver != nil {
					_ = entry.driver.Close()
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}
func (s *Service) Find(ctx context.Context, c Caller, id, query string, limit int) ([]Target, error) {
	return s.find(ctx, c, id, query, limit, "find_targets")
}
func (s *Service) find(ctx context.Context, c Caller, id, query string, limit int, method string) ([]Target, error) {
	if limit < 1 || limit > 50 {
		return nil, Fail("line_limit_invalid")
	}
	_, d, err := s.driver(ctx, c, id, "directory")
	if err != nil {
		return nil, err
	}
	if method == "find_targets" {
		t, e := s.aliasTarget(ctx, c, id, query)
		if e != nil {
			return nil, e
		}
		if t != nil {
			return []Target{*t}, nil
		}
	}
	var candidates []struct {
		HasAvatar     *bool  `json:"has_avatar"`
		Alias         string `json:"alias"`
		StatusMessage string `json:"status_message"`
		ID            string `json:"id"`
		Name          string `json:"name"`
		Kind          string `json:"kind"`
	}
	if err = d.Invoke(ctx, method, map[string]any{"query": query, "limit": limit}, &candidates); err != nil {
		return nil, err
	}
	if len(candidates) > limit {
		return nil, Fail("line_runtime_result_invalid")
	}
	out := make([]Target, 0, len(candidates))
	for _, x := range candidates {
		t, e := s.store.SaveTarget(ctx, id, x.ID, x.Name, x.Kind)
		if e != nil {
			return nil, e
		}
		t.HasAvatar = x.HasAvatar
		t.Alias = x.Alias
		t.StatusMessage = x.StatusMessage
		out = append(out, t)
	}
	return out, nil
}
func (s *Service) Read(ctx context.Context, c Caller, id, ref, cursor string, limit int) ([]Message, error) {
	if limit < 1 || limit > 100 {
		return nil, Fail("line_limit_invalid")
	}
	_, d, err := s.driver(ctx, c, id, "messages.read")
	if err != nil {
		return nil, err
	}
	t, err := s.store.Target(ctx, id, ref)
	if err != nil {
		return nil, err
	}
	var messages []Message
	if err = d.Invoke(ctx, "get_messages", map[string]any{"target": t.ExternalID, "kind": t.Kind, "cursor": cursor, "limit": limit}, &messages); err != nil {
		return nil, err
	}
	if len(messages) > limit {
		return nil, Fail("line_runtime_result_invalid")
	}
	for i := range messages {
		messages[i].Target = ref
		messages[i].ID, err = s.store.MessageRef(ctx, id, ref, messages[i].ID)
		if err != nil {
			return nil, err
		}
	}
	return messages, nil
}
func (s *Service) Send(ctx context.Context, c Caller, id, ref string, content Content, intent string, due int64, zone string, late int64) (Operation, error) {
	scope := "messages.send"
	if due > 0 {
		scope = "schedule"
	}
	a, err := s.account(ctx, c, id, scope)
	if err != nil {
		return Operation{}, err
	}
	if !s.store.Allowed(ctx, c, id, "messages.send") {
		return Operation{}, Fail("line_forbidden")
	}
	if a.Principal == "" {
		return Operation{}, Fail("line_login_required")
	}
	t, err := s.store.Target(ctx, id, ref)
	if err != nil {
		return Operation{}, err
	}
	if t.Kind != "user" {
		return Operation{}, Fail("line_target_write_unverified")
	}
	if original, found, e := s.store.LookupIntent(ctx, c, a, t, content, intent, due, zone, late); e != nil || found {
		return original, e
	}
	if due > 0 {
		if _, err = time.LoadLocation(zone); err != nil || zone == "" || zone == "Local" {
			return Operation{}, Fail("line_timezone_invalid")
		}
		if due <= nowMS() || late < 0 || late > 24*60*60*1000 {
			return Operation{}, Fail("line_schedule_invalid")
		}
	}
	if content.ResourceRef != "" {
		if _, e := s.account(ctx, c, id, "resources"); e != nil {
			return Operation{}, e
		}
		resource, e := s.store.Resource(ctx, c, id, content.ResourceRef)
		if e != nil {
			return Operation{}, e
		}
		if content.Text != "" || content.ReplyTo != "" {
			return Operation{}, Fail("line_resource_caption_unavailable")
		}
		if due >= resource.ExpiresAt {
			return Operation{}, Fail("line_resource_expired")
		}
	}
	if content.ReplyTo != "" {
		if _, err = s.store.ResolveMessage(ctx, id, ref, content.ReplyTo); err != nil {
			return Operation{}, err
		}
	}
	op, err := s.store.Enqueue(ctx, c, a, t, content, intent, due, zone, late)
	if err != nil {
		return op, err
	}
	if op.State == "queued" {
		s.launch(op)
	}
	return op, nil
}
func (s *Service) Receipt(ctx context.Context, c Caller, id string) (Operation, error) {
	op, err := s.store.Operation(ctx, id)
	if err != nil {
		return op, err
	}
	a, _ := s.store.Account(ctx, op.Account)
	if (op.Subject != c.Subject && !(c.Admin && a.Owner == c.Subject)) || !s.store.Allowed(ctx, c, op.Account, "status") {
		return Operation{}, Fail("line_forbidden")
	}
	return op, nil
}

type PairState struct {
	State     string `json:"state"`
	QR        string `json:"qr_data_url,omitempty"`
	PIN       string `json:"pin,omitempty"`
	ExpiresAt int64  `json:"expires_at"`
	Name      string `json:"name,omitempty"`
	Code      string `json:"code,omitempty"`
}

func (s *Service) Pair(ctx context.Context, c Caller, id string, start bool) (PairState, error) {
	return s.pairMode(ctx, c, id, start, false)
}
func (s *Service) pairMode(ctx context.Context, c Caller, id string, start, reauth bool) (PairState, error) {
	a, err := s.account(ctx, c, id, "status")
	if err != nil {
		return PairState{}, err
	}
	if !c.Admin || a.Owner != c.Subject {
		return PairState{}, Fail("line_admin_required")
	}
	d, err := s.ensure(ctx, a)
	if err != nil {
		return PairState{}, err
	}
	method := "pair_state"
	if start {
		method = "pair"
	}
	var state struct {
		PairState
		Principal string `json:"principal"`
	}
	if start && reauth {
		s.stopAccountCalls(id)
	}
	if err = d.Invoke(ctx, method, map[string]bool{"reauthenticate": reauth}, &state); err != nil {
		return PairState{}, err
	}
	if state.State == "pairing" {
		s.store.SetAuth(ctx, id, "pairing")
	}
	if state.State == "awaiting_commit" || state.State == "paired" {
		if err = s.store.BindIdentity(ctx, id, Identity{Principal: state.Principal, Name: state.Name}); err != nil {
			_ = d.Invoke(ctx, "pair_discard", nil, nil)
			s.store.SetAuth(ctx, id, "requires_login")
			return PairState{}, err
		}
	}
	if state.State == "awaiting_commit" {
		if err = d.Invoke(ctx, "pair_commit", map[string]string{"principal": state.Principal}, &state); err != nil {
			s.store.SetAuth(ctx, id, "requires_login")
			return PairState{}, err
		}
		s.store.SetAuth(ctx, id, "stored_login")
	}
	if state.State == "failed" {
		s.store.SetAuth(ctx, id, "requires_login")
		_ = d.Invoke(ctx, "pair_discard", nil, nil)
	}
	return state.PairState, nil
}
func (s *Service) StartScheduler() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-tick.C:
				s.Tick(s.ctx, nowMS())
				s.startPollers(s.ctx)
			}
		}
	}()
}
func (s *Service) Tick(ctx context.Context, now int64) {
	s.expireHandoffs(ctx)
	pending, err := s.store.Pending(ctx, now)
	if err != nil {
		return
	}
	for _, op := range pending {
		if op.State == "scheduled" {
			if now < op.DueAt {
				continue
			}
			// No exact punctual-delivery claim: absent an explicitly granted late window,
			// the first scheduler tick within five seconds is ordinary due-time dispatch.
			tolerance := op.LateWindow
			if tolerance == 0 {
				tolerance = 5000
			}
			if now-op.DueAt > tolerance {
				_, _ = s.store.transitionRevision(ctx, op, "held_late", "not_dispatched", "line_schedule_late")
				continue
			}
			ok, e := s.store.transitionRevision(ctx, op, "queued", "not_dispatched", "")
			if e != nil || !ok {
				continue
			}
			op.State = "queued"
			op.Revision++
		}
		s.launch(op)
	}
}
func (s *Service) launch(op Operation) {
	s.mu.Lock()
	if s.ctx.Err() != nil || s.busy[op.Account] {
		s.mu.Unlock()
		return
	}
	s.busy[op.Account] = true
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		defer func() { s.mu.Lock(); delete(s.busy, op.Account); s.mu.Unlock() }()
		s.dispatch(op)
	}()
}
func (s *Service) dispatch(op Operation) {
	ctx, cancel := context.WithTimeout(s.ctx, 40*time.Second)
	defer cancel()
	fresh, err := s.store.Operation(ctx, op.ID)
	if err != nil || fresh.State != "queued" {
		return
	}
	op = fresh
	caller := Caller{Subject: op.Subject}
	if op.DueAt > 0 {
		if _, err := s.account(ctx, caller, op.Account, "schedule"); err != nil {
			_, _ = s.store.Transition(context.Background(), op.ID, "queued", "held_unavailable", "not_dispatched", SafeCode(err), "")
			return
		}
	}
	a, d, err := s.driver(ctx, caller, op.Account, "messages.send")
	if err != nil {
		_, _ = s.store.Transition(context.Background(), op.ID, "queued", "held_unavailable", "not_dispatched", SafeCode(err), "")
		return
	}
	if a.Revision != op.BindingRevision {
		_, _ = s.store.Transition(ctx, op.ID, "queued", "held_binding", "not_dispatched", "line_binding_changed", "")
		return
	}
	if op.DueAt > 0 && !s.store.Allowed(ctx, caller, op.Account, "schedule") {
		return
	}
	t, err := s.store.Target(ctx, op.Account, op.Target)
	if err != nil {
		return
	}
	if op.DueAt > 0 {
		tolerance := op.LateWindow
		if tolerance == 0 {
			tolerance = 5000
		}
		if nowMS()-op.DueAt > tolerance {
			_, _ = s.store.transitionRevision(ctx, op, "held_late", "not_dispatched", "line_schedule_late")
			return
		}
	}
	ok, err := s.store.transitionRevision(ctx, op, "dispatching", "pending", "")
	if err != nil || !ok {
		return
	}
	content := op.Content
	if content.ReplyTo != "" {
		content.ReplyTo, err = s.store.ResolveMessage(ctx, op.Account, op.Target, content.ReplyTo)
		if err != nil {
			_, _ = s.store.Transition(context.Background(), op.ID, "dispatching", "failed_before_effect", "not_dispatched", SafeCode(err), "")
			return
		}
	}
	var result struct {
		MessageID string `json:"message_id"`
	}
	args := map[string]any{"operation_id": op.ID, "target": t.ExternalID, "content": content}
	if content.ResourceRef != "" {
		resource, e := s.store.Resource(ctx, caller, op.Account, content.ResourceRef)
		if e != nil {
			_, _ = s.store.Transition(context.Background(), op.ID, "dispatching", "failed_before_effect", "not_dispatched", SafeCode(e), "")
			return
		}
		args["resource"] = map[string]any{"data": resource.Data, "sha256": resource.SHA256, "mime": resource.MIME, "name": resource.Name}
	}
	err = d.Invoke(ctx, "send_message", args, &result)
	if err == nil && result.MessageID != "" {
		ref, refErr := s.store.MessageRef(context.Background(), op.Account, op.Target, result.MessageID)
		if refErr != nil {
			_, _ = s.store.Transition(context.Background(), op.ID, "dispatching", "accepted_by_line", "accepted_by_line", "line_message_reference_unavailable", "")
			return
		}
		_, _ = s.store.Transition(context.Background(), op.ID, "dispatching", "accepted_by_line", "accepted_by_line", "", ref)
		return
	}
	state, effect := "unknown", "unknown"
	var failure *Failure
	if errors.As(err, &failure) && failure.BeforeEffect {
		state, effect = "failed_before_effect", "not_dispatched"
	}
	code := SafeCode(err)
	if err == nil {
		code = "line_ack_missing"
	}
	_, _ = s.store.Transition(context.Background(), op.ID, "dispatching", state, effect, code, "")
}
func (s *Service) onEvent(account, epoch string, event RuntimeEvent) {
	s.mu.Lock()
	current := s.runtimes[account]
	valid := current != nil && current.epoch == epoch
	s.mu.Unlock()
	if !valid {
		return
	}
	// Pairing state remains private; never route QR/PIN through generic events.
	if event.Kind == "runtime_failed" {
		s.store.SetAuth(context.Background(), account, event.Code)
	}
}

type Request struct {
	Alias      string   `json:"alias,omitempty"`
	State      string   `json:"state,omitempty"`
	LocalTime  string   `json:"local_time,omitempty"`
	Since      int64    `json:"since,omitempty"`
	Until      int64    `json:"until,omitempty"`
	Resource   string   `json:"resource_ref,omitempty"`
	Message    string   `json:"message_ref,omitempty"`
	Attachment string   `json:"attachment_ref,omitempty"`
	Method     string   `json:"method"`
	Account    string   `json:"account_ref,omitempty"`
	Label      string   `json:"label,omitempty"`
	Target     string   `json:"target_ref,omitempty"`
	Query      string   `json:"query,omitempty"`
	Limit      int      `json:"limit,omitempty"`
	Cursor     string   `json:"cursor,omitempty"`
	Content    Content  `json:"content,omitempty"`
	Intent     string   `json:"intent_key,omitempty"`
	Operation  string   `json:"operation_id,omitempty"`
	DueAt      int64    `json:"due_at,omitempty"`
	Timezone   string   `json:"timezone,omitempty"`
	LateWindow int64    `json:"late_window_ms,omitempty"`
	Revision   uint64   `json:"expected_revision,omitempty"`
	Enabled    bool     `json:"enabled,omitempty"`
	Subject    string   `json:"subject,omitempty"`
	Scopes     []string `json:"scopes,omitempty"`
}

func (s *Service) Execute(ctx context.Context, c Caller, r Request) (any, error) {
	switch r.Method {
	case "get_message_page":
		return s.ReadPage(ctx, c, r)
	case "set_target_alias":
		return s.SetTargetAlias(ctx, c, r)
	case "query_operations":
		return s.QueryOperations(ctx, c, r)
	case "get_operation_detail":
		return s.OperationDetail(ctx, c, r.Operation)
	case "update_scheduled_message":
		return s.UpdateScheduledMessage(ctx, c, r)
	case "resolve_schedule_time":
		if _, e := s.account(ctx, c, r.Account, "schedule"); e != nil {
			return nil, e
		}
		return ResolveScheduleTime(r.LocalTime, r.Timezone)
	case "access":
		return s.Access(ctx, c, r.Account)
	case "list_grants":
		return s.Grants(ctx, c, r.Account)
	case "set_grants":
		return s.SetGrants(ctx, c, r)
	case "list_subscriptions":
		return s.ListSubscriptions(ctx, c, r.Account)
	case "download_resource":
		return s.DownloadResource(ctx, c, r.Account, r.Target, r.Message)
	case "read_resource":
		if !c.Admin {
			return nil, Fail("line_admin_required")
		}
		if _, err := s.account(ctx, c, r.Account, "resources"); err != nil {
			return nil, err
		}
		resource, err := s.store.Resource(ctx, c, r.Account, r.Resource)
		if err != nil {
			return nil, err
		}
		return map[string]any{"data": resource.Data, "name": resource.Name, "mime_type": resource.MIME, "size_bytes": resource.Size, "sha256": resource.SHA256}, nil
	case "import_resource":
		return s.ImportResource(ctx, c, r.Account, r.Attachment)
	case "reschedule_operation":
		return s.Reschedule(ctx, c, r.Operation, r.Revision, r.DueAt, r.Timezone)
	case "cancel_call":
		return s.CancelCall(ctx, c, r.Operation)
	case "get_call":
		return s.CallReceipt(ctx, c, r.Operation)
	case "accounts":
		return s.store.Accounts(ctx, c)
	case "subjects":
		if !c.Admin || s.config.Subjects == nil {
			return nil, Fail("line_admin_required")
		}
		return s.config.Subjects(), nil
	case "create_account":
		return s.Create(ctx, c, r.Label, false)
	case "transfer_owner":
		if !c.Admin || !strings.HasPrefix(r.Subject, "device:") || s.config.SubjectAllowed == nil || !s.config.SubjectAllowed(c.Subject) || !s.config.SubjectAllowed(r.Subject) {
			return nil, Fail("line_admin_required")
		}
		return s.store.TransferOwner(ctx, c, r.Account, r.Subject, r.Revision)
	case "adopt_legacy":
		return s.Create(ctx, c, r.Label, true)
	case "status":
		return s.Status(ctx, c, r.Account)
	case "resume":
		return s.Resume(ctx, c, r.Account)
	case "health":
		return s.Health(ctx, c, r.Account)
	case "reauthenticate":
		return s.pairMode(ctx, c, r.Account, true, true)
	case "pair":
		return s.Pair(ctx, c, r.Account, true)
	case "pair_state":
		return s.Pair(ctx, c, r.Account, false)
	case "find_openchats":
		if r.Limit > 20 {
			return nil, Fail("line_limit_invalid")
		}
		return s.find(ctx, c, r.Account, r.Query, r.Limit, "find_openchats")
	case "find_targets":
		return s.Find(ctx, c, r.Account, r.Query, r.Limit)
	case "subscribe":
		return s.Subscribe(ctx, c, r.Account, r.Target, r.Enabled)
	case "get_messages":
		return s.Read(ctx, c, r.Account, r.Target, r.Cursor, r.Limit)
	case "send_message":
		return s.Send(ctx, c, r.Account, r.Target, r.Content, r.Intent, 0, "", 0)
	case "schedule_message":
		if r.DueAt <= 0 {
			return nil, Fail("line_schedule_invalid")
		}
		return s.Send(ctx, c, r.Account, r.Target, r.Content, r.Intent, r.DueAt, r.Timezone, r.LateWindow)
	case "get_operation":
		return s.Receipt(ctx, c, r.Operation)
	case "list_operations":
		if _, err := s.account(ctx, c, r.Account, "status"); err != nil {
			return nil, err
		}
		return s.store.Operations(ctx, c, r.Account)
	case "cancel_operation":
		if _, err := s.Receipt(ctx, c, r.Operation); err != nil {
			return nil, err
		}
		err := s.store.Cancel(ctx, c, r.Operation, r.Revision)
		return map[string]bool{"cancelled": err == nil}, err
	case "set_enabled":
		if !c.Admin {
			return nil, Fail("line_admin_required")
		}
		err := s.SetEnabled(ctx, c, r.Account, r.Enabled)
		return map[string]bool{"updated": err == nil}, err
	case "grant":
		if !c.Admin || r.Subject == c.Subject || (!strings.HasPrefix(r.Subject, "session:") && !strings.HasPrefix(r.Subject, "device:")) || s.config.SubjectAllowed == nil || !s.config.SubjectAllowed(r.Subject) {
			return nil, Fail("line_admin_required")
		}
		err := s.store.Grant(ctx, c, r.Account, r.Subject, r.Scopes)
		return map[string]bool{"updated": err == nil}, err
	default:
		return nil, Fail("line_method_unavailable")
	}
}
func DecodeRequest(data []byte) (Request, error) {
	var r Request
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return r, Fail("line_request_invalid")
	}
	if d.Decode(&struct{}{}) != io.EOF {
		return r, Fail("line_request_invalid")
	}
	return r, nil
}
