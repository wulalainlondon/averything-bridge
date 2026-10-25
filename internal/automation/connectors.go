package automation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"everything-go/internal/eventinbox"
)

type PollBatch struct {
	Events []eventinbox.Input
	Cursor json.RawMessage
	ETag   string
}

type ActionResult struct {
	ProviderResultID string
}

type SecretResolver interface {
	Resolve(reference string) (string, error)
}

type Connector interface {
	Provider() string
	Poll(context.Context, Account, PollState, SecretResolver) (PollBatch, error)
	Execute(context.Context, Account, Proposal, SecretResolver) (ActionResult, error)
}

var envNamePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,127}$`)

type EnvSecretResolver struct{}

func (EnvSecretResolver) Resolve(reference string) (string, error) {
	name := strings.TrimPrefix(strings.TrimSpace(reference), "env:")
	if !strings.HasPrefix(strings.TrimSpace(reference), "env:") || !envNamePattern.MatchString(name) {
		return "", errors.New("credential reference must use env:NAME")
	}
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("credential %s is unavailable", name)
	}
	return value, nil
}

type Manager struct {
	store      *Store
	resolver   SecretResolver
	connectors map[string]Connector
	now        func() time.Time
}

func NewManager(store *Store, resolver SecretResolver, connectors ...Connector) *Manager {
	manager := &Manager{store: store, resolver: resolver, connectors: make(map[string]Connector), now: time.Now}
	for _, connector := range connectors {
		if connector != nil {
			manager.connectors[connector.Provider()] = connector
		}
	}
	return manager
}

func (m *Manager) ResolveSecret(reference string) (string, error) {
	return m.resolver.Resolve(reference)
}

type EventPublisher func(context.Context, eventinbox.Input) error

func (m *Manager) PollOnce(ctx context.Context, owner string, publish EventPublisher) (bool, error) {
	now := m.now().UnixMilli()
	account, state, ok, err := m.store.ClaimDuePoll(ctx, owner, now, 2*time.Minute)
	if err != nil || !ok {
		return ok, err
	}
	connector := m.connectors[account.Provider]
	if connector == nil {
		_ = m.store.CompletePoll(ctx, account, state, nil, state.ETag, "unsupported_provider", now)
		return true, nil
	}
	batch, pollErr := connector.Poll(ctx, account, state, m.resolver)
	if pollErr != nil {
		_ = m.store.CompletePoll(ctx, account, state, nil, state.ETag, safeErrorCode(pollErr), now)
		return true, pollErr
	}
	for _, event := range batch.Events {
		if err := publish(ctx, event); err != nil {
			_ = m.store.CompletePoll(ctx, account, state, nil, state.ETag, "event_commit_failed", now)
			return true, err
		}
	}
	return true, m.store.CompletePoll(ctx, account, state, batch.Cursor, batch.ETag, "", now)
}

func (m *Manager) ExecuteOnce(ctx context.Context) (bool, error) {
	now := m.now().UnixMilli()
	proposal, ok, err := m.store.ClaimApprovedProposal(ctx, now)
	if err != nil || !ok {
		return ok, err
	}
	snapshot, err := m.store.Snapshot(ctx)
	if err != nil {
		_ = m.store.CompleteProposal(ctx, proposal.ID, "uncertain", "", "account_lookup_failed")
		return true, err
	}
	var account *Account
	for index := range snapshot.Accounts {
		if snapshot.Accounts[index].ID == proposal.AccountID {
			account = &snapshot.Accounts[index]
			break
		}
	}
	if account == nil || !account.Enabled {
		_ = m.store.CompleteProposal(ctx, proposal.ID, "failed", "", "account_unavailable")
		return true, nil
	}
	connector := m.connectors[account.Provider]
	if connector == nil {
		_ = m.store.CompleteProposal(ctx, proposal.ID, "failed", "", "unsupported_provider")
		return true, nil
	}
	result, execErr := connector.Execute(ctx, *account, proposal, m.resolver)
	if execErr != nil {
		_ = m.store.CompleteProposal(ctx, proposal.ID, "uncertain", "", safeErrorCode(execErr))
		return true, execErr
	}
	return true, m.store.CompleteProposal(ctx, proposal.ID, "succeeded", result.ProviderResultID, "")
}

func safeErrorCode(err error) string {
	value := strings.ToLower(strings.TrimSpace(err.Error()))
	value = regexp.MustCompile(`[^a-z0-9_.-]+`).ReplaceAllString(value, "_")
	value = strings.Trim(value, "_")
	if value == "" {
		return "connector_error"
	}
	if len(value) > 80 {
		value = value[:80]
	}
	return value
}
