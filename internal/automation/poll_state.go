package automation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

func (s *Store) ClaimDuePoll(ctx context.Context, owner string, now int64, lease time.Duration) (Account, PollState, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Account{}, PollState{}, false, err
	}
	defer tx.Rollback()
	var account Account
	var state PollState
	var enabled, webhook, poll int
	var cursor string
	err = tx.QueryRowContext(ctx, `SELECT a.id,a.authority_instance_id,a.provider,a.external_account_id,a.display_name,
		a.credential_ref,a.app_secret_ref,a.verify_token_ref,a.enabled,a.webhook_enabled,a.webhook_status,
		a.webhook_checked_at,a.poll_enabled,a.poll_interval_seconds,a.created_at,a.updated_at,
		p.stream,p.cursor_json,p.etag,p.last_attempt_at,p.last_success_at,p.last_error_code,p.consecutive_failures,
		p.next_poll_at,p.lease_owner,p.lease_until,p.revision
		FROM connector_poll_state p JOIN connector_accounts a ON a.id=p.connector_account_id
		WHERE a.authority_instance_id=? AND a.enabled=1 AND a.poll_enabled=1 AND p.next_poll_at<=? AND p.lease_until<=?
		ORDER BY p.next_poll_at,a.id,p.stream LIMIT 1`, s.instanceID, now, now).Scan(
		&account.ID, &account.AuthorityInstanceID, &account.Provider, &account.ExternalAccountID, &account.DisplayName,
		&account.CredentialRef, &account.AppSecretRef, &account.VerifyTokenRef, &enabled, &webhook,
		&account.WebhookStatus, &account.WebhookCheckedAt, &poll, &account.PollIntervalSeconds,
		&account.CreatedAt, &account.UpdatedAt, &state.Stream, &cursor, &state.ETag, &state.LastAttemptAt,
		&state.LastSuccessAt, &state.LastErrorCode, &state.ConsecutiveFailures, &state.NextPollAt,
		&state.LeaseOwner, &state.LeaseUntil, &state.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, PollState{}, false, nil
	}
	if err != nil {
		return Account{}, PollState{}, false, err
	}
	account.Enabled, account.WebhookEnabled, account.PollEnabled = enabled == 1, webhook == 1, poll == 1
	state.AccountID, state.Cursor = account.ID, json.RawMessage(cursor)
	leaseUntil := now + lease.Milliseconds()
	result, err := tx.ExecContext(ctx, `UPDATE connector_poll_state SET lease_owner=?,lease_until=?,last_attempt_at=?,revision=revision+1
		WHERE connector_account_id=? AND stream=? AND lease_until<=?`, owner, leaseUntil, now, account.ID, state.Stream, now)
	if err != nil {
		return Account{}, PollState{}, false, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return Account{}, PollState{}, false, nil
	}
	state.LeaseOwner, state.LeaseUntil, state.LastAttemptAt, state.Revision = owner, leaseUntil, now, state.Revision+1
	if _, err := s.bump(tx); err != nil {
		return Account{}, PollState{}, false, err
	}
	return account, state, true, tx.Commit()
}

func (s *Store) CompletePoll(ctx context.Context, account Account, state PollState, cursor json.RawMessage, etag, errorCode string, now int64) error {
	if len(cursor) == 0 {
		cursor = state.Cursor
	}
	if !json.Valid(cursor) {
		return errors.New("poll cursor must be valid JSON")
	}
	failures := 0
	lastSuccess := now
	delay := time.Duration(account.PollIntervalSeconds) * time.Second
	if errorCode != "" {
		failures = state.ConsecutiveFailures + 1
		lastSuccess = state.LastSuccessAt
		backoff := time.Duration(1<<min(failures, 8)) * time.Minute
		if backoff > time.Hour {
			backoff = time.Hour
		}
		if backoff > delay {
			delay = backoff
		}
	}
	if delay < 30*time.Second {
		delay = 5 * time.Minute
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE connector_poll_state SET cursor_json=?,etag=?,last_success_at=?,
		last_error_code=?,consecutive_failures=?,next_poll_at=?,lease_owner='',lease_until=0,revision=revision+1
		WHERE connector_account_id=? AND stream=? AND lease_owner=?`, string(cursor), etag, lastSuccess,
		errorCode, failures, now+delay.Milliseconds(), account.ID, state.Stream, state.LeaseOwner)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return ErrConflict
	}
	if _, err := s.bump(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
