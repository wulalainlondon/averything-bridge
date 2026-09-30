package automation

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var allowedActionTypes = map[string]bool{
	"facebook.page_post.publish": true,
	"facebook.comment.reply":     true,
	"instagram.comment.reply":    true,
	"threads.post.publish":       true,
}

type ProposalInput struct {
	ID             string
	AccountID      string
	WorkItemID     string
	RunID          string
	ActionType     string
	TargetID       string
	Payload        json.RawMessage
	DisplayPreview string
	ExpiresAt      int64
}

func canonicalPayload(raw json.RawMessage) ([]byte, error) {
	var value any
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return nil, errors.New("payload must be valid JSON")
	}
	canonical, err := json.Marshal(value)
	if err != nil || len(canonical) > 32*1024 {
		return nil, errors.New("payload exceeds 32 KB")
	}
	return canonical, nil
}

func proposalHash(actionType, targetID string, payload []byte) string {
	sum := sha256.Sum256(append([]byte(actionType+"\x00"+targetID+"\x00"), payload...))
	return hex.EncodeToString(sum[:])
}

func (s *Store) CreateProposal(ctx context.Context, in ProposalInput) (Proposal, uint64, error) {
	in.ID, in.AccountID, in.WorkItemID = strings.TrimSpace(in.ID), strings.TrimSpace(in.AccountID), strings.TrimSpace(in.WorkItemID)
	in.ActionType, in.TargetID = strings.TrimSpace(in.ActionType), strings.TrimSpace(in.TargetID)
	if in.ID == "" || in.AccountID == "" || in.WorkItemID == "" || !allowedActionTypes[in.ActionType] || in.TargetID == "" {
		return Proposal{}, 0, errors.New("invalid typed action proposal")
	}
	payload, err := canonicalPayload(in.Payload)
	if err != nil {
		return Proposal{}, 0, err
	}
	now := s.now().UnixMilli()
	if in.ExpiresAt <= now {
		in.ExpiresAt = s.now().Add(24 * time.Hour).UnixMilli()
	}
	hash := proposalHash(in.ActionType, in.TargetID, payload)
	proposal := Proposal{ID: in.ID, AuthorityInstanceID: s.instanceID, AccountID: in.AccountID,
		WorkItemID: in.WorkItemID, RunID: in.RunID, ActionType: in.ActionType, TargetID: in.TargetID,
		Payload: payload, PayloadHash: hash, DisplayPreview: strings.TrimSpace(in.DisplayPreview), Status: "proposed",
		ExpiresAt: in.ExpiresAt, IdempotencyKey: "action:" + hash, Version: 1, CreatedAt: now, UpdatedAt: now}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Proposal{}, 0, err
	}
	defer tx.Rollback()
	var provider string
	err = tx.QueryRowContext(ctx, `SELECT provider FROM connector_accounts WHERE id=? AND enabled=1`, in.AccountID).Scan(&provider)
	if errors.Is(err, sql.ErrNoRows) {
		return Proposal{}, 0, errors.New("connector account is unavailable")
	}
	if err != nil {
		return Proposal{}, 0, err
	}
	if (strings.HasPrefix(in.ActionType, "facebook.") && provider != "meta.facebook") ||
		(strings.HasPrefix(in.ActionType, "instagram.") && provider != "meta.instagram") ||
		(strings.HasPrefix(in.ActionType, "threads.") && provider != "meta.threads") {
		return Proposal{}, 0, errors.New("action type does not match connector provider")
	}
	var governed int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM external_event_routes
		WHERE enabled=1 AND connector_account_id=? AND work_item_id=? AND handling_mode='approved_action')`,
		in.AccountID, in.WorkItemID).Scan(&governed); err != nil || governed != 1 {
		return Proposal{}, 0, errors.New("work item is not governed by an approved-action route")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbound_action_proposals
		(id,authority_instance_id,connector_account_id,work_item_id,run_id,action_type,target_id,payload_json,payload_hash,
		display_preview,status,approved_by_device_id,approved_at,expires_at,executed_at,idempotency_key,provider_result_id,
		error_code,version,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,'proposed','',0,?,0,?,'','',1,?,?)`,
		proposal.ID, proposal.AuthorityInstanceID, proposal.AccountID, proposal.WorkItemID, proposal.RunID,
		proposal.ActionType, proposal.TargetID, string(payload), proposal.PayloadHash, proposal.DisplayPreview,
		proposal.ExpiresAt, proposal.IdempotencyKey, now, now)
	if err != nil {
		return Proposal{}, 0, err
	}
	revision, err := s.bump(tx)
	if err == nil {
		err = tx.Commit()
	}
	return proposal, revision, err
}

func (s *Store) DecideProposal(ctx context.Context, id string, expectedVersion uint64, deviceID, decision, payloadHash string) (Proposal, uint64, error) {
	if deviceID == "" || (decision != "approved" && decision != "rejected") {
		return Proposal{}, 0, errors.New("authenticated human decision is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Proposal{}, 0, err
	}
	defer tx.Rollback()
	proposal, err := getProposal(ctx, tx, id)
	if err != nil {
		return Proposal{}, 0, err
	}
	now := s.now().UnixMilli()
	if proposal.Version != expectedVersion || proposal.Status != "proposed" || proposal.ExpiresAt <= now || proposal.PayloadHash != payloadHash {
		return Proposal{}, 0, ErrConflict
	}
	proposal.Status, proposal.Version, proposal.UpdatedAt = decision, proposal.Version+1, now
	if decision == "approved" {
		proposal.ApprovedByDeviceID, proposal.ApprovedAt = deviceID, now
	}
	_, err = tx.ExecContext(ctx, `UPDATE outbound_action_proposals SET status=?,approved_by_device_id=?,approved_at=?,version=?,updated_at=? WHERE id=? AND version=?`,
		proposal.Status, proposal.ApprovedByDeviceID, proposal.ApprovedAt, proposal.Version, now, proposal.ID, expectedVersion)
	if err != nil {
		return Proposal{}, 0, err
	}
	revision, err := s.bump(tx)
	if err == nil {
		err = tx.Commit()
	}
	return proposal, revision, err
}

func (s *Store) ClaimApprovedProposal(ctx context.Context, now int64) (Proposal, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Proposal{}, false, err
	}
	defer tx.Rollback()
	var id string
	err = tx.QueryRowContext(ctx, `SELECT id FROM outbound_action_proposals WHERE status='approved' AND expires_at>? ORDER BY approved_at,id LIMIT 1`, now).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return Proposal{}, false, nil
	}
	if err != nil {
		return Proposal{}, false, err
	}
	proposal, err := getProposal(ctx, tx, id)
	if err != nil {
		return Proposal{}, false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE outbound_action_proposals SET status='executing',version=version+1,updated_at=? WHERE id=? AND status='approved'`, now, id)
	if err != nil {
		return Proposal{}, false, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return Proposal{}, false, nil
	}
	proposal.Status, proposal.Version, proposal.UpdatedAt = "executing", proposal.Version+1, now
	if _, err := s.bump(tx); err != nil {
		return Proposal{}, false, err
	}
	return proposal, true, tx.Commit()
}

func (s *Store) CompleteProposal(ctx context.Context, id, status, resultID, errorCode string) error {
	if status != "succeeded" && status != "failed" && status != "uncertain" {
		return errors.New("invalid execution result")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.now().UnixMilli()
	result, err := tx.ExecContext(ctx, `UPDATE outbound_action_proposals SET status=?,provider_result_id=?,error_code=?,executed_at=?,version=version+1,updated_at=? WHERE id=? AND status='executing'`,
		status, resultID, errorCode, now, now, id)
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

func getProposal(ctx context.Context, tx *sql.Tx, id string) (Proposal, error) {
	var proposal Proposal
	var payload string
	err := tx.QueryRowContext(ctx, `SELECT id,authority_instance_id,connector_account_id,work_item_id,run_id,action_type,target_id,
		payload_json,payload_hash,display_preview,status,approved_by_device_id,approved_at,expires_at,executed_at,idempotency_key,
		provider_result_id,error_code,version,created_at,updated_at FROM outbound_action_proposals WHERE id=?`, id).Scan(
		&proposal.ID, &proposal.AuthorityInstanceID, &proposal.AccountID, &proposal.WorkItemID, &proposal.RunID,
		&proposal.ActionType, &proposal.TargetID, &payload, &proposal.PayloadHash, &proposal.DisplayPreview, &proposal.Status,
		&proposal.ApprovedByDeviceID, &proposal.ApprovedAt, &proposal.ExpiresAt, &proposal.ExecutedAt,
		&proposal.IdempotencyKey, &proposal.ProviderResultID, &proposal.ErrorCode, &proposal.Version,
		&proposal.CreatedAt, &proposal.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Proposal{}, ErrNotFound
	}
	proposal.Payload = json.RawMessage(payload)
	return proposal, err
}
