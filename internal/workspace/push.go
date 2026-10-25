package workspace

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

// Endpoints belong to the authenticated credential/device, not a caller-supplied
// principal or the owner's legacy FCM device registry. Tokens are never public.
type pushEndpoint struct {
	Grant    Grant  `json:"grant"`
	Token    string `json:"token"`
	Platform string `json:"platform"`
}
type PushDelivery struct {
	ID, RoomID, NotificationID, Token, Platform string
	Grant                                       Grant
	MemberRevision                              uint64
	Attempts                                    int
}
type PushReceipt struct{ State, ProviderID, Reason string }
type PushSender interface {
	Send(context.Context, string, string, string, string, string) PushReceipt
}

func (s *Store) endpointID(a Actor) string {
	return "cw_push_" + s.keyed("push-endpoint", a.ID+"/"+a.DeviceID+"/"+a.CredentialHash)[:24]
}
func (s *Store) protectPush(id string, data []byte) ([]byte, error) {
	block, err := aes.NewCipher(s.secret)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, data, []byte("workspace-push/"+id)), nil
}
func (s *Store) unprotectPush(id string, data []byte) (pushEndpoint, error) {
	var e pushEndpoint
	block, err := aes.NewCipher(s.secret)
	if err != nil {
		return e, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(data) < aead.NonceSize() {
		return e, errors.New("push_endpoint_invalid")
	}
	b, err := aead.Open(nil, data[:aead.NonceSize()], data[aead.NonceSize():], []byte("workspace-push/"+id))
	if err != nil {
		return e, errors.New("push_endpoint_invalid")
	}
	err = json.Unmarshal(b, &e)
	return e, err
}
func (s *Store) pushMutation(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	var in struct {
		Token    string `json:"token"`
		Platform string `json:"platform"`
		Enabled  bool   `json:"enabled"`
	}
	if err := decode(c.Data, &in); err != nil {
		return Result{}, err
	}
	id := s.endpointID(a)
	if !in.Enabled {
		_, err := tx.ExecContext(ctx, "DELETE FROM push_subscriptions WHERE endpoint_id=? AND room_id=?", id, r.ID)
		return Result{}, err
	}
	if len(in.Token) < 20 || len(in.Token) > 4096 || strings.ContainsAny(in.Token, "\r\n\x00") || (in.Platform != "ios" && in.Platform != "android") {
		return Result{}, errors.New("push_endpoint_invalid")
	}
	var revision uint64
	if err := tx.QueryRowContext(ctx, "SELECT revision FROM members WHERE room_id=? AND principal_id=? AND active=1", r.ID, a.ID).Scan(&revision); err != nil {
		return Result{}, err
	}
	payload, _ := json.Marshal(pushEndpoint{Grant: Grant{PrincipalID: a.ID, DeviceID: a.DeviceID, CredentialHash: a.CredentialHash, Owner: a.Owner}, Token: in.Token, Platform: in.Platform})
	encrypted, err := s.protectPush(id, payload)
	if err != nil {
		return Result{}, err
	}
	// A token registered by a different identity must not remain subscribed to
	// that identity after a device/account switch. No token value enters logs.
	tokenHash := digest([]byte(in.Token))
	if _, err = tx.ExecContext(ctx, "DELETE FROM push_subscriptions WHERE endpoint_id IN (SELECT id FROM push_endpoints WHERE token_hash=? AND id<>?)", tokenHash, id); err != nil {
		return Result{}, err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM push_endpoints WHERE token_hash=? AND id<>?", tokenHash, id); err != nil {
		return Result{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO push_endpoints VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,token_hash=excluded.token_hash", id, a.ID, tokenHash, encrypted); err != nil {
		return Result{}, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO push_subscriptions VALUES(?,?,?) ON CONFLICT(endpoint_id,room_id) DO UPDATE SET member_revision=excluded.member_revision", id, r.ID, revision)
	return Result{}, err
}
func (s *Store) queuePush(ctx context.Context, tx *sql.Tx, r Room, principal, notice string) error {
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO push_jobs(id,endpoint_id,room_id,notification_id,member_revision,state,attempts,next_at,provider_id,reason)
 SELECT ? || '/' || p.endpoint_id,p.endpoint_id,p.room_id,?,p.member_revision,'queued',0,?,'','' FROM push_subscriptions p JOIN push_endpoints e ON e.id=p.endpoint_id WHERE p.room_id=? AND e.principal_id=?`, notice, notice, s.now().UnixMilli(), r.ID, principal)
	return err
}
func (s *Store) PushPending(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT id FROM push_jobs WHERE state='queued' AND next_at<=? ORDER BY next_at,id LIMIT 20", s.now().UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
func (s *Store) ClaimPush(ctx context.Context, id string) (PushDelivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PushDelivery{}, err
	}
	defer tx.Rollback()
	p, err := s.validatePush(ctx, tx, id)
	if err != nil {
		return p, err
	}
	result, err := tx.ExecContext(ctx, "UPDATE push_jobs SET state='dispatching',attempts=attempts+1 WHERE id=? AND state='queued' AND next_at<=?", id, s.now().UnixMilli())
	if err != nil {
		return p, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return p, errors.New("push_not_claimable")
	}
	p.Attempts++
	return p, tx.Commit()
}
func (s *Store) validatePush(ctx context.Context, q sqlReader, id string) (PushDelivery, error) {
	p := PushDelivery{ID: id}
	var endpoint string
	var payload []byte
	err := q.QueryRowContext(ctx, `SELECT j.endpoint_id,j.room_id,j.notification_id,j.member_revision,j.attempts,e.payload FROM push_jobs j JOIN push_endpoints e ON e.id=j.endpoint_id JOIN push_subscriptions sub ON sub.endpoint_id=e.id AND sub.room_id=j.room_id AND sub.member_revision=j.member_revision WHERE j.id=?`, id).Scan(&endpoint, &p.RoomID, &p.NotificationID, &p.MemberRevision, &p.Attempts, &payload)
	if err != nil {
		return p, errors.New("push_authorization_revoked")
	}
	e, err := s.unprotectPush(endpoint, payload)
	if err != nil {
		return p, err
	}
	a := actorForGrant(e.Grant)
	if _, _, err = s.access(ctx, q, a, p.RoomID, "read"); err != nil {
		return p, errors.New("push_authorization_revoked")
	}
	var revision uint64
	if err = q.QueryRowContext(ctx, "SELECT revision FROM members WHERE room_id=? AND principal_id=? AND active=1", p.RoomID, a.ID).Scan(&revision); err != nil || revision != p.MemberRevision {
		return p, errors.New("push_authorization_revoked")
	}
	notice, err := getEntity(ctx, q, p.RoomID, p.NotificationID)
	if err != nil {
		return p, err
	}
	body, err := bodyOf[Notification](notice)
	if err != nil || body.PrincipalID != a.ID || body.Read {
		return p, errors.New("push_notice_unavailable")
	}
	p.Token = e.Token
	p.Platform = e.Platform
	p.Grant = e.Grant
	return p, nil
}
func (s *Store) ValidatePush(ctx context.Context, id string) error {
	_, err := s.validatePush(ctx, s.db, id)
	return err
}
func (s *Store) FinishPush(ctx context.Context, p PushDelivery, receipt PushReceipt) error {
	state := receipt.State
	next := int64(0)
	if state == "retryable" && p.Attempts < 4 {
		state = "queued"
		next = s.now().Add(time.Duration(p.Attempts*p.Attempts) * time.Minute).UnixMilli()
	} else if state == "retryable" {
		state = "failed"
	}
	if state != "queued" && state != "accepted" && state != "unknown" && state != "failed" && state != "revoked" {
		return errors.New("push_receipt_invalid")
	}
	_, err := s.db.ExecContext(ctx, "UPDATE push_jobs SET state=?,next_at=?,provider_id=?,reason=? WHERE id=? AND state='dispatching'", state, next, receipt.ProviderID, receipt.Reason, p.ID)
	return err
}
func (s *Store) DiscardPush(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE push_jobs SET state='revoked',reason='push_authorization_unavailable' WHERE id=? AND state='queued'", id)
	return err
}
