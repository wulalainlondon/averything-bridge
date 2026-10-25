package linebridge

import (
	"context"
	"database/sql"
	"errors"
)

// MessageRef binds a provider message identifier to one account and room. The
// provider ID remains encrypted and is resolved only at the SDK boundary.
func (s *Store) MessageRef(ctx context.Context, account, target, external string) (string, error) {
	if external == "" || len(external) > 128 {
		return "", Fail("line_message_invalid")
	}
	if _, err := s.Target(ctx, account, target); err != nil {
		return "", err
	}
	ref := "lm_" + s.digest(account + "\x00" + target + "\x00" + external)[:32]
	value, err := s.seal(account, external)
	if err != nil {
		return "", err
	}
	_, err = s.db.ExecContext(ctx, "INSERT OR IGNORE INTO line_message_refs VALUES(?,?,?,?)", ref, account, target, value)
	return ref, err
}
func (s *Store) ResolveMessage(ctx context.Context, account, target, ref string) (string, error) {
	var encoded []byte
	err := s.db.QueryRowContext(ctx, "SELECT external FROM line_message_refs WHERE ref=? AND account=? AND target=?", ref, account, target).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return "", Fail("line_message_not_found")
	}
	if err != nil {
		return "", err
	}
	var external string
	err = s.unseal(account, encoded, &external)
	return external, err
}

// SealEnvelope lets the existing Event Inbox own encrypted event bodies and
// checkpoints without introducing a second LINE event database.
func (s *Store) SealEnvelope(account string, value any) ([]byte, error) {
	return s.seal(account, value)
}
func (s *Store) OpenEnvelope(account string, value []byte, out any) error {
	return s.unseal(account, value, out)
}
func (s *Store) EventKey(account, key string) string {
	return s.digest("event\x00" + account + "\x00" + key)
}
