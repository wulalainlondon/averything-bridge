package linebridge

import (
	"context"
	"strings"
)

// TransferOwner retains the account/profile and every receipt. It requires the
// current owner to disable the account first. Existing sharing/subscriptions are
// revoked atomically, and the binding revision prevents old jobs from resuming
// under the replacement device. Pairing membership is checked by Service.
func (s *Store) TransferOwner(ctx context.Context, c Caller, id, subject string, revision uint64) (Account, error) {
	if !c.Admin || !strings.HasPrefix(subject, "device:") || subject == c.Subject {
		return Account{}, Fail("line_owner_transfer_invalid")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Account{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, "UPDATE line_accounts SET owner=?,revision=revision+1 WHERE id=? AND owner=? AND enabled=0 AND revision=?", subject, id, c.Subject, revision)
	if err != nil {
		return Account{}, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return Account{}, err
	}
	if n != 1 {
		return Account{}, Fail("line_owner_transfer_conflict")
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM line_grants WHERE account=?", id); err != nil {
		return Account{}, err
	}
	for _, scope := range scopes {
		if _, err = tx.ExecContext(ctx, "INSERT INTO line_grants VALUES(?,?,?)", id, subject, scope); err != nil {
			return Account{}, err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE line_subscriptions SET enabled=0 WHERE account=?", id); err != nil {
		return Account{}, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO line_grant_revisions VALUES(?,2) ON CONFLICT(account) DO UPDATE SET revision=revision+1", id); err != nil {
		return Account{}, err
	}
	if err = tx.Commit(); err != nil {
		return Account{}, err
	}
	return s.Account(ctx, id)
}
