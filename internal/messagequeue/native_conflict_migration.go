package messagequeue

import (
	"database/sql"
	"fmt"
)

// Additive in the existing canonical DB. Queue2 markers remain quarantined;
// API4 tuple facts remain untouched. All combined writers use explicit columns.
func migrateNativeConflictSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`PRAGMA table_info(task_native_conflicts)`)
	if err != nil {
		return err
	}
	columns := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def sql.NullString
		if err = rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		columns[name] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, name := range []string{"thread_id", "turn_id"} {
		if !columns[name] {
			if _, err = tx.Exec(fmt.Sprintf("ALTER TABLE task_native_conflicts ADD COLUMN %s TEXT NOT NULL DEFAULT ''", name)); err != nil {
				return err
			}
		}
	}
	if _, err = tx.Exec(`CREATE TABLE IF NOT EXISTS task_native_conflict_evidence(session_id TEXT NOT NULL,request_id TEXT NOT NULL,thread_id TEXT NOT NULL,turn_id TEXT NOT NULL,PRIMARY KEY(session_id,request_id,thread_id,turn_id)); INSERT OR IGNORE INTO task_native_conflict_evidence(session_id,request_id,thread_id,turn_id) SELECT session_id,request_id,thread_id,turn_id FROM task_native_conflicts WHERE thread_id!='' AND turn_id!='';`); err != nil {
		return err
	}
	return tx.Commit()
}
