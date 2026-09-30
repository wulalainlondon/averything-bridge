package workitems

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	defaultChangeLimit = 256
	maxChangeLimit     = 1000
	deviceCursorTTL    = 30 * 24 * time.Hour
	minimumChangeTail  = 512
)

type Diagnostics struct {
	Revision        uint64 `json:"revision"`
	Projects        int    `json:"projects"`
	Items           int    `json:"items"`
	ActiveRuns      int    `json:"active_runs"`
	RetainedChanges int    `json:"retained_changes"`
	ActiveDevices   int    `json:"active_devices"`
}

func (s *Store) Diagnostics(ctx context.Context) (Diagnostics, error) {
	var d Diagnostics
	queries := []struct {
		query string
		dest  any
	}{
		{"SELECT COALESCE(MAX(revision),0) FROM work_changes", &d.Revision},
		{"SELECT COUNT(*) FROM work_projects WHERE archived_at IS NULL", &d.Projects},
		{"SELECT COUNT(*) FROM work_items WHERE archived_at IS NULL", &d.Items},
		{"SELECT COUNT(*) FROM work_item_runs WHERE finished_at IS NULL", &d.ActiveRuns},
		{"SELECT COUNT(*) FROM work_changes", &d.RetainedChanges},
		{"SELECT COUNT(*) FROM work_device_cursors WHERE authority_instance_id=? AND updated_at>=?", &d.ActiveDevices},
	}
	for i, query := range queries {
		var err error
		if i == len(queries)-1 {
			err = s.db.QueryRowContext(ctx, query.query, s.instanceID, s.now().Add(-deviceCursorTTL).UnixMilli()).Scan(query.dest)
		} else {
			err = s.db.QueryRowContext(ctx, query.query).Scan(query.dest)
		}
		if err != nil {
			return d, err
		}
	}
	return d, nil
}

func (s *Store) SnapshotForDevice(ctx context.Context, deviceID string) (DeviceSnapshot, error) {
	base, err := s.Snapshot(ctx)
	if err != nil {
		return DeviceSnapshot{}, err
	}
	reads := make(map[string]uint64)
	if deviceID != "" {
		rows, err := s.db.QueryContext(ctx, `SELECT work_item_id,read_activity_revision
			FROM work_item_reads WHERE device_id=?`, deviceID)
		if err != nil {
			return DeviceSnapshot{}, err
		}
		for rows.Next() {
			var id string
			var revision uint64
			if err := rows.Scan(&id, &revision); err != nil {
				rows.Close()
				return DeviceSnapshot{}, err
			}
			reads[id] = revision
		}
		if err := rows.Close(); err != nil {
			return DeviceSnapshot{}, err
		}
	}
	result := DeviceSnapshot{Revision: base.Revision, Projects: base.Projects,
		SessionLinks: base.SessionLinks, Dependencies: base.Dependencies, Comments: base.Comments,
		Runs: base.Runs, Attachments: base.Attachments, Activities: base.Activities, Workflows: base.Workflows,
		Bootstraps: base.Bootstraps}
	result.Items = make([]ItemView, 0, len(base.Items))
	for _, item := range base.Items {
		unread := 0
		if item.ActivityRevision > reads[item.ID] {
			unread = 1
		}
		result.Items = append(result.Items, ItemView{WorkItem: item, Unread: unread})
	}
	return result, nil
}

// ChangesSince returns an ordered delta. compacted is true when the requested
// cursor predates the retained log and the caller must send a full snapshot.
func (s *Store) ChangesSince(ctx context.Context, since uint64, limit int) ([]Change, uint64, bool, error) {
	if limit <= 0 {
		limit = defaultChangeLimit
	}
	if limit > maxChangeLimit {
		limit = maxChangeLimit
	}
	var minRevision, maxRevision uint64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MIN(revision),0),COALESCE(MAX(revision),0)
		FROM work_changes`).Scan(&minRevision, &maxRevision); err != nil {
		return nil, 0, false, err
	}
	// A cache can legitimately be ahead after the Bridge data directory is
	// restored/reset while the stable authority identity remains configured.
	// There is no valid forward delta from that base; force an authoritative
	// snapshot so the client can replace the stale cache instead of looping.
	if since > maxRevision {
		return nil, maxRevision, true, nil
	}
	if minRevision > 0 && since+1 < minRevision {
		return nil, maxRevision, true, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT revision,entity,entity_id,kind,payload,created_at
		FROM work_changes WHERE revision>? ORDER BY revision LIMIT ?`, since, limit)
	if err != nil {
		return nil, maxRevision, false, err
	}
	defer rows.Close()
	changes := make([]Change, 0, limit)
	for rows.Next() {
		var change Change
		var payload string
		if err := rows.Scan(&change.Revision, &change.Entity, &change.EntityID, &change.Kind,
			&payload, &change.CreatedAt); err != nil {
			return nil, maxRevision, false, err
		}
		change.Payload = []byte(payload)
		changes = append(changes, change)
	}
	return changes, maxRevision, false, rows.Err()
}

func (s *Store) AckSync(ctx context.Context, deviceID string, delivered, acked uint64) error {
	if deviceID == "" {
		return errors.New("device id is required")
	}
	if acked > delivered {
		delivered = acked
	}
	var latest uint64
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(revision),0) FROM work_changes").Scan(&latest); err != nil {
		return err
	}
	if delivered > latest {
		delivered = latest
	}
	if acked > latest {
		acked = latest
	}
	now := s.now().UnixMilli()
	_, err := s.db.ExecContext(ctx, `INSERT INTO work_device_cursors
		(device_id,authority_instance_id,delivered_revision,acked_revision,updated_at)
		VALUES(?,?,?,?,?)
		ON CONFLICT(device_id,authority_instance_id) DO UPDATE SET
		delivered_revision=MAX(delivered_revision,excluded.delivered_revision),
		acked_revision=MAX(acked_revision,excluded.acked_revision),updated_at=excluded.updated_at`,
		deviceID, s.instanceID, delivered, acked, now)
	return err
}

func (s *Store) MarkRead(ctx context.Context, deviceID, itemID string, revision uint64) (ItemView, error) {
	if deviceID == "" {
		return ItemView{}, errors.New("device id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ItemView{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, itemID)
	if err != nil {
		return ItemView{}, err
	}
	if revision > item.ActivityRevision {
		revision = item.ActivityRevision
	}
	now := s.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_reads
		(device_id,work_item_id,read_activity_revision,updated_at) VALUES(?,?,?,?)
		ON CONFLICT(device_id,work_item_id) DO UPDATE SET
		read_activity_revision=MAX(read_activity_revision,excluded.read_activity_revision),
		updated_at=excluded.updated_at`, deviceID, itemID, revision, now); err != nil {
		return ItemView{}, err
	}
	if err := tx.Commit(); err != nil {
		return ItemView{}, err
	}
	unread := 0
	if revision < item.ActivityRevision {
		unread = 1
	}
	return ItemView{WorkItem: item, Unread: unread}, nil
}

func (s *Store) MutationResponse(ctx context.Context, deviceID, mutationID string) ([]byte, bool, error) {
	if deviceID == "" || mutationID == "" {
		return nil, false, nil
	}
	var response string
	err := s.db.QueryRowContext(ctx, `SELECT response FROM work_mutation_dedup
		WHERE device_id=? AND mutation_id=?`, deviceID, mutationID).Scan(&response)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return []byte(response), true, nil
}

func (s *Store) RememberMutation(ctx context.Context, deviceID, mutationID string, response []byte) error {
	if deviceID == "" || mutationID == "" {
		return errors.New("device and mutation id are required")
	}
	if len(response) == 0 {
		return errors.New("mutation response is required")
	}
	if len(response) > 256*1024 {
		return fmt.Errorf("mutation response exceeds 256 KiB")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO work_mutation_dedup
		(device_id,mutation_id,response,created_at) VALUES(?,?,?,?)
		ON CONFLICT(device_id,mutation_id) DO NOTHING`, deviceID, mutationID, string(response), s.now().UnixMilli())
	return err
}

// CompactChanges expires inactive cursor quorum members and prunes only the
// prefix acknowledged by every active device, while always keeping a recovery
// tail. Returning expired devices receive a snapshot.
func (s *Store) CompactChanges(ctx context.Context) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	cutoffTime := s.now().Add(-deviceCursorTTL).UnixMilli()
	if _, err := tx.ExecContext(ctx, `DELETE FROM work_device_cursors
		WHERE authority_instance_id=? AND updated_at<?`, s.instanceID, cutoffTime); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM work_mutation_dedup WHERE created_at<?`, cutoffTime); err != nil {
		return 0, err
	}
	var latest uint64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(revision),0) FROM work_changes").Scan(&latest); err != nil {
		return 0, err
	}
	if latest <= minimumChangeTail {
		return 0, tx.Commit()
	}
	retainFrom := latest - minimumChangeTail + 1
	var minAck sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MIN(acked_revision) FROM work_device_cursors
		WHERE authority_instance_id=?`, s.instanceID).Scan(&minAck); err != nil {
		return 0, err
	}
	deleteBefore := retainFrom
	if minAck.Valid && uint64(minAck.Int64)+1 < deleteBefore {
		deleteBefore = uint64(minAck.Int64) + 1
	}
	result, err := tx.ExecContext(ctx, "DELETE FROM work_changes WHERE revision<?", deleteBefore)
	if err != nil {
		return 0, err
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return removed, tx.Commit()
}
