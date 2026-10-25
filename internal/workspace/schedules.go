package workspace

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "time/tzdata"
)

// NextOccurrence uses a local wall-clock rule. Nonexistent DST times are
// skipped; when the wall time repeats, only its first absolute occurrence is
// eligible. The schedule revision is included in the persisted occurrence key.
func NextOccurrence(rule Schedule, after time.Time) (time.Time, error) {
	loc, err := time.LoadLocation(rule.Timezone)
	if err != nil || rule.Hour < 0 || rule.Hour > 23 || rule.Minute < 0 || rule.Minute > 59 {
		return time.Time{}, errors.New("schedule_time_invalid")
	}
	allowed := map[int]bool{}
	for _, day := range rule.Weekdays {
		if day < 0 || day > 6 {
			return time.Time{}, errors.New("schedule_weekday_invalid")
		}
		allowed[day] = true
	}
	local := after.In(loc)
	year, month, day := local.Date()
	for offset := 0; offset < 370; offset++ {
		date := time.Date(year, month, day+offset, 12, 0, 0, 0, loc)
		if len(allowed) > 0 && !allowed[int(date.Weekday())] {
			continue
		}
		y, m, d := date.Date()
		candidate := time.Date(y, m, d, rule.Hour, rule.Minute, 0, 0, loc)
		cy, cm, cd := candidate.In(loc).Date()
		if cy != y || cm != m || cd != d || candidate.In(loc).Hour() != rule.Hour || candidate.In(loc).Minute() != rule.Minute {
			continue
		}
		// time.Date may select either side of a fold. Search the surrounding
		// possible offset changes and deterministically choose the earliest.
		first := candidate
		for delta := -180; delta < 0; delta++ {
			other := candidate.Add(time.Duration(delta) * time.Minute)
			oy, om, od := other.In(loc).Date()
			if oy == y && om == m && od == d && other.In(loc).Hour() == rule.Hour && other.In(loc).Minute() == rule.Minute && other.Before(first) {
				first = other
			}
		}
		if first.After(after) {
			return first.UTC(), nil
		}
	}
	return time.Time{}, errors.New("schedule_next_unavailable")
}

// Missing wall times have no UTC instant. Persist their intended date/time,
// then append a skipped receipt at the next valid scheduler wake. No substitute
// time invokes the model, and restart cannot silently erase the omission.
func planScheduleNext(rule Schedule, after time.Time) (Schedule, error) {
	next, err := NextOccurrence(rule, after)
	if err != nil {
		return rule, err
	}
	loc, _ := time.LoadLocation(rule.Timezone)
	rule.NextUTC = next.UnixMilli()
	rule.PendingDSTSkips = nil
	local := after.In(loc)
	y, m, d := local.Date()
	ny, nm, nd := next.In(loc).Date()
	last := time.Date(ny, nm, nd, 12, 0, 0, 0, time.UTC)
	allowed := map[int]bool{}
	for _, day := range rule.Weekdays {
		allowed[day] = true
	}
	for offset := 0; offset < 370; offset++ {
		intended := time.Date(y, m, d+offset, 12, 0, 0, 0, time.UTC)
		if !intended.Before(last) {
			break
		}
		if len(allowed) > 0 && !allowed[int(intended.Weekday())] {
			continue
		}
		if offset == 0 && local.Hour()*60+local.Minute() >= rule.Hour*60+rule.Minute {
			continue
		}
		yr, mo, day := intended.Date()
		candidate := time.Date(yr, mo, day, rule.Hour, rule.Minute, 0, 0, loc)
		cy, cm, cd := candidate.In(loc).Date()
		if cy != yr || cm != mo || cd != day || candidate.In(loc).Hour() != rule.Hour || candidate.In(loc).Minute() != rule.Minute {
			rule.PendingDSTSkips = append(rule.PendingDSTSkips, fmt.Sprintf("%04d-%02d-%02dT%02d:%02d", yr, mo, day, rule.Hour, rule.Minute))
		}
	}
	return rule, nil
}
func (s *Store) scheduleMutation(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	if _, _, err := s.access(ctx, tx, a, r.ID, "schedule"); err != nil {
		return Result{}, err
	}
	if !a.Owner {
		return Result{}, errors.New("paired_owner_schedule_approval_required")
	}
	out := Result{}
	switch c.Action {
	case "save_schedule":
		var in Schedule
		if err := decode(c.Data, &in); err != nil {
			return out, err
		}
		var err error
		if in.Title, err = text(in.Title, 1, 160); err != nil {
			return out, err
		}
		if in.Instruction, err = text(in.Instruction, 1, 32000); err != nil {
			return out, err
		}
		if err = references(ctx, tx, r, in.References); err != nil {
			return out, err
		}
		agent, err := getEntity(ctx, tx, r.ID, in.AgentID)
		if err != nil || agent.Kind != "agent" {
			return out, errors.New("agent_unavailable")
		}
		if in.LateMinutes == 0 {
			in.LateMinutes = 10
		}
		if in.LateMinutes < 1 || in.LateMinutes > 60 {
			return out, errors.New("schedule_late_window_invalid")
		}
		in, err = planScheduleNext(in, s.now())
		if err != nil {
			return out, err
		}
		e := Entity{ID: s.entityID("schedule", a, c.MutationID), Kind: "schedule"}
		in.DefinitionRevision = 1
		if c.EntityID != "" {
			e, err = s.getForUpdate(ctx, tx, c, "schedule")
			if err != nil {
				return out, err
			}
			old, err := bodyOf[Schedule](e)
			if err != nil {
				return out, err
			}
			in.DefinitionRevision = old.DefinitionRevision + 1
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM grants WHERE entity_id=?", e.ID); err != nil {
			return out, err
		}
		if err = s.saveGrant(ctx, tx, e.ID, a, r.ACLRevision); err != nil {
			return out, err
		}
		e, err = s.put(ctx, tx, a, r, e, in)
		if err != nil {
			return out, err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO schedule_due VALUES(?,?,?,?) ON CONFLICT(schedule_id) DO UPDATE SET next_utc=excluded.next_utc,enabled=excluded.enabled", e.ID, r.ID, in.NextUTC, in.Enabled)
		return withEntity(out, e, err)
	case "set_schedule_enabled":
		e, err := s.getForUpdate(ctx, tx, c, "schedule")
		if err != nil {
			return out, err
		}
		rule, err := bodyOf[Schedule](e)
		if err != nil {
			return out, err
		}
		var in struct {
			Enabled bool `json:"enabled"`
		}
		if err = decode(c.Data, &in); err != nil {
			return out, err
		}
		rule.Enabled = in.Enabled
		rule.DefinitionRevision++
		rule, err = planScheduleNext(rule, s.now())
		if err != nil {
			return out, err
		}
		if in.Enabled {
			if err = references(ctx, tx, r, rule.References); err != nil {
				return out, err
			}
			if _, err = tx.ExecContext(ctx, "DELETE FROM grants WHERE entity_id=?", e.ID); err != nil {
				return out, err
			}
			if err = s.saveGrant(ctx, tx, e.ID, a, r.ACLRevision); err != nil {
				return out, err
			}
		}
		e, err = s.put(ctx, tx, a, r, e, rule)
		if err != nil {
			return out, err
		}
		_, err = tx.ExecContext(ctx, "UPDATE schedule_due SET enabled=?,next_utc=? WHERE schedule_id=?", rule.Enabled, rule.NextUTC, e.ID)
		return withEntity(out, e, err)
	case "resolve_occurrence":
		e, err := s.getForUpdate(ctx, tx, c, "occurrence")
		if err != nil {
			return out, err
		}
		o, err := bodyOf[Occurrence](e)
		if err != nil || o.State != "held" {
			return out, errors.New("occurrence_not_held")
		}
		var in struct {
			RunNow bool `json:"run_now"`
		}
		if err = decode(c.Data, &in); err != nil {
			return out, err
		}
		o.State = "skipped"
		o.Reason = "human_skipped_missed_occurrence"
		if in.RunNow {
			schedule, err := getEntity(ctx, tx, r.ID, o.ScheduleID)
			if err != nil || schedule.Kind != "schedule" {
				return out, errors.New("schedule_unavailable")
			}
			rule, err := bodyOf[Schedule](schedule)
			if err != nil || rule.DefinitionRevision != o.ScheduleRevision {
				return out, errors.New("schedule_revision_changed")
			}
			task, err := s.createTask(ctx, tx, a, r, e.ID+"/human-run", TaskInput{Title: rule.Title, Instruction: rule.Instruction, AgentID: rule.AgentID, References: rule.References}, "", "scheduled", 1, "", nil)
			if err != nil {
				return out, err
			}
			o.State = "queued"
			o.TaskID = task.ID
			o.Reason = "human_explicit_late_run"
		}
		e, err = s.put(ctx, tx, a, r, e, o)
		return withEntity(out, e, err)
	}
	return out, errors.New("workspace_action_unsupported")
}

// Tick schedules at most 20 due occurrences per pass. Each occurrence and its
// original prepared task are committed together. There is no cron subprocess,
// catch-up storm or implicit permission to send the generated content outside.
func (s *Store) Tick(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.now()
	rows, err := tx.QueryContext(ctx, "SELECT schedule_id,room_id FROM schedule_due WHERE enabled=1 AND next_utc<=? ORDER BY next_utc LIMIT 20", now.UnixMilli())
	if err != nil {
		return err
	}
	type key struct{ id, room string }
	keys := []key{}
	for rows.Next() {
		var k key
		if err = rows.Scan(&k.id, &k.room); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, k)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, k := range keys {
		e, err := getEntity(ctx, tx, k.room, k.id)
		if err != nil {
			return err
		}
		rule, err := bodyOf[Schedule](e)
		if err != nil {
			return err
		}
		g, err := grant(ctx, tx, e.ID)
		if err != nil {
			return err
		}
		a := actorForGrant(g)
		room, err := roomByID(ctx, tx, k.room)
		if err != nil {
			return err
		}
		due := time.UnixMilli(rule.NextUTC)
		loc, err := time.LoadLocation(rule.Timezone)
		if err != nil {
			return err
		}
		wall := due.In(loc).Format("2006-01-02T15:04")
		occurrenceID := "cw_occurrence_" + s.keyed("occurrence", fmt.Sprintf("%s/%d/%s", e.ID, rule.DefinitionRevision, wall))[:24]
		if _, oldErr := getEntity(ctx, tx, room.ID, occurrenceID); oldErr == nil {
			return errors.New("schedule_occurrence_conflict")
		}
		o := Occurrence{ScheduleID: e.ID, ScheduleRevision: rule.DefinitionRevision, WallTime: wall, DueAt: rule.NextUTC, State: "held"}
		_, _, accessErr := s.access(ctx, tx, a, room.ID, "schedule")
		if accessErr != nil || room.ACLRevision != g.ACLRevision {
			o.Reason = "schedule_authorization_changed"
		} else if now.Sub(due) > time.Duration(rule.LateMinutes)*time.Minute {
			o.Reason = "missed_schedule_outside_late_window"
		} else {
			if _, err = tx.ExecContext(ctx, "SAVEPOINT schedule_task"); err != nil {
				return err
			}
			task, taskErr := s.createTask(ctx, tx, a, room, occurrenceID+"/task", TaskInput{Title: rule.Title, Instruction: rule.Instruction, AgentID: rule.AgentID, References: rule.References}, "", "scheduled", 1, "", nil)
			if taskErr != nil {
				if _, err = tx.ExecContext(ctx, "ROLLBACK TO schedule_task"); err != nil {
					return err
				}
				o.Reason = taskErr.Error()
			} else {
				o.State = "queued"
				o.TaskID = task.ID
			}
			if _, err = tx.ExecContext(ctx, "RELEASE schedule_task"); err != nil {
				return err
			}
		}
		for _, gapWall := range rule.PendingDSTSkips {
			gapID := "cw_occurrence_" + s.keyed("occurrence", fmt.Sprintf("%s/%d/%s", e.ID, rule.DefinitionRevision, gapWall))[:24]
			if _, existing := getEntity(ctx, tx, room.ID, gapID); existing == nil {
				continue
			}
			gap := Occurrence{ScheduleID: e.ID, ScheduleRevision: rule.DefinitionRevision, WallTime: gapWall, DueAt: 0, State: "skipped", Reason: "nonexistent_local_time_dst_gap_no_utc_instant"}
			if _, err = s.put(ctx, tx, Actor{ID: "schedule:" + e.ID}, room, Entity{ID: gapID, Kind: "occurrence"}, gap); err != nil {
				return err
			}
		}
		if _, err = s.put(ctx, tx, Actor{ID: "schedule:" + e.ID}, room, Entity{ID: occurrenceID, Kind: "occurrence"}, o); err != nil {
			return err
		}
		if o.State == "held" {
			if err = s.notify(ctx, tx, Actor{ID: "schedule:" + e.ID}, room, g.PrincipalID, "schedule_held", occurrenceID); err != nil {
				return err
			}
		}
		// Missed occurrences do not enumerate every historical day. Advance to
		// the next future wall time, retaining this concrete missed receipt.
		rule, err = planScheduleNext(rule, now)
		if err != nil {
			return err
		}
		if _, err = s.put(ctx, tx, Actor{ID: "schedule:" + e.ID}, room, e, rule); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE schedule_due SET next_utc=? WHERE schedule_id=?", rule.NextUTC, e.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}
