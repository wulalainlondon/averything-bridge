// Package mediaservice owns media-only receipts, independent of task ledgers.
package mediaservice

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Request struct {
	ID        string                    `json:"id"`
	Thread    string                    `json:"thread"`
	Method    string                    `json:"method"`
	Operation string                    `json:"operation,omitempty"`
	Input     backend.VoiceMediaRequest `json:"input"`
}
type Result struct {
	backend.VoiceMediaResult
	OperationID     string `json:"operation_id"`
	Accepted        bool   `json:"accepted"`
	Dispatched      *bool  `json:"dispatched"`
	SuggestedAction string `json:"suggested_action"`
	Historical      bool   `json:"historical,omitempty"`
}
type Store struct {
	mu sync.Mutex
	db *sql.DB
}

func Open(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("media_store_directory_required")
	}
	dir := filepath.Join(root, "media-control")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "operations.db")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	f.Close()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS media_operations (thread TEXT NOT NULL, id TEXT NOT NULL, session TEXT NOT NULL, generation TEXT NOT NULL, voice TEXT NOT NULL, fingerprint TEXT NOT NULL, created INTEGER NOT NULL, result BLOB NOT NULL, PRIMARY KEY(thread,id));`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}
func (s *Store) Close() error { return s.db.Close() }

// Authorization and dispatch always use the caller's context. Recording a
// known result is a separate bounded local durability step: a caller timeout
// must not erase the command ID and turn it into an apparently new intent.
func (s *Store) saveResult(ctx context.Context, thread, id string, raw []byte) error {
	write, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	_, err := s.db.ExecContext(write, `UPDATE media_operations SET result=? WHERE thread=? AND id=?`, raw, thread, id)
	return err
}

func decorate(r backend.VoiceMediaResult, id string, accepted, dispatched bool) Result {
	x := Result{VoiceMediaResult: r, OperationID: id, Accepted: accepted, Dispatched: &dispatched, SuggestedAction: "none"}
	switch {
	case r.ErrorCode == "audio_route_unverified" || r.AudioCheck == "shared_media_route_unverified":
		x.SuggestedAction = "check_bluetooth_media_connection"
	case r.ErrorCode == "voice_media_player_not_ready":
		x.SuggestedAction = "music_unavailable_for_this_call"
	case r.ErrorCode == "autoplay_blocked":
		x.SuggestedAction = "tap_official_play"
	case r.ErrorCode == "music_open_required":
		x.SuggestedAction = "tap_official_open"
	case r.ErrorCode == "foreground_required":
		x.SuggestedAction = "bring_app_foreground"
	case r.AudioCheck == "system_media_volume_zero" || r.AudioCheck == "system_media_muted":
		x.SuggestedAction = "adjust_media_volume"
	case r.AudioCheck == "player_muted" || r.AudioCheck == "player_volume_zero":
		x.SuggestedAction = "adjust_player_volume"
	case r.AudioCheck == "bluetooth_call_media_unverified":
		x.SuggestedAction = "check_bluetooth_output"
		if r.Audio != nil && r.Audio.SpeakerRouteAvailable != nil && *r.Audio.SpeakerRouteAvailable {
			x.SuggestedAction = "try_phone_speaker"
		}
	case r.AudioCheck == "bluetooth_media_route_unknown":
		x.SuggestedAction = "check_bluetooth_output"
	case r.State == "queued" || r.State == "buffering":
		x.SuggestedAction = "wait"
	case r.State == "expired" || r.State == "voice_ended":
		x.SuggestedAction = "reconnect_voice"
	case r.State == "unknown":
		x.SuggestedAction = "check_operation"
	}
	return x
}

// Persist before invoking the handler. A missing result always remains unknown;
// neither retries nor a process restart invoke the effect a second time.
func (s *Store) Execute(ctx context.Context, o backend.MediaOrigin, q Request, run func() (backend.VoiceMediaResult, error)) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o.ThreadID == "" || o.Generation == "" || o.SessionID == "" || q.Thread != o.ThreadID {
		return Result{}, errors.New("media_origin_invalid")
	}
	id := q.ID
	if q.Method == "status" && q.Operation != "" {
		id = q.Operation
	}
	var session, generation, voice, fp string
	var created int64
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT session,generation,voice,fingerprint,created,result FROM media_operations WHERE thread=? AND id=?`, q.Thread, id).Scan(&session, &generation, &voice, &fp, &created, &raw)
	encoded, _ := json.Marshal([]any{q.Method, q.Input})
	hash := sha256.Sum256(encoded)
	fingerprint := hex.EncodeToString(hash[:])
	if err == nil {
		if session != o.SessionID {
			return Result{}, errors.New("media_origin_invalid")
		}
		if q.Method != "status" && fp != fingerprint {
			return Result{}, errors.New("media_intent_conflict")
		}
		var old Result
		if json.Unmarshal(raw, &old) != nil {
			return Result{}, errors.New("media_receipt_corrupt")
		}
		if q.Method != "status" {
			return historical(old), nil
		}
		// A proven refusal has no device command to observe. Keep its exact
		// cause even after a new track, voice generation or process restart.
		if old.State == "failed" && old.Dispatched != nil && !*old.Dispatched {
			return historical(old), nil
		}
		if generation != o.Generation || voice != o.VoiceID || time.Now().UnixMilli()-created > 24*time.Hour.Milliseconds() {
			old.State = "expired"
			old.Observed = false
			old.PlaybackConfirmed = false
			old.ActionConfirmed = false
			old.Audio = nil
			old.AudioCheck = "stale"
			old.Historical = true
			old.SuggestedAction = "reconnect_voice"
			return historical(old), nil
		}
		// Never let a completed receipt refresh itself from some newer command.
		fresh, e := run()
		if e != nil {
			if e.Error() == "media_voice_expired" {
				old.State = "voice_ended"
				old = historical(old)
				old.SuggestedAction = "reconnect_voice"
				return old, nil
			}
			return historical(old), nil
		}
		if fresh.CommandID != "" && fresh.CommandID == old.CommandID {
			old = decorate(fresh, id, true, old.Dispatched != nil && *old.Dispatched)
			b, _ := json.Marshal(old)
			if e = s.saveResult(ctx, q.Thread, id, b); e != nil {
				return Result{}, e
			}
			return old, nil
		}
		if fresh.CommandID != "" && fresh.CommandID != old.CommandID {
			old.State = "superseded"
		}
		return historical(old), nil
	}
	if err != sql.ErrNoRows {
		return Result{}, err
	}
	if q.Method == "status" {
		if q.Operation != "" {
			return Result{}, errors.New("media_operation_not_found")
		}
		r, e := run()
		return decorate(r, "", false, false), e
	}
	var count, bytes int64
	if e := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(LENGTH(result)),0) FROM media_operations`).Scan(&count, &bytes); e != nil {
		return Result{}, e
	}
	// Receipts are not evicted to make space: doing so could revive an old intent.
	if count >= 4096 || bytes >= 16<<20 {
		return Result{}, errors.New("media_receipt_capacity")
	}
	unknown := decorate(backend.VoiceMediaResult{State: "unknown", AudioCheck: "unknown", ErrorCode: "media_outcome_unknown_do_not_replay"}, id, true, false)
	unknown.Dispatched = nil
	b, _ := json.Marshal(unknown)
	if _, e := s.db.ExecContext(ctx, `INSERT INTO media_operations(thread,id,session,generation,voice,fingerprint,created,result) VALUES(?,?,?,?,?,?,?,?)`, q.Thread, id, o.SessionID, o.Generation, o.VoiceID, fingerprint, time.Now().UnixMilli(), b); e != nil {
		return Result{}, e
	}
	r, e := run()
	if e != nil {
		var refused *backend.VoiceMediaNotDispatchedError
		if errors.As(e, &refused) && r.CommandID == "" {
			r.State = "failed"
			r.ErrorCode = refused.Error()
			r.Observed, r.PlaybackConfirmed, r.ActionConfirmed = false, false, false
			result := decorate(r, id, true, false)
			b, _ = json.Marshal(result)
			if saveErr := s.saveResult(ctx, q.Thread, id, b); saveErr != nil {
				return unknown, saveErr
			}
			return result, nil
		}
		// An error can arrive after delivery. Preserve uncertainty, never replay.
		return unknown, e
	}
	result := decorate(r, id, true, r.CommandID != "")
	b, _ = json.Marshal(result)
	if e = s.saveResult(ctx, q.Thread, id, b); e != nil {
		return unknown, e
	}
	return result, nil
}
func historical(r Result) Result {
	if r.Endpoint != nil {
		endpoint := *r.Endpoint
		endpoint.Verification = "historical_receipt"
		r.Endpoint = &endpoint
	}
	r.Historical = true
	r.Observed = false
	r.PlaybackConfirmed = false
	r.ActionConfirmed = false
	r.Audio = nil
	r.VoiceAudio = nil
	r.AudioCheck = "stale"
	if r.LastObservedMS > 0 && time.Now().UnixMilli()-r.LastObservedMS > 30000 {
		r.Observed = false
		r.PlaybackConfirmed = false
		r.ActionConfirmed = false
		r.Audio = nil
		r.AudioCheck = "stale"
		r.Historical = true
	}
	return r
}
