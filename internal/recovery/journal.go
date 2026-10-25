package recovery

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const MaxObservations = 256
const maxJournalBytes = 512 * 1024

type Observation struct {
	At        int64    `json:"at"`
	SessionID string   `json:"session_id"`
	RequestID string   `json:"request_id,omitempty"`
	ThreadID  string   `json:"thread_id,omitempty"`
	TurnID    string   `json:"turn_id,omitempty"`
	Failure   Failure  `json:"failure"`
	Context   Context  `json:"context"`
	Decision  Decision `json:"decision"`
}

// Journal is a private, bounded observation log, not a recovery input store.
// Each executor owns one journal; all writes through it are serialized.
type Journal struct {
	mu   sync.Mutex
	Path string
}

func (j *Journal) Record(o Observation) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Path == "" {
		return nil
	}
	var records []Observation
	f, err := os.Open(j.Path)
	if err == nil {
		data, readErr := io.ReadAll(io.LimitReader(f, maxJournalBytes+1))
		_ = f.Close()
		if readErr != nil {
			return readErr
		}
		if len(data) > maxJournalBytes {
			return errors.New("recovery observation journal exceeds bound")
		}
		if err := json.Unmarshal(data, &records); err != nil {
			return errors.New("invalid recovery observation journal")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if len(records) >= MaxObservations {
		records = records[len(records)-(MaxObservations-1):]
	}
	records = append(records, o)
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	if len(data) > maxJournalBytes {
		return errors.New("recovery observation exceeds journal bound")
	}
	if err := os.MkdirAll(filepath.Dir(j.Path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(j.Path), ".recovery-observations-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(data); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp.Name(), j.Path)
}
