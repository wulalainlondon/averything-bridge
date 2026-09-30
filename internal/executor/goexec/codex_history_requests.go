package goexec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// A transcript's native turn ID is not the Bridge command request ID. Persist
// their exact association from turn/start's RPC response before it is lost on
// process exit. Never reconstruct this association from content or timestamps.
type codexTurnRequests struct {
	Version  int               `json:"version"`
	ThreadID string            `json:"thread_id"`
	Requests map[string]string `json:"requests"`
}

func (c *Codex) turnRequestsPath(threadID string) string {
	sum := sha256.Sum256([]byte(threadID))
	return filepath.Join(c.dataDir, "codex-turn-requests", hex.EncodeToString(sum[:])+".json")
}

func (c *Codex) loadTurnRequests(threadID string) (codexTurnRequests, error) {
	r := codexTurnRequests{Version: 1, ThreadID: threadID, Requests: map[string]string{}}
	f, err := os.Open(c.turnRequestsPath(threadID))
	if os.IsNotExist(err) {
		return r, nil
	}
	if err != nil {
		return r, err
	}
	defer f.Close()
	const maxBytes = 8 << 20
	data, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return r, err
	}
	if len(data) > maxBytes {
		return r, fmt.Errorf("turn request journal too large")
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return r, err
	}
	if r.Version != 1 || r.ThreadID != threadID || r.Requests == nil {
		return r, fmt.Errorf("invalid turn request journal")
	}
	return r, nil
}

func (c *Codex) rememberTurnRequest(threadID, turnID, requestID string) error {
	return c.saveTurnRequest(threadID, turnID, requestID, false)
}

// Only an accepted turn/start response can transfer the synthetic observer
// identity to the submitting Bridge request. Real Bridge identities stay
// immutable, and merely observing activity never claims an external turn.
func (c *Codex) rememberAcceptedTurnRequest(threadID, turnID, requestID string) error {
	return c.saveTurnRequest(threadID, turnID, requestID, true)
}

func (c *Codex) saveTurnRequest(threadID, turnID, requestID string, accepted bool) error {
	if threadID == "" || turnID == "" || requestID == "" {
		return nil
	}
	c.historyRequestMu.Lock()
	defer c.historyRequestMu.Unlock()
	r, err := c.loadTurnRequests(threadID)
	if err != nil {
		return err
	}
	if previous, exists := r.Requests[turnID]; exists {
		if previous != requestID {
			if !accepted || previous != "codex_external_"+turnID {
				return fmt.Errorf("conflicting turn request association")
			}
		} else {
			return nil
		}
	}
	r.Requests[turnID] = requestID
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(data) > 8<<20 {
		return fmt.Errorf("turn request journal too large")
	}
	path := c.turnRequestsPath(threadID)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".turn-requests-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (c *Codex) withHistoryRequestIDs(messages []map[string]any, threadID string) []map[string]any {
	c.historyRequestMu.Lock()
	r, err := c.loadTurnRequests(threadID)
	c.historyRequestMu.Unlock()
	if err != nil {
		log.Printf("[codex] history request identity unavailable: %v", err)
		return messages
	}
	out := make([]map[string]any, len(messages))
	for i, message := range messages {
		out[i] = message
		turnID, _ := message["source_turn_id"].(string)
		if requestID := r.Requests[turnID]; requestID != "" {
			copy := make(map[string]any, len(message)+1)
			for key, value := range message {
				copy[key] = value
			}
			copy["request_id"] = requestID
			out[i] = copy
		}
	}
	return out
}
