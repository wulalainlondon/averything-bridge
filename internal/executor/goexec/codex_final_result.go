package goexec

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"everything-go/internal/session"
)

// FinalAnswerForRequest reads only the native final_answer of the exact Bridge
// request. Streaming text_chunk also contains commentary and is not a safe
// delegation result, especially after a Bridge restart.
func (c *Codex) FinalAnswerForRequest(s *session.Session, requestID string) (string, bool, error) {
	threadID := s.ResumeID()
	if threadID == "" || requestID == "" {
		return "", false, nil
	}
	c.historyRequestMu.Lock()
	requests, err := c.loadTurnRequests(threadID)
	c.historyRequestMu.Unlock()
	if err != nil {
		return "", false, err
	}
	turnID := ""
	for nativeID, bridgeID := range requests.Requests {
		if bridgeID == requestID {
			turnID = nativeID
			break
		}
	}
	if turnID == "" {
		return "", false, nil
	}
	path := c.findCodexSessionFile(threadID)
	if path == "" {
		return "", false, nil
	}
	r, closeFn, err := openCodexRollout(path)
	if err != nil {
		return "", false, err
	}
	defer closeFn()
	reader := bufio.NewReaderSize(r, 1<<20)
	currentTurn := ""
	var answers []string
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			var row codexHistoryRow
			if json.Unmarshal(line, &row) == nil {
				switch row.Type {
				case "turn_context", "event_msg":
					var event struct {
						Type   string `json:"type"`
						TurnID string `json:"turn_id"`
					}
					_ = json.Unmarshal(row.Payload, &event)
					if row.Type == "turn_context" || event.Type == "task_started" {
						currentTurn = event.TurnID
					} else if event.Type == "task_complete" || event.Type == "turn_aborted" {
						currentTurn = ""
					}
				case "response_item":
					if currentTurn != turnID {
						break
					}
					payload := parseCodexHistoryPayload(row.Payload)
					if payload.Type == "message" && payload.Role == "assistant" && payload.Phase == "final_answer" {
						if answer := strings.TrimSpace(extractCodexText(payload.Content)); answer != "" {
							answers = append(answers, answer)
						}
					}
				}
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return "", false, readErr
			}
			break
		}
	}
	if len(answers) == 0 {
		return "", false, nil
	}
	return strings.Join(answers, "\n\n"), true, nil
}
