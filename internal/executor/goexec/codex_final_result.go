package goexec

import (
	"bufio"
	"context"
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
	return c.finalForExactRequest(s.ResumeID(), requestID, "")
}
func (c *Codex) ExactQueueFinal(thread, request, turn string) (bool, error) {
	return c.ExactQueueFinalForReceipt(context.Background(), thread, request, turn, "")
}
func (c *Codex) finalForExactRequest(threadID, requestID, expectedTurn string) (string, bool, error) {
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
			if turnID != "" && turnID != nativeID {
				return "", false, errors.New("ambiguous native request mapping")
			}
			turnID = nativeID
		}
	}
	if turnID == "" {
		return "", false, nil
	}
	if expectedTurn != "" && expectedTurn != turnID {
		return "", false, errors.New("native request mapping mismatch")
	}
	path := ""
	if expectedTurn != "" {
		// Reconciliation never discovers runtime files. Only the provider-owned,
		// previously indexed exact thread can be read; legacy history stays separate.
		c.rolloutMu.Lock()
		if c.rolloutRoot == c.sessionsRoot {
			path = c.rolloutByID[threadID]
		}
		c.rolloutMu.Unlock()
		if path == "" {
			return "", false, errors.New("exact native final index unavailable")
		}
	} else {
		path = c.findCodexSessionFile(threadID)
	}
	if path == "" {
		return "", false, nil
	}
	r, closeFn, err := openCodexRollout(path)
	if err != nil {
		return "", false, err
	}
	defer closeFn()
	var input io.Reader = r
	if expectedTurn != "" {
		input = io.LimitReader(r, 64<<20)
	}
	reader := bufio.NewReaderSize(input, 1<<20)
	total := 0
	currentTurn := ""
	var answers []string
	for {
		var line []byte
		var readErr error
		if expectedTurn != "" {
			line, readErr = reader.ReadSlice('\n')
			if errors.Is(readErr, bufio.ErrBufferFull) {
				return "", false, errors.New("exact final line exceeds bound")
			}
		} else {
			line, readErr = reader.ReadBytes('\n')
		}
		total += len(line)
		if expectedTurn != "" && total >= 64<<20 {
			return "", false, errors.New("exact final read exceeds bound")
		}
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
							if expectedTurn != "" {
								return answer, true, nil
							}
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
