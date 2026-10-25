package goexec

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

const asyncReplyOpen = "<send_user_message_question_reply>"
const asyncReplyClose = "</send_user_message_question_reply>"

type nativeQuestionReply struct {
	Answer         string `json:"answer"`
	Question       string `json:"question"`
	QuestionItemID string `json:"questionItemId"`
}

type asyncScanStamp struct {
	path           string
	size, modified int64
}

type nativeAsyncItem struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Delivery  string `json:"delivery"`
	Questions []struct {
		Title   string   `json:"title"`
		Options []string `json:"options"`
	} `json:"questions"`
	Content json.RawMessage `json:"content"`
}

func asyncQuestionID(callID string, index int) string {
	data, _ := json.Marshal([]any{"request_user_input_async", callID, index})
	return string(data)
}

func asyncRequestID(threadID, questionID string) string {
	sum := sha256.Sum256([]byte(threadID + "\x00" + questionID))
	return "ui_async_" + hex.EncodeToString(sum[:])
}

// Native replies are typed records, not arbitrary text mentioning the marker.
// JSON encoding keeps answer text (including XML-like text) inside the record.
func parseAsyncReply(text string) []nativeQuestionReply {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, asyncReplyOpen) || !strings.HasSuffix(text, asyncReplyClose) {
		return nil
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(text, asyncReplyOpen), asyncReplyClose)
	var replies []nativeQuestionReply
	if json.Unmarshal([]byte(raw), &replies) != nil || len(replies) == 0 {
		return nil
	}
	for _, reply := range replies {
		var id []json.RawMessage
		var tool, call string
		var index int
		if json.Unmarshal([]byte(reply.QuestionItemID), &id) != nil || len(id) != 3 ||
			json.Unmarshal(id[0], &tool) != nil || tool != "request_user_input_async" ||
			json.Unmarshal(id[1], &call) != nil || call == "" ||
			json.Unmarshal(id[2], &index) != nil || index < 0 {
			return nil
		}
	}
	return replies
}

func (c *Codex) asyncTombstonePath(thread string) string {
	sum := sha256.Sum256([]byte(thread))
	return filepath.Join(c.dataDir, "codex-async-resolved", hex.EncodeToString(sum[:])+".json")
}

func (c *Codex) readAsyncResolved(thread string) ([]string, error) {
	if c.dataDir == "" {
		return nil, nil
	}
	f, err := os.Open(c.asyncTombstonePath(thread))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (8<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 8<<20 {
		return nil, fmt.Errorf("async question journal too large")
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// This journal contains only IDs, never answers. Canonical native user messages
// are also reconciled so an answer from another client closes the mobile card.
func (c *Codex) loadAsyncResolvedLocked(thread string) {
	if c.asyncResolved == nil {
		c.asyncResolved = map[string]bool{}
	}
	key := "loaded:" + thread
	if c.asyncResolved[key] {
		return
	}
	ids, err := c.readAsyncResolved(thread)
	if err != nil {
		// Do not replace unreadable history with an empty set and resurrect old
		// questions. Leave the damaged journal intact for explicit recovery.
		c.asyncResolved["unavailable:"+thread] = true
		log.Printf("[codex] async question journal unavailable: %v", err)
	} else {
		for _, id := range ids {
			c.asyncResolved[asyncRequestID(thread, id)] = true
		}
	}
	c.asyncResolved[key] = true
}

func (c *Codex) resolveAsyncReplies(s *session.Session, thread, text string) {
	replies := parseAsyncReply(text)
	if len(replies) == 0 {
		return
	}
	c.asyncMu.Lock()
	defer c.asyncMu.Unlock()
	c.loadAsyncResolvedLocked(thread)
	// Preserve existing durable tombstones, including questions outside the tail.
	ids, readErr := c.readAsyncResolved(thread)
	seen := map[string]bool{}
	changed := false
	for _, id := range ids {
		seen[id] = true
	}
	for _, reply := range replies {
		id := asyncRequestID(thread, reply.QuestionItemID)
		c.asyncResolved[id] = true
		if !seen[reply.QuestionItemID] {
			changed = true
			ids = append(ids, reply.QuestionItemID)
			seen[reply.QuestionItemID] = true
		}
		c.interMu.Lock()
		ci, exists := c.interactions[id]
		delete(c.interactions, id)
		c.interMu.Unlock()
		if exists {
			c.sink.Emit(backend.NewInteractionResolved(id, ci.payload.SessionID, "resolved"))
		}
	}
	if c.dataDir == "" || !changed || readErr != nil {
		return
	}
	data, err := json.Marshal(ids)
	path := c.asyncTombstonePath(thread)
	if err == nil && len(data) <= 8<<20 {
		err = os.MkdirAll(filepath.Dir(path), 0700)
	} else if err == nil {
		err = fmt.Errorf("async question journal too large")
	}
	if err == nil {
		var f *os.File
		f, err = os.CreateTemp(filepath.Dir(path), ".async-resolved-*")
		if err == nil {
			defer os.Remove(f.Name())
			if _, err = f.Write(data); err == nil {
				err = f.Sync()
			}
			closeErr := f.Close()
			if err == nil {
				err = closeErr
			}
			if err == nil {
				err = os.Rename(f.Name(), path)
			}
			if err == nil {
				dir, openErr := os.Open(filepath.Dir(path))
				if openErr != nil {
					err = openErr
				} else {
					err = dir.Sync()
					_ = dir.Close()
				}
			}
		}
	}
	if err != nil {
		log.Printf("[codex] persist async question resolution: %v", err)
	}
}

func (c *Codex) observeAsyncItem(s *session.Session, thread string, params json.RawMessage) {
	var p struct {
		TurnID string          `json:"turnId"`
		Item   nativeAsyncItem `json:"item"`
	}
	if json.Unmarshal(params, &p) != nil {
		return
	}
	c.applyAsyncItem(s, thread, p.Item, time.Now().UnixMilli())
	st := c.state(s.ID)
	st.mu.Lock()
	request, turn := st.reqID, st.currentTurnID
	owned := st.turnActive && !st.turnStartPending && st.threadID == thread && p.TurnID != "" && p.TurnID == turn
	st.mu.Unlock()
	observer, ok := c.taskProvider.(backend.TaskQuestionObserver)
	if !ok || !owned || p.Item.Delivery != "async" {
		return
	}
	for index := range p.Item.Questions {
		id := asyncRequestID(thread, asyncQuestionID(p.Item.ID, index))
		c.interMu.Lock()
		pending, exists := c.interactions[id]
		c.interMu.Unlock()
		if exists {
			observer.ObserveTaskQuestion(context.Background(), backend.TaskQuestionEvidence{Session: s, RequestID: request, ThreadID: thread, TurnID: turn, CallID: p.Item.ID, Payload: pending.payload})
		}
	}
}

func (c *Codex) applyAsyncItem(s *session.Session, thread string, item nativeAsyncItem, createdAt int64) {
	if s.State() == session.Closed {
		return
	}
	if strings.EqualFold(item.Type, "userMessage") {
		c.resolveAsyncReplies(s, thread, extractCodexText(item.Content))
		return
	}
	if !strings.EqualFold(item.Type, "agentMessage") || item.Delivery != "async" || item.ID == "" || thread == "" {
		return
	}
	// Child native threads reject direct user input. Never misroute their answers
	// to the parent's conversation merely because notifications share a session.
	st := c.state(s.ID)
	st.mu.Lock()
	root := firstNonEmpty(st.threadID, s.ResumeID())
	st.mu.Unlock()
	if root != thread {
		return
	}
	c.asyncMu.Lock()
	defer c.asyncMu.Unlock()
	c.loadAsyncResolvedLocked(thread)
	if c.asyncResolved["unavailable:"+thread] {
		c.sink.Emit(backend.NewSessionWarning(s.ID, "Native async question resolution history is unavailable; pending questions cannot be safely restored"))
		return
	}
	for index, question := range item.Questions {
		if strings.TrimSpace(question.Title) == "" {
			continue
		}
		qid := asyncQuestionID(item.ID, index)
		id := asyncRequestID(thread, qid)
		if c.asyncResolved[id] {
			continue
		}
		options := []backend.UserInputOption{}
		for _, label := range question.Options {
			options = append(options, backend.UserInputOption{ID: label, Label: label})
		}
		blocking := false
		payload := backend.UserInputPayload{RequestID: id, SessionID: s.ID, Source: backend.Codex,
			Kind: "request_user_input_async", ToolUseID: item.ID, RequestingAgent: "codex", CreatedAt: createdAt,
			IsBlocking: &blocking, Status: "pending", Questions: []backend.UserInputQuestion{{
				QuestionID: qid, Text: question.Title, Type: "question", FreeForm: true, Options: options,
			}}}
		c.interMu.Lock()
		_, exists := c.interactions[id]
		if !exists {
			c.interactions[id] = codexInteraction{payload: payload, nativeThread: thread}
		}
		c.interMu.Unlock()
		if !exists {
			c.sink.Emit(backend.NewUserInputRequest(payload))
		}
	}
}

func (c *Codex) PrepareAsyncReply(id string, answers map[string]any, cancelled bool) (string, string, bool, error) {
	c.interMu.Lock()
	ci, exists := c.interactions[id]
	c.interMu.Unlock()
	if !exists || ci.nativeThread == "" {
		return "", "", false, nil
	}
	if cancelled {
		return ci.payload.SessionID, "", true, fmt.Errorf("Native async questions have no cancellation RPC; answer or leave the question pending")
	}
	replies := []nativeQuestionReply{}
	for _, q := range ci.payload.Questions {
		answer, ok := answers[q.QuestionID].(string)
		if !ok || strings.TrimSpace(answer) == "" {
			return ci.payload.SessionID, "", true, fmt.Errorf("An explicit answer is required")
		}
		replies = append(replies, nativeQuestionReply{Answer: answer, Question: q.Text, QuestionItemID: q.QuestionID})
	}
	data, err := json.Marshal(replies)
	return ci.payload.SessionID, asyncReplyOpen + "\n" + string(data) + "\n" + asyncReplyClose, true, err
}

// Only canonical event/item and user response rows are read. Function-call
// snippets and embedded conversation/context snapshots are not pending asks.
func (c *Codex) ReconcileAsyncQuestions(s *session.Session) {
	if s.Backend() != backend.Codex || s.ResumeID() == "" || s.State() == session.Closed {
		return
	}
	thread := s.ResumeID()
	c.asyncMu.Lock()
	if c.asyncSessions == nil {
		c.asyncSessions = map[string]*session.Session{}
	}
	c.asyncSessions[thread] = s
	c.asyncMu.Unlock()
	path := c.findCodexSessionFile(thread)
	if path == "" {
		return
	}
	c.asyncScanMu.Lock()
	defer c.asyncScanMu.Unlock()
	fi, err := os.Stat(path)
	if err != nil {
		return
	}
	stamp := asyncScanStamp{path: path, size: fi.Size(), modified: fi.ModTime().UnixNano()}
	key := s.ID + "\x00" + thread
	if c.asyncScanned == nil {
		c.asyncScanned = map[string]asyncScanStamp{}
	}
	if c.asyncScanned[key] == stamp {
		return
	}
	r, closeFn, err := openCodexRollout(path)
	if err != nil {
		return
	}
	defer closeFn()
	// Stream the complete canonical rollout, not a chat-history tail: an old
	// unanswered question can precede hundreds of MB of continued model output.
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 32<<20)
	// First collect replies, then publish questions: answered history never
	// briefly appears as a new prompt/notification during a reconnect.
	items := []struct {
		item    nativeAsyncItem
		created int64
	}{}
	replyTexts := []string{}
	for scanner.Scan() {
		line := scanner.Bytes()
		if !bytes.Contains(line, []byte("send_user_message_question_reply")) &&
			!(bytes.Contains(line, []byte(`"async"`)) && bytes.Contains(line, []byte(`"questions"`))) {
			continue
		}
		var row codexHistoryRow
		if json.Unmarshal(line, &row) != nil {
			continue
		}
		if row.Type == "response_item" {
			p := parseCodexHistoryPayload(row.Payload)
			if p.Type == "message" && p.Role == "user" {
				text := extractCodexText(p.Content)
				if len(parseAsyncReply(text)) > 0 {
					replyTexts = append(replyTexts, text)
				}
			}
		} else if row.Type == "event_msg" {
			var event struct {
				Type string          `json:"type"`
				Item nativeAsyncItem `json:"item"`
			}
			if json.Unmarshal(row.Payload, &event) == nil && event.Type == "item_completed" {
				if strings.EqualFold(event.Item.Type, "userMessage") {
					text := extractCodexText(event.Item.Content)
					if len(parseAsyncReply(text)) > 0 {
						replyTexts = append(replyTexts, text)
					}
				}
				if event.Item.Delivery != "async" {
					continue
				}
				ts, _ := time.Parse(time.RFC3339Nano, row.Timestamp)
				items = append(items, struct {
					item    nativeAsyncItem
					created int64
				}{event.Item, ts.UnixMilli()})
			}
		}
		if len(items)+len(replyTexts) > 10000 {
			c.sink.Emit(backend.NewSessionWarning(s.ID, "Too many native async question records to reconcile safely"))
			return
		}
	}
	if err := scanner.Err(); err != nil {
		c.sink.Emit(backend.NewSessionWarning(s.ID, "Native async questions could not be fully reconciled: "+err.Error()))
		return
	}
	for _, text := range replyTexts {
		c.resolveAsyncReplies(s, thread, text)
	}
	for _, entry := range items {
		c.applyAsyncItem(s, thread, entry.item, entry.created)
	}
	c.asyncScanned[key] = stamp
}

// Check the exact session/native thread before any send. Never recover a stale
// question by forking or creating a replacement conversation.
func (c *Codex) validateAsyncReply(s *session.Session, requestID, content string) (string, error) {
	if !strings.HasPrefix(requestID, "ui_async_") {
		return "", nil
	}
	replies := parseAsyncReply(content)
	thread := s.ResumeID()
	if len(replies) != 1 || thread == "" || asyncRequestID(thread, replies[0].QuestionItemID) != requestID {
		return "", fmt.Errorf("Async reply no longer matches the original native conversation")
	}
	c.asyncMu.Lock()
	c.loadAsyncResolvedLocked(thread)
	resolved, unavailable := c.asyncResolved[requestID], c.asyncResolved["unavailable:"+thread]
	c.asyncMu.Unlock()
	if unavailable {
		return "", fmt.Errorf("Native question resolution history is unavailable; reply was not sent")
	}
	if resolved {
		return "", fmt.Errorf("Native question was already answered; duplicate reply was not sent")
	}
	return thread, nil
}

func (c *Codex) ensureExactAsyncThread(s *session.Session, st *codexState, thread string) error {
	st.ensureMu.Lock()
	defer st.ensureMu.Unlock()
	st.mu.Lock()
	have := st.threadID
	st.mu.Unlock()
	if have == thread {
		return nil
	}
	if have != "" || s.ResumeID() != thread {
		return fmt.Errorf("Async reply conversation changed")
	}
	params := map[string]any{"threadId": thread, "excludeTurns": true, "approvalPolicy": "never"}
	if err := c.applyPMThreadPolicy(s, params); err != nil {
		return err
	}
	raw, err := c.rpcCall("thread/resume", params, 15*time.Second)
	if err != nil {
		return err
	}
	if extractThreadID(raw, "") != thread {
		return fmt.Errorf("Async reply resume returned a different conversation")
	}
	st.mu.Lock()
	st.threadID = thread
	st.mu.Unlock()
	c.mu.Lock()
	c.threadToSession[thread] = s
	c.mu.Unlock()
	return nil
}
