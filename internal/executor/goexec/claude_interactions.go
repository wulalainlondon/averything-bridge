package goexec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// AskUserQuestion interaction handling — the Go port of
// bridge/interactions.py + claude_interactions.py. When the Claude CLI emits an
// AskUserQuestion tool_use it blocks on stdin; we surface it as a
// user_input_request, hold it in `pending`, and write the app's answer back as a
// tool_result into the same session's stdin to resume the turn.

type pendingInteraction struct {
	payload   backend.UserInputPayload
	sessionID string
	toolUseID string
	doneCh    chan struct{}
	// resultCh is set when the interaction originated from the ask_user MCP tool
	// (the preferred path). RespondUserInput delivers the answer here instead of
	// writing a stdin tool_result, and the blocked MCP handler returns it to
	// Claude as the tool result — which Claude honors, unlike an injected answer
	// for the built-in AskUserQuestion. nil → legacy native-tool stdin path.
	resultCh chan interactionAnswer
}

// interactionAnswer is delivered to a waiting ask_user MCP handler.
type interactionAnswer struct {
	answers   map[string]any
	cancelled bool
}

// registerMCPInteraction registers an interaction raised by the ask_user MCP
// tool: it normalizes the questions, emits user_input_request to the app, and
// returns the payload plus a channel the handler blocks on for the answer.
func (c *Claude) registerMCPInteraction(sessionID string, input json.RawMessage) (backend.UserInputPayload, chan interactionAnswer) {
	payload := backend.UserInputPayload{
		RequestID:       "ui_" + randHex(12),
		SessionID:       sessionID,
		Source:          "claude",
		Kind:            "ask_user_question",
		Header:          interactionHeader(input),
		RequestingAgent: "ask_user",
		Questions:       normalizeQuestions(input),
		CreatedAt:       time.Now().UnixMilli(),
		Status:          "pending",
	}
	ch := make(chan interactionAnswer, 1)
	c.interMu.Lock()
	c.pending[payload.RequestID] = &pendingInteraction{payload: payload, sessionID: sessionID, resultCh: ch}
	c.interMu.Unlock()
	c.sink.Emit(backend.NewUserInputRequest(payload))
	c.observeDelegatedQuestion(sessionID, nil, payload)
	log.Printf("[%s] ask_user MCP → user_input_request %s (%d question(s))", sessionID, payload.RequestID, len(payload.Questions))
	return payload, ch
}

// RegisterUserInputRequest converts an AskUserQuestion tool_use into a
// user_input_request, stores it, and broadcasts it. Keyed by a fresh request_id
// (the app answers with that id; the tool_use_id is accepted as an alias).
func (c *Claude) RegisterUserInputRequest(s *session.Session, toolUseID, agent string, input json.RawMessage) <-chan struct{} {
	questions := normalizeQuestions(input)
	doneCh := make(chan struct{})
	payload := backend.UserInputPayload{
		RequestID:       "ui_" + randHex(12),
		SessionID:       s.ID,
		Source:          "claude",
		Kind:            "ask_user_question",
		Header:          interactionHeader(input),
		ToolUseID:       toolUseID,
		RequestingAgent: agent,
		Questions:       questions,
		CreatedAt:       time.Now().UnixMilli(),
		Status:          "pending",
	}
	c.interMu.Lock()
	c.pending[payload.RequestID] = &pendingInteraction{payload: payload, sessionID: s.ID, toolUseID: toolUseID, doneCh: doneCh}
	c.interMu.Unlock()

	c.sink.Emit(backend.NewUserInputRequest(payload))
	c.observeDelegatedQuestion(s.ID, s, payload)
	log.Printf("[%s] AskUserQuestion → user_input_request %s (%d question(s))", s.ID, payload.RequestID, len(questions))
	return doneCh
}

func (c *Claude) registerInteraction(s *session.Session, toolUseID, agent string, input json.RawMessage) {
	c.RegisterUserInputRequest(s, toolUseID, agent, input)
}

// RespondUserInput writes the app's answer back into the paused session's stdin
// as a tool_result and resolves the interaction. The id may be the request_id or
// the tool_use_id (alias). Returns false if no pending interaction matches.
func (c *Claude) RespondUserInput(id string, answers map[string]any, cancelled bool) bool {
	c.interMu.Lock()
	pi := c.pending[id]
	if pi == nil {
		for rid, p := range c.pending {
			if p.toolUseID == id {
				pi, id = p, rid
				break
			}
		}
	}
	if pi != nil {
		delete(c.pending, id)
	}
	c.interMu.Unlock()
	if pi == nil {
		return false
	}

	if pi.resultCh != nil {
		// ask_user MCP path: hand the answer to the blocked tool handler, which
		// returns it to Claude as the (honored) MCP tool result.
		pi.resultCh <- interactionAnswer{answers: answers, cancelled: cancelled}
	} else {
		// Legacy native AskUserQuestion path: write the answer back into the
		// paused session's stdin as a tool_result. Claude Code expects a
		// human-readable sentence keyed by question TEXT and option LABEL; the
		// JSON wrapper reads as "dismissed" (and headless mode rejects injected
		// answers entirely — that's why the MCP path above is preferred).
		c.mu.Lock()
		p := c.procs[pi.sessionID]
		c.mu.Unlock()
		output := buildUserInputResultText(pi.payload, answers, cancelled)
		if p != nil {
			if _, err := p.stdin.Write(toolResultMessageJSON(pi.toolUseID, output)); err == nil {
				_ = p.stdin.Flush()
			}
		}
	}
	if pi.doneCh != nil {
		close(pi.doneCh)
	}

	status := "resolved"
	if cancelled {
		status = "cancelled"
	}
	c.sink.Emit(backend.NewInteractionResolved(pi.payload.RequestID, pi.sessionID, status))
	return true
}

// dropInteraction removes a pending interaction without resolving it (used when
// the ask_user handler times out), and tells the app it expired.
func (c *Claude) dropInteraction(requestID string) {
	c.interMu.Lock()
	pi := c.pending[requestID]
	delete(c.pending, requestID)
	c.interMu.Unlock()
	if pi != nil {
		c.sink.Emit(backend.NewInteractionResolved(requestID, pi.sessionID, "expired"))
	}
}

// PendingInteractions returns the open interactions, optionally filtered by
// session. Never nil so the wire array is [] not null.
func (c *Claude) PendingInteractions(sessionID string) []backend.UserInputPayload {
	c.interMu.Lock()
	defer c.interMu.Unlock()
	out := []backend.UserInputPayload{}
	for _, p := range c.pending {
		if sessionID == "" || p.sessionID == sessionID {
			out = append(out, p.payload)
		}
	}
	return out
}

// cancelInteractionsFor drops every pending interaction for a session (its proc
// is being killed) and tells the app they're resolved, so no prompt dangles.
func (c *Claude) cancelInteractionsFor(sessionID string) {
	c.interMu.Lock()
	var ids []string
	for rid, p := range c.pending {
		if p.sessionID == sessionID {
			ids = append(ids, rid)
			if p.resultCh != nil {
				p.resultCh <- interactionAnswer{cancelled: true} // unblock the MCP handler
			}
			if p.doneCh != nil {
				close(p.doneCh)
			}
			delete(c.pending, rid)
		}
	}
	c.interMu.Unlock()
	for _, rid := range ids {
		c.sink.Emit(backend.NewInteractionResolved(rid, sessionID, "cancelled"))
	}
}

// buildUserInputResultText renders the answer in Claude Code's native
// AskUserQuestion tool_result shape: `User has answered your questions:
// "<question>"="<label>"[, ...]. You can now continue with the user's answers in
// mind.` The app sends answers keyed by question_id → option id(s); we resolve
// those to the question text + option label(s) from the stored payload so Claude
// recognizes the selection. Falls back to the raw key/value when an answer isn't
// keyed by a known question_id (e.g. free-form).
func buildUserInputResultText(payload backend.UserInputPayload, answers map[string]any, cancelled bool) string {
	const dismissed = "The user dismissed the question(s) without answering. Continue without their input."
	if cancelled {
		return dismissed
	}
	var parts []string
	answered := map[string]bool{}
	for _, q := range payload.Questions {
		v, ok := answers[q.QuestionID]
		if !ok {
			continue
		}
		answered[q.QuestionID] = true
		if labels := resolveOptionLabels(q, v); labels != "" {
			parts = append(parts, fmt.Sprintf("%q=%q", q.Text, labels))
		}
	}
	// Answers not matched to any known question_id (free-form or alternate keys).
	for k, v := range answers {
		if answered[k] {
			continue
		}
		if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
			parts = append(parts, fmt.Sprintf("%q=%q", k, s))
		}
	}
	if len(parts) == 0 {
		return dismissed
	}
	return "User has answered your questions: " + strings.Join(parts, ", ") +
		". You can now continue with the user's answers in mind."
}

// resolveOptionLabels turns an answer value (a single option id, a list of ids,
// or free-form text) into a human-readable label string, mapping option ids to
// their labels via the question's options.
func resolveOptionLabels(q backend.UserInputQuestion, v any) string {
	idToLabel := make(map[string]string, len(q.Options))
	for _, o := range q.Options {
		idToLabel[o.ID] = o.Label
	}
	label := func(s string) string {
		if l, ok := idToLabel[s]; ok && l != "" {
			return l
		}
		return s // already a label, or free-form text
	}
	switch x := v.(type) {
	case string:
		return label(x)
	case []any:
		var ls []string
		for _, it := range x {
			if s := strings.TrimSpace(fmt.Sprint(it)); s != "" {
				ls = append(ls, label(s))
			}
		}
		return strings.Join(ls, ", ")
	case []string:
		var ls []string
		for _, s := range x {
			ls = append(ls, label(s))
		}
		return strings.Join(ls, ", ")
	default:
		return strings.TrimSpace(fmt.Sprint(x))
	}
}

// toolResultMessageJSON builds the stream-json `user` frame carrying a
// tool_result for the paused AskUserQuestion tool_use, mirroring
// claude_interactions.py's _write_stream_json payload.
func toolResultMessageJSON(toolUseID, output string) []byte {
	type tr struct {
		Type      string `json:"type"`
		ToolUseID string `json:"tool_use_id"`
		Content   string `json:"content"`
	}
	type msg struct {
		Role    string `json:"role"`
		Content []tr   `json:"content"`
	}
	type frame struct {
		Type    string `json:"type"`
		Message msg    `json:"message"`
	}
	f := frame{Type: "user"}
	f.Message.Role = "user"
	f.Message.Content = []tr{{Type: "tool_result", ToolUseID: toolUseID, Content: output}}
	b, _ := json.Marshal(f)
	return append(b, '\n')
}

// --- AskUserQuestion input normalization (port of interactions.py) ----------

func normalizeQuestions(input json.RawMessage) []backend.UserInputQuestion {
	var cmd map[string]any
	if json.Unmarshal(input, &cmd) != nil || cmd == nil {
		cmd = map[string]any{}
	}
	rawQuestions, _ := cmd["questions"].([]any)
	if len(rawQuestions) == 0 {
		rawQuestions = []any{cmd} // treat the whole input as a single question
	}

	out := make([]backend.UserInputQuestion, 0, len(rawQuestions))
	for idx, raw := range rawQuestions {
		q, ok := raw.(map[string]any)
		if !ok {
			q = map[string]any{"text": toStr(raw)}
		}
		options := normalizeOptions(q["options"], q["choices"])
		qtype := firstStr(q, "type", "kind")
		multi := boolField(q, "multiSelect", "multi_select", "multiple")
		freeForm := boolField(q, "freeForm", "free_form", "allowFreeForm")
		if qtype == "" {
			switch {
			case multi:
				qtype = "multi_choice"
			case len(options) > 0:
				qtype = "choice"
			default:
				qtype = "question"
				freeForm = true
			}
		}
		qid := firstStr(q, "question_id", "id")
		if qid == "" {
			qid = "q" + strconv.Itoa(idx+1)
		}
		text := firstStr(q, "text", "question", "label")
		if text == "" {
			text = "Question"
		}
		out = append(out, backend.UserInputQuestion{
			QuestionID:  qid,
			Text:        text,
			Header:      firstStr(q, "header", "title"),
			Type:        qtype,
			Options:     options,
			MultiSelect: multi,
			FreeForm:    freeForm,
		})
	}
	return out
}

func normalizeOptions(raws ...any) []backend.UserInputOption {
	var list []any
	for _, r := range raws {
		if l, ok := r.([]any); ok {
			list = l
			break
		}
	}
	out := []backend.UserInputOption{}
	for idx, raw := range list {
		if o, ok := raw.(map[string]any); ok {
			label := firstStr(o, "label", "text", "value", "id")
			if label == "" {
				label = strconv.Itoa(idx)
			}
			out = append(out, backend.UserInputOption{
				ID:          optionID(o, idx),
				Label:       label,
				Description: firstStr(o, "description", "detail"),
				Recommended: boolField(o, "recommended", "isRecommended"),
			})
		} else {
			s := toStr(raw)
			id := s
			if id == "" {
				id = strconv.Itoa(idx)
			}
			out = append(out, backend.UserInputOption{ID: id, Label: s})
		}
	}
	return out
}

func optionID(o map[string]any, idx int) string {
	if v := firstStr(o, "id", "value", "label"); v != "" {
		return v
	}
	return strconv.Itoa(idx)
}

func interactionHeader(input json.RawMessage) string {
	var cmd map[string]any
	if json.Unmarshal(input, &cmd) != nil {
		return "Question"
	}
	if h := firstStr(cmd, "header", "title"); h != "" {
		return h
	}
	return "Question"
}

// --- small helpers ----------------------------------------------------------

func firstStr(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s := toStr(v); s != "" {
				return s
			}
		}
	}
	return ""
}

func boolField(m map[string]any, keys ...string) bool {
	for _, k := range keys {
		if v, ok := m[k].(bool); ok && v {
			return true
		}
	}
	return false
}

func toStr(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'g', -1, 64)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

func randHex(n int) string {
	b := make([]byte, (n+1)/2)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)[:n]
}

func (c *Claude) observeDelegatedQuestion(id string, s *session.Session, payload backend.UserInputPayload) {
	observer, ok := c.taskProvider.(backend.TaskQuestionObserver)
	if !ok {
		return
	}
	c.mu.Lock()
	p := c.procs[id]
	c.mu.Unlock()
	if p == nil {
		return
	}
	p.mu.Lock()
	request := p.reqID
	p.mu.Unlock()
	if s == nil && p.taskLease != nil {
		p.taskLease.mu.Lock()
		s = p.taskLease.s
		p.taskLease.mu.Unlock()
	}
	if s == nil || request == "" || s.ActiveQueuedID() != request {
		return
	}
	observer.ObserveTaskQuestion(context.Background(), backend.TaskQuestionEvidence{Session: s, RequestID: request, ThreadID: s.ResumeID(), CallID: payload.ToolUseID, Payload: payload})
}
