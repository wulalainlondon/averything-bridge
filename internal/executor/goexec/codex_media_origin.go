package goexec

import (
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// Inputs arrive exclusively through the subscribed native provider connection.
// Thread environment variables are routing hints, never admission evidence.
type mediaOriginState struct {
	mu              sync.Mutex
	handoffs        map[[32]byte][]backend.MediaOrigin
	turns           map[string]backend.MediaOrigin
	commands        map[string]backend.MediaOrigin
	blocked         bool
	launcher        string
	pending         map[string]mediaPendingInput
	pendingCommands map[string][]string
}
type mediaPendingInput struct {
	hash   [32]byte
	origin backend.MediaOrigin
}

func mediaInputHash(input, transcript string) [32]byte {
	b, _ := json.Marshal([]string{input, transcript})
	return sha256.Sum256(b)
}
func (c *Codex) observeMediaHandoff(call *codexVoiceCall, raw json.RawMessage) {
	if call.mediaGeneration == "" || len(raw) > 128<<10 {
		return
	}
	var p struct {
		Item struct {
			Type       string                        `json:"type"`
			ID         string                        `json:"handoff_id"`
			Input      string                        `json:"input_transcript"`
			Transcript []struct{ Role, Text string } `json:"active_transcript"`
		} `json:"item"`
	}
	if json.Unmarshal(raw, &p) != nil || p.Item.Type != "handoff_request" || p.Item.ID == "" {
		return
	}
	lines := []string{}
	for _, e := range p.Item.Transcript {
		lines = append(lines, e.Role+": "+e.Text)
	}
	transcript := strings.Join(lines, "\n")
	input := p.Item.Input
	if input == "" {
		input = transcript
	}
	if input == "" {
		return
	}
	m := &c.mediaOrigins
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.handoffs == nil {
		m.handoffs = map[[32]byte][]backend.MediaOrigin{}
	}
	if len(m.handoffs) >= 1024 {
		m.blocked = true
		return
	}
	k := mediaInputHash(input, transcript)
	o := backend.MediaOrigin{SessionID: call.sessionID, ThreadID: call.threadID, VoiceID: call.voiceID, Generation: call.mediaGeneration, RequestID: p.Item.ID}
	for _, old := range m.handoffs[k] {
		if old == o {
			return
		}
	}
	m.handoffs[k] = append(m.handoffs[k], o)
	m.bindPendingLocked(k)
}

// Native user/command notifications may precede the raw handoff notification.
// Match only a unique unconsumed provider handoff to a unique native input.
// Retired, unconsumed handoffs remain candidates; they cannot be replaced by B.
func (m *mediaOriginState) bindPendingLocked(hash [32]byte) {
	index := -1
	count := 0
	for i, o := range m.handoffs[hash] {
		if o.TurnID == "" {
			index = i
			count++
		}
	}
	if count != 1 {
		return
	}
	o := m.handoffs[hash][index]
	key := ""
	count = 0
	for k, p := range m.pending {
		if p.hash == hash && p.origin.ThreadID == o.ThreadID && p.origin.SessionID == o.SessionID {
			if _, ok := m.turns[k]; !ok {
				key = k
				count++
			}
		}
	}
	if count != 1 {
		return
	}
	p := m.pending[key]
	o.TurnID = p.origin.TurnID
	m.handoffs[hash][index] = o
	m.turns[key] = o
	delete(m.pending, key)
	m.bindCommandsLocked(key, o)
}
func (m *mediaOriginState) bindCommandsLocked(key string, o backend.MediaOrigin) {
	for _, id := range m.pendingCommands[key] {
		ck := o.ThreadID + "|" + id
		if old, ok := m.commands[ck]; ok && (old.SessionID != o.SessionID || old.Generation != o.Generation) {
			continue
		}
		if len(m.commands) >= 4096 {
			m.blocked = true
			return
		}
		m.commands[ck] = o
	}
	delete(m.pendingCommands, key)
}

var mediaIntentInCommand = regexp.MustCompile(`^vop_[A-Za-z0-9_-]{8,100}$`)

func (c *Codex) observeMediaNative(s *session.Session, method string, raw json.RawMessage) {
	if method != "item/started" && method != "turn/started" {
		return
	}
	if len(raw) > 256<<10 {
		return
	}
	var p struct {
		ThreadID string `json:"threadId"`
		TurnID   string `json:"turnId"`
		Turn     struct {
			ID    string            `json:"id"`
			Items []json.RawMessage `json:"items"`
		} `json:"turn"`
		Item json.RawMessage `json:"item"`
	}
	if json.Unmarshal(raw, &p) != nil || p.ThreadID != s.ResumeID() {
		return
	}
	turn := firstNonEmpty(p.TurnID, p.Turn.ID)
	if turn == "" {
		return
	}
	items := p.Turn.Items
	if len(p.Item) > 0 {
		items = append(items, p.Item)
	}
	m := &c.mediaOrigins
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blocked {
		return
	}
	if m.turns == nil {
		m.turns = map[string]backend.MediaOrigin{}
		m.commands = map[string]backend.MediaOrigin{}
		m.pending = map[string]mediaPendingInput{}
		m.pendingCommands = map[string][]string{}
	}
	for _, rawItem := range items {
		var item struct {
			Type, Command, Source string
			CommandActions        []struct{ Type, Command string }
			Content               []struct{ Type, Text string }
		}
		if json.Unmarshal(rawItem, &item) != nil {
			continue
		}
		key := p.ThreadID + "|" + turn
		if item.Type == "userMessage" {
			c.voiceMu.Lock()
			liveVoice := c.voiceCalls[p.ThreadID]
			active := liveVoice != nil && liveVoice.sessionID == s.ID && liveVoice.mediaGeneration != ""
			c.voiceMu.Unlock()
			if !active {
				continue
			}
			var text strings.Builder
			for _, v := range item.Content {
				if v.Type == "text" {
					text.WriteString(v.Text)
				}
			}
			// Full, sole delegation envelope from a native userMessage. A keyword,
			// quoted transcript, history read or model message does not qualify.
			t := strings.TrimSpace(text.String())
			if !strings.HasPrefix(t, "<realtime_delegation>") || !strings.HasSuffix(t, "</realtime_delegation>") {
				continue
			}
			var delegation struct {
				XMLName    xml.Name
				Input      string `xml:"input"`
				Transcript string `xml:"transcript_delta"`
			}
			if xml.Unmarshal([]byte(t), &delegation) != nil || delegation.XMLName.Local != "realtime_delegation" {
				continue
			}
			if _, ok := m.turns[key]; ok {
				continue
			}
			if len(m.turns)+len(m.pending) >= 4096 {
				m.blocked = true
				return
			}
			hash := mediaInputHash(delegation.Input, delegation.Transcript)
			if old, ok := m.pending[key]; ok && old.hash != hash {
				m.blocked = true
				return
			}
			m.pending[key] = mediaPendingInput{hash: hash, origin: backend.MediaOrigin{SessionID: s.ID, ThreadID: p.ThreadID, TurnID: turn}}
			m.bindPendingLocked(hash)
		}
		if item.Type == "commandExecution" && (item.Source == "agent" || item.Source == "unifiedExecStartup") && len(item.CommandActions) == 1 && item.CommandActions[0].Type == "unknown" {
			argv, ok := simpleMediaCommand(item.CommandActions[0].Command)
			launcher := m.launcher
			if launcher == "" {
				home, _ := os.UserHomeDir()
				launcher = filepath.Join(home, ".local", "bin", "bridge-media")
			}
			if !ok || len(argv) < 3 || argv[0] != launcher {
				continue
			}
			id := ""
			count := 0
			for i := 1; i < len(argv); i++ {
				if argv[i] == "--id" && i+1 < len(argv) {
					id = argv[i+1]
					count++
					i++
				} else if strings.HasPrefix(argv[i], "--id=") {
					id = strings.TrimPrefix(argv[i], "--id=")
					count++
				}
			}
			if count != 1 || !mediaIntentInCommand.MatchString(id) {
				continue
			}
			if len(m.commands)+len(m.pendingCommands) >= 4096 {
				m.blocked = true
				return
			}
			if len(m.pendingCommands[key]) >= 64 {
				m.blocked = true
				return
			}
			m.pendingCommands[key] = append(m.pendingCommands[key], id)
			if o, ok := m.turns[key]; ok {
				m.bindCommandsLocked(key, o)
			}
		}
	}
}

// Parse a single literal invocation, never a shell program. Expansion, pipelines,
// redirects, substitutions and command chaining cannot establish CLI origin.
func simpleMediaCommand(command string) ([]string, bool) {
	var args []string
	var word strings.Builder
	quote := rune(0)
	escaped, started := false, false
	for _, r := range command {
		if escaped {
			word.WriteRune(r)
			escaped = false
			started = true
			continue
		}
		if quote == '\'' {
			if r == '\'' {
				quote = 0
			} else {
				word.WriteRune(r)
			}
			started = true
			continue
		}
		if r == '\\' {
			escaped = true
			started = true
			continue
		}
		if quote == '"' {
			if r == '"' {
				quote = 0
			} else if r == '$' || r == '`' {
				return nil, false
			} else {
				word.WriteRune(r)
			}
			started = true
			continue
		}
		switch r {
		case '\'', '"':
			quote = r
			started = true
		case ' ', '\t':
			if started {
				args = append(args, word.String())
				word.Reset()
				started = false
			}
		case '\n', '\r', ';', '|', '&', '<', '>', '(', ')', '$', '`':
			return nil, false
		default:
			word.WriteRune(r)
			started = true
		}
	}
	if quote != 0 || escaped {
		return nil, false
	}
	if started {
		args = append(args, word.String())
	}
	return args, len(args) > 0
}

func (c *Codex) ResolveMediaOrigin(thread, operation string) (backend.MediaOrigin, error) {
	m := &c.mediaOrigins
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.blocked {
		if o, ok := m.commands[thread+"|"+operation]; ok {
			return o, nil
		}
	}
	return backend.MediaOrigin{}, errors.New("origin_context_unavailable")
}
func (c *Codex) mediaOriginForTurn(thread, turn string) (backend.MediaOrigin, error) {
	m := &c.mediaOrigins
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.blocked {
		if o, ok := m.turns[thread+"|"+turn]; ok {
			return o, nil
		}
	}
	return backend.MediaOrigin{}, errors.New("origin_context_unavailable")
}
