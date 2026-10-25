// bridge-idle-watch is a one-shot companion for an already-running Bridge.
// It never restarts a service, starts/stops a native turn, or deploys anything.
// Its only write to Bridge is one durable, correlated notification to the
// explicitly selected session after a corroborated quiet window.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
	_ "modernc.org/sqlite"
)

type config struct {
	Bridge, Socket, DataDir, SessionsFile, Target, TargetThread, Authority, Job, StateFile string
	Poll, Quiet                                                                            time.Duration
	Once, Notify                                                                           bool
	NonBlockingProjectionFile                                                              string
}

type savedSession struct {
	Name     string `json:"name"`
	Backend  string `json:"backend"`
	ResumeID string `json:"resume_id"`
	UUID     string `json:"claude_uuid"`
}

func (s savedSession) thread() string {
	if s.ResumeID != "" {
		return s.ResumeID
	}
	return s.UUID
}

type sessionView struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Backend   string `json:"backend"`
	Streaming bool   `json:"is_streaming"`
	Queue     int    `json:"queue_length"`
}
type runtimeView struct {
	ID       string `json:"session_id"`
	Phase    string `json:"phase"`
	Queue    int    `json:"queue_length"`
	Request  string `json:"active_request_id"`
	Terminal string `json:"last_terminal_status"`
}
type bridgeSnapshot struct {
	Sessions          []sessionView
	Runtimes          map[string]runtimeView
	Streaming, Queued int
}
type threadView struct {
	ID     string `json:"id"`
	Status struct {
		Type string `json:"type"`
	} `json:"status"`
}
type tracked struct {
	Name    string `json:"name"`
	Request string `json:"request_id,omitempty"`
}
type report struct {
	Job        string             `json:"job"`
	Target     string             `json:"target_session"`
	Phase      string             `json:"phase"`
	UpdatedAt  int64              `json:"updated_at_ms"`
	QuietSince int64              `json:"quiet_since_ms,omitempty"`
	Tracked    map[string]tracked `json:"tracked"`
	Busy       []string           `json:"busy,omitempty"`
	Attention  []string           `json:"attention,omitempty"`
	Error      string             `json:"error_code,omitempty"`
	RequestID  string             `json:"notification_request_id,omitempty"`
}

func main() {
	c := config{}
	flag.StringVar(&c.Bridge, "bridge", "ws://127.0.0.1:8766/", "existing local Bridge")
	flag.StringVar(&c.Socket, "socket", "", "existing Codex daemon Unix socket")
	flag.StringVar(&c.DataDir, "data-dir", "", "Bridge runtime directory")
	flag.StringVar(&c.SessionsFile, "sessions-file", "", "saved sessions source")
	flag.StringVar(&c.Target, "target-session", "", "exact existing notification recipient")
	flag.StringVar(&c.TargetThread, "target-thread", "", "expected native identity of recipient")
	flag.StringVar(&c.Authority, "authority", "", "expected Bridge authority")
	flag.StringVar(&c.Job, "job", "", "stable one-shot job identity")
	flag.StringVar(&c.StateFile, "state", "", "task-owned checkpoint file")
	flag.DurationVar(&c.Poll, "poll", 30*time.Second, "check interval")
	flag.DurationVar(&c.Quiet, "quiet", 60*time.Second, "continuous corroborated idle window")
	flag.BoolVar(&c.Once, "once", false, "read-only preflight; no notification/checkpoint writes")
	flag.BoolVar(&c.Notify, "notify", false, "authorize one notification, not a deployment")
	flag.StringVar(&c.NonBlockingProjectionFile, "nonblocking-projections", "", "exact audited question projections; native activity and durable commands still block")
	flag.Parse()
	if err := validateConfig(c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, c); err != nil {
		fmt.Fprintln(os.Stderr, "idle-watch:", err)
		os.Exit(1)
	}
}

func validateConfig(c config) error {
	u, err := url.Parse(c.Bridge)
	if err != nil || u.Scheme != "ws" || u.Hostname() != "127.0.0.1" || u.User != nil || u.RawQuery != "" {
		return errors.New("loopback Bridge without URL credentials required")
	}
	if c.Socket == "" || c.DataDir == "" || c.SessionsFile == "" || c.Target == "" || c.TargetThread == "" || c.Authority == "" || c.Job == "" || len(c.Job) > 80 {
		return errors.New("socket, data-dir, sessions-file, target identities, authority and job are required")
	}
	if !c.Once && (!c.Notify || !filepath.IsAbs(c.StateFile)) {
		return errors.New("watch requires explicit -notify and an absolute -state path")
	}
	if c.Poll < 5*time.Second || c.Quiet < 30*time.Second {
		return errors.New("poll >= 5s and quiet >= 30s required")
	}
	return nil
}

func run(ctx context.Context, c config) error {
	r := report{Job: c.Job, Target: c.Target, Phase: "waiting", Tracked: map[string]tracked{}}
	if !c.Once {
		if raw, err := os.ReadFile(c.StateFile); err == nil {
			if json.Unmarshal(raw, &r) != nil || r.Job != c.Job || r.Target != c.Target {
				return errors.New("checkpoint_identity_mismatch")
			}
			if r.Phase == "notified" || r.Phase == "cancelled" {
				return nil
			}
			if r.Tracked == nil {
				r.Tracked = map[string]tracked{}
			}
		}
	}
	// A process restart cannot inherit evidence of an uninterrupted quiet window.
	r.QuietSince = 0
	for {
		if !c.Once {
			if _, err := os.Stat(c.StateFile + ".cancel"); err == nil {
				r.Phase = "cancelled"
				r.UpdatedAt = time.Now().UnixMilli()
				return saveReport(c.StateFile, r)
			}
		}
		cycle, cancel := context.WithTimeout(ctx, 45*time.Second)
		busy, attention, err := inspect(cycle, c, r.Tracked)
		cancel()
		r.UpdatedAt, r.Busy, r.Attention, r.Error = time.Now().UnixMilli(), busy, attention, ""
		if err != nil {
			r.Error = err.Error()
			r.QuietSince = 0
			r.Phase = "state_unknown"
		} else if len(busy) > 0 {
			r.QuietSince = 0
			r.Phase = "waiting"
		} else {
			if r.QuietSince == 0 {
				r.QuietSince = r.UpdatedAt
			}
			r.Phase = "quiet_window"
		}
		if c.Once {
			return json.NewEncoder(os.Stdout).Encode(r)
		}
		if err := saveReport(c.StateFile, r); err != nil {
			return err
		}
		if r.Phase == "quiet_window" && time.Duration(r.UpdatedAt-r.QuietSince)*time.Millisecond >= c.Quiet {
			// Recheck immediately before enqueueing. Notification is not a lease:
			// the recipient must check again before its actual deployment.
			check, stop := context.WithTimeout(ctx, 45*time.Second)
			busy, attention, err = inspect(check, c, r.Tracked)
			if err == nil && len(busy) == 0 {
				r.RequestID = "idle-watch-" + c.Job
				err = notify(check, c, r.RequestID, notification(attention, c))
				if err == nil {
					r.Phase = "notified"
					r.Attention = attention
					r.UpdatedAt = time.Now().UnixMilli()
					stop()
					return saveReport(c.StateFile, r)
				}
			}
			stop()
			r.QuietSince = 0
		}
		timer := time.NewTimer(c.Poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func inspect(ctx context.Context, c config, seen map[string]tracked) ([]string, []string, error) {
	projections := map[string]struct {
		RequestID string `json:"request_id"`
		ThreadID  string `json:"thread_id"`
	}{}
	if c.NonBlockingProjectionFile != "" {
		if err := readJSON(c.NonBlockingProjectionFile, &projections); err != nil {
			return nil, nil, errors.New("nonblocking_projection_audit_unavailable")
		}
	}
	var saved map[string]savedSession
	if err := readJSON(c.SessionsFile, &saved); err != nil {
		return nil, nil, errors.New("session_source_unavailable")
	}
	if saved[c.Target].thread() != c.TargetThread {
		return nil, nil, errors.New("target_thread_changed")
	}
	snapshot, err := readBridge(ctx, c)
	if err != nil {
		return nil, nil, err
	}
	targetFound := false
	byID := map[string]sessionView{}
	busy := []string{}
	projectedStreaming := 0
	for _, s := range snapshot.Sessions {
		byID[s.ID] = s
		if s.ID == c.Target {
			targetFound = true
		}
		v, ok := snapshot.Runtimes[s.ID]
		if !ok {
			return nil, nil, errors.New("runtime_coverage_incomplete")
		}
		projection, audited := projections[s.ID]
		ignoreProjection := audited && projection.RequestID != "" && v.Request == projection.RequestID &&
			v.Phase == "waiting" && projection.ThreadID != "" && projection.ThreadID == saved[s.ID].thread()
		if ignoreProjection {
			// An actual reply may use the same ID as the question. Even a settled
			// receipt disqualifies this classification; never ignore by ID prefix.
			db, err := sql.Open("sqlite", "file:"+filepath.Join(c.DataDir, "message_queue.sqlite")+"?mode=ro")
			if err != nil {
				return nil, nil, errors.New("projection_queue_unavailable")
			}
			var count int
			err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM queue_commands WHERE session_id=? AND request_id=?", s.ID, projection.RequestID).Scan(&count)
			db.Close()
			if err != nil {
				return nil, nil, errors.New("projection_queue_unavailable")
			}
			ignoreProjection = count == 0
		}
		if ignoreProjection && s.Streaming {
			projectedStreaming++
		}
		if (s.Streaming && !ignoreProjection) || s.Queue > 0 || v.Queue > 0 || (activePhase(v.Phase) && !ignoreProjection) {
			busy = append(busy, label(s.ID, s.Name))
			if s.ID != c.Target {
				seen[s.ID] = tracked{Name: s.Name, Request: v.Request}
			}
		}
	}
	if !targetFound {
		return nil, nil, errors.New("target_session_missing")
	}
	if snapshot.Queued > 0 {
		busy = append(busy, "Bridge queue")
	}
	queueAttention, err := durableQueue(ctx, c.DataDir, c.Target, seen)
	if err != nil {
		return nil, nil, err
	}
	for _, item := range queueAttention {
		if item.state == "uncertain" {
			continue
		}
		busy = append(busy, label(item.id, saved[item.id].Name)+" (durable queue)")
	}
	streaming := 0
	for _, s := range snapshot.Sessions {
		if s.Streaming {
			streaming++
		}
	}
	// sessions_list also includes runtime presentation, whereas request_status
	// counts actual Session actors. Subtract only the exact audited projections.
	if streaming-projectedStreaming != snapshot.Streaming {
		return nil, nil, errors.New("bridge_snapshot_changed")
	}
	p, err := openNative(ctx, c.Socket)
	if err != nil {
		return nil, nil, errors.New("native_daemon_unavailable")
	}
	defer p.close()
	ids, err := p.loaded(ctx)
	if err != nil {
		return nil, nil, err
	}
	all := map[string]bool{c.TargetThread: true}
	for id, projection := range projections {
		if saved[id].thread() != projection.ThreadID || projection.ThreadID == "" {
			return nil, nil, errors.New("nonblocking_projection_thread_changed")
		}
		all[projection.ThreadID] = true
	}
	for _, id := range ids {
		all[id] = true
	}
	for id := range seen {
		if thread := saved[id].thread(); thread != "" {
			all[thread] = true
		}
	}
	// A goal can continue between apparently completed turns. Include goals
	// last reported active, then verify their current status with native RPC.
	var goals struct {
		Items map[string]struct {
			Goal *struct {
				Status string `json:"status"`
			} `json:"goal"`
		} `json:"items"`
	}
	if err := readJSON(filepath.Join(c.DataDir, "goal_snapshots.json"), &goals); err != nil {
		return nil, nil, errors.New("goal_state_unavailable")
	}
	for id, v := range goals.Items {
		if id != c.Target && v.Goal != nil && v.Goal.Status == "active" {
			if thread := saved[id].thread(); thread != "" {
				all[thread] = true
				seen[id] = tracked{Name: saved[id].Name, Request: snapshot.Runtimes[id].Request}
			}
		}
	}
	byThread := map[string]string{}
	for id := range byID {
		s := saved[id]
		if s.thread() != "" {
			byThread[s.thread()] = id
		}
	}
	byThread[c.TargetThread] = c.Target
	goalStates := map[string]string{}
	nativeAttention := []string{}
	for thread := range all {
		state, err := p.state(ctx, thread)
		if err != nil {
			return nil, nil, err
		}
		id := byThread[thread]
		if state == "active" {
			busy = append(busy, label(thread, saved[id].Name))
			if id != "" && id != c.Target {
				seen[id] = tracked{Name: saved[id].Name, Request: snapshot.Runtimes[id].Request}
			}
		} else if state == "systemError" && thread != c.TargetThread {
			nativeAttention = append(nativeAttention, label(thread, saved[id].Name)+" (native system error)")
		} else if state != "idle" && state != "notLoaded" {
			return nil, nil, errors.New("native_state_unknown")
		}
		if id != "" && id != c.Target && saved[id].Backend == "codex" {
			goal, err := p.goal(ctx, thread)
			if err != nil {
				return nil, nil, err
			}
			goalStates[id] = goal
			if goal == "active" {
				busy = append(busy, label(id, saved[id].Name)+" (active goal)")
				seen[id] = tracked{Name: saved[id].Name, Request: snapshot.Runtimes[id].Request}
			}
		}
	}
	attention := nativeAttention
	for _, item := range queueAttention {
		if item.state == "uncertain" {
			attention = append(attention, label(item.id, saved[item.id].Name)+" (uncertain durable command)")
		}
	}
	for id, tracked := range seen {
		v, ok := snapshot.Runtimes[id]
		if !ok || byID[id].ID == "" {
			attention = append(attention, label(id, tracked.Name)+" (missing session)")
			continue
		}
		if !activePhase(v.Phase) && !byID[id].Streaming {
			if v.Terminal != "completed" || v.Phase == "failed" || v.Phase == "interrupted" {
				attention = append(attention, label(id, tracked.Name)+" (not successfully completed)")
			}
			if tracked.Request != "" && v.Request != tracked.Request {
				attention = append(attention, label(id, tracked.Name)+" (request changed)")
			}
		}
		if goal := goalStates[id]; goal != "" && goal != "active" && goal != "complete" {
			attention = append(attention, label(id, tracked.Name)+" (goal "+goal+")")
		}
	}
	sort.Strings(busy)
	sort.Strings(attention)
	return unique(busy), unique(attention), nil
}

type queueItem struct{ id, request, state string }

func durableQueue(ctx context.Context, dir, target string, seen map[string]tracked) ([]queueItem, error) {
	path := filepath.Join(dir, "message_queue.sqlite")
	if _, err := os.Stat(path); err != nil {
		return nil, errors.New("durable_queue_unavailable")
	}
	dsn := (&url.URL{Scheme: "file", Path: path}).String() + "?mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, errors.New("durable_queue_unavailable")
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "SELECT session_id, request_id, state FROM queue_commands WHERE state IN ('queued','running','steering','uncertain')")
	if err != nil {
		return nil, errors.New("durable_queue_unavailable")
	}
	defer rows.Close()
	items := []queueItem{}
	for rows.Next() {
		var v queueItem
		if rows.Scan(&v.id, &v.request, &v.state) != nil {
			return nil, errors.New("durable_queue_unavailable")
		}
		items = append(items, v)
		if v.id != target && v.state != "uncertain" {
			t := seen[v.id]
			t.Request = v.request
			seen[v.id] = t
		}
	}
	if rows.Err() != nil {
		return nil, errors.New("durable_queue_unavailable")
	}
	return items, nil
}
func unique(values []string) []string {
	result := []string{}
	for _, v := range values {
		if len(result) == 0 || result[len(result)-1] != v {
			result = append(result, v)
		}
	}
	return result
}

func activePhase(s string) bool {
	return s == "queued" || s == "running" || s == "waiting" || s == "stopping"
}
func label(id, name string) string {
	if name != "" {
		return name
	}
	return id
}
func notification(attention []string, c config) string {
	if len(attention) > 0 {
		return "[Bridge 單次監控通知] 其他 session 已無活動，但尚不能視為全部成功完成。請勿部署或重啟，先核對監控紀錄並回報原因。紀錄：" + c.StateFile
	}
	return "[Bridge 單次監控通知] 已在至少 " + c.Quiet.String() + " 的觀察窗內反覆核對 Bridge 執行狀態、佇列、Codex 原生 thread 與 active goal，目前皆已閒置。依使用者先前授權，接續處理本專案已驗證的 Bridge 調整部署與前端測試。這則通知本身尚未部署；執行前必須重新確認沒有其他 active session、active goal 或排隊訊息，備份並遵循正式 release 流程，保留所有對話與未提交變更。不得直接安裝、覆蓋或重置手機 App。監控紀錄：" + c.StateFile
}

func readJSON(path string, value any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(raw) > 32<<20 {
		return errors.New("snapshot_too_large")
	}
	return json.Unmarshal(raw, value)
}
func saveReport(path string, r report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".idle-watch-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = json.NewEncoder(f).Encode(r); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(f.Name(), path)
}

type native struct {
	conn      *websocket.Conn
	transport *http.Transport
	id        int
}

func openNative(ctx context.Context, socket string) (*native, error) {
	t := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "unix", socket)
	}}
	conn, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: t}})
	if err != nil {
		t.CloseIdleConnections()
		return nil, err
	}
	conn.SetReadLimit(4 << 20)
	p := &native{conn: conn, transport: t}
	if _, err = p.call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "bridge-idle-watch", "version": "1"}, "capabilities": map[string]bool{"experimentalApi": true}}); err == nil {
		err = writeWS(ctx, conn, map[string]string{"method": "initialized"})
	}
	if err != nil {
		p.close()
		return nil, err
	}
	return p, nil
}
func (p *native) close() { p.conn.CloseNow(); p.transport.CloseIdleConnections() }
func (p *native) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	// Hard allowlist: no resume, subscribe, model turn, goal writes or interrupt.
	if method != "initialize" && method != "thread/loaded/list" && method != "thread/read" && method != "thread/goal/get" {
		return nil, errors.New("native_write_forbidden")
	}
	p.id++
	if err := writeWS(ctx, p.conn, map[string]any{"id": p.id, "method": method, "params": params}); err != nil {
		return nil, errors.New("native_rpc_unavailable")
	}
	for {
		_, raw, err := p.conn.Read(ctx)
		if err != nil {
			return nil, errors.New("native_rpc_unavailable")
		}
		var v struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if json.Unmarshal(raw, &v) != nil || v.ID != p.id {
			continue
		}
		if len(v.Error) > 0 && string(v.Error) != "null" {
			return nil, errors.New("native_rpc_rejected")
		}
		if len(v.Result) > 0 {
			return v.Result, nil
		}
	}
}
func (p *native) loaded(ctx context.Context) ([]string, error) {
	ids := []string{}
	cursor := ""
	seen := map[string]bool{}
	for page := 0; page < 16; page++ {
		params := map[string]any{"limit": 100}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := p.call(ctx, "thread/loaded/list", params)
		if err != nil {
			return nil, err
		}
		var v struct {
			Data []string `json:"data"`
			Next *string  `json:"nextCursor"`
		}
		if json.Unmarshal(raw, &v) != nil || v.Data == nil {
			return nil, errors.New("native_listing_unknown")
		}
		ids = append(ids, v.Data...)
		if v.Next == nil || *v.Next == "" {
			return ids, nil
		}
		cursor = *v.Next
		if seen[cursor] {
			return nil, errors.New("native_listing_unknown")
		}
		seen[cursor] = true
	}
	return nil, errors.New("native_listing_unknown")
}
func (p *native) state(ctx context.Context, id string) (string, error) {
	raw, err := p.call(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": false})
	if err != nil {
		return "", err
	}
	var v struct {
		Thread threadView `json:"thread"`
	}
	if json.Unmarshal(raw, &v) != nil || v.Thread.ID != id || v.Thread.Status.Type == "" {
		return "", errors.New("native_identity_unknown")
	}
	return v.Thread.Status.Type, nil
}
func (p *native) goal(ctx context.Context, id string) (string, error) {
	raw, err := p.call(ctx, "thread/goal/get", map[string]string{"threadId": id})
	if err != nil {
		return "", err
	}
	var v struct {
		Goal *struct {
			ThreadID string `json:"threadId"`
			Status   string `json:"status"`
		} `json:"goal"`
	}
	if json.Unmarshal(raw, &v) != nil {
		return "", errors.New("native_goal_unknown")
	}
	if v.Goal == nil {
		return "", nil
	}
	if v.Goal.ThreadID != id || v.Goal.Status == "" {
		return "", errors.New("native_goal_unknown")
	}
	return v.Goal.Status, nil
}

func writeWS(ctx context.Context, conn *websocket.Conn, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, raw)
}
func localID(id, authority string) (string, error) {
	if !strings.HasPrefix(id, "sk1:") {
		return id, nil
	}
	p := strings.SplitN(id, ":", 3)
	if len(p) != 3 || p[1] != authority {
		return "", errors.New("session_authority_mismatch")
	}
	return url.PathUnescape(p[2])
}
func connectBridge(ctx context.Context, c config) (*websocket.Conn, error) {
	var pairing struct {
		Devices []struct {
			Token string `json:"token"`
		} `json:"devices"`
	}
	if readJSON(filepath.Join(c.DataDir, "pairing.json"), &pairing) != nil || len(pairing.Devices) == 0 || pairing.Devices[0].Token == "" {
		return nil, errors.New("bridge_credential_unavailable")
	}
	conn, _, err := websocket.Dial(ctx, c.Bridge, nil)
	if err != nil {
		return nil, errors.New("bridge_unavailable")
	}
	conn.SetReadLimit(32 << 20)
	// A distinct ephemeral client identity cannot evict any actual phone. Never
	// claim/rebind a token, alter enrollment, or use a probe to bypass its guard.
	err = writeWS(ctx, conn, map[string]any{"type": "hello", "protocol_version": 3, "replay_ack": true, "auth_token": pairing.Devices[0].Token, "device_id": "idle-watch-" + c.Job, "device_name": "Bridge one-shot idle watcher", "client_surface": "qa"})
	if err != nil {
		conn.CloseNow()
		return nil, errors.New("bridge_unavailable")
	}
	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			conn.CloseNow()
			return nil, errors.New("bridge_handshake_unavailable")
		}
		var v struct {
			Type      string `json:"type"`
			Instance  string `json:"instance_id"`
			Authority string `json:"authority_instance_id"`
			Locked    bool   `json:"locked_to_me"`
		}
		if json.Unmarshal(raw, &v) != nil {
			continue
		}
		if v.Type == "error" {
			conn.CloseNow()
			return nil, errors.New("bridge_auth_rejected")
		}
		if v.Type == "hello_ack" {
			if v.Instance == "" {
				v.Instance = v.Authority
			}
			if v.Instance != c.Authority || !v.Locked {
				conn.CloseNow()
				return nil, errors.New("bridge_identity_mismatch")
			}
			return conn, nil
		}
	}
}
func readBridge(ctx context.Context, c config) (bridgeSnapshot, error) {
	s := bridgeSnapshot{Runtimes: map[string]runtimeView{}}
	conn, err := connectBridge(ctx, c)
	if err != nil {
		return s, err
	}
	defer conn.CloseNow()
	for _, kind := range []string{"request_status", "request_sessions_list", "request_runtime_snapshot"} {
		if writeWS(ctx, conn, map[string]string{"type": kind}) != nil {
			return s, errors.New("bridge_read_unavailable")
		}
	}
	gotStatus, gotSessions, gotRuntime := false, false, false
	for !gotStatus || !gotSessions || !gotRuntime {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return s, errors.New("bridge_read_unavailable")
		}
		var v struct {
			Type     string        `json:"type"`
			Sessions []sessionView `json:"sessions"`
			Items    []runtimeView `json:"items"`
			Status   *struct {
				Streaming int `json:"sessions_streaming"`
				Queued    int `json:"queued_commands"`
				Total     int `json:"sessions_total"`
			} `json:"status"`
		}
		if json.Unmarshal(raw, &v) != nil {
			continue
		}
		switch v.Type {
		case "error":
			return s, errors.New("bridge_read_rejected")
		case "status_result":
			if v.Status == nil || v.Status.Total <= 0 {
				return s, errors.New("bridge_status_unknown")
			}
			s.Streaming, s.Queued = v.Status.Streaming, v.Status.Queued
			gotStatus = true
		case "sessions_list":
			if v.Sessions == nil {
				return s, errors.New("bridge_sessions_unknown")
			}
			s.Sessions = v.Sessions
			for i := range s.Sessions {
				id, err := localID(s.Sessions[i].ID, c.Authority)
				if err != nil {
					return s, err
				}
				s.Sessions[i].ID = id
			}
			gotSessions = true
		case "session_runtime_snapshot":
			if v.Items == nil {
				return s, errors.New("bridge_runtime_unknown")
			}
			for _, r := range v.Items {
				id, err := localID(r.ID, c.Authority)
				if err != nil {
					return s, err
				}
				r.ID = id
				s.Runtimes[id] = r
			}
			gotRuntime = true
		}
	}
	return s, nil
}
func notify(ctx context.Context, c config, request, content string) error {
	conn, err := connectBridge(ctx, c)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	if err := writeWS(ctx, conn, map[string]string{"type": "message", "session_id": c.Target, "request_id": request, "content": content}); err != nil {
		return errors.New("notification_send_failed")
	}
	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return errors.New("notification_ack_unknown")
		}
		var v struct {
			Type    string `json:"type"`
			Request string `json:"request_id"`
			Session string `json:"session_id"`
			State   string `json:"state"`
			Status  string `json:"status"`
		}
		if json.Unmarshal(raw, &v) != nil || v.Request != request {
			continue
		}
		id, err := localID(v.Session, c.Authority)
		if err != nil || id != c.Target {
			return errors.New("notification_identity_mismatch")
		}
		if v.Type == "message_ack" {
			if v.Status != "queued" || v.State == "rejected" || v.State == "failed" {
				return errors.New("notification_rejected")
			}
			return nil
		}
		if v.Type == "error" || v.Type == "session_command_failed" {
			return errors.New("notification_rejected")
		}
	}
}
