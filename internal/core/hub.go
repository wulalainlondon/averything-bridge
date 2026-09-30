// Package core is the Go connection layer: WebSocket termination, the client
// registry, envelope routing, and session management. It is written against the
// executor.Executor interface and knows nothing about how turns are actually
// run — that is the seam that lets the same core back configs 2 and 3.
package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"everything-go/internal/attachmentjournal"
	"everything-go/internal/automation"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/delegation"
	"everything-go/internal/deviceinventory"
	"everything-go/internal/eventinbox"
	"everything-go/internal/executor"
	"everything-go/internal/fcm"
	"everything-go/internal/feed"
	"everything-go/internal/governance"
	"everything-go/internal/inbox"
	"everything-go/internal/media"
	"everything-go/internal/messagequeue"
	"everything-go/internal/nativewatch"
	"everything-go/internal/notificationreply"
	"everything-go/internal/protocol"
	"everything-go/internal/relay"
	"everything-go/internal/runtime"
	"everything-go/internal/runtimejournal"
	"everything-go/internal/search"
	"everything-go/internal/session"
	"everything-go/internal/toolenv"
	"everything-go/internal/widgetaccess"
	"everything-go/internal/workitems"
)

// Config carries the connection-identity fields surfaced in hello_ack so the
// app can label the bridge and respect its filesystem jail.
type Config struct {
	InstanceName string
	InstanceID   string
	RootDir      string
	DataDir      string
	LanIP        string
	// ResolveLANIP is evaluated for each handshake; laptops can change networks
	// while the hub remains alive. Nil preserves static embedders/tests.
	ResolveLANIP func() string
	TailscaleIP  string
	Backends     []backend.Definition
	CodexRemote  string
	Port         int
}

// Hub owns the set of connected clients and the session registry, and acts as
// the executor.Sink (Emit broadcasts an event to connected clients, or buffers
// it when none are connected so a reconnecting client can recover it).
type Hub struct {
	recapJobs           chan struct{}
	pmMu                sync.Mutex
	pmEnabled           bool
	pmSecret            []byte
	pmURL               string
	toolOperations      *toolenv.Store
	toolRepairRunning   atomic.Bool
	deviceInventory     *deviceinventory.Store
	messageQueue        *messagequeue.Store
	delegations         *delegation.Store
	delegationWake      chan struct{}
	messageQueueMu      sync.Mutex
	maintenanceHolds    map[string]func()
	maintenanceCheck    sync.Mutex
	registry            *session.Registry
	exec                executor.Executor
	shells              *runtime.ShellManager
	pairing             *governance.Pairing
	perms               *governance.PermissionManager
	offline             *governance.OfflineBuffer
	goals               *governance.GoalStateStore
	search              *search.Index
	fcm                 *fcm.Notifier
	workAttentionPush   func(instanceID, workItemID, title string, revision uint64, kind string)
	runtimeStatusPush   func(instanceID, instanceName, sessionID, sessionName, phase, stage, stageMessage string, revision uint64, updatedAt, activeStartedAt int64, activeRequestID string, queueLength int, reply fcm.ReplyAction)
	feed                *feed.Store
	inbox               *inbox.Store
	mediaScan           *media.Scanner
	attachments         *attachmentjournal.Store
	controls            *governance.SessionControlStore
	runtimes            *runtimejournal.Store
	widgetMu            sync.Mutex
	widgetSigner        *widgetaccess.Signer
	recoveredRuntimes   []runtimejournal.View
	replyCaps           *notificationreply.Capabilities
	notificationReplies *notificationreply.Store
	work                *workitems.Service
	events              *eventinbox.Store
	automation          *automation.Store
	automationManager   *automation.Manager
	relay               *relay.Store
	relayPeers          relay.Peers
	cfg                 Config
	client              clientproto.AppV1
	gen                 string // per-boot generation id

	iceServers []webrtc.ICEServer // STUN/TURN for WebRTC answers (default: Google STUN)

	mu      sync.RWMutex
	clients map[*Client]struct{}

	// latestByDevice maps device_id → its newest client. A single device keeps
	// exactly one live client; a new connection from the same device evicts the
	// old one (the mobile half-disconnect "storm" otherwise piles up zombies).
	// This also restores parity with Python's ws_ref-rebind (latest socket wins).
	latestMu       sync.Mutex
	latestByDevice map[string]*Client

	turnMu   sync.Mutex
	turnText map[string]*strings.Builder // session_id -> assistant text this turn
	// transcriptChanged wakes the out-of-process search indexer with the exact
	// transcript path after a Bridge-owned turn completes. Native filesystem
	// watching remains a fallback for turns written by an external CLI.
	transcriptChanged func(string)

	steerMu      sync.Mutex
	steerResults map[string]protocol.SteerResult // session_id/request_id -> terminal acknowledgement

	storm *stormGuards // dedupe/throttle/semaphore for heavy handlers

	// nativeDirty is set by the native-session watcher when an import changes
	// the registry. A single coalescer goroutine drains it on a timer so a
	// startup scan of hundreds of transcripts produces ONE persist + ONE
	// sessions_list broadcast instead of hundreds (which OOM-killed the app).
	nativeDirty atomic.Bool

	// tunnelURL is the current cloudflared public URL, set by NotifyTunnelURL
	// and included in hello_ack so reconnecting clients always get the latest URL
	// even if they missed the FCM push when the tunnel started.
	tunnelURLMu sync.RWMutex
	tunnelURL   string

	// restart, if set, actually restarts the bridge (wired in main to a
	// self-re-exec). nil → restart_bridge answers "not configured", mirroring
	// Python's gate on an unset restart-trigger path.
	restart func()

	replayMu    sync.Mutex
	replayLease *replayLease

	attachmentReplayMu sync.Mutex
	attachmentReplays  map[string]*attachmentReplayLease // device_id + session_id -> lease

	// WorkItem mutations are serialized around the durable device/mutation-id
	// lookup and result write. Entity versions still provide DB-level conflict
	// safety; this lock prevents two concurrent retry frames from both running.
	workMutationMu      sync.Mutex
	workWake            chan struct{}
	workScheduler       atomic.Bool
	automationWake      chan struct{}
	automationScheduler atomic.Bool
	relayWake           chan struct{}
	relayScheduler      atomic.Bool
	relayNonceMu        sync.Mutex
	relayNonces         map[string]int64
}

func NewHub(reg *session.Registry, cfg Config, pairing *governance.Pairing, port int) *Hub {
	cfg.Port = port
	h := &Hub{
		recapJobs:           make(chan struct{}, 4),
		toolOperations:      toolenv.Open(cfg.DataDir),
		registry:            reg,
		pairing:             pairing,
		offline:             governance.NewOfflineBuffer(),
		goals:               governance.NewGoalStateStore(goalSnapshotPath(cfg.DataDir)),
		cfg:                 cfg,
		client:              clientproto.NewAppV1(),
		gen:                 randomID(),
		clients:             make(map[*Client]struct{}),
		latestByDevice:      make(map[string]*Client),
		turnText:            make(map[string]*strings.Builder),
		steerResults:        make(map[string]protocol.SteerResult),
		iceServers:          stunServers,
		storm:               newStormGuards(),
		mediaScan:           media.NewScanner(port),
		attachments:         attachmentjournal.New(cfg.DataDir),
		controls:            governance.NewSessionControlStore(cfg.DataDir),
		runtimes:            runtimejournal.New(cfg.DataDir),
		notificationReplies: notificationreply.NewStore(cfg.DataDir),
		attachmentReplays:   make(map[string]*attachmentReplayLease),
		delegationWake:      make(chan struct{}, 1),
		workWake:            make(chan struct{}, 1),
		automationWake:      make(chan struct{}, 1),
		relayWake:           make(chan struct{}, 1),
		relayNonces:         make(map[string]int64),
	}
	if store, err := messagequeue.Open(cfg.DataDir); err != nil {
		log.Printf("[message-queue] storage unavailable: %v", err)
	} else if err = store.Recover(); err != nil {
		store.Close()
		log.Printf("[message-queue] recovery unavailable: %v", err)
	} else {
		h.messageQueue = store
	}
	if store, err := delegation.Open(cfg.DataDir); err != nil {
		log.Printf("[delegation] storage unavailable: %v", err)
	} else {
		h.delegations = store
	}
	if cfg.DataDir != "" {
		if inventory, err := deviceinventory.Open(cfg.DataDir); err != nil {
			log.Printf("[device-inventory] unavailable: %v", err)
		} else {
			h.deviceInventory = inventory
			h.syncDeviceInventory()
		}
	}
	if capabilities, err := notificationreply.NewCapabilities(cfg.DataDir, cfg.InstanceID); err != nil {
		log.Printf("[notification-reply] capability initialization failed: %v", err)
	} else {
		h.replyCaps = capabilities
	}
	// No Session worker can be active while a new Hub is being constructed.
	// Recover stale persisted phases exactly once here, never during reconnect.
	h.recoveredRuntimes = h.runtimes.RecoverAllStale()
	if cfg.TailscaleIP != "" {
		h.mediaScan.SetTailscaleIP(cfg.TailscaleIP)
	}
	if cfg.LanIP != "" {
		h.mediaScan.SetLanIP(cfg.LanIP)
	}
	// Shell output is broadcast to connected clients via the Hub sink.
	h.shells = runtime.NewShellManager(h.Emit)
	// Permission gate for high-risk ops (kill_process / shell_input); broadcasts
	// permission_request/result via the hub. Mode from BRIDGE_PERMISSION_MODE
	// (default enforce, mirroring Python prod).
	h.perms = governance.NewPermissionManager(h.Emit, os.Getenv("BRIDGE_PERMISSION_MODE"))
	return h
}

// SetExecutor wires the backend after construction (the executor needs the Hub
// as its Sink, so the Hub is built first).
func (h *Hub) SetExecutor(e executor.Executor) {
	h.exec = e
	h.restoreMaintenanceHolds()
	h.resumeNotificationReplies()
	h.resumeQueuedMessages()
}

func (h *Hub) SetRelay(store *relay.Store, peers relay.Peers) {
	h.relay, h.relayPeers = store, peers
}

// authValid mirrors bridge_v2.py:_is_auth_token_valid. BRIDGE_AUTH_TOKEN (a
// manual override) takes priority; otherwise, if the bridge is claimed, only the
// paired token is accepted. An unclaimed bridge is enrolled separately through
// the LAN-only provisional handshake; it never grants full anonymous access.
// expected pre-trimmed.
func (h *Hub) authValid(provided string) bool {
	if expected := strings.TrimSpace(os.Getenv("BRIDGE_AUTH_TOKEN")); expected != "" {
		return provided != "" && provided == expected
	}
	if h.pairing.IsLocked() {
		return h.pairing.LockedTo(provided)
	}
	return false
}

// HTTPAuthorized applies the same paired-device credential used by the
// WebSocket handshake to auxiliary HTTP APIs. Bearer headers are preferred;
// the query form exists only for browser media elements that cannot attach a
// header and must never be logged or persisted as canonical data.
func (h *Hub) HTTPAuthorized(r *http.Request) bool {
	provided := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if provided == "" {
		provided = strings.TrimSpace(r.URL.Query().Get("auth_token"))
	}
	return h.authValid(provided)
}

// SetSearch wires the search index (nil disables the search command family).
func (h *Hub) SetSearch(s *search.Index) {
	h.search = s
	if s != nil {
		go h.reconcileRestartMarkersFromSearch()
	}
}

// SetTranscriptChangeNotifier wires the exact-path search-index wake used by
// terminal turns. It is intentionally separate from SetSearch because the
// resident Hub only reads the index while a short-lived child writes it.
func (h *Hub) SetTranscriptChangeNotifier(notify func(string)) {
	h.transcriptChanged = notify
}

func (h *Hub) notifyTranscriptChanged(sessionID string) {
	if h.transcriptChanged == nil {
		return
	}
	s, ok := h.registry.Get(sessionID)
	if !ok {
		return
	}
	path, ok := h.transcriptPath(s)
	if !ok || path == "" {
		return
	}
	h.transcriptChanged(path)
}

// SetFCM wires the push notifier (nil disables push).
func (h *Hub) SetFCM(n *fcm.Notifier) {
	h.fcm = n
	h.workAttentionPush = nil
	h.runtimeStatusPush = nil
	if n != nil {
		h.workAttentionPush = n.NotifyWorkAttention
		h.runtimeStatusPush = n.NotifySessionStatusWithAuthority
		// Construction recovers stale active turns before FCM is wired. Publish
		// those terminal revisions now so a lock-screen card from the previous
		// Bridge process cannot remain stuck as "running".
		for _, view := range h.recoveredRuntimes {
			h.notifyRuntimeStatus(view)
		}
		h.recoveredRuntimes = nil
	}
}

// SetTunnelURL updates the tunnel base URL used when building media/document
// URLs in scan results. Call this whenever the tunnel address changes.
func (h *Hub) SetTunnelURL(wsURL string) {
	h.mediaScan.SetTunnelURL(wsURL)
}

// NotifyTunnelURL pushes the new tunnel URL to the device via FCM, updates
// the media scanner, and stores it so reconnecting clients get it in hello_ack.
func (h *Hub) NotifyTunnelURL(wsURL string) {
	h.tunnelURLMu.Lock()
	h.tunnelURL = wsURL
	h.tunnelURLMu.Unlock()
	h.mediaScan.SetTunnelURL(wsURL)
	if h.fcm == nil {
		return
	}
	go h.fcm.NotifyTunnelURL(wsURL, h.cfg.InstanceID)
}

// SetFeed wires the feed store (nil → feed commands answer empty/no-op).
func (h *Hub) SetFeed(f *feed.Store) { h.feed = f }

// SetInbox wires the file-push inbox (nil → push_file/get_inbox answer empty/no-op).
func (h *Hub) SetInbox(i *inbox.Store) { h.inbox = i }

// SetWorkItems wires the native WorkItem authority. nil keeps compatibility
// for tests and legacy deployments that do not advertise work_items_v1.
func (h *Hub) SetWorkItems(service *workitems.Service) { h.work = service }

// SetEventInbox wires the source-neutral, durable external event authority.
func (h *Hub) SetEventInbox(store *eventinbox.Store) { h.events = store }

// SetRestart wires the bridge-restart action (nil → restart_bridge is a no-op
// that answers "not configured"). main wires this to a self-re-exec.
func (h *Hub) SetRestart(fn func()) { h.restart = fn }

// StartNativeWatcher imports native Claude/Codex CLI JSONL sessions into the
// bridge registry so desktop CLI work appears in the app's normal session list.
func (h *Hub) StartNativeWatcher(ctx context.Context, onTranscriptChange ...func(string)) {
	if os.Getenv("EVERYTHING_GO_NATIVE_WATCH") == "0" {
		log.Printf("[nativewatch] disabled by EVERYTHING_GO_NATIVE_WATCH=0")
		return
	}
	opts := nativewatch.DefaultOptions()
	turnWatcher := nativewatch.NewTurnWatcher()
	go turnWatcher.Run(ctx, func(activity nativewatch.TurnActivity) {
		if s, ok := h.registry.Get(activity.Session.ID); ok && s.ResumeID() == activity.Session.ResumeID {
			if observer, supported := h.exec.(backend.NativeTurnObserver); supported {
				observer.ObserveNativeLifecycle(s, activity.TurnID, activity.Phase)
			}
		}
	})
	if v := strings.TrimSpace(os.Getenv("EVERYTHING_GO_NATIVE_POLL_INTERVAL")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			opts.PollInterval = d
		}
	}
	if v := strings.TrimSpace(os.Getenv("EVERYTHING_GO_NATIVE_DEBOUNCE")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			opts.Debounce = d
		}
	}
	if v := strings.TrimSpace(os.Getenv("EVERYTHING_GO_NATIVE_LOOKBACK")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			opts.InitialLookback = d
		}
	}

	// Coalescer: a startup scan imports hundreds of transcripts back-to-back.
	// Broadcasting a full sessions_list per import floods the app (each summary
	// re-queries the search index for previews/last_ts) and OOM-kills it. The
	// watcher callback only flags dirty; this goroutine emits at most once per
	// tick — one persist + one broadcast no matter how many imports landed.
	go func() {
		t := time.NewTicker(1500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if h.nativeDirty.Swap(false) {
					h.registry.Persist()
					h.Emit(h.client.SessionsList(h.sessionSummaries()))
				}
			}
		}
	}()

	notifyTranscriptChange := func(path string) {
		for _, notify := range onTranscriptChange {
			if notify != nil {
				notify(path)
			}
		}
	}
	go nativewatch.WatchChanges(ctx, opts, func(ns nativewatch.NativeSession) {
		if h.cfg.RootDir != "" && !pathInsideRoot(ns.Cwd, h.cfg.RootDir) {
			return
		}
		s, changed := h.registry.UpsertExternal(ns.ID, ns.Name, ns.Cwd, ns.Backend, ns.ResumeID, ns.LastUsed)
		if s != nil {
			ns.ID = s.ID // Registry may have deduplicated a Bridge-created session.
			turnWatcher.Track(ns)
		}
		if !changed {
			return
		}
		log.Printf("[nativewatch] imported %s resume=%s cwd=%q", ns.Backend, ns.ResumeID, ns.Cwd)
		h.nativeDirty.Store(true)
	}, notifyTranscriptChange)
}

func pathInsideRoot(path, root string) bool {
	if path == "" || root == "" {
		return false
	}
	rel, err := filepath.Rel(realpath(root), realpath(path))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// connectedDeviceIDs returns the distinct device ids currently connected, except
// `exclude`. Used to target a file push at every device but the sender.
func (h *Hub) connectedDeviceIDs(exclude string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := map[string]bool{}
	out := []string{}
	for c := range h.clients {
		d := c.deviceID
		if d == "" || d == exclude || seen[d] {
			continue
		}
		seen[d] = true
		out = append(out, d)
	}
	return out
}

// Emit implements executor.Sink. With clients connected it marshals the event
// once and delivers it to every client. With none connected it buffers the
// event for replay on the next reconnect (the offline-recovery path). Safe for
// concurrent use.
func (h *Hub) Emit(event any) {
	observedTerminal := false
	if observed, ok := event.(backend.ObservedTurn); ok {
		event, observedTerminal = h.observedTurnEvent(observed)
		if !observedTerminal {
			return
		}
	}
	if maintenance, ok := event.(backend.Maintenance); ok {
		h.applyMaintenanceHold(maintenance)
		return
	}
	// Executor progress is intentionally internal. Convert it to the durable,
	// revisioned per-device runtime view instead of leaking a second transient
	// status protocol that reconnecting clients could miss.
	if progress, ok := event.(protocol.TurnProgress); ok {
		h.driveRuntimeState(progress)
		return
	}
	// Attachments use their own durable per-device journal. They must never enter
	// the global offline buffer: a desktop ACK must not consume a phone delivery.
	if _, isMedia := event.(protocol.Media); isMedia {
		h.emitAttachment(event)
		return
	}
	if _, isDocument := event.(protocol.Document); isDocument {
		h.emitAttachment(event)
		return
	}
	// Backends may discover a local generated image before the core has enough
	// network context to construct a phone-reachable URL. Resolve it at the Hub,
	// which owns the media server and tunnel/Tailscale/LAN address selection.
	if mediaEvent, ok := event.(protocol.Media); ok && mediaEvent.URL == "" && mediaEvent.Path != "" {
		mediaEvent.URL = h.mediaScan.LocalURL(mediaEvent.Path)
		event = mediaEvent
	}
	logOutbound(event)
	if h.goals.Apply(event) {
		switch e := event.(type) {
		case protocol.GoalUpdate:
			log.Printf("[goal] snapshot session=%s status=%s updated_at=%d", e.SessionID, e.Goal.Status, e.Goal.UpdatedAt)
		case protocol.GoalCleared:
			log.Printf("[goal] snapshot cleared session=%s", e.SessionID)
		}
	}

	// Record terminal state before exposing the terminal event, but hold the
	// actor release until done -> runtime has been published in order. Marking
	// the Session idle now also lets a client safely reclaim control as soon as
	// it observes an active-writer error.
	terminalView, terminalChanged, terminalEvent := h.recordTerminalRuntime(event)
	// Persist the terminal revision before building a completion push. It must
	// be self-contained even when a suspended iPhone missed all running events.
	h.accumulateTurn(event, terminalView)
	if terminalEvent {
		h.wakeDelegations()
	}
	if terminalEvent {
		switch e := event.(type) {
		case protocol.Error:
			h.finishPMTurn(terminalView.SessionID, terminalView.ActiveRequestID, "failed", e.Message)
		case protocol.Stopped:
			h.finishPMTurn(terminalView.SessionID, terminalView.ActiveRequestID, "interrupted", "工作已停止；請確認目前成果後繼續。")
		}
	}
	releaseTurn := func() {}
	if terminalEvent {
		h.finishQueuedMessage(terminalView)
		// After a Bridge restart the native daemon can still be executing the
		// accepted request. Its correlated terminal settles the recovered queue
		// entry, but an observer must never release a different actor's work.
		if !observedTerminal {
			if s, ok := h.registry.Get(terminalView.SessionID); ok {
				releaseTurn = s.PrepareEndTurn()
			}
		}
	}

	h.mu.RLock()
	n := len(h.clients)
	h.mu.RUnlock()

	if n == 0 {
		h.offline.Append(event)
	} else {
		h.mu.RLock()
		clients := make([]*Client, 0, len(h.clients))
		for c := range h.clients {
			clients = append(clients, c)
		}
		h.mu.RUnlock()
		for _, c := range clients {
			c.enqueueEventUnlogged(event)
		}
	}

	// Persist on the events that change durable session state: a new resume id
	// (session_uuid) or a completed turn (done) — mirrors the Python bridge's
	// persist-on-turn-complete trigger.
	switch event.(type) {
	case protocol.SessionUUID:
		// A changed physical thread is the durable identity of a logical
		// session. Persist synchronously before the executor can accept another
		// turn or the service can restart; otherwise a crash can revive the old
		// generation and fork history again.
		h.registry.Persist()
	case protocol.SessionMetaUpdated:
		// Runtime-derived model/effort metadata is part of the canonical Session
		// identity and must survive a Bridge restart just like an explicit config
		// change. Pin/hidden updates are harmless to persist here as well.
		h.registry.Persist()
	case protocol.Done:
		go h.registry.Persist()
	}

	// Persist and publish the compact authoritative lifecycle only after the
	// original event. This preserves text -> attachment -> done ordering for
	// existing clients while still making reconnect state durable.
	if !terminalEvent {
		h.driveRuntimeState(event)
	} else {
		if terminalChanged {
			h.publishRuntime(terminalView)
		}
		releaseTurn()
	}
}

// broadcastOnline delivers canonical state changes to clients that are
// currently connected without writing the legacy global offline journal.
// Callers must have their own durable reconnect source (for example the Event
// Inbox store); otherwise an offline device would miss the event.
func (h *Hub) broadcastOnline(event any) error {
	logOutbound(event)
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		c.enqueueEventUnlogged(event)
	}
	return nil
}

// replayOffline flushes buffered events to a single reconnecting client, in
// order. Mirrors bridge/offline_replay.py (called after sessions_list).
func (h *Hub) replayOffline(c *Client) {
	h.startOfflineReplay(c)
}

// driveTurnState advances the session state machine off the executor's terminal
// events. done/stopped/error end the in-flight turn, which releases the
// session's turn worker to run the next queued message. Idempotent per turn.
func (h *Hub) driveTurnState(event any) {
	var sessionID string
	switch e := event.(type) {
	case protocol.Done:
		sessionID = e.SessionID
	case protocol.Stopped:
		sessionID = e.SessionID
	case protocol.Error:
		sessionID = e.SessionID
	default:
		return
	}
	if sessionID == "" {
		return
	}
	if s, ok := h.registry.Get(sessionID); ok {
		s.EndTurn()
	}
}

// accumulateTurn tracks assistant text_chunks per session and, on the turn's
// done event, fires a task-done push with the accumulated summary. stopped
// clears the buffer without notifying.
func (h *Hub) accumulateTurn(event any, terminal runtimejournal.View) {
	switch e := event.(type) {
	case protocol.TextChunk:
		h.turnMu.Lock()
		b := h.turnText[e.SessionID]
		if b == nil {
			b = &strings.Builder{}
			h.turnText[e.SessionID] = b
		}
		b.WriteString(e.Content)
		h.turnMu.Unlock()
	case protocol.Done:
		h.turnMu.Lock()
		b := h.turnText[e.SessionID]
		delete(h.turnText, e.SessionID)
		h.turnMu.Unlock()

		var text string
		if b != nil {
			text = b.String()
		}
		h.finishPMTurn(e.SessionID, e.RequestID, "succeeded", text)
		if preview := truncateGraphemes(normalizePreviewText(text), 160); preview != "" {
			if _, changed, err := h.registry.CommitPreviewAndPersist(e.SessionID, preview, "assistant", time.Now().UnixMilli()); err != nil {
				log.Printf("[session-preview] terminal commit failed session=%s request=%s: %v", e.SessionID, e.RequestID, err)
			} else if changed {
				log.Printf("[session-preview] terminal commit session=%s request=%s", e.SessionID, e.RequestID)
			}
		}
		// Wake indexing even when the native watcher missed/coalesced the write.
		// The notifier is also called for empty/error-only replies so the durable
		// transcript remains the ultimate history source.
		h.notifyTranscriptChanged(e.SessionID)
		h.completeRelayRun(e.RequestID, "succeeded", "", text)

		// Scan for media/document paths regardless of FCM being configured.
		h.scanAndEmitMedia(e.SessionID, e.RequestID, text)

		if h.fcm == nil || text == "" {
			return
		}
		// Explicit WorkItem runs have their own durable attention notification,
		// emitted by projectWorkRun after the authoritative lifecycle update. Do
		// not also send the legacy task_done push for the same request. Ownership
		// is read from SQLite (including terminal runs), not an in-memory flag, so
		// event ordering and Bridge restarts cannot reintroduce duplicates.
		if !h.shouldNotifyTaskDone(e.SessionID, e.RequestID) {
			return
		}
		name := e.SessionID
		if s, ok := h.registry.Get(e.SessionID); ok {
			if n := s.Name(); n != "" {
				name = n
			}
		}
		replyURL, fallbackURL, capability, expiresAt := h.notificationReplyAction(e.SessionID)
		projection := fcm.TerminalProjection{}
		if terminal.SessionID == e.SessionID && terminal.ActiveRequestID == e.RequestID && terminal.Phase == "completed" {
			projection = fcm.TerminalProjection{Revision: terminal.Revision, UpdatedAt: terminal.UpdatedAt,
				ActiveStartedAt: terminal.ActiveStartedAt, CompletedAt: terminal.CompletedAt}
		}
		go h.fcm.NotifyTaskDoneWithAuthority(h.cfg.InstanceID, h.cfg.InstanceName, name, text, e.SessionID, e.RequestID,
			fcm.ReplyAction{URL: replyURL, FallbackURL: fallbackURL, Capability: capability, ExpiresAt: expiresAt}, projection)
	case protocol.Stopped:
		h.turnMu.Lock()
		delete(h.turnText, e.SessionID)
		h.turnMu.Unlock()
	}
}

func (h *Hub) shouldNotifyTaskDone(sessionID, requestID string) bool {
	if h.work == nil || requestID == "" {
		return true
	}
	owned, err := h.work.OwnsRequest(context.Background(), sessionID, requestID)
	if err != nil {
		// Preserve the established notification path if the Work store is
		// temporarily unreadable; losing every completion push is worse than a
		// possible duplicate, and the error remains observable.
		log.Printf("[fcm] work request ownership lookup failed session=%s request=%s: %v", sessionID, requestID, err)
		return true
	}
	if owned {
		log.Printf("[fcm] task_done suppressed for work request session=%s request=%s", sessionID, requestID)
		return false
	}
	return true
}

// scanAndEmitMedia scans accumulated turn text for file paths and emits
// protocol.Media / protocol.Document events for any that exist on disk.
// Mirrors Python bridge's scan_for_media, called at the same time as FCM notify.
func (h *Hub) scanAndEmitMedia(sessionID, requestID, text string) {
	if text == "" {
		return
	}
	var cwd string
	if s, ok := h.registry.Get(sessionID); ok {
		cwd = s.Cwd()
	}
	results := h.mediaScan.Scan(text, sessionID, requestID, cwd)
	log.Printf("[media] scan session=%s textLen=%d cwd=%q found=%d", sessionID, len(text), cwd, len(results))
	for _, r := range results {
		switch v := r.(type) {
		case protocol.Media:
			log.Printf("[media] emit type=media url=%s req=%s", v.URL, v.RequestID)
		case protocol.Document:
			log.Printf("[media] emit type=document url=%s req=%s", v.URL, v.RequestID)
		}
		h.Emit(r)
	}
}

func (h *Hub) addClient(c *Client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) removeClient(c *Client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	// Drop the latest-device pointer only if it still points at this client (a
	// newer client may have already replaced it).
	if c.deviceID != "" {
		h.latestMu.Lock()
		if h.latestByDevice[c.deviceID] == c {
			delete(h.latestByDevice, c.deviceID)
		}
		h.latestMu.Unlock()
	}
	h.releaseReplayLease(c)
	h.releaseAttachmentReplay(c)
}

func goalSnapshotPath(dataDir string) string {
	if strings.TrimSpace(dataDir) == "" {
		return ""
	}
	return filepath.Join(dataDir, "goal_snapshots.json")
}

func marshalEvent(event any, authorityInstanceID string) ([]byte, error) {
	data, err := json.Marshal(event)
	if err != nil || strings.TrimSpace(authorityInstanceID) == "" {
		return data, err
	}
	return scopeSessionIDsJSON(data, authorityInstanceID)
}

func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
