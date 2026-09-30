# everything-go

Averything's production Bridge. This repository is the sole writable backend
source; the workspace sibling `bridge/` is a generated public mirror.

Protocol semantics come from `../contracts/v3/protocol.schema.json`; generated
Go/TypeScript/Swift/Android constants and executable vectors prevent clients
from silently defining different identity or lifecycle rules.

## Topology

One managed Go Bridge owns Session metadata, runtime, attachments, delivery
cursors, Work Items and external events. It connects to the managed shared
Codex app-server over a Unix socket and exposes protocol v3 to clients. LAN,
Tailscale and Tunnel URLs are replaceable transports, never data identity.

The connection core (WS termination, envelope routing, session registry) is
written **once** against the `executor.Executor` interface. Swapping `--executor`
swaps the entire backend without touching connection or routing code — that seam
is what makes configs 2 and 3 share the same core.

## Architecture

```
cmd/everything-go        entry, flags, wiring
internal/protocol        external wire contract (mirrors bridge.ts) — envelope + event builders
internal/session         session registry (plain data)
internal/core            connection core: hub (Sink) + client (read/write) + router (envelope dispatch)
internal/executor        Executor + Sink interfaces — the seam
internal/executor/goexec GoExecutor: Claude CLI subprocess + NDJSON parser (config 2); hosts the in-process ask_user MCP server for interactive prompts
internal/netsvc          network presence: LAN UDP discovery beacon + mDNS + cloudflared tunnel
```

The router only inspects `{type, session_id}` to route; payloads are forwarded
to the executor opaquely. The 803-line Zod schema lives only in the app.

## Local development build (never deploy this output)

```bash
go build -o everything-go ./cmd/everything-go
./everything-go --port 8767 --executor go
```

Production deployment is only through the Developer-ID-signed Release flow in
`docs/RELEASE.md`. A local build does not authorize restarting Wulala or Morrie.

Discovery services are intentionally opt-in. Keep them off for fixed-endpoint
P2 validation; enable them only when testing the network-presence layer:

```bash
./everything-go --port 8767 --executor go --discovery --mdns
./everything-go --port 8767 --executor go --tunnel
```

### Remote WebSocket Backend

`remote-ws` lets the Go connection core talk to a model worker over a backend
WebSocket while preserving the same app-facing bridge protocol:

```bash
go build -o /tmp/everything-go-remote-echo ./examples/remote_ws_echo
./everything-go --port 8767 --executor go --remote-ws-url ws://127.0.0.1:9001/backend
```

Create or switch a session to backend `remote-ws`. The backend-facing protocol
is documented in [`docs/REMOTE_WS_PROTOCOL.md`](docs/REMOTE_WS_PROTOCOL.md).

## Historical migration verification matrix

The table below is retained as migration evidence only. Python is retired and
is not a supported runtime or fallback.

This project is in a **core-hardening phase**. Phase 1 (core hardening) is done:
the session core is method-encapsulated with an explicit state machine and a
per-session turn queue (single-flight, race-safe close), the connection now
enforces a hello-first + auth-token handshake gate (not just advisory hello_ack
flags), and the connection core, session lifecycle, offline replay, search, and
FCM shaping carry automated `go test -race` coverage. Next is Phase 2
(fixed-endpoint E2E against the real app). The **Verified** column is the source
of truth — not "implemented", but "proven to work, and how".

Verification levels:

- **U** — automated `go test` unit/integration coverage (regression-protected).
- **E** — manually exercised this session with a protocol-faithful Python WS
  client (real CLI turns). *Not* automated, *not* the real app — see Phase 2.
- **A** — Phase 2 (A core loop + B peripherals): exercised end-to-end with the
  **real Capacitor app on a physical device** (Galaxy S10+), driven over adb,
  verified via the protocol frame log. The single connection was isolated to
  this Go bridge (prod made non-discoverable). Found+fixed two cwd-`~` parity
  gaps — Python `os.path.expanduser`s the app's literal `~` cwd, Go didn't:
  (1) spawn `chdir ~` crashed; (2) `get_git_diff` returned `no_cwd`. Both now
  resolved via `runtime.ExpandPath`, expanded once at `new_session` (mirrors
  `session_routes.py`) plus defensively at the git-diff call site for sessions
  restored from an older persistence file.
- **C** — credential/transport chain verified, but not full delivery (e.g. FCM
  reached Google and authed; actual device delivery untested).
- **—** — not built.

| Area | Go | Verified | Notes |
|------|:--:|:--------:|-------|
| protocol parse / outbound event schema | ✓ | **U** | ParseInbound, tristate pinned/hidden, nullable usage windows, never-null arrays |
| pairing (claim/unclaim) | ✓ | **U** | single-owner lock + pairing.json persistence; claim-by-another rejected |
| handshake/auth gate (enforced) | ✓ | **U** | first frame must be hello; locked bridge / BRIDGE_AUTH_TOKEN rejects bad token at handshake & never registers the client (httptest WS integration); mirrors connection.py + _is_auth_token_valid |
| offline buffer + reconnect replay | ✓ | **U** | 0-client buffering, text_chunk merge, 10k cap; clients negotiate `replay_ack`, receive bounded 64-event batches, and the bridge commits only after `offline_replay_ack`. Disconnect-before-ACK resends the batch; legacy clients are paced one event at a time. U-tested with 2,050 events (> old 1,024 send queue), reconnect mid-batch, and `-race` |
| Codex Goal durable sync | ✓ | **U** | post-turn `thread/goal/get` reconciliation; atomic `goal_snapshots.json`; `goals_snapshot` on every hello; per-session offline goal updates coalesce to latest; app also refreshes unconditionally after `done` |
| session state machine + per-session turn queue | ✓ | **U** | idle/streaming/stopping/closed transitions; turns serialized (no concurrent turn per session); close stops worker |
| authoritative multi-device runtime | ✓ | **U** | durable `session_runtime.json`; monotonic per-session revisions and fine-grained received/queued/resuming/starting/submitting/waiting/thinking/tooling/composing/terminal stages; per-device delivery/read cursors; reconnect snapshot drives history reconciliation and unread without cross-device ACK loss |
| Search (SQLite FTS5 CJK) | ✓ | **U**+E+**A** | real-DB roundtrip: ASCII/CJK trigram, 1–2 char CJK LIKE fallback, context, pagination; real app `request_search` "宇宙" → 14 hits w/ CJK-LIKE-fallback notice. (`request_session_list` is wired but the app has no live UI caller — U-tested only) |
| fcm summary shaping | ✓ | **U** | markdown strip, last-paragraph, first-sentence, 120-rune cap (Python-parity incl. CJK split) |
| Connection: hello/ping | ✓ | **U**+E+**A** | hello_ack/pong; real app handshake + close-code 1000 parity; reconnect 3/3 after server restart (gen epoch reconciles) |
| Session: new/list/close/clear | ✓ | **U**+E+**A** | client-supplied session_id; real app new_session for claude (cwd `~`) & codex (cwd /Users/wulala); restart roundtrip U-tested |
| Prompt: message/stop | ✓ | **U**+E+**A** | real app message→chunk→done + stop→stopped, both backends; claude single-chunk vs codex incremental token stream observed |
| Claude backend | ✓ | E+**A** | CLI + NDJSON, resume via session_uuid; real app turn verified; cwd `~` expanduser fixed |
| Codex backend | ✓ | E+**A** | app-server JSON-RPC, threads, auto-approve; real app turn verified (userMessage/reasoning/agentMessage tool events + token stream) |
| Ollama backend | ✓ | E | HTTP streaming, in-memory history |
| Persistence (survive restart) | ✓ | E+**A** | everything_go_sessions.json; real app: both sessions (claude+codex) restored across 3 server restarts |
| History / resume listing | ✓ | E+**A** | Claude JSONL; snapshot+delta; content_hash byte-identical to Python (incl. `< > &`); real app request_history + get_resumable_sessions on reconnect |
| rename / set_effort / set_session_meta / switch_config | ✓ | E+**A** | real app switch_session_config verified |
| usage (get_usage) | ✓ | E+**A** | claude.ai OAuth (bun+keychain) + codex rate limits; real app usage_report (five_hour/seven_day) observed |
| browse_dir / open_file | ✓ | E+**A** | listing + change-detect hash + active/resumable sessions (2-stage); real app open_file → file_opened: text content delivered + rendered, binary → `preview supports text files only` (Python-parity) |
| shell / tasks / processes / request_status | ✓ | E+**A** | /bin/bash -s stream; live pid; get/kill_process; real app shell_input→shell_output (PTY echo), get_tasks/get_processes, request_status → status_result (go_version/permission_mode/platform) |
| get_git_diff | ✓ | E+**A** | diff HEAD + auto-init baseline; unborn-HEAD → empty (Python-parity); real app roundtrip — fixed cwd `~` → `no_cwd` parity gap (see **A** legend). NB: a `~`/home cwd auto-inits a repo + `git add -A` over all of $HOME (slow; shared Python behavior, not a Go divergence) |
| FCM push | ✓ | **C** | HTTP v1 + service-account OAuth2; authed + rejected only the fake device token. Real delivery untested |
| hello_ack identity fields | ✓ | E | instance_name / root_dir / data_dir / lan_ip + proactive sessions_list |
| LAN discovery beacon (UDP) | ✓ | **U** | Opt-in with `--discovery`. `internal/netsvc` emits schema-valid announce on udp/8767 (app DiscoveryAnnounceSchema), captured off the wire + validated; broadcast egresses the LAN NIC to the real netmask-derived directed broadcast **and** 255.255.255.255 (no send errors). Fixes a latent bug shared with discovery_broadcaster.py: the class-C a.b.c.255 guess is wrong on a /22 (sends to a host, "host is down") — Go derives broadcast from the actual mask (unit-tested /8,/20,/22,/24). *Egress to the phone's subnet verified; the app-side `discovered` callback was not re-observed (release build hides WebView console from logcat; discovery only runs while disconnected) — same native plugin already proven against Python's identical beacon.* |
| mDNS (`_bridge._tcp`) | ✓ | **U** | Opt-in with `--mdns`. PTR/SRV/TXT(version=2)/A records byte-parity with zeroconf ServiceInfo (unit-tested pack + query-match); joins multicast on the LAN NIC (receives on macOS alongside the system mDNSResponder). App doesn't consume mDNS (uses the UDP beacon), so left at U |
| Cloudflare tunnel | ✓ | **U**+E | `cloudflared tunnel --url http://localhost:PORT`, trycloudflare URL parsed + captured (verified live against a throwaway port — not run against the skip-permissions bridge to avoid public exposure). Self-managed mode; external tunnel-file watcher not ported |
| Gemini backend | — | — | per-session JSON-RPC |
| message attachments (images / files) | ✓ | **U**+E | a `message` may carry `images:[{data(base64),media_type}]` + `files:[{name,content,media_type}]`; the claude backend builds stream-json content blocks in claude_cli.py order (images → files[pdf=document / else fenced text] → text). **U**: block order/shape pinned (`-race`). **E**: live `claude` — sent a 16×16 red PNG, asked the color → **"Red."** Codex/Ollama accept the params but are text-only for now. **CLI quirk found via A/B**: a non-ASCII char (em-dash) in `--append-system-prompt` makes the claude CLI **hang** when the same turn carries an image — kept the ask_user steering prompt ASCII-only so images + the MCP tool coexist |
| file push to device (media inbox) | ✓ | **U** | `internal/inbox` implements the inline file-push path: `push_file` reads and base64-inlines files up to 50 MB, targets connected devices except the sender, persists `inbox.json`, replays pending pushes on hello, and drains on `file_push_ack`. Firebase Storage / signed-URL large-file fallback is not ported; oversized files return a clear error |
| Interactions (ask the user) | ✓ | **U**+E+**A** | **Answering works end-to-end** via an in-process **`ask_user` MCP tool** (`goexec/mcp_askuser.go`) — the same approach Happy/ccpocket use (Agent SDK + MCP), implemented natively in Go and keeping CLI OAuth (no `ANTHROPIC_API_KEY`). The bridge hosts a minimal Streamable-HTTP MCP server (initialize/tools/list/tools/call; bridge session id in the URL path `/mcp/<id>`); claude is spawned with `--mcp-config` (HTTP, 30-min `MCP_TOOL_TIMEOUT`) + an `--append-system-prompt` steering it to call `mcp__ask_user__ask_question` instead of the built-in AskUserQuestion. The tool handler runs **in-process**: it raises `user_input_request` to the app (reusing the existing wire — no app change), blocks on `user_input_response`, and returns the answer as the MCP tool result — which Claude **honors** and continues the turn. **E (live `claude`)**: prompted to ask a color → Claude called `ask_user` → answered "Blue" → Claude replied **"You picked: Blue"**. **A (real Galaxy S10+)**: a fresh session's `user_input_request` broadcast to the app, which rendered its UserInputPrompt overlay (CJK question/options); tapped 藍色 + 送出 on the physical device → Claude continued **"你選了藍色"**. **U**: MCP initialize/tools/list/blocking tools/call round-trip + GET→405, register/respond/cancel/expire, question/option normalization (`-race`). Wire: `user_input_request` / `user_input_response` / `pending_interactions_list` / `interaction_resolved`; stop/clear/close cancel dangling prompts. *Why the MCP tool, not the built-in*: the `claude` CLI in headless `--print` mode auto-resolves the built-in AskUserQuestion as "dismissed" and ignores an injected stdin tool_result (verified — and Python prod has the same limitation); an MCP tool's result is the normal, honored path. The legacy native-tool stdin path is kept as a best-effort fallback |
| **feed (read + write)** | ✓ | **U**+E+**A** | `internal/feed` — full port of feed_ops.py: `feed_push` (store HTML/markdown article ≤5 MB to feed/articles/, dedup by client_dedup_key, index.json) → `feed_ack` + broadcast `feed_new` + FCM `NotifyFeedNew`; `feed_list_request`→`feed_list` (newest-first, dedup key stripped, GC of >7-day soft-deletes); `feed_fetch`→`feed_detail`; `feed_mark_read`/`feed_delete`→broadcast `feed_updated`. **U**: push/dedup/list/fetch/mark/delete/oversize/persist (`-race`). **E**: WS round-trip (CJK title + HTML). **A**: pushed to Go bridge → showed on real **Galaxy S10+** 收件匣→訂閱 feed (card "📰 郵報 #2 · POCKET_GAMER · Just now"), tapped → HTML article rendered ("即時測試" h1 + body). This is the LINE→feed product target. |
| instances / inbox (read) | ✓ | **U**+**A** | the app polls `list_instances`/`get_inbox` on connect; Go answers valid empty instances plus persisted inbox items. Instance *write* ops (start/stop/upsert/delete) are still unbuilt |
| permission approval | ✓ | **U**+E | `governance/permission.go` — port of permission_manager.py. Gates `kill_process` + `shell_input`: `Request` broadcasts `permission_request`, blocks on the user's `permission_response` (or TTL→deny), returns the decision; device-bound (only the requester can approve); modes off/warn/enforce via `BRIDGE_PERMISSION_MODE` (default enforce). Run in a goroutine so the read loop keeps handling the response (no deadlock — the exact hazard runtime_ops.py warns of). **U**: off/warn/enforce, approve/deny, device-mismatch, timeout (`-race`). **E**: WS — `kill_process` → permission_request → approve/deny → result, read loop never blocked |

### Chrome browser-origin approvals

Codex Browser Use requests access through `mcpServer/elicitation/request`. The Go
bridge can forward those prompts to the app or apply a controlled policy:

```bash
# Ask on the phone (default)
BRIDGE_BROWSER_ORIGIN_MODE=ask bash install.sh

# Allow only exact origins/hosts and wildcard subdomains
BRIDGE_BROWSER_ORIGIN_MODE=allowlist \
BRIDGE_BROWSER_ALLOWED_ORIGINS='https://studio.youtube.com,*.canva.com' \
bash install.sh

# Automatically allow every valid HTTP(S) origin for the current Codex session
BRIDGE_BROWSER_ORIGIN_MODE=allow_all bash install.sh
```

`deny` rejects ordinary origin access. Automatic approval never covers raw CDP
or other sensitive Chrome capabilities; those remain explicit, deny-by-default
phone prompts. Choosing “always allow” manually persists the browser permission,
while automatic approvals use session scope only.

Before app-server starts, the bridge atomically sets
`disable_auto_review = true` in `~/.codex/browser/config.toml`. This makes Codex
forward browser consent to the bridge instead of deciding first with its
built-in reviewer. Set `BRIDGE_BROWSER_MANAGE_AUTO_REVIEW=0` only when that
built-in reviewer should remain authoritative.

Current Codex builds require `danger-full-access` for Browser Use to read its
approval configuration and reach external origins. When no sandbox is selected,
the bridge therefore matches the Python backend and uses that mode while browser
networking is enabled. An explicitly selected sandbox is always preserved. Set
`BRIDGE_BROWSER_ENABLE_NETWORK=0` to retain the network-isolated
`workspace-write` default; external Chrome navigation will then be rejected
before an origin prompt can be answered.
| WebRTC P2P (Pion DataChannel) | ✓ | **U**+**A** | `internal/core/webrtc.go` — bridge is the answerer (bakes ICE into the SDP answer like aiortc, no outbound trickle); `webrtc_offer`→`webrtc_answer`, applies the app's trickled `webrtc_ice`; on DataChannel "bridge" open it promotes the channel to a full client (`serveConn` runs the same handshake+route loop over a `dcConn` wireConn) and emits `webrtc_ready`. A promoted PC detaches from the signaling client's lifecycle (connection.py:422 intent). **U**: in-process Pion-client ↔ bridge negotiation + DataChannel promotion + hello/hello_ack over the DC, `-race`. **A**: real Galaxy Note20 app over **4G cellular** — `pc state connecting→connected` (real CGNAT NAT traversal, STUN-only sufficed), DataChannel opened, promoted to `connected (webrtc)`, app commands flowed P2P. Found+fixed a process-crashing bug (background `sendHistory` enqueue after disconnect → send-on-closed-channel panic; now never closes `send`, gates on a `quit` channel — mirrors the mailbox fix). NB an **app**-side bug surfaced: on cellular the tunnel candidate URL is `tunnelUrl.trim()` but `adoptWinner(ws, ws.url)` passes the browser-normalized `ws.url` (trailing slash), so `winnerUrl === tunnelUrl` fails and the upgrade never starts unless the stored tunnelUrl carries the trailing slash |

### What "E" does NOT yet cover

E-level features were driven by a hand-written Python WS client, not the real
React/Capacitor app, and have no regression tests. Promoting them to trusted
status is **Phase 2 (fixed-endpoint E2E)** + backfilling **U** coverage. Until
then, treat E rows as "works in a demo", not "hardened".

> Single-controller note: like Python's `ws_ref` rebind, the Go core now keeps
> **one live client per device** (`latestByDevice`): a newer hello from the same
> device evicts the older client (shutdown + close). Events still broadcast to the
> set of live clients and buffer when *zero* are connected, so a reconnecting
> client recovers missed events.

### Anti-storm hardening (mobile half-disconnect)

The mobile app half-disconnects and reconnects in bursts (WebRTC↔WS flap,
backgrounding, flaky LAN/Tailscale), which otherwise spawns a request storm. The
Go core defends in `internal/core/storm.go`:

- **#1 latest-device-wins** — `latestByDevice`; a new hello evicts the same
  device's old client. Verified live (3 same-device WS → only newest survives).
- **#2 client context** — each client has a `ctx` cancelled on teardown/eviction.
- **#3 current-client guard** — `Client.live()` (not torn-down AND still latest);
  every heavy handler (history / resumable / browse stage-2 / usage / search)
  checks it before computing/returning, so a replaced client's work is dropped.
- **#4/#6/#7 coalesce + cache** — `singleflight` collapses concurrent identical
  `request_history` / `get_resumable_sessions` / browse scans into one execution;
  short TTL (history 2 s, resumable 5 s) absorbs sequential reconnect bursts.
- **#9 semaphore** — a global cap (6) on concurrent heavy tasks.
- **#10/#11 observability** — `[storm]`/`[conn]` logs (device, transport, queue
  depth, close reason); send-buffer overflow drops the client (never blocks the hub).
- **#15 regression tests** (`storm_test.go`, `-race`): 5 same-device → 1 current;
  stale client drops results; **100 identical `request_history` → 1 `LoadHistory`**;
  **100 `get_resumable_sessions` → 1 provider scan**; overflow drops client, hub healthy.

Deferred (round 2): #8 feed_list throttle (cheap + would starve reconnects).
Per-session Goal state is now coalesced and replay is application-ACKed; generic
terminal-event dedup remains covered by client request/seq dedup plus history
reconciliation.
