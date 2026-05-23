# Bridge — Model Onboarding Guide

## System Purpose

Python WebSocket server connecting a React/Capacitor app to local AI runtimes
(Claude CLI, Codex CLI, Ollama). Clients send JSON commands; bridge routes to
the selected backend, streams normalized events back.

---

## Entry Point & Startup

**File**: `bridge_v2.py`  
**Run**: `python bridge_v2.py [flags]`

| Flag | Default | Purpose |
|------|---------|---------|
| `--port` | 8766 | WebSocket port (also serves HTTP media on same port) |
| `--tunnel` | off | Start cloudflared public tunnel |
| `--backend` | claude | Default AI backend |
| `--model` | — | Model name (Ollama only) |
| `--ollama-host` | http://localhost:11434 | Ollama base URL |

**Startup sequence** (in `main()`):
1. Init Firebase (FCM + Storage)
2. Restore sessions from `saved_sessions.json`
3. Register mDNS service (`bridge.local`)
4. Start `websockets` server on `0.0.0.0 + ::` port 8766
   - `ping_interval=30`, `ping_timeout=60`, `compression="deflate"`
   - Doubles as HTTP server: `/media/*` requests handled by `_media_request_handler`

---

## Topology

```
app (React/Capacitor)
  ↕ WebSocket JSON :8766
bridge_v2.py
  ├─ ClaudeCliBackend      subprocess + NDJSON stream
  ├─ CodexAppServerBackend persistent `codex app-server` + JSON-RPC
  └─ OllamaBackend         HTTP POST to localhost:11434/api/chat
```

---

## Inbound Message Flow

```
ws.recv() raw frame
  → json.loads()
  → validate_client_msg()        type in _KNOWN_MSG_TYPES + required fields check
  → _dispatch_ws_message()       cascades through handler layers in order:
      handle_system_msg()        handlers/system_ops.py
      handle_runtime_msg()       handlers/runtime_ops.py
      handle_file_msg()          handlers/file_ops.py
      handle_webrtc_message()    webrtc_signaling.py
      handle_low_coupling_message()  message_router.py
          → handle_session_message()  session_routes.py
          → handle_prompt_message()   prompt_routes.py
```

---

## Global Registries

| Registry | Type | Lifecycle |
|----------|------|-----------|
| `_SESSIONS` | `dict[str, Session]` | Built on `new_session`; removed on `close_session`; restored from disk at startup |
| `_SHELL_SESSIONS` | `dict[str, ShellSession]` | Built on `shell_create`; removed on `shell_close`; max 5 concurrent |
| `_BACKENDS` | `dict[str, Backend]` | Lazy singleton per backend name, created on first use |
| `_READ_CURSORS` | `dict[str, dict[str, int]]` | session_id → device_id → seq; tracks per-device unread progress |

---

## Session Dataclass (key fields)

```python
session_id, name, cwd, backend, model
resume_id          # Claude conversation UUID or Codex thread ID
is_streaming, is_stopping
ws_ref             # active WebSocket; rebound on client reconnect
offline_buffer     # list[dict] — events buffered while client disconnected
effort             # "auto" | "low" | "medium" | "high" | "highest"
sandbox            # "read-only" | "workspace-write" | "danger-full-access"
current_request_id # injected into streaming events
```

---

## Backend Interface (`backends/base.py`)

```python
# Abstract — must implement all four
spawn(session)                              # establish connection / start subprocess
send(session, content, images, files)      # stream a response
stop(session)                              # abort current response
clear(session)                             # reset history

# Optional overrides (defaults are no-ops or False)
close(session)                             # default: calls stop()
supports_resume() → bool                   # default: False
fetch_usage(ws)                            # default: no-op
get_resumable_sessions(limit) → list[dict] # default: []
load_session_history(resume_id, limit)     # default: []
```

| Capability | Claude CLI | Codex | Ollama |
|-----------|:---------:|:-----:|:------:|
| supports_resume | ✓ | ✓ | ✓ |
| fetch_usage | ✓ | ✓ | ✗ |
| get_resumable_sessions | ✓ | ✓ | ✗ |
| load_session_history | ✓ | ✓ | ✓ |

---

## Claude CLI Backend (`backends/claude_cli.py`)

**Spawn command**:
```
claude --print --input-format stream-json --output-format stream-json
       --verbose --dangerously-skip-permissions
       [--resume <uuid>] [--effort <level>]
```

**NDJSON stream** — one JSON object per line, types:
- `assistant` → text chunks, thinking, tool calls
- `tool_result` → tool execution output
- `result` → turn complete; carries new `session_id` (used as next `resume_id`) and usage

**resume_id sources** (in priority order):
1. Client sends `resume_claude_id` in `new_session` message
2. Extracted from `result` event → persisted immediately
3. Fallback: scan newest `.jsonl` in `~/.claude/projects/<cwd>/`

**Stop**: SIGTERM → wait 2 s → SIGKILL → re-spawn a fresh process  
**FCM notification**: sent automatically when a Claude turn completes

---

## Codex Backend (`backends/codex_appserver.py`)

**Server**: one persistent `codex app-server` subprocess (singleton; `_start_lock`
prevents concurrent re-starts). Communicates over **newline-delimited JSON-RPC**
on stdin/stdout (limit 128 MiB).

**Session mapping**: each bridge session maps to one Codex thread.
- New session → `thread/start` RPC; `resume_id` = returned `thread.id`
- Resume → `thread/resume` RPC with existing `thread.id`
- Notifications arrive as unsolicited RPC frames; routed back via `_thread_to_session`

---

## Ollama Backend (`backends/ollama.py`)

**Endpoint**: `POST {host}/api/chat`  
**Body**: `{"model": …, "messages": […], "stream": true}`  
**Format**: newline-delimited JSON; `done: true` signals end  
**History**: in-memory per session, capped at 200 messages

---

## Event System (`backends/events.py`)

All session-scoped events funnel through `send_event(session, event)`:

1. Inject `session_id` (and `request_id` when streaming)
2. Try `_EVENT_DISPATCHER` (broadcast layer for multi-client)
3. Direct `session.ws_ref.send()`
4. **Offline buffer** — max 10,000 events; `text_chunk` merges into the
   previous buffer entry instead of dropping; other types drop the oldest

**`_evt_*` builders** (session-scoped events):
`text_chunk`, `tool_start`, `tool_result`, `tool_end`, `media`, `document`,
`thinking_chunk`, `done`, `stopped`, `error`, `session_warning`,
`session_died`, `session_closed`, `resume_progress`

**`_msg_*` builders** (connection-wide messages):
`pong`, `error`, `session_created`, `session_renamed`, `session_history`,
`history_snapshot`, `history_delta`, `resumable_sessions`, `session_uuid`,
`shell_created/output/closed`, `tasks_list`, `task_killed`,
`processes_list`, `process_killed`, `dir_listing`, `usage_report`

**Media scan**: `scan_for_media(text, session)` regex-matches file paths in
assistant output, classifies by extension (image / video / document), generates
URLs via `http://127.0.0.1:9090/<path>` (local) or `{MEDIA_BASE_URL}/media/<path>`
(tunnel), then emits `_evt_media` / `_evt_document` events.

---

## Shell Sessions

```python
@dataclass
class ShellSession:
    shell_id: str                  # "sh_<random>"
    proc: asyncio.subprocess.Process   # /bin/bash -s
    ws_ref: Any
    cwd: str
    read_task: Optional[asyncio.Task]
```

Commands: `shell_create` → spawn `/bin/bash -s`, start reader task  
`shell_input` → write line to stdin  
`shell_close` → terminate process, pop from registry  
Max concurrent shells: 5

---

## Persistence

| File | Format | Purpose |
|------|--------|---------|
| `saved_sessions.json` | `dict[session_id → metadata]` | name, resume_id, cwd, backend, model, sandbox, last_used; pruned >90d or >200 entries |
| `session_meta.json` | `dict[session_id → {pinned, hidden}]` | UI pin/hide state |
| `read_cursors.json` | `dict[session_id → {device_id → seq}]` | Per-device unread tracking |

Sessions written on: turn complete, `new_session`, `rename_session`, `switch_session_config`.  
Sessions restored on: startup via `_restore_sessions_from_disk()`.

---

## Network Layer

**`hello_ack` fields** (sent in response to every `hello` message):

| Field | Type | Source |
|-------|------|--------|
| `client_id` | str | assigned by bridge on connect |
| `device_id` | str | echoed from client |
| `device_name` | str | echoed from client |
| `is_locked` | bool | pairing state (bridge_v2.py only) |
| `locked_to_me` | bool | pairing state (bridge_v2.py only) |
| `instance_name` | str | `--instance-name` flag or `BRIDGE_INSTANCE_NAME` env |
| `root_dir` | str | filesystem jail root (`""` = no jail) |
| `data_dir` | str | instance persistence directory |

**mDNS**: `zeroconf` library registers `_bridge._tcp.local.` at startup.
Clients discover via `bridge.local`. Disabled by `BRIDGE_DISABLE_MDNS=1`.

**Cloudflare Tunnel**: `cloudflared tunnel --url http://localhost:{port}`.
Activated by `--tunnel` flag or `BRIDGE_AUTO_TUNNEL=1` (auto-triggers 120 s
after last client disconnect). Tunnel URL pushed to mobile via FCM.

**Firebase FCM** (`bridge_v2.py` → `notify_fcm()`):
- Init: reads `serviceAccountKey.json`, sets up Firebase Admin SDK
- Token: stored in `fcm_token.txt`, updated by `fcm_token` command from client
- Sends push on Claude turn complete (3 retries, exponential backoff)
- Payload: `{title: "✓ <session_name>", body: <first 160 chars>, data: {session_id}}`

---

## All Inbound Command Types

**Session mgmt**: `new_session`, `close_session`, `rename_session`,
`clear_session`, `switch_session_config`, `set_session_meta`

**Prompt**: `message`, `stop`

**System**: `ping`, `hello`, `get_usage`, `get_resumable_sessions`,
`request_sessions_list`, `request_history`, `get_all_sessions`,
`set_effort`, `fcm_token`

**Runtime**: `shell_create`, `shell_input`, `shell_close`,
`get_tasks`, `kill_task`, `get_processes`, `kill_process`

**File**: `push_file`, `file_push_ack`, `browse_dir`

**Search**: `request_search`, `request_search_health`,
`request_session_list`, `request_search_context`

**WebRTC**: `webrtc_offer`, `webrtc_answer`, `webrtc_ice`

---

## Key Files

```
bridge_v2.py                         Entry, registries, WS handler, startup
bridge/backends/base.py              Backend abstract interface
bridge/backends/claude_cli.py        Claude subprocess backend
bridge/backends/codex_appserver.py   Codex JSON-RPC backend
bridge/backends/ollama.py            Ollama HTTP backend
bridge/backends/events.py            Event/message builders, send_event, media scan
bridge/backends/history.py           History loading utilities
bridge/handlers/session_routes.py    Session CRUD commands
bridge/handlers/prompt_routes.py     message / stop commands
bridge/handlers/runtime_ops.py       Shell, tasks, processes
bridge/handlers/file_ops.py          File push / browse
bridge/handlers/system_ops.py        Usage, resumable sessions
bridge/handlers/message_router.py    Top-level command dispatcher
bridge/session_registry.py           Persistence: save/restore sessions
docs/WS_PROTOCOL.md                  Canonical wire protocol reference
```

---

## Extension Patterns

### Add a backend
1. Implement `Backend` in `bridge/backends/`
2. Register in `_get_or_create_backend()` in `bridge_v2.py`
3. Update frontend: `app/src/schemas/bridge.ts` + `app/src/store/settingsSlice.ts`
4. Verify `get_tasks`, resume listing, and usage behaviour

### Add a command type
1. Add to `_KNOWN_MSG_TYPES` and `_INBOUND_REQUIRED` in `bridge_v2.py`
2. Implement handler in the appropriate `handlers/` file
3. Wire into `_dispatch_ws_message()` call chain

### Add a WebSocket event (backend → frontend)
1. Add `_evt_*` or `_msg_*` builder in `backends/events.py`
2. Extend `BridgeEventSchema` in `app/src/schemas/bridge.ts`
3. Handle in `app/src/hooks/ws/eventRouter.ts`
4. If persisted: update `app/src/schemas/persist.ts` + bump version + add migration

### Change persisted session/message shape
Mandatory simultaneous triple update:
1. Type in `app/src/types/bridge.ts`
2. Schema in `app/src/schemas/persist.ts`
3. Version bump + migration in persist middleware

Missing any one causes **silent data wipe** on next launch.
