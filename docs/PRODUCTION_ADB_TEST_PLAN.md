# Historical Go Bridge Production ADB Migration Test Plan

> Archived evidence only. The Python Bridge referenced below is retired and
> must not be started, deployed, or used as a recovery fallback.

This document is the handoff checklist for validating the Go bridge with a real Android device through ADB before promoting it to production.

Target repository:

```bash
/Users/wulala/Downloads/Helper/claude-bridge
```

Go bridge:

```bash
/Users/wulala/Downloads/Helper/claude-bridge/go
```

Android app:

```bash
/Users/wulala/Downloads/Helper/claude-bridge/app
```

## Goal

Prove that the Go bridge can replace the Python bridge for real mobile app usage, not only unit tests or a synthetic WebSocket client.

Production readiness requires:

- Real Android device can connect to the Go bridge.
- Core chat loop works for Claude, Codex, and Ollama where available.
- History, resume, reconnect, compact, AskUserQuestion, tool output, and large output paths behave correctly.
- Reconnect storms and app background/resume do not corrupt state or drop turns.
- No lingering test servers remain after the run.

## App Feature Coverage Requirement

This is not only a backend chat test. The production pass must cover every app-facing feature that the React/Capacitor app can call through the bridge protocol.

Use this matrix as the source of truth for coverage. Mark each item `PASS`, `FAIL`, `N/A`, or `NOT IMPLEMENTED IN GO`.

| Area | App command / event surface | Must verify on phone |
|---|---|---|
| Connection | `hello`, `hello_ack`, `ping`, `pong`, reconnect, offline replay | App reaches connected state, reconnects after bridge restart, no stale disconnected banner |
| Session list | `request_sessions_list`, `sessions_list`, `get_all_sessions`, `sessions_list_append` | Dashboard/session list updates, bridge ownership is correct, hidden/pinned state survives |
| Session lifecycle | `new_session`, `session_created`, `close_session`, `session_closed`, `rename_session`, `session_renamed` | Create, rename, close from app UI |
| Session config | `switch_session_config`, `set_effort`, `set_session_meta`, `session_meta_updated` | Backend/model/sandbox/effort/pinned/hidden update correctly |
| Chat stream | `message`, `message_ack`, `thinking_chunk`, `text_chunk`, `tool_start`, `tool_result`, `tool_end`, `todo_update`, `done`, `stopped`, `error` | Claude/Codex/Ollama message loop, tools, todos, stop, error states |
| Attachments | `message.images`, `message.files`, `media`, `document` | Image, text file, PDF/document behavior |
| Compact/session commands | `session_command_queued`, `session_command_started`, `session_command_done`, `session_command_failed` | Manual and auto compact state does not leave UI stuck |
| History | `request_history`, `history_snapshot`, `history_delta`, `history_sync_hint`, before-page | Initial load, delta merge, scroll-up pagination, compact does not hide older messages |
| Resume | `get_resumable_sessions`, `resumable_sessions`, `session_uuid`, `resume_progress` | Resume list, reopened sessions, UUID capture |
| Fork | `fork_session`, `session_forked`, `fork_error` | Fork from message/session and continue turn |
| Usage | `get_usage`, `usage_report` | Usage card/report appears or clear unsupported error |
| Files | `browse_dir`, `dir_listing`, `open_file`, `file_opened` | File browser, folder cache/hash, text preview, unsupported file error |
| File push/inbox | `file_push`, `file_push_ack`, `get_inbox`, `inbox_list` | Receive pushed file, ack, pending inbox persistence |
| Feed | `feed_list_request`, `feed_list`, `feed_fetch`, `feed_detail`, `feed_mark_read`, `feed_delete`, `feed_new`, `feed_updated`, `feed_ack` | Feed list/detail/read/delete |
| Search | `request_search`, `search_result`, `request_search_health`, `search_health`, `request_session_list`, `session_list`, `request_search_context`, `search_context` | ASCII/CJK search, health, session list, context view |
| Runtime ops | `request_status`, `status_result`, `get_tasks`, `tasks_list`, `kill_task`, `task_killed`, `get_processes`, `processes_list`, `kill_process`, `process_killed` | Status page/tasks/process list/kill guarded path |
| Shell | `shell_create`, `shell_created`, `shell_input`, `shell_output`, `shell_close`, `shell_closed` | Interactive shell opens, outputs, closes |
| Permission | `permission_request`, `permission_response`, `permission_result` | Approve/deny protected operation without read-loop deadlock |
| User input | `user_input_request`, `user_input_response`, `pending_interactions_list`, `interaction_resolved`, `interaction_expired` | AskUserQuestion prompt, answer, cancel, pending replay |
| Instances | `list_instances`, `instances_list`, `start_instance`, `instance_started`, `stop_instance`, `instance_stopped`, `upsert_instance`, `instance_upserted`, `delete_instance`, `instance_deleted`, `instance_error` | Instance UI behavior; mark unsupported if Go intentionally lacks write ops |
| Git diff | `get_git_diff`, `git_diff_result` | Diff view works for repo cwd and non-repo cwd |
| Pairing/security | `claim_bridge`, `claim_ack`, `unclaim_bridge`, `unclaim_ack`, auth token failure | Claim/unclaim flow and locked bridge rejection |
| Discovery/presence | UDP discovery, mDNS optional, `tunnel_url` optional | Fixed endpoint first, discovery/tunnel only after core pass |
| WebRTC | `webrtc_offer`, `webrtc_answer`, `webrtc_ice`, `webrtc_ready` | Optional P2P upgrade and command flow over DataChannel |
| Agent tree | `get_agent_tree`, `agent_tree` | Subagent tree appears for Claude sessions with subagents |

If any row is `NOT IMPLEMENTED IN GO`, record whether it blocks production. Do not silently skip it.

## Required Device

Use a real Android device connected by USB with ADB enabled.

Known-good examples:

- Galaxy S10+ `SM-G975F`
- Galaxy Note20

Confirm:

```bash
adb devices -l
```

Expected:

```text
<serial> device product:... model:... device:...
```

If the device is unauthorized, unlock the phone and accept the USB debugging prompt.

## Preflight

Run all commands on `wulalamacbook`.

```bash
cd /Users/wulala/Downloads/Helper/claude-bridge/go
go test ./...
```

Must pass.

Check no old Go test bridge is already listening:

```bash
lsof -nP -iTCP:8767 -sTCP:LISTEN || true
lsof -nP -iTCP:9001 -sTCP:LISTEN || true
```

Expected: no output.

Check local IP reachable by phone:

```bash
ipconfig getifaddr en0
```

Record it:

```text
GO_BRIDGE_HOST=<mac_lan_ip>
GO_BRIDGE_PORT=8767
```

Example:

```text
192.168.68.50:8767
```

## Build Go Bridge

```bash
cd /Users/wulala/Downloads/Helper/claude-bridge/go
go build -o everything-go ./cmd/everything-go
```

Start fixed-endpoint Go bridge:

```bash
cd /Users/wulala/Downloads/Helper/claude-bridge/go
./everything-go --port 8767 --executor go
```

Keep this terminal open. Save important logs.

Do not enable mDNS, discovery, Cloudflare tunnel, or WebRTC for the first production pass. Fixed endpoint must pass first.

## Build And Install Android App

In a second terminal:

```bash
cd /Users/wulala/Downloads/Helper/claude-bridge/app
npm run typecheck
npm run test
npm run build
npx cap sync android
cd android
./gradlew assembleDebug
adb install -r app/build/outputs/apk/debug/app-debug.apk
```

If testing first-install behavior, clear app data before launching:

```bash
adb shell pm clear com.everything.app
```

If the package name differs, find it:

```bash
adb shell pm list packages | grep -i everything
```

Launch app:

```bash
adb shell monkey -p com.everything.app 1
```

## Log Capture

Capture app logs during the whole run:

```bash
adb logcat -c
adb logcat \
  | grep -Ei "Capacitor|chromium|everything|bridge|websocket|webrtc|fatal|exception|error"
```

In another terminal, optionally capture all logs to a file:

```bash
adb logcat -v time > /tmp/go_bridge_adb_full.log
```

Bridge logs should remain visible in the Go bridge terminal.

## App Bridge Setup

In the app:

1. Open bridge settings.
2. Add a bridge named `Go-Production-E2E`.
3. Host: Mac LAN IP, for example `192.168.68.50`.
4. Port: `8767`.
5. Set this bridge active.

Expected:

- App shows connected.
- No persistent disconnected banner.
- Go bridge logs show one client connected.
- `hello_ack` and `sessions_list` are processed without crash.

Failure:

- If header says connected but a disconnected banner remains, capture screenshot and logcat.
- If app falls back to Python `8766`, this is a blocker for production because active bridge selection is polluted.

## Test 1: Empty Session Load

Clear Go sessions if needed, then open the app on Go bridge.

Expected:

- No permanent "loading history" state.
- Empty session view renders.
- No Python sessions are injected into Go bridge state.

Pass criteria:

- Fresh Go state shows either no sessions or only Go-created sessions.
- App does not silently reconnect to Python bridge.

## Test 2: Claude Basic Turn

Create a new Claude session on Go bridge.

Prompt:

```text
回覆 OK，其他不要說
```

Expected:

- Text chunk appears.
- Turn ends with done.
- Session remains usable.
- `session_uuid` is emitted or resume id is captured.

Bridge log should not show:

- process died
- unknown session
- thread not found
- send timeout

ADB logcat should not show:

- `FATAL EXCEPTION`
- `TypeError`
- `ReferenceError`
- WebView crash

## Test 3: Codex Basic Turn

Create or switch to a Codex session.

Prompt:

```text
回覆 OK，其他不要說
```

Expected:

- Codex app-server starts if needed.
- Text response arrives.
- Done event arrives.
- Reusing the same session for another prompt works.

Second prompt:

```text
你還記得我剛剛要你回什麼嗎？簡短回答
```

Expected:

- No `thread not found`.
- If app-server restarted, stale thread retry should recover automatically.

## Test 4: Ollama Basic Turn

Only run if local Ollama is available.

Model example:

```text
qwen2.5:7b
```

Prompt:

```text
Reply exactly: OK
```

Expected:

- Non-empty assistant response.
- Ollama HTTP errors surface as visible `ollama_error`, not silent empty assistant text.

## Test 5: History Snapshot And Delta

On a session with at least two completed turns:

1. Leave the session.
2. Reopen it.
3. Confirm previous messages render.
4. Send a new message.
5. Reopen again.

Expected:

- Previously completed messages remain visible.
- New messages merge without duplicates.
- No old assistant message disappears after reconnect.

Check Go history behavior:

- Snapshot may return tail window only.
- Delta must be used when a known cursor exists.
- `has_more_before=true` must not cause local SQLite messages outside the tail window to be deleted.

Blocker:

- A message confirmed present in JSONL disappears from UI after compact/reopen and cannot be recovered by scrolling upward.

## Test 6: Before Page / Scroll Up

Use a long session with more than the default history limit.

Steps:

1. Open the session.
2. Scroll to top.
3. Trigger older history page load.

Expected:

- Older messages prepend.
- Scroll position stays stable enough to continue reading.
- Missing old assistant replies can be recovered by paging before.

Blocker:

- Before-page request loops.
- Older messages duplicate heavily.
- Scroll jumps to bottom and prevents reading.

## Test 7: Compact Lifecycle

Run for Claude and Codex.

Manual compact prompt:

```text
/compact
```

Expected events:

- `session_command_started`
- `session_command_done` on success
- `session_command_failed` on failure

Expected UI:

- Compact loading/banner state appears if supported.
- Compact completion clears active command state.
- No ordinary user/assistant bubble is left stuck as streaming.

After compact, send:

```text
compact 後回覆 OK
```

Expected:

- Session still works.
- History cursor is preserved.
- App does not fall back to a fresh tail-only snapshot that hides older messages.

## Test 8: Auto Compact

This is hard to force naturally. Use an existing long session near context threshold or temporarily lower the threshold in a test branch.

Expected:

- Auto compact emits `session_command_started`.
- Auto compact emits `session_command_done` or `session_command_failed`.
- User turn completes normally before compact starts.
- Auto compact does not recursively trigger another compact immediately.

Failure:

- Queue stays blocked.
- Session remains streaming after compact.
- History cursor resets and old messages disappear.

## Test 9: AskUserQuestion / User Input

Use Claude.

Prompt:

```text
請用工具詢問我喜歡紅色還是藍色，等我回答後只說「你選了：<答案>」。
```

Expected:

- App shows user input prompt.
- Choose an option on the phone.
- Claude continues after answer.
- `interaction_resolved` arrives.
- Pending interaction list becomes empty.

Also test timeout/cancel path if practical:

- Start AskUserQuestion.
- Stop the session.

Expected:

- Pending prompt is cancelled.
- No stale prompt remains after reopening.

## Test 10: Large Tool Output

Use Claude or Codex with a tool command that emits large output.

Example prompt:

```text
在目前目錄執行一個會輸出很多行但不修改檔案的命令，然後簡短總結。
```

Expected:

- UI does not freeze permanently.
- Tool output is truncated at bridge limit.
- WebSocket remains connected or reconnects cleanly.
- Later text/done events still arrive.

Pass criteria:

- Large tool result does not block other sessions for multiple seconds.
- No unbounded memory growth.
- No missing terminal done event.

## Test 11: App Background / Resume

Steps:

1. Start a turn.
2. Press Home while it is streaming.
3. Open LINE or another app for 30-60 seconds.
4. Return to the app.

Expected:

- At most one active WebSocket per device after reconnect settles.
- No reconnect storm.
- Offline buffered events replay.
- Turn is either complete, stopped, or visibly errored; not stuck forever.

Bridge logs to inspect:

- same device old client evicted
- no repeated heavy history scans
- no keepalive timeout cascade

## Test 12: Network Flap

Do not switch Wi-Fi networks for the first pass. Only test normal app backgrounding.

Then optional:

1. Turn phone Wi-Fi off.
2. Wait for disconnect.
3. Turn Wi-Fi on.
4. Reopen app.

Expected:

- Reconnect succeeds.
- Session list reconciles.
- History request storm is coalesced.
- No duplicate session creation.

## Test 13: Bridge Restart While App Is Open

With app connected to Go bridge:

1. Stop Go bridge with Ctrl-C.
2. Wait until app shows disconnected.
3. Restart:

```bash
cd /Users/wulala/Downloads/Helper/claude-bridge/go
./everything-go --port 8767 --executor go
```

Expected:

- App reconnects.
- Existing sessions restore from Go persistence.
- History loads.
- Next message works.

Run this three times.

Blocker:

- App reconnects to Python bridge unexpectedly.
- Session exists in UI but every new turn fails.
- Codex stale thread does not recover.

## Test 14: Multi-Client / Latest Device Wins

If a second client is available, connect both.

For same device reconnect:

- Repeated app foreground/resume should evict old same-device socket.
- Only newest client remains live.

Expected:

- No duplicate delivery to stale socket.
- No background tasks keep sending to old client.

## Test 15: Feed / Inbox

If feed push tooling is available, push an article into Go bridge.

Expected:

- `feed_new` arrives.
- Phone inbox/feed UI displays the item.
- Detail page opens and renders HTML/markdown.
- Mark read/delete updates state.

## Test 16: File Push / Open File

Test small text file:

```text
open_file /path/to/small.txt
```

Expected:

- `file_opened` event arrives.
- Text preview renders.

Test binary or unsupported file:

Expected:

- Clear unsupported preview error.
- No app crash.

## Test 17: Search

Search ASCII and CJK terms.

Examples:

```text
OK
宇宙
測試
```

Expected:

- Search result event arrives.
- CJK fallback works.
- Pagination works.

## Test 18: Permission / Processes / Shell

If permission approval is enabled:

1. Trigger `get_processes`.
2. Trigger safe shell command:

```bash
pwd
```

3. Trigger protected process kill only against a harmless test process.

Expected:

- Permission prompt appears where required.
- Approve/deny both work.
- Read loop does not deadlock while waiting for permission response.

## Test 19: WebRTC Optional Pass

Only after fixed WebSocket endpoint passes.

Enable WebRTC path according to app settings.

Expected:

- WebSocket signaling connects.
- WebRTC offer/answer completes.
- DataChannel opens.
- App promotes connection to WebRTC.
- Commands still work over DataChannel.

Failure to upgrade to WebRTC is not a blocker for fixed-endpoint production unless production requires WebRTC.

## Test 20: Cloudflare / Tunnel Optional Pass

Only after fixed endpoint and WebRTC pass.

Start Go bridge with tunnel only if public exposure is acceptable for the test:

```bash
./everything-go --port 8767 --executor go --tunnel
```

Expected:

- Tunnel URL discovered.
- App connects through tunnel.
- Basic turn and reconnect pass.

Do not run tunnel against sensitive skip-permissions sessions unless explicitly approved.

## Test 21: Session Config, Metadata, Pinned, Hidden

From the dashboard/session settings:

1. Rename a session.
2. Change backend/model/sandbox where the UI allows it.
3. Set effort to a non-default value.
4. Toggle pinned.
5. Toggle hidden.
6. Request sessions list again or restart the app.

Expected:

- `session_renamed` or local rename state is reflected.
- `switch_session_config` changes future turns.
- `set_effort` is reflected in next backend spawn.
- `set_session_meta` persists pinned/hidden.
- Hidden sessions behave correctly in list filters.

Blocker:

- UI shows one backend/model but Go bridge uses another.
- Pinned/hidden state is lost after reconnect.

## Test 22: Fork Session

Use a session with several messages.

Steps:

1. Long press or use UI action to fork from a specific message.
2. Name the fork.
3. Open the forked session.
4. Send a new message in the fork.

Expected:

- `session_forked` arrives.
- Fork contains history up to the selected message.
- New message does not mutate the parent session.

Failure:

- `fork_error` without a clear reason.
- Fork opens but has empty/wrong history.

## Test 23: Usage Report

From UI action or settings:

```text
get_usage
```

Expected:

- `usage_report` is accepted by schema.
- Five-hour / seven-day windows render if backend supports them.
- Unsupported backend returns clear empty/unsupported state, not a crash.

## Test 24: Browse Directory Cache And File Open

In Files page:

1. Browse default cwd.
2. Open a subdirectory.
3. Navigate back.
4. Re-open same directory to exercise `client_hash` cache behavior.
5. Open a text file.
6. Open a binary or large unsupported file.

Expected:

- `dir_listing` renders entries, sessions, active/resumable sessions if present.
- Cached browse does not show stale content after directory changes.
- `file_opened` text content renders.
- Unsupported file shows an error message, not a crash.

## Test 25: File Push And Inbox

Trigger a file push from bridge or another client if available.

Expected:

- Phone receives `file_push`.
- App sends `file_push_ack`.
- `get_inbox` returns the item before ack and clears after ack where applicable.
- Reconnect replays pending file pushes.

If there is no file-push producer in the UI, use a protocol test client to send through the Go bridge and confirm phone receives it.

## Test 26: Feed Full Flow

Use existing feed push mechanism or a protocol client.

Steps:

1. Push an article.
2. Open Feed/Inbox UI.
3. Request feed list.
4. Open detail.
5. Mark read.
6. Delete.

Expected:

- `feed_new` appears.
- `feed_list` includes item.
- `feed_detail` renders HTML/markdown.
- `feed_updated` reflects read/delete.
- Deleted item does not reappear after reconnect unless expected by retention policy.

## Test 27: Search Full Flow

Run from app search UI.

Queries:

```text
OK
測試
宇宙
```

Also test:

- `request_search_health`
- `request_session_list`
- `request_search_context`

Expected:

- `search_result` renders hits.
- CJK query works.
- Health state is visible.
- Session list pagination works.
- Context panel opens around the selected message.

## Test 28: Runtime Status, Tasks, Processes

From status/tasks/process UI:

1. Request status.
2. Request tasks.
3. Request processes.
4. Kill only a harmless test process.

Expected:

- `status_result` includes Go runtime details.
- `tasks_list` reflects active Claude/Codex process where applicable.
- `processes_list` renders.
- `process_killed` / `task_killed` reports success/failure clearly.

Do not kill production/user processes during the test.

## Test 29: Shell

Open app shell UI.

Commands:

```bash
pwd
echo go-bridge-shell-ok
```

Then close shell.

Expected:

- `shell_created` returns id.
- `shell_output` streams data.
- `shell_closed` arrives.
- Shell cannot keep running after close/reconnect unless intentionally persisted.

## Test 30: Permission Approval

Enable enforce/warn mode if configurable.

Trigger a protected operation, for example `kill_process` against a harmless test process or a protected shell command.

Expected:

- `permission_request` renders on phone.
- Deny returns `permission_result` deny and operation does not run.
- Approve returns allow and operation runs.
- While waiting for approval, WebSocket read loop still receives `permission_response`.

Blocker:

- Bridge deadlocks waiting for permission response.

## Test 31: Pending Interactions Replay

Start an AskUserQuestion and disconnect/reconnect before answering.

Expected:

- Reconnect triggers `pending_interactions_list`.
- App shows the pending question.
- Answer resolves it.
- `interaction_resolved` removes it from pending UI.

Also verify stop/clear/close cancels pending prompts.

## Test 32: Instances UI

Open instances UI if present.

Commands to cover:

- `list_instances`
- `start_instance`
- `stop_instance`
- `upsert_instance`
- `delete_instance`

Expected:

- If Go implements the operation, corresponding event arrives.
- If Go does not implement write ops yet, app shows a clear unsupported/error state.
- No schema parse errors or silent hangs.

Record exact support status. This row cannot be omitted from production report.

## Test 33: Git Diff

Use a session cwd that is a git repo.

Steps:

1. Request git diff.
2. Create a small uncommitted change.
3. Request git diff again.
4. Test a non-repo cwd.

Expected:

- `git_diff_result` renders changed file/diff.
- Non-repo cwd returns clear empty/error result.
- Home `~` cwd expands correctly and does not return `no_cwd`.

## Test 34: Pairing / Claim / Auth

If bridge lock/auth is enabled:

1. Claim bridge with valid token.
2. Try invalid token from another client.
3. Unclaim bridge.
4. Reconnect phone.

Expected:

- `claim_ack` accepted for valid token.
- Bad token rejected during handshake/claim.
- `unclaim_ack` clears lock state.
- No unauthorized client enters normal command path.

## Test 35: Agent Tree

Use a Claude session that creates subagents if possible.

Steps:

1. Trigger a task likely to create subagents.
2. While it runs, request agent tree.
3. Observe automatic updates if Go poller emits them.

Expected:

- `agent_tree` schema accepted.
- Tree shows agent id/type, prompt id, tool calls, output preview.
- Updates do not flood the UI.

If no subagent can be naturally produced, mark this as limited and use a fixture/protocol test for Go backend.

## Test 36: Media And Document Events

If backend can emit `media` or `document` events:

1. Trigger image/media output.
2. Trigger HTML/PDF document output.

Expected:

- Media block renders.
- Document block opens.
- Missing local file or unsupported MIME gives a clear error.

If current Go backends never emit these events, mark `NOT IMPLEMENTED IN GO` and decide whether production needs them.

## Test 37: Offline Queue And Message Ack

Steps:

1. Disconnect phone network or stop bridge.
2. Send a message while offline if UI allows queued send.
3. Restore bridge/network.

Expected:

- App queues or rejects the message clearly.
- `message_ack` resolves queued state when delivered.
- No duplicate user messages after reconnect.

## Test 38: Full App Navigation Smoke

Manually visit every primary app screen while connected to Go:

- Chat
- Dashboard/session list
- Files
- Search
- Feed/Inbox
- Settings/bridge edit
- Instances
- Tasks/processes/status
- Shell

Expected:

- No screen gets stuck loading due to missing Go event.
- No uncaught JS exception in logcat.
- No schema parse error for Go events.

## Required Evidence To Collect

For each test run, save:

- Device model and Android version:

```bash
adb shell getprop ro.product.model
adb shell getprop ro.build.version.release
```

- Go commit/status:

```bash
cd /Users/wulala/Downloads/Helper/claude-bridge/go
git status --short
git rev-parse --short HEAD
```

- Go test output:

```bash
go test ./...
```

- Bridge run command.
- App APK build command and install result.
- ADB logcat excerpt for failures.
- Go bridge log excerpt for failures.
- Screenshots or screen recording for UI problems.

Optional screen recording:

```bash
adb shell screenrecord /sdcard/go_bridge_e2e.mp4
adb pull /sdcard/go_bridge_e2e.mp4 /tmp/go_bridge_e2e.mp4
```

Stop recording with Ctrl-C.

## Pass / Fail Gate

Production candidate requires all of these to pass:

- `go test ./...`
- Fixed endpoint app connection to Go bridge
- Claude basic turn
- Codex basic turn
- History snapshot/delta
- Reconnect after Go bridge restart
- App background/resume
- Compact lifecycle events
- No stale permanent streaming state
- No unrecoverable missing message after compact/history reload
- No app crash in logcat
- No Go panic
- No listener left on test ports after shutdown
- Every row in "App Feature Coverage Requirement" is marked `PASS`, `N/A`, or explicitly accepted as `NOT IMPLEMENTED IN GO`

Production blocker examples:

- Fresh install silently reconnects to Python when Go is selected.
- Any normal turn finishes in JSONL but cannot be recovered by history paging.
- Compact leaves session permanently streaming.
- Reconnect creates multiple live sockets for the same device and causes event duplication.
- Large tool output causes bridge-wide stall or lost done event.
- Codex `thread not found` is surfaced to user instead of recovered.
- AskUserQuestion prompt cannot be answered or leaves stale pending prompts.

## Shutdown

Stop Go bridge with Ctrl-C.

Confirm ports are free:

```bash
lsof -nP -iTCP:8767 -sTCP:LISTEN || true
lsof -nP -iTCP:9001 -sTCP:LISTEN || true
```

Expected: no output.

## Final Report Template

Use this template after the ADB run:

```markdown
# Go Bridge ADB Production Readiness Report

Date:
Tester/model:
Device:
Android:
Go commit:
App commit:
APK build:
Bridge command:

## Summary

PASS / FAIL:

## Passed

- ...

## Failed / Blockers

- ...

## Warnings / Non-blocking

- ...

## Logs

Go bridge:

```text
...
```

ADB logcat:

```text
...
```

## Decision

Ready for production: yes/no
Required fixes before production:
```
```
