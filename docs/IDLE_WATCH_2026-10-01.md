# One-shot idle notification — 2026-10-01

User authorization: wait for currently running sessions to finish, notify the
existing Bridge conversation, then continue Bridge deployment and frontend
testing. Monitoring must not interrupt running work or require a Bridge restart.

The companion `cmd/bridge-idle-watch` runs beside the existing Bridge under the
task-owned LaunchAgent `com.everything-go.idle-watch`. It checks every 30 seconds
and requires a 60-second observation window plus a final recheck before issuing
one durable `message` to the explicitly verified existing session.

## Safety boundaries

- Uses authenticated loopback Bridge status/session/runtime snapshots with a
  distinct ephemeral client identity. It does not claim/rebind credentials,
  evict a phone, change pairing, or abuse the connection-probe write restriction.
- Credentials are read into memory from the existing pairing file, not placed
  in argv, URLs, checkpoints, prompts, or logs.
- Reads durable queue metadata using SQLite `mode=ro`. Pending/running/steering
  entries prevent readiness; uncertain entries require attention, not deployment.
- Native RPC has an explicit read-only allowlist: initialize, loaded-thread
  listing, thread summaries with `includeTurns:false`, and goal reads. It never
  resumes/forks/subscribes to a thread, starts a model turn, changes a goal,
  interrupts a task, starts another daemon, or copies private transcript content.
- Native activity corroborates Bridge state, so a watchdog-induced false
  terminal cannot be mistaken for completed work.
- Active goals block readiness even between apparently completed native turns.
  Failed/interrupted work, paused/limited goals and system errors are not success.
- Unknown state, incomplete coverage, changed recipient thread, wrong Bridge
  authority, unavailable queue/database/daemon or invalid native schema fail closed.
- The notification target is excluded from the watched-success requirements,
  but must itself be idle before the notification is sent. All newly observed
  work also blocks readiness, avoiding restart while a new task has begun.
- A fixed request ID is retried only for an uncertain notification ACK. Bridge
  durable message deduplication prevents duplicate model turns. Completed job
  checkpoints prevent resending after process/login restart.
- Notification is not a deployment lease. The recipient must recheck current
  activity immediately before actual deployment, back up, use release procedures,
  preserve unrelated edits/conversations, and must not install/reset phone apps.
- The companion itself contains no deployment or service-restart action.

## Installed watch

- Job: `20261001-after-current-sessions`
- Target: `jl_x_01a028c5-67b` / native
  `01a028c5-67ba-71d2-bd9c-093bfd3c4fa3` (existing “bridge” conversation).
- Initial watched sessions: `s_ZpbNZLlX` (修仙), `s_slNsupn6` (數獨).
- State: `~/.everything-go-runtime/maintenance-watches/20261001-after-current-sessions.json`
- Agent: `~/Library/LaunchAgents/com.everything-go.idle-watch.plist`
- Binary: `~/.everything-go-runtime/maintenance-watches/bin/bridge-idle-watch-20261001`

The computer, Bridge and native daemon must remain available. If asleep/offline,
no completion is inferred and no deployment occurs. If a watched task fails,
the eventual idle notification requests investigation rather than deployment.

The LaunchAgent working directory is its owned runtime folder, not the project
under Downloads. A protected Downloads working directory can block macOS's
`getcwd` during standalone process initialization. The monitor does not require
project-file access; no privacy grants or TCC database changes are needed.

Cancellation: create the exact state's `.cancel` marker; the process records
`cancelled` and exits. Then boot out only this watch's LaunchAgent if desired.
Do not stop `com.everything-go.app` or Codex to cancel a watch.

## Verification

- Isolated real-WebSocket/Unix-socket tests cover successful terminal state,
  busy recipient, false Bridge completion versus native activity, active goal
  between turns, queues, failures, paused goals, system errors, unknown/incomplete
  state, changed identity, correct ACK/recipient, explicit authorization,
  forbidden native writes, private one-shot checkpoints and cancellation.
- Race tests and `go vet` pass for this command.
- Real read-only preflight identifies 修仙/its active goal and the target's live
  turn, and confirms 數獨's completed request. No production test message or model
  request is sent to validate the monitor.

No Bridge/frontend deployment is performed merely by enabling the watch. The
registered monitor and its eventual delivery receipt must be checked separately.
