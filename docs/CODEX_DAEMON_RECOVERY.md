# Codex daemon health and recovery

Implemented 2026-09-05. Applies to the Go executor in `go/internal/executor/goexec/` and daemon mode. Private stdio mode is unchanged.

## Behavior

A typed thread/turn RPC response timeout or transport write deadline schedules one asynchronous health check. The original operation is never replayed. The error explicitly reports that its server-side outcome is unknown. New thread/turn RPCs are temporarily rejected as **not submitted**; interrupt and unsubscribe remain available. Already submitted operations are not resent.

The probe uses a separate Unix WebSocket connection and the initialize/initialized handshake. It selects up to two distinct existing thread IDs (preferring the failed thread and known Bridge threads, otherwise a bounded thread/list). Each `thread/read` uses `includeTurns:false` and its own fresh connection. No probe starts, resumes, edits or subscribes to a thread. A timed-out WebSocket is never reused as evidence of a second failed thread.

- Any successful thread read rules out daemon-wide failure. Partial failure reports `thread_degraded` and does not restart the daemon.
- When reads succeed and this Bridge has no active work or pending RPCs, only its local connection is replaced. Other daemon clients are unaffected.
- Failed probes keep the circuit open and are repeated after 30 seconds, even without another user request. Connect/initialize errors and insufficient known threads do not justify daemon restart.
- Two successive rounds must each initialize successfully and time out reading two distinct threads before reporting `restart_required`.
- Version mismatch remains a separate diagnostic. Installing a newer binary does not automatically restart a healthy shared daemon.

## Restart ownership

Health checks and failure isolation are enabled by default. Shared clients do **not** automatically restart the daemon.

For a deployment whose operator designates this Bridge as the daemon recovery supervisor, set this environment variable in that Bridge's service configuration:

```text
EVERYTHING_GO_CODEX_DAEMON_RECOVERY_OWNER=true
```

This explicitly permits that supervisor to restart the shared daemon after corroborated failure. It must be an operational ownership decision: one Bridge cannot infer whether unrelated desktop clients have active work when the daemon itself is unresponsive. Leave it unset for clients that must not interrupt other daemon users. Custom socket overrides cannot acquire restart ownership. Automatic restart is disabled on Windows.

The recovery supervisor requires no locally active work, takes a non-blocking OS file lock under the same CODEX_HOME, and re-probes under the lock. A shared timestamp enforces at most one restart **attempt** every five minutes, including failed attempts. It captures diagnostics before invoking the installed CLI's `app-server daemon restart`, bounded to 30 seconds. Another read probe must succeed before readiness is restored. Existing disconnect handling invalidates stale thread bindings and reconnects the Bridge; no turn is replayed.

For a shared deployment without an owner, stop submitting new work, inspect the diagnostics, and coordinate a managed restart:

```sh
codex app-server daemon version
codex app-server daemon restart
codex app-server daemon version
```

Do not kill arbitrary child processes or delete writer-lock files. After recovery, reconcile the thread/turn history before deciding whether to submit a task again. Timeout does not prove that thread/start or turn/start failed to execute.

## Diagnostics

`status_result.status.backend_runtimes.codex` includes:

- `health_status`, `health_checked_at_ms`, `health_detail`
- `health_reads`, `health_read_timeouts`, `health_restart_owner`
- Existing CLI, managed binary and running daemon versions

The app's Bridge status output displays health alongside versions. New-request errors distinguish a request that was never submitted from an earlier request whose server outcome is unknown.

Private, bounded artifacts in `<data-dir>/diagnostics/`:

- `codex-health.json`: latest failed probe, latency/outcome samples and runtime versions; atomically replaced, mode 0600.
- `codex-health-before-restart.json`: snapshot retained before the latest restart attempt.
- `codex-daemon.stderr-tail.log`: last 64 KiB of daemon stderr before restart.
- `codex-daemon-processes.txt`: daemon and direct child process metadata, excluding command arguments.
- `codex-daemon.sample.txt`: best-effort macOS process sample, bounded to five seconds. This is not a Rust async-task dump and does not prove a particular lock leak.

The artifacts contain no probe response bodies or conversation history. Existing daemon stderr can contain task information; keep the diagnostic directory private. Evidence capture failures do not prevent the bounded recovery attempt.

## Verification

Unit tests use a fake Unix WebSocket daemon and fake restart executable. They cover independent read timeouts, healthy and partially healthy threads, insufficient evidence, active-work guards, explicit ownership, cross-process locking, persisted cooldown, readiness verification and no automatic replay. Executor/core regression tests should run with `-race`.

Optional production **read-only** smoke check (does not restart or start a model turn):

```sh
cd go
EVERYTHING_GO_RUN_CODEX_HEALTH_PROBE=1 go test ./internal/executor/goexec -run '^TestHealthLiveReadOnly$' -v -count=1
```

This patch provides detection and controlled recovery. It does not claim to fix an unconfirmed internal Codex Rust lock or task leak.
