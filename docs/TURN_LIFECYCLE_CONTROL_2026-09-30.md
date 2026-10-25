# Long-running turns and confirmed stop control

Date: 2026-09-30. Source changes and local verification only. The running 0.2.67 Bridge was not restarted, deployed, or used as a test executor; existing AI tasks were left running.

## Incident

The `修仙` session started a Codex turn at 18:03:02. At 20:03:02 the generic session watchdog released its queue and the generic terminal sink emitted `executor turn timed out without a terminal event`. The native Codex turn was still producing file changes and command events afterwards. A later request then collided with the original native turn identity.

Two independent fixed two-hour timers treated elapsed duration as a terminal even when the backend remained active. The Codex adapter's progress-aware liveness logic did not prevent those upper-layer timers from firing.

## Changes

- Remove the session worker's total-turn watchdog. It waits for a real terminal, a confirmed cancellation path, or explicit session closure.
- Remove terminal-sink deadline failure generation. The legacy `NewTerminalSinkWithTimeout` constructor remains source-compatible but ignores its duration; it cannot emit an unconfirmed timeout or free native ownership.
- Default Codex inactivity handling to warning-only. Continuous progress and waiting for human input remain valid long-running states. An explicitly configured internal inactivity-abort policy is still supported for existing adapter tests; no production option enables it by default.
- Preserve the current request identity while requesting stop. A generic stop ACK is not `stopped`, and the router no longer calls `EndTurn` just because `Stop` returned.
- Codex manual stop propagates interrupt RPC failure, retains ownership on ACK, and waits for the exact native terminal event. If a turn identity is not yet available during preparation/submission, stop is reported as unconfirmed and can be retried; work is not falsely marked stopped.
- A failed stop restores only the same live request's runtime presentation through a compare-and-swap. It cannot resurrect a completed request or overwrite a newer run. The original queue lock remains held; frontend retry remains possible.
- A naturally completed Codex turn wins a race with stop intent. Optional auto-compaction is skipped when stop was requested rather than rewriting successful completion as interruption.
- Claude manual stop requests process termination, keeps its process binding while exit remains unconfirmed, and emits a correlated stopped event only after actual child-process exit. Manual stop does not auto-restart that process.

## Verification

Targeted tests and race checks cover session actor ownership, generic terminal tracking, Codex/Claude adapters, core stop routing and runtime-journal restoration. Added regression scenarios include legacy deadline constructor crossing, stale unrelated terminal rejection, warning-only inactivity through 24 hours of logical time, interrupt failure, ACK without terminal, next queued turn waiting for confirmation, retry after failed stop, stop/completion races, confirmed Claude child exit, and compare-and-swap protection for completed/newer requests.

Full `go test ./...`, targeted `go test -race`, `go vet ./...`, and a non-running compile are required before a release. Logs are kept in the private local QA directory. A real long-running customer task is not used to trigger watchdog or stop tests.

## Release boundary

These edits do not hot-patch the already running Bridge. Its pre-existing timers remain until a separately authorized safe update. Do not claim the live session is protected by the new policy before that update.

No customer Google credential, managed push service, iOS/Android binary, or unrelated dirty-worktree change is included in this lifecycle change. Existing explicit clear/close operations and backend-specific process-exit recovery are outside this adjustment; clearing a conversation is not equivalent to a non-destructive stop request.
