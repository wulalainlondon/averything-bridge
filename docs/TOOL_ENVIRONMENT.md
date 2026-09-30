# Tool environment diagnostics and controlled maintenance

Implemented 2026-09-15. This is an additive `tool_environment_v1` capability,
not a change to chat completion or to browser permissions.

## Read-only behavior (enabled by default)

After a supported client receives a Codex `session_uuid`/`session_init_info`,
or opens **對話資訊 → 工具環境**, it requests a thread-specific inventory.
Diagnostics use `thread/read` and paginated `mcpServerStatus/list` through the
existing shared-daemon connection. They never start/restart the daemon, resume
a stored thread, edit config, read auth files or run a model turn.

The snapshot includes the last checked time, generation, running/disk version
when known, sanitized service/tool names, runtime connection state and evidence
level. Tool descriptions, input schemas, secrets, URLs and page content are not
returned. Concurrent reads are coalesced; successful results cache for 60 s.
Manual refresh bypasses the cache. Connection and MCP startup changes invalidate
prepared maintenance generations. Different authorities never share UI state.

Live P0 observations on this host: the old and new threads both returned the
configured-MCP inventory. `cua_repl` was unknown for the old thread and connected
for the newer one, but its tools map was empty even for the working conversation.
Therefore inventory does NOT enumerate every model-visible integrated tool.
Neither a successful inventory nor `connected` proves Brave control.

For operator read-only diagnostics (no production configuration writes):

```sh
go run ./cmd/codex-tool-probe -socket /absolute/path/to/existing/daemon.sock -thread THREAD_ID
```

## Controlled reload (disabled by default)

The host operator must arrange a maintenance window across ALL users of the
shared daemon before starting a Bridge with:

```sh
EVERYTHING_GO_CODEX_TOOL_MAINTENANCE=1
```

This flag is never changed from the mobile UI or by diagnostics. It is not a
replacement for coordination with other CLI/desktop users. Do not permanently
enable it on a busy shared host. Wulala deployment leaves this flag disabled.

The current tested schema allowlist is daemon 0.153.2 / 0.153.4. Other versions
retain read-only diagnosis and require schema verification before maintenance
is offered. Do not infer a running version from a thread's creation version.

1. `prepare_tool_environment_repair` returns a two-minute plan bound to the
   device, authority, session/thread, runtime generation, settings, control
   state and inventory fingerprint.
2. `apply_tool_environment_repair` requires that token, a stable operation ID,
   and explicit confirmation of the daemon-wide scope.
3. A durable journal is saved before mutation. Bridge blocks new local Codex
   submissions, waits for accepted local work to drain, and probes ALL loaded
   daemon threads. Pending work/unknown state prevents reload. Stop/read calls
   and other backends remain available. The wait expires after two minutes.
4. A non-blocking OS file lock coordinates Bridge processes on the same socket;
   an RPC submission gate prevents local submission races. Neither lock controls
   arbitrary external clients, hence the required operator maintenance window.
5. Exactly one reload request is sent per accepted operation. Its ACK is followed
   by another sanitized inventory read. Result: **completed_unverified**, not
   "browser repaired". A separate user-authorized Brave read is still required.

`cancel_tool_environment_repair` cancels only `waiting_idle`, never an RPC
already sent. A timeout after mutation is `indeterminate`; no automatic retry,
daemon restart, config rollback or original-message replay occurs. The journal
survives reconnect/restart. Repeated operation IDs return their previous result.
Indeterminate operations block another repair until an operator has established
the result; the UI does not pretend that reading a fresh inventory resolves an
uncorrelated mutation. Corrupt/full/unwritable journals fail closed for repair
without disabling read-only diagnostics or ordinary chat.

## Optional native continuation branch

The same prepare/apply workflow with `action: "fork"` uses official
`thread/fork` with `excludeTurns: true`. It never copies or edits the native
JSONL file, never changes the parent ResumeID and never migrates queued input.
The response's parent link and cwd are validated. The child is registered with
the parent's local model/permission preferences, while native history remains
owned by Codex. User navigation to the child is explicit.

A fork may inherit the old environment: branch creation is not a guarantee of
CUA upgrade. If validation fails after creation, the result is indeterminate;
do not create another child blindly. A known child ID is preserved when later
inventory verification fails, even if local registration has not completed.

## Wire contract and UI

Commands: `request_tool_environment`, `prepare_tool_environment_repair`,
`apply_tool_environment_repair`, `request_tool_environment_operation`,
`cancel_tool_environment_repair`. Payload lives in `tool_environment`, containing
only action, repair_token, operation_id, maintenance_confirmed and refresh.

Responses: `tool_environment_snapshot` and `tool_environment_operation`.
They are client-directed, are not in offline task replay, and cannot settle
chat turns, Goals, Widget completion or unread counters. The client polls only
operation reads after reconnect; it never automatically resends apply.

The local visual harness `/qa/tool-environment/index.html` uses the actual
component, command builder and reducer with a synthetic transport. It never
connects to a real Bridge. This harness verifies UI behavior, not live reload.

## Validation and rollout

- Go unit/integration fixtures: nonce/identity/settings/consent binding,
  duplicate applies, journal restart/corruption, cancellation, pagination,
  sanitization, other active threads, generation changes and native fork.
- Race tests cover the new store, core coordinator and Codex executor.
- App schema/reducer/component/boundary tests and actual browser fixture QA.
- Live daemon inventory probe is read-only. No live reload or fork was performed
  against the user's running conversations during implementation.
- Roll out the signed Wulala Go app only; retain the previous signed app for
  rollback. No automatic change to Morrie or the shared Codex daemon.
- Frontend changes require a separately built/distributed App to expose the new
  panel. Go deployment alone does not update already installed mobile clients.
