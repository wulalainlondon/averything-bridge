# LINE account service

This implementation adds a Go-owned LINE control plane and one isolated Node
account process per LINE identity. The initial host is macOS. It uses the pinned
unofficial LINEJS adapter; capabilities that still need a phone test are reported
separately from implemented code and live read-only checks.

## Source and ownership

* `internal/linebridge`: account grants, encrypted operation/resource storage,
  fixed sender bindings, schedules, receipts, call reservations and supervision.
* `services/line-account`: typed, private account RPC; login/refresh/E2EE state,
  bounded directory/history, Talk/OpenChat polling and encrypted media transfer.
* `services/line-voice`: one-call media worker, Opus normalization and the pinned
  PLANET transaction-correlation patch. Permanent login material stays with the
  account process. A call worker receives only its call route through stdin.
* `internal/linevoice`: phone WebRTC leg and bounded Opus queues.
* `internal/core/line_*`: authenticated API, native caller binding, account-scoped
  Event Inbox projection and voice handoff.
* `internal/eventinbox/batch.go`: normalized events and encrypted source cursor
  commit in the same transaction. Each source has its own retention quota.

Account metadata has one editable owner in the LINE store. Node does not open
the Go database. The existing Event Inbox owns event retention and checkpoints.
The existing attachment registry/Bridge Drop supplies files; model tools accept
opaque references rather than filesystem paths. Local process isolation is a
fault boundary; the host OS and the approved Keychain helper remain trusted.

## Runtime contract

Account RPC is NDJSON on private stdin/stdout, with protocol version `1`, an
exact account reference, request ID and runtime epoch. Requests are multiplexed
with a bounded pending set. Stale/mismatched replies are rejected. Auth material
is absent from argv, inherited unrelated environment variables, public account
objects and diagnostic logs. Ready/alive are process observations; `health`
performs a live profile/principal check.

The call worker uses protocol `2` and must announce ready before receiving its
single start command. The Go broker consumes a durable intent before obtaining
the route. Native handoffs bind the original conversation, phone, LINE account,
target and a stable call ID; pending reservations expire. A restart or uncertain
invite never causes automatic redial. The initial broker admits one LINE call
at a time on an authority, while account message/event processes remain separate.

The tested runtime is Node 26.7.0; startup currently requires Node major 26.
Dependencies are pinned to LINEJS 3.4.2, types 3.5.0, undici 7.30.0 and thrift
0.23.0. The transport patch checks the exact upstream source SHA before loading.
`scripts/stage_line_runtime.mjs` builds a new artifact, installs production
dependencies from lockfiles and hashes all regular runtime files plus the Node
binary. `LINE_SERVICE_MANIFEST` enables Go verification before a child starts.

## Durable operator configuration and fixed entry

The primary local configuration is DATA_DIR/line-service.json, owned by the
operator (0600, no symlink), outside every versioned release. It contains no
credentials. The actual executable resolves its own signed Resources/line bundle
and verifies its manifest and pinned Node before accessing the existing store.

Example (operator paths must be the original verified paths):

    {"version":1,"enabled":true,"node":"/pinned/node",
     "keychain_helper":"/existing/approved/helper","legacy_dir":"/original/profile",
     "allow_write":true,"allow_events":true,"allow_schedules":true,
     "allow_resources":true,"allow_calls":false}

Existing account grants, encrypted store and profile ownership remain separate.
No profile/key/login or grant is recreated. If an existing store loses its
configuration, startup fails visibly; explicit enabled:false is the supported
disable operation. Explicit older environment deployments remain supported when
no stable config exists. A retired release's line.env.sh is not authoritative.

Before promoting any local Go candidate, run scripts/check_release_services.py
with --candidate, --expected-binary-sha256 and the original --data-dir. It invokes
--check-line-config, not an unknown positional command that could start an old
server. This preflight checks resources and config only, with no account network
operation or Keychain access. An older binary or a candidate ignoring enabled
LINE fails the gate. Recheck on the exact signed staged executable.

When the service is enabled, ~/.local/bin/bridge-line is atomically installed.
Use through exec_command as one literal command, including older native threads:

    /Users/USER/.local/bin/bridge-line --id lop_UNIQUE_QUERY --method accounts --params '{}'

Use actual returned account_ref/target_ref only. Supported account/message,
receipt, attachment, subscription and schedule operations use the existing
conversation grants. No grant/pair/management, shell transport overrides or
phone-call handoff is exposed by the fixed entry. Sends require explicit user
authorization. Only a real provider-observed exact command authorizes the same
thread/active request/payload. Environment IDs alone, shell chains, copied
commands, another conversation and retired voice owners cannot authorize it.
CLI write intents remain bound to the original conversation plus --id across
native-command changes/restarts; changed payload cannot reuse an existing intent.
After uncertainty, query intent_status with original_intent and account_ref under a new query ID to locate the exact receipt; never guess the latest operation. A missing row is not definitive pre-execution refusal. Do not blindly replay.
Read-only health may perform ordinary SDK refresh; accounts/status alone does not
prove live connectivity. No messages/calls are sent to test access.

LINE remains separate from bridge-media and its playback ledger. Phone LINE
retirement and the call feature flag remain unchanged. Voice startup instructions
provide the fixed entry without requiring old native dynamic-tool hot reload.

## User and tool surfaces

The primary product is a voice-first AI assistant. App settings expose the
existing account/access/health and owner-sharing controls; a second LINE inbox,
chat browser or schedule dashboard is not part of the current user requirement.
The AI reads selected messages, sends explicitly requested content, manages
schedules and requests source-bound phone handoff. See LINE_VOICE_ASSISTANT.txt
for exact methods, current limits and the distinction between code and live QA.

`POST /api/line/v1/request` requires an exact paired-device bearer credential and
`X-Bridge-Device-ID`; it has no loopback authentication bypass. Pairing and grant
management require the account's original owner. A granted conversation uses
the `bridge_line` native namespace. Caller/session/thread/turn ownership comes
from the actual app-server invocation. Delegated children are excluded. Model
tools cannot pair accounts, grant themselves access, select arbitrary SDK RPCs
or read arbitrary local files.

An account owner can use the paired-device management API `transfer_owner`
after disabling the account, specifying the exact current binding revision and
another currently paired device. The transaction retains the account/profile and
all receipts, revokes previous sharing, stops subscriptions and advances the
binding revision. The new owner must explicitly enable it. This API is excluded
from native model tools. Device identity repair in the Android management page
uses an explicit existing-credential claim against the known original Bridge
authority; ordinary probes and startup do not change the pairing.

Fresh QR login stays encrypted in staging. Go checks principal identity and
duplicate binding before committing the login. Same-account re-login preserves
old E2EE key entries and an encrypted previous-login file. A wrong-account scan
discards staging while retaining the original encrypted login.

Native account tools include `accounts`, `status`, `health`, `find_targets`,
`find_openchats`, `get_messages`, `subscribe`, `list_attachments`,
`import_resource`, `download_resource`, `send_message`, `schedule_message`,
`get_operation`, `list_operations`, `cancel_operation`, `reschedule_operation`,
`handoff_call`, `get_call` and `cancel_call`.

## Effect and recovery rules

* `queued`/`scheduled`: persisted intent, no dispatch yet.
* `dispatching`: effect may be in progress.
* `accepted_by_line`: LINE returned an acknowledgement; this is not read/delivery
  confirmation from the recipient.
* `unknown`: retain the original operation and investigate; never retry under a
  new intent automatically.
* `failed_before_effect`: the adapter identified failure before chat dispatch.
* `held_late`/`held_binding`/`held_unavailable`: human review/rescheduling required.

Schedules store UTC milliseconds, IANA timezone, fixed account/principal binding
and revision. The ordinary scheduler tolerance is five seconds; an explicitly
granted late window can extend it. Sleeping/offline hosts do not silently send
late. Cancellation/rescheduling use a receipt revision and refuse ambiguous or
already-dispatched effects. Grant/control/binding changes are checked again
before execution.

History reads do not mark LINE messages read. Event Inbox read/dismiss are local
UI state. Talk and each OpenChat room have separate encrypted checkpoints.
Full-sync responses create an explicit gap record; polling success does not
prove complete historical recovery. Incoming content remains inert and does
not authorize a reply or start an AI turn.

Attachments are capped at 8 MiB, frozen by hash, encrypted in the cache, scoped
to account permissions and retained up to 30 days with an account quota. Images
use the E2EE media path; other types currently send as files. Captions/replies
combined with an attachment are rejected. Upload failure is distinguished from
an unknown chat-send result after upload. Downloads currently resolve a known
message within the bounded recent-message window. Binary content stays out of
model tool results. App clients verify size/hash before opening a download.

Authorized bounded tool results may naturally appear in native runtime history.
The integration does not directly rewrite native JSONL or bulk-inject chat logs.

## Voice acceptance and rollout

The implemented first path retires AI audio, waits for its terminal/cleanup ACK,
then starts the LINE leg on the same phone and conversation. Android preserves
the accepted audio-focus, local-release and foreground-service guards. A failed
release, stale socket, wrong phone, expired reservation or cancellation prevents
handoff. Android and iOS now implement exact-reference source-bound handoff; their
real-device qualification is tracked separately. iOS waits for CallKit audio
deactivation. Web-only clients still advertise no handoff support. Answer-first
handoff is not included. Operator enablement and original account grants remain
required; compiled code alone is not permission or a successful phone call.

The call worker starts a monotonic 20-minute timer after remote answer/media
readiness. It maintains silent media when needed and emits bounded structural
diagnostics/frame counts. No audio recording or speech recognition is performed.
The original T103/511 failures remain in evidence; repaired real 60-second and
20-minute calls still require current explicit test authorization and a person
to answer/verify both directions. Unknown media wrappers are counted, not
declared supported.

Before replacing a managed host: record the current signed bundle, launcher,
authority/data/session-store paths and pairing state; preserve the shared native
app-server owner. Package from a clean source snapshot and verify signature,
source hash and runtime manifest. Use the same managed service and authority.
Do not modify other Go instances. Preserve the old bundle and launcher for
rollback. Turning the feature off retains account metadata, encrypted logins,
receipts and unknown effects. It must not replay old calls or messages.

Second-account human pairing is deferred at the user's request in the current
rollout. Two isolated real Node processes and mock two-principal workflows have
passed; this does not replace a real two-account acceptance test.
