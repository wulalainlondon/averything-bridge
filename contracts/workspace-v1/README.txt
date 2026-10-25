Bridge collaboration workspace v1 — additive opt-in contract

Activation
  --collaboration-workspace
  --workspace-tools-config /absolute/private/operator-reviewed.json (optional)
  --workspace-push-config /absolute/private/operator-reviewed-push.json (optional)
Missing/invalid explicitly enabled workspace storage is a startup error. Existing
LINE configuration, store, pairing and guard are not migrated or re-created.
This feature is not enabled by compiling a candidate. Releases must retain the
stable service guard and verify required existing capabilities before promotion.

Transport and identity
Paired-owner commands use the original authenticated WS as workspace_request with
request_id and workspace:{action,authority,...}. Owner identity comes from the
actual paired connection/current credential. Actor, grant, service account,
caller, device, native thread and tool adapter authority are never client grants.
Guests use only /api/workspace/v1/guest: the first frame is workspace_guest_hello
with a pre-generated retained credential, device_id, name and optional invite.
The guest endpoint never registers a legacy Hub client or publishes legacy data.
TLS is required by remote guest clients. Credentials and invite codes are not URL
parameters. Membership, original credential and issuer member revision are
rechecked at acceptance; a used invite may only recover its original credential.

Mutation/recovery
Persist mutation_id before sending; do not generate another ID after timeout.
workspace_result echoes request_id, authority, ok and data.mutation_id. Its
mutation_state is refused (no commit), committed, or unknown. A committed native
setup error remains pending. status.original_mutation retrieves the original
receipt under current permission. Only explicit refusal or the matching committed
receipt clears a client's durable pending operation. Reads never require a new
mutation. Byte limits/page limits do not mean the complete data set was loaded.
The strict shared App schema is src/collaborationWorkspace/model.ts; native DTOs
are NativeCollaborationRooms.swift. Inbound core whitelists are in workspace.go,
Store.ReadAction and Store.mutate. Unknown fields/actions fail closed.

Execution
create_agent creates a fresh room-exclusive original registry/native binding.
bind_agent can use an existing private partner only in a single-member room.
A guest-created task awaits a paired owner before native admission. Room task
records pin immutable context, role, ACL, original session/config/request and
existing Task API seal; the original durable queue remains the execution engine.
Workspace workers have read-only web research and the two pinned-source read
tools. They do not inherit host filesystem, shell, plugins, account writes, LINE,
controller, delegation or approvals. Member changes require new clean agent
contexts for new work. Ordinary native conversations retain their own policy.
Synthetic provider tests verify mechanics; only actual provider evidence can
qualify native consumption/tools. QA needs a distinct native session and sealed
JSON owner_round_id/verdict. Prior FAIL and UNKNOWN are immutable history.

Storage and sync
DataDir/collaboration-workspace-v1 is private. Its exact cursor.key is retained;
missing keys are errors. Authority is pinned. Future schema versions are rejected
before DDL. Scope-bound opaque cursors, room sequences and current membership
protect pages, search, events, immutable versions and attachments. Events repair
loaded older records; membership generation changes discard cached projections.
Clients keep private device/authority/room/thread drafts, never automatic sends.
Document export is explicit, rechecks pinned references and includes document
text/version metadata only; attached binaries are not silently included.

Push and external effects
Workspace FCM is separate from the paired-owner device registry. The human opts
in per room and actual credential/device. Endpoint tokens are encrypted at rest.
Each delivery rechecks current credential/member revision; payloads contain only
a generic alert and room/notification routing IDs. Opening reauthorizes content.
FCM acceptance is not device delivery/read. Unknown sends remain unreplayed;
explicit retryable refusal is bounded. Sleep/offline delivery is not guaranteed.
Gmail/Calendar adapters use only the exact private operator profile and Keychain
entry; account listing is not authenticated health. Calendar conflicts, times,
timezone, attendees and email attachment hashes are reviewable before approval.
Approved content/account/destination/revision/expiry are pinned; unknown effects
use original provider lookup only. Schedules never grant external send authority.

Original project/work integration
A linked project must exist in the original WorkItem authority and remain active
at native admission. Owner catalog references contain only id/name/version, not
paths or private project context. The same room task r_ is journaled atomically
with original queue admission under its actual authenticated_workspace transport
and workspace_grant namespace; it is not a native/controller impersonation.
Linked tasks have real reserved WorkItem/Run rows, which are observation-only,
manual, excluded from the ordinary scheduler/recovery and legacy work views.
Current room membership is required by original TaskAPI reads as well as room
reads. Frozen pages bind a membership fingerprint. Queued native/history/replay
frames recheck current permission at socket write; disabled workspace support
still quarantines reserved sessions. The global search index excludes room native
transcripts; use room search. Native generated host-file events cannot publish
through the legacy global media registry. Room blobs use the scoped gateway.
