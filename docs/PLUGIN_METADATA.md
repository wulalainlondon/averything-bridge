# Bounded provider metadata candidate

This source subset extends the deployed 0.2.94 source. It has no deployment,
host installation or App/Swift adoption receipt. RELEASE_VERSION and TaskAPI
rc.1 are unchanged. Observation and metadata status are independent wire
families; they do not negotiate rc.2, Continuation candidate.2 or focus v2.

The original authenticated Hub exposes GET `/api/plugin-metadata/v1/status`
with no query arguments, and GET `/api/internal-agents/v1/observation` with
exactly one `session_id` query argument. The HTTP authorizer is unchanged;
observation rechecks it after provider I/O. The original paired socket accepts
`request_plugin_metadata_status` and `request_internal_agent_observation`.
Observation requires an existing selected session in the Hub root scope.
Hidden sessions are refused; client JSON cannot supply authority, provider
paths, root, epoch, task binding or QA. An optional `obs_` read correlation
produces `internal_agent_owner_binding`; this is a metadata read correlation,
never a dispatch request or canonical acceptance receipt.

Status separates host installed, enabled, loaded and discovery (all unknown)
from this process's loaded status and these Hub metadata routes. It does not
inspect a host registry. The existing status-only external Plugin package is
not replaced, installed or granted a business caller. Native tool registration
cannot prove that package's installation/load/discovery.

The Codex daemon adapter uses an already-connected writer and original known
child-ID cache, or attaches to the existing configured local daemon socket for
one original selected root. It never launches, resumes, reconnects, discovers
threads or reads turns/history. Scope includes a boot nonce, transport/socket
epoch and original configuration revision. Stdio and other providers remain
unsupported in this mode. Socket attachment requires an existing absolute
same-UID Unix socket; absent or retired transport refuses. Every metadata read
uses `thread/read(includeTurns:false)` and the whitelist retains identity,
parent, session tree and lifecycle only. The provider response may carry
private summary fields; they are discarded, not retained in the projection.

Coverage is only `partial_provider_child_cache` or
`partial_selected_native_root`; an empty child list is not full Root inventory.
The Root collaboration callback is unsupported. No installation/ingest route,
TeamReader, native runtime callback, canonical resolver, publisher or second
task store is included. Children stay unmatched; native final, QA and delivery
are unverified. Idle/completed text and names cannot become sealed final or QA.
The Hub refuses accidental canonical relation/QA widening by a provider.

Bounds: 16 children, 64 KiB decoded metadata/frame on the separate attachment,
three-second observation deadline and fixed attachment frame budget. The shared
existing RPC transport keeps its original wire allocation/cancellation limits;
the whitelist/deadline do not promise a hard global memory or cancellation bound.
On the shared writer, metadata uses its own context-aware response wait and
epoch-checked correlation, bypassing thread/turn health admission. Metadata
timeout/cancel retires only that pending ID; late replies are consumed and
discarded. It does not send a native turn interrupt, retry, discover threads,
schedule health checks, change the health circuit or replace/restart transport.
Malformed/rejected replies remain unverified metadata. A cancelled observation
cannot establish fresh registration, host loading or complete Root coverage.

A physical shared connection failure is different: the original transport
reader still fails all pending RPCs, advances its epoch, invalidates live routing
and applies its existing reconnect policy. Metadata does not initiate or suppress
that owner policy. Ordinary thread/turn RPC timeout still uses the existing
health/recovery gates. Context cancellation does not preempt a shared io.Writer
write or lock acquisition; those retain the original transport bounds.
Offline cache retains the actual observation timestamp, changes attempted_at
and removes binding/verification. Scope/config/backend/registry/pairing changes
invalidate the read. These are bounded read checks, not a complete future Root
callback or cross-process authorization architecture.

Wire schemas live in `contracts/internal-agent-observation/v1` and
`contracts/plugin-metadata/v1`. The status advertises the embedded observation
schema hash and the original embedded TaskAPI rc.1 version/hash. Schema validation
does not authenticate a caller or attest QA. Fixtures test the real provider and
Hub code but do not prove host/native/UI runtime adoption.

External QA must review this exact delta before App/Swift owner integration.
For trusted same-task QA, the original canonical owner must supply an existing
exact receipt and relation, role, review scope, complete resultVersion,
command/definition identity, native seal and independent publisher. Full Root
inventory separately needs the original native callback. Report navigation and
return anchors separately require original owner closures; these do not gate
ordinary file delivery or unrelated App/Swift work.
