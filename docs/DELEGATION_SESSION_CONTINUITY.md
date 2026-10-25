# Parent-owned delegation continuation

The logical task stays with its original owner session and native thread. An
independent QA session is bound once and reused for FAIL/correction/PASS rounds.
This release extends the exact live 0.2.102 handoff source; it does not replace the
controller guard or require a global grant. Children are workers: neither formal
nor legacy children receive management tools or management authorization.

## Supported routes

| Target | Legal continuation | Evidence/return |
|---|---|---|
| TaskAPI delegation queue round | Existing `bridge_tasks.task_append` (`append` envelope); optional `return_to_parent:true` with `expected_previous_round_id` | Existing scoped journal/idempotency receipt, capacity reservation, `expected_previous_round_id` CAS for a requested parent handoff |
| Legacy `bridge_sessions.delegate_session` child, idle or active with a queue supplement | `bridge_sessions.continue_delegation`, `mode:queue` | A new persisted ordinary `r_` request and delegation round; original session/thread/profile |
| Explicit active supplement to either kind of owned delegated child | `bridge_sessions.continue_delegation`, `mode:steer`, exact `active_request_id` | Existing native queue promotion/strict steer ACK; shares the exact original execution request/turn, reserves its own result/return |
| Managed target via controller | Still `controller_managed_target_forbidden` | Refusal retained; no ordinary-message fallback |
| API child via native queue continuation | `delegation_use_existing_task_append` | Use the original append implementation, not a second admission path |

The new native entry is a provider API and a registered dynamic tool, bound by
the app-server's actual active original-parent thread/turn/request/call. It is not
a new public unauthenticated HTTP endpoint and is not currently a callable tool
in an old gateway deployment. Voice/unowned tool turns do not gain this entry.
No wire field can select a caller, original parent, sandbox, model or replacement
thread. Native tool arguments use the published contract in
`contracts/delegation/v1/continuity.schema.json`; trailing JSON, duplicate keys,
extra permission fields and missing steer anchors are rejected.

## Identity, history and authority

Every bounded original record has an additive logical relation. Every accepted
round records an immutable predecessor and monotonic version, dispatcher,
request and separate parent-return request. Original results, statuses, sealed
returns, failed QA and acceptance receipts are never replaced. API task snapshots
include bounded `continuity` metadata alongside their original command axes.
Parent chat returns include the logical ID, round, predecessor and dispatcher;
chat-history metadata exposes the same relation. It creates no extra session.
`get_delegation_rounds` pages 128 rounds at a time using `after_version`; deleted
children do not erase readable original receipts. QA and handoff/delivery remain
separate from execution success.

The local delegation store is pinned to the configured authority at startup;
a later foreign instance cannot adopt it. On first upgrade, this additive pin
must be made under the unchanged original instance/data configuration (operator
cutover gate); old live102 rows never contained an explicit instance column.
Old rows are grouped from their actual same-parent/same-child persisted records.
Their native thread can only be pinned from the exact ORIGINAL native acceptance
receipt, with conflict checks and the accepted config/profile. A current registry
thread or pasted summary cannot substitute for that receipt. Missing or uncertain
initial native acceptance is a refusal, not successor authorization.

Revalidation covers registered original parent and target identity, authority,
original parent native thread/config revision, accepted child backend/model/effort/
sandbox/cwd/config, canonical workspace inode, archive/delete and current control.
The delegation grant is the original persisted child capability plus source
binding; it does **not** borrow the controller's unrelated global grant. Formal
workers keep their original immutable TaskAPI scope/profile and namespace; their
parent append still uses the original scoped gateway verifier. Scope drift,
foreign parents/generations and target drift cannot expand access.

Configured capacity remains active6 / queued64 / reserved-delivery128. Rounds,
not unique sessions, reserve future returns. Same-child queued rounds wait until
the preceding final is collected; late final sealing frees a global active slot
without allowing same-thread overlap. Steer supplements share one execution slot
and each reserve one result. Revocation/failed admission rolls back its receipt,
relation and capacity in the same store transaction. Execution guards revalidate
again immediately before Send/Steer.

## Retry and recovery

Keep the original native tool call/request identity or TaskAPI idempotency key.
Same intent returns the exact original receipt; changed intent conflicts. Native
same-call deduplication and predecessor CAS are serialized with bounded admission.
A queued ACK is acceptance only. Look up the original round/request before claiming
start; matching native terminal/final evidence is required for completion.

A durable steer claim and the existing `steering` receipt precede the RPC. A lost
ACK/restart remains uncertain and is never promoted/sent again. Definitive refusal
is a failed supplement; the normal actor Send is blocked for explicit-steer rows,
so it cannot silently become a later turn. A wrong execution/turn ACK never seals
a result. Frozen handoff snapshots and return requests survive restart unchanged;
existing queue receipts own their original payloads, so no new return is minted.

Delegated context cannot automatically cold-rollover or recover onto another
native thread. Runtime refusal reports `delegation_context_limit_successor_required`
or `delegation_original_thread_unavailable_successor_required`; closed/archived/
deleted workers have an explicit availability/reason in relation metadata.
There is no automatic successor creation in this entry. The original parent must
make an explicitly authorized successor decision with that concrete reason,
retain the predecessor and handoff version, and identify the new context honestly.
A healthy task uses its original session; controller or ordinary-message fallbacks
are never used to force continuation. Do not use absence/unknown acceptance as a
reason to create a replacement worker.

## Independent QA example

1. Owner round finishes with its exact final. QA finishes independently with a
   final JSON such as `{"owner_round_id":"dg_owner","verdict":"FAIL"}`.
2. Original parent calls `bind_delegation_qa` once with the two delegation IDs;
   distinct session IDs **and** native thread IDs are required. This binds sessions,
   not a claim about two different human people.
3. Parent calls `record_delegation_review` for the exact two completed rounds.
   The verdict is read from QA's sealed final; the owner cannot supply a verdict.
4. Queue correction to the original owner through the route above; then queue
   recheck to the original QA. Each has a fresh request and result/QA version.
5. QA final identifies the new owner round and `PASS`; record that version. The
   previous owner final and FAIL remain immutable. Inspect delivery separately.

JSON examples are in `docs/examples/delegation-continuation.json` and
`docs/examples/task-append-continuation.json`. Replace expectations with original
read receipts; these are examples, not authority or paid runtime probes.

Owner synthetic tests cover this chain with unchanged session/thread IDs and four
frozen handoff returns. They do not certify installed native tool loading, real
runtime execution, independent external QA or deployment. Those axes remain
pending until the coordinator validates the exact frozen candidate and performs
one necessary already-authorized original workflow round after external QA.

## Stored terminal return and execution admission

Formal terminal return resolves the original logical root SourceBinding, immutable
API caller/profile/workspace provenance, original grant digest and approved
source epoch. It still requires the original registered writable parent in scope,
its unchanged native thread and current approved revision, and this round's own
source binding and before-Send target expectation. Malformed metadata, foreign
provenance and unapproved epochs fail closed without creating a return receipt.

A verified pre-execution failure, queued cancellation or provision refusal has no
native acceptance. Its stored terminal notice can return without fabricating one.
A stored completed result can return after its child closes or changes future
settings. Neither case renews execution authority, rewrites accepted metadata or
pins a native thread. The accepted historical workspace identity remains stored;
returning old data does not require executing in that workspace again.

Append, continue, reauthorization and child Send still require the original exact
native receipt/thread, current executable child, accepted profile, workspace and
sandbox. Closed, deleted, archived or drifted children cannot resume through this
return path. Execution, delivery and handoff snapshot remain separate axes.

An existing original parent-return receipt owns its request ID and frozen payload
across store reopen. Reconciliation never rereads a later handoff to replace it.
Uncertain acceptance is retained under that original receipt; it is not resent or
steered, and no replacement request/session or ordinary-message fallback is used.
