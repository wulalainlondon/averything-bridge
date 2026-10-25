# Continuation on existing parent threads

The installed Codex 0.160.0 protocol accepts `dynamicTools` on `thread/start`,
but not on `thread/resume` or `turn/start`. Passing it to resume does not prove
that a new tool is available in an existing thread. Never fork a parent merely
to obtain the new delegation tools or claim that doing so preserves context.

New parent threads can use `bridge_sessions.continue_delegation`,
`get_delegation_rounds`, `bind_delegation_qa` and `record_delegation_review`.
Existing parents can use the explicit `input.continuity` extension through their
already registered `bridge_tasks.task_get` and `bridge_tasks.task_append` tools.
This route is handled only by the authenticated native/MCP parent invocation.
It does not add a controller grant or make a child a manager. HTTP/paired-client
requests do not use this adapter.

Read example (standard Task API envelope omitted):

```json
{"continuity":{"action":"rounds","arguments":{"delegation_id":"<original dg receipt>"}}}
```

Append actions and `arguments` use the corresponding delegation v1 schemas:

- `continue`: `continue_delegation` arguments, including exact predecessor,
  authority, native thread and configuration revision; queue or explicit steer.
- `bind_qa`: owner_delegation_id and qa_delegation_id. The native threads must differ.
- `record_review`: owner_round_id and qa_round_id. Verdict is extracted from the
  sealed QA final; callers cannot supply a replacement verdict.

Append always requires the existing envelope's idempotency_key. Do not reuse a
key for different input. Successful results are tagged under result.continuity;
the original formal Task API receipt path remains unchanged for ordinary inputs.
The compatibility journal fixes each parent/key to one intent across parent
turns. A command reserved before a crash is never blindly repeated; a recovered
continue can return the original core receipt. Otherwise it remains uncertain.

A prior command can be inspected without another effect using task_get:

```json
{"continuity":{"action":"command","arguments":{"idempotency_key":"<original key>"}}}
```

All core profile, source-binding, scope, CAS, native-thread, queue and frozen
result protections remain authoritative. This extension is not a conversion of
a legacy dg identifier into an invented canonical ta1 receipt. It does not
modify past results, reset context, silently replace a session or publish QA on
behalf of its reviewer. Handoff snapshots remain fixed per return preparation.

## Explicit source revision reauthorization

A changed parent configuration remains denied by default. After explicit human
approval, the unchanged original native parent may append `reauthorize_pair`.
Its closed arguments pin `owner_delegation_id`, `qa_delegation_id`, `instance_id`,
`expected_source_thread_id`, `expected_source_revision`, `new_source_revision`,
`expected_owner_thread_id`, `expected_qa_thread_id`, and both
`expected_owner_config_revision` / `expected_qa_config_revision`.

The new revision must match current authenticated parent settings and be greater
than the exact prior effective revision. Both logical roots must belong to this
parent, have independent original native threads, preserve their accepted child
profiles/workspace/sandbox, and have no unfinished execution or delivery. Existing
owner/QA responsibility cannot change. Admission appends both source authority
epochs atomically under predecessor CAS; it never edits original source bindings,
old results or frozen return snapshots. A subsequent drift still fails closed.
New rounds bind to the newly approved revision. The usual idempotency journal
applies; an uncertain reserved command is not replayed under another key.

This is an explicit permission-changing operation: caller authentication alone
is not human approval. Agents must obtain human authorization for the concrete
pair and old/new source revisions before invoking it. It remains unavailable to
workers, global controllers, foreign parents and paired-client transport.

Terminal return uses source/epoch-only validation described in
`DELEGATION_SESSION_CONTINUITY.md`; a stored failure/result can be reported even
when the original worker cannot start another turn. This does not change the
compatibility command schema, continuation authorization or any prior receipt.
