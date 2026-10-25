# Delegation execution capacity

Server flag: `--delegation-capacity-config /absolute/server-owned.json`.
Without it, each Bridge startup selects defaults of 6 active reservations,
64 waiting turns, and 128 outstanding result/delivery reservations per parent.
The configuration is persisted for atomic admission/recovery, not model-writable
policy. A configuration file supplies complete values:

```json
{"default":{"active":6,"queued":64,"delivery":128},"parents":{"parent-session-id":{"active":4,"queued":32,"delivery":64}}}
```

Ranges: active 1–256, queued 0–4096, delivery active–8192. Reject unknown fields,
invalid bounds and trailing JSON. Shrinking limits does not terminate accepted
work; admission/promotion waits for counts to drain. Changing limits does not
change delegation authorization or child scopes.

| State | Execution reservation | Outstanding result reservation |
|---|---:|---:|
| queued | 0 | 1 |
| provisioning / running | 1 | 1 |
| collecting (native terminal, awaiting final seal) | 0 | 1 |
| terminal + delivery pending / queued | 0 | 1 |
| terminal + delivered / failed / not_requested | 0 | 0 |

Admission reserves a future result even for a waiting task. This prevents a
batch of simultaneous finishes/cancellations from overflowing the delivery
outbox. Delivery backpressure may therefore stop admission while execution
capacity is free. A receipt promises durable acceptance, not native execution.
`provisioning` conservatively counts as an execution reservation before native
start; only exact native evidence establishes running/completion.

SQLite IMMEDIATE writer transactions (WAL, synchronous FULL) serialize native
and Task API admission, parent budget decisions, promotion, cancellation and
origin uniqueness. API intent, outbox, profile, binding and accepted commitment
commit together. FIFO uses commit-order rowid. A continuation of the same child
waits for its prior execution; an ineligible same-child head does not block
another eligible child. A fixed-high-water cyclic scan advances past 128 stuck
rows and remains fair under new arrivals.

Source thread/configuration and directory identity are persisted and checked
again before execution; append also pins the existing child's thread/config.
The candidate's accepted-commitment semantics remain: switching off NEW
admissions does not revoke previously accepted waiting work. Cancellation stays
available. Missing/changed source bindings fail before effect. Native uncertainty
retains a reservation without a timeout retry. A Bridge recovery marker never
proves native termination. Missing final remains collecting without fabricating
a successful empty answer.

Native `bridge_sessions.get_capacity` reports this parent's independent limits
and counts. `cancel_waiting_delegation` accepts only an owned delegation ID still
waiting for capacity; a repeat cancellation is idempotent. Stale native turn
bindings cannot invoke either tool. No model-supplied parent identity is used.

The closed Task API rc.1/rc.2 schema and hashes are unchanged.
`capabilities.limits.max_pending_children` is the compatibility name for the
configured execution limit, not the sum of execution, waiting and delivery.
An accepted waiting task projects admission=queued, execution=not_started.

For Task API cancellation before a child native thread exists, send the original
`command_receipt_id`, current task revision and mode=queued, with `target`
comparing the SOURCE parent's existing instance/session/thread/configuration
(from the task's source identity). The server binds this to the original parent
and command, rather than inventing a child's expected native thread. Once native
provisioning starts, existing child-target cancellation rules apply. Conflicting
keys, stale revisions and another parent's cancellation are refused.

Task API append to a delegated child uses another durable reservation under the
same parent and original child thread/scope. It retains the original Task API
identity, preserves command-specific results and does not request a duplicate
parent return. Bare child messages without a capacity reservation are rejected;
use authenticated append for continuation. Ordinary non-delegation sessions
are outside this per-parent delegation budget.

Exactly-once refers to canonical admission/cancellation/promotion and logical
queue identity. A native effect and the local database are not a distributed
transaction. Ambiguous native consumption is not replayed under a fresh ID.
Deployment therefore still requires original native final and delivery evidence,
independent QA, complete quiet and the original backend/S10 release gates.

Rollback to an older scheduler requires no queued/collecting/continuation work
or unacknowledged returns. Never restore a stale database snapshot after new
admissions, delete receipts or rewrite a dispatch ledger to force downgrade.
