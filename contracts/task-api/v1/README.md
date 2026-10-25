# Task API release candidate 1.0.0-rc.1

This standalone contract is a new implementation artifact. The sealed plan's 1.0.0 draft/index remains unchanged; neither was deployed. Go embeds this exact schema and uses github.com/santhosh-tekuri/jsonschema/v6 **v6.0.2**, with the full Draft 2020-12 meta/vocabulary. SDK uses AJV **8.17.1** Ajv2020. Both consume identical golden.json. Remote schema loading, coercion and runtime defaults are disabled.

Transport rules also reject duplicate keys (escaped aliases included), trailing JSON, invalid Unicode, nesting >64 and frames >256KiB. All wire numeric fields use **integer JSON lexemes within ±9007199254740991**. Decimal/exponent spellings are rejected before JS rounding, including 1.0/1e3, underflow and near-safe fractional values. Generic provider JSON parsing is separate and retains json.Number so Claude result cost/usage decimals are not mistaken for wire identities.

D0 changes: receipt lookup names authority/path/op/key, plus task for append/cancel/internal delivery. Namespace is `(authority,stable_bound_scope,namespace_generation,path,operation,task_id_or_create,key)`. Same key across different tasks/stores is independent; same namespace/payload returns the same effect receipt, different payload conflicts. Reconnect/process generation is NOT the durable namespace generation. Get uses the policy-authorized namespace/locator, never reroutes raw lookup fields. Minimal intent/effect tombstones persist for namespace lifetime; body expiry never makes an old key executable again.

Error responses preserve bounded unknown operation and request_api_version, while the envelope api_version remains rc.1. Malformed non-string/oversized headers become null with invalid_argument. Snapshot cursors have a dedicated 16000-character/encoded-byte bound. Page membership, row revisions and data freeze at original per-store watermarks; keyset is task_id. All pages return that vector for event continuation. Changed scope/views, tampering, expired/lost freeze require cursor_expired; no silent capture of a newer vector.

Target.native_thread compare field means **existing Bridge ResumeID**: public Codex thread.id or Claude init/result session_id. It is not a Claude message UUID, tool_use id or process generation. Provider execution/anchor is discriminated separately; no synthetic Codex turns.

TaskService, Gateway/Authorizer, SnapshotSource and provider verifier ports are implemented, tested interfaces. There is NO production canonical-store implementation, HTTP/WS/MCP registration, provider load or live model proof in this slice. Original policies/guards remain untouched; unbound/unloaded ports return typed unsupported/caller_unbound. New workers receive explicit codex/gpt-6.1-sol/high defaults before the authorized owner gateway; legacy APIs retain their defaults.

Run: go test ./contracts/task-api/v1 ./internal/taskapi; provider fixture tests: go test ./internal/executor/goexec -run TestTaskAPI. B2/B3/B5/B7 need owner accepted base and hooks; fixture maps exist only in *_test.go. No new task DB/ledger.

Session-continuity candidate extension: delegation `append` retains its original
scoped journal and adds optional `return_to_parent:true` with `expected_previous_round_id` CAS. The new round has an
independent return receipt/frozen handoff. Task DTOs expose bounded `continuity`
round metadata; original command execution/final/QA/delivery axes are preserved.
Explicit delegated steering uses the original-parent native tool in
`contracts/delegation/v1`, while controller managed-target refusal stays intact.
Schema hash must be rebound to the exact candidate during tool registration;
old/native/unloaded tools are not evidence that this new contract was loaded.
