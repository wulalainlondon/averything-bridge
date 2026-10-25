teammate_profile_v1 — host-owned desktop teammate metadata

Transport: authenticated paired-human WebSocket, negotiated hello capability.
Request: {type:partner_profile_request,request_id:<correlation>,partner:<input>}.
Response: partner_profile_result with matching request_id, authority, action, ok;
failed effects have error and never an inferred successful profile.

Input authority must match this host. session_id and profile.session_id accept
raw IDs or sk1 IDs belonging to the same exact host; native thread/configuration
are checked before every mutation. Invalid/foreign nested IDs are refused.

Actions:
list: optional digest is an opaque revision:offset cursor, 100 records per page;
      response profiles, revision, has_more, next_cursor. No avatar bytes.
save: mutation_id, session_id, expected_thread, expected_config_revision,
      expected_revision and full profile. Optional avatar_png is base64 PNG.
move: same frozen binding and profile, direction +/-1, expected_roster_revision;
      adjacent eligible records reordered atomically, scoped to permitted Sessions.
status: original mutation_id; return this principal's original immutable receipt.
avatar: session_id and digest must match a currently permitted profile; response
        avatar_png and digest, with bytes verified by SHA256 on both clients.

A profile contains id, session_id, native_thread_id, revision, role_revision,
name, label, color, description, section, order, pinned, main, hidden, avatar.
Main/pin/group/hidden affect presentation only. First observed native thread can
initialize an empty profile binding once; a nonempty binding cannot be replaced.

Mutation keys are durably journaled under device + credential generation.
CAS failures, lost acknowledgements, receipt reads and later profile revisions
are distinct. A retry cannot silently overwrite or repeat a previous operation.
The authoritative profile store uses atomic durable replacement; client SQLite
stores only scoped cache, drafts and mutation/recovery journals.

Saved role preferences are frozen in each next ordinary human request; raw user
input remains independent. Existing active/queued requests retain their original
role, settings and request identity. This adds no tools, grants or system prompt.
Only supported original Codex/Claude targets may apply a role. Worker/PM/control,
question reply, callback, slash-command and imported native-task flows are not
silently changed by a presentation profile.

This extension does not implement shared multi-bot chat, global memory, routines,
new cloud computers, automatic call permissions or an avatar generation service.
Task execution/final/QA/delivery and original device voice/callback contracts
remain authoritative. Related Swift selected-session task reads use original
TaskAPI snapshot scope/cursor/coverage; uncertain coverage stays explicit.
