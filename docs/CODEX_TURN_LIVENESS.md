# Codex long-turn liveness

## Incident and correction (2026-09-15)

A Bridge-owned turn started at 06:54:15 Taiwan time and was interrupted at
08:34:15, exactly 100 minutes later, despite model/tool output at 08:34:10.
The durable message queue recorded `Codex turn timed out`; app-server recorded
the Bridge's `turn/interrupt`. The 100-minute total-turn timer existed in v0.2.44
and v0.2.51 and was independent of useful progress.

The fixed implementation removes that total wall-clock timer. Ordinary turns
are now governed by progress/inactivity and explicit cancellation:

- Progress continues: no Bridge total-duration cutoff.
- No events for 5 minutes: a nonterminal warning, once per idle period.
- No events for 30 minutes: request interruption of the captured current
  thread/turn, and record `inactivity_timeout` with session, request, thread,
  turn and last-event time in the Bridge log.
- Pending blocking user input: human wait does not consume the inactivity
  window. Submitting an answer resets it before the next model event.
- Manual stop: remains available and distinct from inactivity timeout.

An interrupt ACK is not treated as the terminal event. If interruption cannot
be confirmed, the worker remains occupied, the client receives a warning, and
the Bridge does not start another queued turn or blindly repeat the RPC.
The correlated `turn/completed` with `interrupted` settles the operation. An
inactivity stop emits error code `codex_inactivity_timeout` and a user-readable
reason through the existing error-message UI. A manual/external interruption
emits stopped, never successful done. A natural completion racing an interrupt
remains a natural completion.

This does not remove independent model/service limits, shell-command timeouts,
compact-operation deadlines, or the existing inactivity policy. A quiet tool
with no observable events for 30 minutes may still trigger the watchdog.
No original prompt or completed work is replayed or deleted by this change.

## Verification

Deterministic tests cross 99, 100, 101 minutes, 6 hours and 24 hours of logical
time with fresh progress; no model call or real multi-hour sleep is required.
Tests also cover a 48-hour human wait, answer/resume, single warnings,
current-turn-only interruption, RPC failure, pending cancellation, natural
completion races, manual/external stop and the final correlated error event.

The source fix requires a new signed Go bundle and a separately coordinated
Wulala restart to take effect. The currently installed v0.2.51 binary is not
modified merely by editing or testing these files. Morrie is not changed.
