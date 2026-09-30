# Managed chat queue and same-turn steering

The `message_queue_v1` capability adds durable ownership of ordinary `message`
requests and safe operations on waiting messages. Older clients can keep using
`message` / `message_ack`; their request IDs also gain durable deduplication.

## Commands and events

- `request_message_queue { session_id }` returns `message_queue_snapshot`.
- `promote_queued_message { session_id, request_id }` promotes an existing waiting
  message into the current Codex turn. Content and attachments come from the
  stored message, never from the promotion command.
- `cancel_queued_message { session_id, request_id }` cancels a waiting or
  unconfirmed item. It cannot cancel an executing or currently steering item.
- `queue_action_result` acknowledges an action as `accepted`, `retained`,
  `pending`, `rejected`, or `uncertain`. Clients keep server ownership and must
  not turn `retained` into a second ordinary `message` send.

Snapshots contain a monotonically increasing per-session `revision` and ordered
items. Each item includes `request_id`, `sequence`, `state`, a content preview,
attachment counts/names, and creation/update timestamps in Unix milliseconds.
Attachment bodies stay in the server's SQLite payload, not the snapshot.

States: `queued`, `running`, `steering`, `steered`, `cancelled`, `completed`,
`failed`, `uncertain`. The snapshot includes all unfinished items and up to 100
recent receipts. It is independent of transcript history; a queued item need
not yet exist in the AI provider's transcript.

## Ordering and recovery

`message_ack` is sent after writing `message_queue.sqlite` with SQLite FULL
synchronization. The request ID is a durable idempotency key per session.
Terminal payload blobs are released while receipt hashes are retained, so a
retry of the same accepted request cannot run it again.

The session actor reserves a named waiting item before steering, holding the
next-turn boundary until the result is known. Success removes the original
callback. A definite rejection restores its original position. A lost or
ambiguous response removes the callback from automatic execution and marks the
item `uncertain`; checking the existing reply is required before deciding what
to do next. Repeated clicks while a promotion is pending do not start another
RPC. Cancellation of other waiting items remains possible during promotion.

After process restart, queued payloads resume in original order. Items that
were `running` or `steering` recover as `uncertain`, avoiding blind replay of a
possibly applied side effect. Closing the frontend does not cancel server-owned
work. Stopping a turn holds the next-turn boundary while the stop RPC unwinds;
it does not cancel subsequent waiting messages.

## Compatibility

Clients enable the new controls only when `message_queue_v1` is advertised and
steering is advertised by the active Codex backend. Claude remains FIFO-only.
An older `steer_message` addressing an already managed request is routed through
the same safe promotion path and receives a legacy `steer_result` response.

## Isolated UI verification

Run `go run ./internal/core/testdata/queue_fixture.go` from this repository. It
binds only `127.0.0.1:18766`, uses the test token `queue-fixture-token`, and never
calls an AI provider. Send a first message and a waiting follow-up from a client,
then promote/cancel the follow-up. `/fixture/state` exposes executed request IDs;
`/fixture/finish` ends the current fake turn. `/fixture/mode?value=reject` simulates
a definite steering rejection and `value=uncertain` simulates a lost result.
This fixture is for local testing only, not deployment.
