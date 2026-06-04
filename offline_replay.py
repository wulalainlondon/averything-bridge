"""Offline event replay helpers for reconnecting clients."""
from __future__ import annotations

import json
from typing import Any, Iterable

import client_manager


async def replay_offline_buffers(ws: Any, sessions: Iterable[Any]) -> int:
    """Replay buffered session events to a reconnecting client.

    Takes a snapshot of each session's offline_buffer before sending so that
    events appended by concurrent producers during replay are not lost.
    Only the successfully-sent events are removed from the live buffer;
    any events appended after the snapshot was taken are preserved.
    If the socket fails mid-replay, the unsent portion of the snapshot is
    re-prepended so the next reconnect sees them.

    Each session snapshot is sent as one per-ws locked batch so live session
    drain tasks cannot interleave between replayed frames.
    """
    replayed = 0
    for session in list(sessions):
        if not session.offline_buffer:
            continue
        # Snapshot without clearing — other producers may still append.
        snapshot = session.offline_buffer[:]
        sent_count = await client_manager.send_text_batch(
            ws,
            [json.dumps(evt) for evt in snapshot],
        )
        replayed += sent_count
        if sent_count < len(snapshot):
            # Remove only the events we already sent from the live buffer.
            # The remaining front already starts with the unsent tail of
            # the snapshot, followed by any events appended during replay.
            del session.offline_buffer[:sent_count]
            return replayed
        # All snapshot events sent successfully.  Remove exactly those entries
        # from the front of the live buffer (new appends stay intact).
        del session.offline_buffer[:sent_count]
    return replayed
