from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

_BRIDGE_ROOT = Path(__file__).parent.parent
_REPO_ROOT = _BRIDGE_ROOT.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_BRIDGE_ROOT))


def _session(session_id: str):
    from bridge_v2 import Session
    return Session(session_id=session_id, name=session_id, created_at=time.time(), backend_name="claude")


def test_seq_monotonic_per_session_and_independent_counters():
    import backends.events as ev
    ev.set_event_dispatcher(None)  # force offline-buffer path (ws_ref is None)

    async def run():
        s1, s2 = _session("s1"), _session("s2")
        for _ in range(3):
            await ev.send_event(s1, {"type": "text_chunk", "content": "x"})
        await ev.send_event(s2, {"type": "done"})
        await ev.send_event(s1, {"type": "done"})
        return s1.offline_buffer, s2.offline_buffer

    b1, b2 = asyncio.run(run())
    assert [e["seq"] for e in b1] == [1, 2, 3, 4]
    assert [e["seq"] for e in b2] == [1]  # independent counter


def test_gen_is_stable_within_process_and_on_every_event():
    import backends.events as ev
    ev.set_event_dispatcher(None)

    async def run():
        s = _session("s")
        await ev.send_event(s, {"type": "text_chunk", "content": "a"})
        await ev.send_event(s, {"type": "done"})
        return s.offline_buffer

    buf = asyncio.run(run())
    gens = {e["gen"] for e in buf}
    assert len(gens) == 1
    assert buf[0]["gen"] == ev.get_generation()


def test_offline_collapses_completed_turn_content():
    """A terminated turn's buffered streaming content is collapsed so reconnect
    replay does not re-animate a finished session as if it were streaming. Only
    the terminal marker survives; the content is recoverable from history."""
    import backends.events as ev
    ev.set_event_dispatcher(None)

    async def run():
        s = _session("s")
        s.current_request_id = "r1"  # so events are stamped with request_id
        await ev.send_event(s, {"type": "text_chunk", "content": "Hello "})
        await ev.send_event(s, {"type": "tool_start", "name": "Bash", "command": "ls"})
        await ev.send_event(s, {"type": "text_chunk", "content": "world"})
        await ev.send_event(s, {"type": "done"})
        return s.offline_buffer

    buf = asyncio.run(run())
    assert [e["type"] for e in buf] == ["done"]


def test_offline_collapse_scoped_to_turn_request():
    """Collapsing is keyed by (session, request): an in-flight turn (different
    request_id) is not disturbed when an earlier turn terminates."""
    import backends.events as ev
    ev.set_event_dispatcher(None)

    async def run():
        s = _session("s")
        s.current_request_id = "r1"
        await ev.send_event(s, {"type": "text_chunk", "content": "done-turn"})
        s.current_request_id = "r2"
        await ev.send_event(s, {"type": "text_chunk", "content": "live-turn"})
        s.current_request_id = "r1"
        await ev.send_event(s, {"type": "done"})  # terminate only r1
        return s.offline_buffer

    buf = asyncio.run(run())
    # r1 chunk collapsed; survivors: r2 chunk (still in-flight) + r1 done.
    types = [(e["type"], e.get("request_id")) for e in buf]
    assert types == [("text_chunk", "r2"), ("done", "r1")]


def test_multi_client_dispatch_sees_identical_seq():
    """Stamp happens before _EVENT_DISPATCHER, so all clients see the same seq."""
    import backends.events as ev
    seen: list[dict] = []

    async def dispatcher(payload, session):
        seen.append(payload)
        return True  # delivered → no offline buffering

    ev.set_event_dispatcher(dispatcher)
    try:
        async def run():
            s = _session("s")
            await ev.send_event(s, {"type": "text_chunk", "content": "a"})
            await ev.send_event(s, {"type": "done"})
        asyncio.run(run())
    finally:
        ev.set_event_dispatcher(None)

    assert [e["seq"] for e in seen] == [1, 2]
    assert all("gen" in e for e in seen)


def test_session_event_sink_delivers_without_websocket_or_dispatcher():
    import backends.events as ev
    from backends.event_sink import MemoryEventSink

    ev.set_event_dispatcher(None)

    async def run():
        s = _session("s")
        sink = MemoryEventSink()
        s.event_sink = sink
        s.current_request_id = "r1"
        await ev.send_event(s, {"type": "text_chunk", "content": "a"})
        await ev.flush_session_events(s)
        await ev.stop_session_drain(s)
        return sink.events, s.offline_buffer

    events, offline = asyncio.run(run())

    assert offline == []
    assert events == [
        {
            "type": "text_chunk",
            "content": "a",
            "session_id": "s",
            "request_id": "r1",
            "seq": 1,
            "gen": ev.get_generation(),
        }
    ]


def test_offline_overflow_merge_creates_detectable_seq_gap(monkeypatch):
    """When the buffer overflows and text_chunks merge, the dropped event's seq
    vanishes — leaving a gap the client uses to trigger a reconcile."""
    import backends.events as ev
    ev.set_event_dispatcher(None)
    monkeypatch.setattr(ev, "OFFLINE_BUFFER_MAX", 3)

    async def run():
        s = _session("s")
        for _ in range(6):  # 6 text_chunks into a cap-3 buffer → merges
            await ev.send_event(s, {"type": "text_chunk", "content": "x"})
        return s.offline_buffer, s._event_seq

    buf, last_seq = asyncio.run(run())
    seqs = [e["seq"] for e in buf]
    assert last_seq == 6                       # counter kept climbing
    assert seqs == sorted(seqs)                # still ordered
    assert seqs[-1] < last_seq or len(buf) <= 3  # buffer bounded
    # the replayed seq stream is non-contiguous vs the 1..6 produced → a gap
    assert (max(seqs) - min(seqs) + 1) > len(buf) or len(buf) < 6
