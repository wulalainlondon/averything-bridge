from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

_REPO_ROOT = Path(__file__).parent.parent.parent
_BRIDGE_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_BRIDGE_ROOT))


class _DoneEvent:
    def __init__(self) -> None:
        self.was_set = False

    def set(self) -> None:
        self.was_set = True


def _session(text: str = "hello"):
    return SimpleNamespace(
        session_id="s1",
        is_streaming=True,
        is_stopping=True,
        accumulated_text=text,
        turn_done_event=_DoneEvent(),
    )


def test_settle_turn_state_clears_flags():
    from backends.turn_lifecycle import settle_turn_state

    session = _session()
    settle_turn_state(session, clear_accumulated=True, clear_stopping=True)

    assert session.is_streaming is False
    assert session.turn_done_event.was_set is True
    assert session.accumulated_text == ""
    assert session.is_stopping is False


def test_emit_turn_stopped_ends_tools_and_clears_text(monkeypatch):
    import backends.turn_lifecycle as lifecycle
    from backends.turn_lifecycle import emit_turn_stopped

    sent: list[dict] = []

    async def fake_send_event(session, event):
        sent.append(event)

    monkeypatch.setattr(lifecycle, "send_event", fake_send_event)

    class Tools:
        def __init__(self) -> None:
            self.reasons: list[str] = []

        async def end_all(self, session, reason: str) -> None:
            self.reasons.append(reason)

    async def run():
        session = _session()
        tools = Tools()
        await emit_turn_stopped(session, tool_lifecycle=tools)
        assert tools.reasons == ["stopped"]
        assert session.is_streaming is False
        assert session.accumulated_text == ""

    asyncio.run(run())

    assert sent == [{"type": "stopped"}]


def test_emit_turn_error_sends_code(monkeypatch):
    import backends.turn_lifecycle as lifecycle
    from backends.turn_lifecycle import emit_turn_error

    sent: list[dict] = []

    async def fake_send_event(session, event):
        sent.append(event)

    monkeypatch.setattr(lifecycle, "send_event", fake_send_event)

    async def run():
        session = _session()
        await emit_turn_error(session, "failed", "boom")
        assert session.is_streaming is False
        assert session.accumulated_text == ""

    asyncio.run(run())

    assert sent == [{"type": "error", "message": "failed", "code": "boom"}]
