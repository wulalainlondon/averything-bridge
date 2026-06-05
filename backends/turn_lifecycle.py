"""Shared backend turn termination helpers."""

from .events import send_event, emit_done, _evt_stopped, _evt_error


def settle_turn_state(session, *, clear_accumulated: bool = True, clear_stopping: bool = False) -> None:
    session.is_streaming = False
    turn_done = getattr(session, "turn_done_event", None)
    if turn_done is not None:
        turn_done.set()
    if clear_accumulated:
        session.accumulated_text = ""
    if clear_stopping:
        session.is_stopping = False


async def _end_tools(session, tool_lifecycle, reason: str) -> None:
    if tool_lifecycle is not None:
        await tool_lifecycle.end_all(session, reason)


async def emit_turn_done(session, *, tool_lifecycle=None, reason: str = "done") -> None:
    await _end_tools(session, tool_lifecycle, reason)
    settle_turn_state(session, clear_accumulated=False)
    await emit_done(session)


async def emit_turn_stopped(session, *, tool_lifecycle=None, reason: str = "stopped") -> None:
    await _end_tools(session, tool_lifecycle, reason)
    settle_turn_state(session, clear_accumulated=True)
    await send_event(session, _evt_stopped())


async def emit_turn_error(
    session,
    message: str,
    code: str | None = None,
    *,
    tool_lifecycle=None,
    reason: str = "error",
) -> None:
    await _end_tools(session, tool_lifecycle, reason)
    settle_turn_state(session, clear_accumulated=True)
    await send_event(session, _evt_error(message, code))
