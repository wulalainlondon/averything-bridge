"""Session prompt queue execution.

This module owns the prompt lifecycle after a command has been enqueued.  It is
kept free of bridge_v2 imports so it can be tested without starting the full
WebSocket bridge.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Protocol

from backends.events import _evt_error, send_event


class BackendLike(Protocol):
    async def send(self, session: Any, content: str, images: list | None, files: list | None) -> None:
        ...


GetBackend = Callable[[Any], BackendLike]
BroadcastJson = Callable[[dict], Awaitable[None]]

log = logging.getLogger("bridge_v2")


def log_prompt_lifecycle(stage: str, session: Any, request_id: str, **fields: Any) -> None:
    """Emit a compact trace line for prompt delivery and queue execution."""
    extra = " ".join(f"{k}={v!r}" for k, v in fields.items() if v is not None)
    log.info(
        "[prompt] stage=%s session=%s request=%s queue=%d processing=%s streaming=%s%s%s",
        stage,
        session.session_id,
        request_id,
        len(session.queue),
        session.processing,
        session.is_streaming,
        " " if extra else "",
        extra,
    )


async def run_session_queue(
    session: Any,
    *,
    get_backend: GetBackend,
    broadcast_json: BroadcastJson,
) -> None:
    """Drain a session prompt queue in order.

    The bridge message handler owns validation/enqueueing.  This runner owns the
    command execution lifecycle: started -> backend.send -> done/failed -> pop.
    """
    if session.processing:
        log.debug("[prompt] queue runner skipped; already processing session=%s", session.session_id)
        return
    session.processing = True
    try:
        while session.queue:
            cmd = session.queue[0]
            session.current_request_id = cmd.request_id
            log_prompt_lifecycle(
                "started",
                session,
                cmd.request_id,
                client_id=cmd.client_id,
                device_id=cmd.device_id,
            )
            await broadcast_json({
                "type": "session_command_started",
                "session_id": session.session_id,
                "request_id": cmd.request_id,
                "device_id": cmd.device_id,
                "queue_length": len(session.queue),
            })

            try:
                await get_backend(session).send(session, cmd.content, cmd.images, cmd.files)
                # backend.send() spawns a subprocess and returns immediately;
                # wait here so session.processing stays True until streaming ends,
                # preventing the next queued command from seeing is_streaming=True.
                # W6: watchdog — bound the spin so a zombie subprocess (is_streaming
                # never cleared, no user stop) doesn't hang the queue forever.
                _SPIN_TIMEOUT = 7200  # 2 h — generous margin above TOOL_IDLE_TIMEOUT_SECS
                _spin_start = time.monotonic()
                while session.is_streaming:
                    if session.is_stopping:
                        return  # stop() is responsible for sending the stopped event
                    await asyncio.sleep(0.15)
                    if time.monotonic() - _spin_start > _SPIN_TIMEOUT:
                        log.warning(
                            "[%s] is_streaming stuck for >%ds — forcing idle (zombie subprocess?)",
                            session.session_id, _SPIN_TIMEOUT,
                        )
                        session.is_streaming = False
                        await send_event(session, _evt_error("Backend timed out (streaming stuck)"))
                        break
                session.recent_request_ids.add(cmd.request_id)
                if len(session.recent_request_ids) > 500:
                    session.recent_request_ids = set(list(session.recent_request_ids)[-250:])
                log_prompt_lifecycle("done", session, cmd.request_id)
                await broadcast_json({
                    "type": "session_command_done",
                    "session_id": session.session_id,
                    "request_id": cmd.request_id,
                    "queue_length": max(0, len(session.queue) - 1),
                })
            except Exception as exc:
                log.error("[%s] backend exception in queue: %s", session.session_id, exc, exc_info=True)
                log_prompt_lifecycle("failed", session, cmd.request_id, error=str(exc))
                session.is_streaming = False
                await send_event(session, _evt_error(str(exc)))
                await broadcast_json({
                    "type": "session_command_failed",
                    "session_id": session.session_id,
                    "request_id": cmd.request_id,
                    "message": str(exc),
                    "queue_length": max(0, len(session.queue) - 1),
                })
            finally:
                if session.queue and session.queue[0].request_id == cmd.request_id:
                    session.queue.popleft()
                    log_prompt_lifecycle("popped", session, cmd.request_id)
                session.current_request_id = ""
    finally:
        session.processing = False
