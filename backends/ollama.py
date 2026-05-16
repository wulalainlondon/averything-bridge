"""
Ollama backend — streams responses from a local Ollama server via HTTP.
Requires: aiohttp>=3.9
"""

import asyncio
import json
import logging
import os
import time

try:
    import aiohttp
    _AIOHTTP_AVAILABLE = True
except ImportError:
    _AIOHTTP_AVAILABLE = False

from .base import Backend
from .events import (
    send_event,
    _evt_text_chunk, _evt_done, _evt_error, _evt_stopped,
    _evt_session_warning, _evt_session_closed,
)
from .history import complete_history_message, clamp_history_limit, slice_history

log = logging.getLogger("bridge_v2")
OLLAMA_HISTORY_CAP = clamp_history_limit(os.environ.get("BRIDGE_OLLAMA_HISTORY_CAP", "200"))


class OllamaBackend(Backend):
    def __init__(self, model: str = "llama3.2", host: str = "http://localhost:11434"):
        if not _AIOHTTP_AVAILABLE:
            log.warning("aiohttp not installed — Ollama backend disabled. Run: pip install aiohttp")
        self.model = model
        self.host = host
        self._histories: dict[str, list] = {}  # session_id → message history

    async def spawn(self, session) -> None:
        self._histories[session.session_id] = []

    async def send(self, session, content: str,
                   images: list | None = None, files: list | None = None) -> None:

        if not _AIOHTTP_AVAILABLE:
            await send_event(session, _evt_error("aiohttp not installed. Run: pip install aiohttp", "backend_unavailable"))
            return

        if session.is_streaming:
            await send_event(session, _evt_error("Session is busy", "session_busy"))
            return

        session.is_streaming = True
        session.accumulated_text = ""

        history = self._histories.setdefault(session.session_id, [])
        history.append({
            "role": "user",
            "content": content,
            "timestamp": int(time.time() * 1000),
            "source_message_id": f"ollama:{session.session_id}:msg:{len(history)}",
        })
        if len(history) > OLLAMA_HISTORY_CAP:
            del history[:-OLLAMA_HISTORY_CAP]

        try:
            async with aiohttp.ClientSession() as client:
                async with client.post(
                    f"{self.host}/api/chat",
                    json={"model": self.model, "messages": history, "stream": True},
                    timeout=aiohttp.ClientTimeout(total=300),
                ) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"Ollama HTTP {resp.status}")

                    full_text = ""
                    async for line in resp.content:
                        line = line.strip()
                        if not line:
                            continue
                        data = json.loads(line)
                        chunk = data.get("message", {}).get("content", "")
                        if chunk:
                            full_text += chunk
                            session.accumulated_text += chunk
                            await send_event(session, _evt_text_chunk(chunk))
                        if data.get("done"):
                            break

                    history.append({
                        "role": "assistant",
                        "content": full_text,
                        "timestamp": int(time.time() * 1000),
                        "source_message_id": f"ollama:{session.session_id}:msg:{len(history)}",
                    })
                    if len(history) > OLLAMA_HISTORY_CAP:
                        del history[:-OLLAMA_HISTORY_CAP]
                    session.is_streaming = False
                    await send_event(session, _evt_done())

        except Exception as exc:
            session.is_streaming = False
            await send_event(session, _evt_error(f"Ollama error: {exc}"))

    async def stop(self, session) -> None:
        session.is_stopping = True
        session.is_streaming = False
        await send_event(session, _evt_stopped())

    async def clear(self, session) -> None:
        self._histories[session.session_id] = []
        session.is_streaming = False
        await send_event(session, _evt_session_warning("History cleared."))

    async def close(self, session) -> None:
        self._histories.pop(session.session_id, None)
        # Removal from _SESSIONS is the bridge handler's responsibility
        await send_event(session, _evt_session_closed())

    def supports_resume(self) -> bool:
        return True

    async def load_session_history(
        self,
        resume_id: str,
        limit: int = 120,
        known_last_source_message_id: str = "",
        mode: str = "snapshot",
        before_source_message_id: str = "",
    ) -> list[dict] | dict:
        history = self._histories.get(resume_id, [])
        messages = [
            complete_history_message(
                source="ollama",
                source_session_id=resume_id,
                source_message_id=str(item.get("source_message_id") or f"ollama:{resume_id}:msg:{i}"),
                role=str(item.get("role")),
                content=str(item.get("content", "")),
                timestamp=int(item.get("timestamp") or 0) or None,
                blocks=[{"type": "text", "text": str(item.get("content", ""))}],
            )
            for i, item in enumerate(history)
            if item.get("role") in {"user", "assistant"} and item.get("content")
        ]
        return slice_history(
            messages,
            limit=clamp_history_limit(limit),
            known_last_source_message_id=known_last_source_message_id,
            mode=mode,
            before_source_message_id=before_source_message_id,
        )
