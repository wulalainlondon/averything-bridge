"""Pending structured user interactions shared by bridge backends."""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any


BroadcastJson = Callable[[dict], Awaitable[int]]
ResolveCallback = Callable[["PendingInteraction", dict], Awaitable[None]]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _option_id(option: Any, index: int) -> str:
    if isinstance(option, dict):
        return _safe_str(option.get("id") or option.get("value") or option.get("label") or index)
    return _safe_str(option or index)


def _normalize_options(raw_options: Any) -> list[dict]:
    if not isinstance(raw_options, list):
        return []
    options: list[dict] = []
    for idx, option in enumerate(raw_options):
        if isinstance(option, dict):
            label = _safe_str(option.get("label") or option.get("text") or option.get("value") or option.get("id") or idx)
            options.append({
                "id": _option_id(option, idx),
                "label": label,
                "description": _safe_str(option.get("description") or option.get("detail")),
                "recommended": bool(option.get("recommended") or option.get("isRecommended")),
            })
        else:
            options.append({
                "id": _option_id(option, idx),
                "label": _safe_str(option),
                "description": "",
                "recommended": False,
            })
    return options


def normalize_questions(command: Any) -> list[dict]:
    """Convert Claude/Codex-flavored question payloads into bridge wire shape."""
    if isinstance(command, str):
        try:
            command = json.loads(command)
        except Exception:
            command = {"questions": [{"text": command}]}
    if not isinstance(command, dict):
        command = {"questions": [{"text": _safe_str(command)}]}

    raw_questions = command.get("questions")
    if not isinstance(raw_questions, list) or not raw_questions:
        raw_questions = [command]

    questions: list[dict] = []
    for idx, raw in enumerate(raw_questions):
        q = raw if isinstance(raw, dict) else {"text": raw}
        options = _normalize_options(q.get("options") or q.get("choices"))
        qtype = _safe_str(q.get("type") or q.get("kind") or "")
        multi = bool(q.get("multiSelect") or q.get("multi_select") or q.get("multiple"))
        free_form = bool(q.get("freeForm") or q.get("free_form") or q.get("allowFreeForm"))
        if not qtype:
            if multi:
                qtype = "multi_choice"
            elif options:
                qtype = "choice"
            else:
                qtype = "question"
                free_form = True
        questions.append({
            "question_id": _safe_str(q.get("question_id") or q.get("id") or f"q{idx + 1}"),
            "text": _safe_str(q.get("text") or q.get("question") or q.get("label") or "Question"),
            "header": _safe_str(q.get("header") or q.get("title")),
            "type": qtype,
            "options": options,
            "multi_select": multi,
            "free_form": free_form,
        })
    return questions


@dataclass
class PendingInteraction:
    request_id: str
    session_id: str
    source: str
    kind: str
    questions: list[dict]
    header: str = ""
    tool_use_id: str = ""
    request_id_aliases: set[str] = field(default_factory=set)
    requesting_agent: str = ""
    created_at: int = field(default_factory=_now_ms)
    expires_at: int | None = None
    status: str = "pending"
    raw_command: Any = None

    def to_wire(self) -> dict:
        payload: dict = {
            "request_id": self.request_id,
            "session_id": self.session_id,
            "source": self.source,
            "kind": self.kind,
            "header": self.header,
            "questions": self.questions,
            "created_at": self.created_at,
            "status": self.status,
        }
        if self.expires_at is not None:
            payload["expires_at"] = self.expires_at
        if self.tool_use_id:
            payload["tool_use_id"] = self.tool_use_id
        if self.requesting_agent:
            payload["requesting_agent"] = self.requesting_agent
        return payload


class PendingInteractionsRegistry:
    def __init__(self) -> None:
        self._pending: dict[str, PendingInteraction] = {}
        self._aliases: dict[str, str] = {}
        self._callbacks: dict[str, ResolveCallback] = {}
        self._lock = asyncio.Lock()

    async def create(
        self,
        *,
        session_id: str,
        source: str,
        kind: str,
        questions: list[dict],
        header: str = "",
        tool_use_id: str = "",
        requesting_agent: str = "",
        raw_command: Any = None,
        expires_in_ms: int | None = None,
        resolve_callback: ResolveCallback | None = None,
        broadcast_json: BroadcastJson | None = None,
    ) -> PendingInteraction:
        request_id = f"ui_{uuid.uuid4().hex[:12]}"
        expires_at = _now_ms() + expires_in_ms if expires_in_ms else None
        aliases = {request_id}
        if tool_use_id:
            aliases.add(tool_use_id)
        item = PendingInteraction(
            request_id=request_id,
            session_id=session_id,
            source=source,
            kind=kind,
            questions=questions,
            header=header,
            tool_use_id=tool_use_id,
            request_id_aliases=aliases,
            requesting_agent=requesting_agent,
            expires_at=expires_at,
            raw_command=raw_command,
        )
        async with self._lock:
            self._pending[request_id] = item
            for alias in aliases:
                self._aliases[alias] = request_id
            if resolve_callback is not None:
                self._callbacks[request_id] = resolve_callback
        if broadcast_json is not None:
            await broadcast_json({"type": "user_input_request", **item.to_wire()})
        return item

    async def list_pending(self, session_id: str = "") -> list[dict]:
        await self.expire_due()
        async with self._lock:
            items = list(self._pending.values())
        if session_id:
            items = [item for item in items if item.session_id == session_id]
        return [item.to_wire() for item in items if item.status == "pending"]

    async def resolve(self, response: dict, *, broadcast_json: BroadcastJson | None = None) -> PendingInteraction | None:
        request_id = _safe_str(response.get("request_id"))
        if not request_id:
            return None
        async with self._lock:
            canonical = self._aliases.get(request_id, request_id)
            item = self._pending.pop(canonical, None)
            callback = self._callbacks.pop(canonical, None)
            if item is None:
                return None
            for alias in item.request_id_aliases:
                self._aliases.pop(alias, None)
            item.status = "resolved"
        if callback is not None:
            await callback(item, response)
        if broadcast_json is not None:
            await broadcast_json({
                "type": "interaction_resolved",
                "request_id": item.request_id,
                "session_id": item.session_id,
                "status": "resolved",
            })
        return item

    async def cancel(self, request_id: str, *, broadcast_json: BroadcastJson | None = None) -> PendingInteraction | None:
        return await self._finish(request_id, "cancelled", broadcast_json=broadcast_json)

    async def expire_due(self, *, broadcast_json: BroadcastJson | None = None) -> list[PendingInteraction]:
        now = _now_ms()
        expired: list[PendingInteraction] = []
        async with self._lock:
            for request_id, item in list(self._pending.items()):
                if item.expires_at is None or item.expires_at > now:
                    continue
                self._pending.pop(request_id, None)
                self._callbacks.pop(request_id, None)
                for alias in item.request_id_aliases:
                    self._aliases.pop(alias, None)
                item.status = "expired"
                expired.append(item)
        if broadcast_json is not None:
            for item in expired:
                await broadcast_json({
                    "type": "interaction_expired",
                    "request_id": item.request_id,
                    "session_id": item.session_id,
                    "status": "expired",
                })
        return expired

    async def _finish(self, request_id: str, status: str, *, broadcast_json: BroadcastJson | None) -> PendingInteraction | None:
        async with self._lock:
            canonical = self._aliases.get(request_id, request_id)
            item = self._pending.pop(canonical, None)
            self._callbacks.pop(canonical, None)
            if item is None:
                return None
            for alias in item.request_id_aliases:
                self._aliases.pop(alias, None)
            item.status = status
        if broadcast_json is not None:
            await broadcast_json({
                "type": "interaction_resolved" if status == "cancelled" else "interaction_expired",
                "request_id": item.request_id,
                "session_id": item.session_id,
                "status": status,
            })
        return item


REGISTRY = PendingInteractionsRegistry()
