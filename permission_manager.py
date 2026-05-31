from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass
class PermissionRequest:
    request_id: str
    requester_device_id: str
    action: str
    title: str
    justification: str
    command_preview: str
    risk_level: str
    session_id: str
    created_at_ms: int
    expires_at_ms: int
    resolved: bool = False
    decision: str = "pending"


class PermissionManager:
    def __init__(
        self,
        broadcast_json: Callable[[dict], Awaitable[int]],
        ttl_seconds: int = 60,
        mode: str = "enforce",
    ) -> None:
        self._broadcast_json = broadcast_json
        self._ttl_seconds = max(5, int(ttl_seconds))
        self._mode = (mode or "enforce").strip().lower()
        self._pending: dict[str, PermissionRequest] = {}
        self._waiters: dict[str, asyncio.Future[bool]] = {}

    def mode(self) -> str:
        return self._mode

    async def request(
        self,
        *,
        requester_device_id: str,
        action: str,
        title: str,
        justification: str,
        command_preview: str = "",
        risk_level: str = "high",
        session_id: str = "",
    ) -> bool:
        if self._mode == "off":
            return True
        if self._mode == "warn":
            await self._broadcast_json(
                {
                    "type": "permission_result",
                    "request_id": "",
                    "session_id": session_id,
                    "action": action,
                    "decision": "warn_auto_approved",
                    "message": "permission mode=warn",
                }
            )
            return True

        now = int(time.time() * 1000)
        rid = f"perm_{uuid.uuid4().hex[:12]}"
        req = PermissionRequest(
            request_id=rid,
            requester_device_id=requester_device_id,
            action=action,
            title=title,
            justification=justification,
            command_preview=command_preview[:500],
            risk_level=risk_level,
            session_id=session_id,
            created_at_ms=now,
            expires_at_ms=now + self._ttl_seconds * 1000,
        )
        self._pending[rid] = req

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[bool] = loop.create_future()
        self._waiters[rid] = fut

        await self._broadcast_json(
            {
                "type": "permission_request",
                "request_id": rid,
                "session_id": session_id,
                "action": action,
                "title": title,
                "justification": justification,
                "command_preview": req.command_preview,
                "risk_level": risk_level,
                "expires_at": req.expires_at_ms,
            }
        )

        try:
            approved = await asyncio.wait_for(fut, timeout=self._ttl_seconds)
        except asyncio.TimeoutError:
            approved = False
            await self.resolve(
                request_id=rid,
                decision="expired",
                responder_device_id="",
            )
        finally:
            self._waiters.pop(rid, None)
            self._pending.pop(rid, None)
        return bool(approved)

    async def resolve(
        self,
        *,
        request_id: str,
        decision: str,
        responder_device_id: str,
    ) -> bool:
        req = self._pending.get(request_id)
        if req is None or req.resolved:
            return False

        # Bind response to requester device to prevent a different connected
        # client from approving high-risk operations.
        if responder_device_id and responder_device_id != req.requester_device_id:
            await self._broadcast_json(
                {
                    "type": "permission_result",
                    "request_id": request_id,
                    "session_id": req.session_id,
                    "action": req.action,
                    "decision": "denied",
                    "message": "response device mismatch",
                }
            )
            return False

        req.resolved = True
        req.decision = decision
        approved = decision == "approve"

        fut = self._waiters.get(request_id)
        if fut and not fut.done():
            fut.set_result(approved)

        await self._broadcast_json(
            {
                "type": "permission_result",
                "request_id": request_id,
                "session_id": req.session_id,
                "action": req.action,
                "decision": "approved" if approved else ("expired" if decision == "expired" else "denied"),
                "message": "",
            }
        )
        return True

