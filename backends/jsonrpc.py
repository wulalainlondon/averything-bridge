"""
Shared JSON-RPC 2.0 over newline-delimited stdio helper.
Used by CodexAppServerBackend (singleton proc) and GeminiCliBackend (per-session proc).
"""
from __future__ import annotations
import asyncio
import json
import logging

log = logging.getLogger("bridge_v2")


class JsonRpcPlumber:
    """Wraps a single asyncio subprocess and provides JSON-RPC 2.0 request/notify primitives."""

    def __init__(self, name: str = "rpc") -> None:
        self._name = name
        self._next_id: int = 1
        self._futures: dict[int, asyncio.Future] = {}

    def _alloc_id(self) -> int:
        rid = self._next_id
        self._next_id += 1
        return rid

    async def write(self, proc: asyncio.subprocess.Process, obj: dict) -> None:
        assert proc.stdin
        proc.stdin.write((json.dumps(obj) + "\n").encode())
        await proc.stdin.drain()

    async def request(self, proc: asyncio.subprocess.Process,
                      method: str, params: dict | None,
                      timeout: float = 30.0) -> dict:
        rid = self._alloc_id()
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._futures[rid] = fut
        req: dict = {"id": rid, "method": method}
        if params is not None:
            req["params"] = params
        await self.write(proc, req)
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        except asyncio.TimeoutError:
            self._futures.pop(rid, None)
            raise TimeoutError(f"[{self._name}] '{method}' timed out after {timeout}s")

    async def notify(self, proc: asyncio.subprocess.Process,
                     method: str, params: dict | None = None) -> None:
        msg: dict = {"method": method}
        if params is not None:
            msg["params"] = params
        await self.write(proc, msg)

    def dispatch_response(self, msg: dict) -> bool:
        """Route an RPC response/error to the pending future. Returns True if consumed."""
        rid = msg.get("id")
        if rid is None:
            return False
        if "result" in msg:
            fut = self._futures.pop(rid, None)
            if fut and not fut.done():
                fut.set_result(msg["result"])
            return True
        if "error" in msg:
            fut = self._futures.pop(rid, None)
            if fut and not fut.done():
                fut.set_exception(RuntimeError(str(msg["error"])))
            return True
        return False

    def fail_all(self, exc: Exception) -> None:
        """Called when the process dies — reject all pending futures."""
        for fut in self._futures.values():
            if not fut.done():
                fut.set_exception(exc)
        self._futures.clear()
