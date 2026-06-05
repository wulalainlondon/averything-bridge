"""Transport adapter shape for bridge connections."""
from __future__ import annotations

from typing import Any, Protocol


class BridgeTransport(Protocol):
    remote_address: Any
    request: Any

    async def recv(self) -> Any: ...

    async def send(self, data: Any) -> None: ...

    async def close(self, *args, **kwargs) -> None: ...

    def __aiter__(self): ...

    async def __anext__(self) -> Any: ...


def transport_user_agent(transport: BridgeTransport) -> str:
    """Return the transport User-Agent, or an empty string when unavailable."""
    try:
        request = getattr(transport, "request", None)
        headers = getattr(request, "headers", None) if request is not None else None
        if headers is None:
            return ""
        return str(headers.get("User-Agent", "") or "")
    except Exception:
        return ""


def transport_remote_address(transport: BridgeTransport) -> Any:
    """Return a best-effort remote address for logging."""
    return getattr(transport, "remote_address", None)
