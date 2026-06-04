"""Shared route helpers."""
from __future__ import annotations

from typing import Any

import client_manager


async def safe_send_json(ws: Any, payload: dict) -> None:
    try:
        await client_manager.send_json(ws, payload)
    except Exception:
        pass
