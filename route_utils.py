"""Shared route helpers."""
from __future__ import annotations

import json
from typing import Any


async def safe_send_json(ws: Any, payload: dict) -> None:
    try:
        await ws.send(json.dumps(payload))
    except Exception:
        pass
