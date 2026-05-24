"""WebSocket handling for structured user-input interactions."""
from __future__ import annotations

import json
from typing import Any, Awaitable, Callable

from interactions import REGISTRY

BroadcastJson = Callable[[dict], Awaitable[int]]
SessionBackend = Callable[[Any], Any]

RESPONSE_TYPES = frozenset({
    "user_input_response",
    "request_user_input_response",
    "choice_response",
    "multi_choice_response",
    "form_response",
    "confirmation_response",
    "question_response",
    "questions_response",
    "pending_decision_result",
})

LIST_TYPES = frozenset({
    "pending_interactions_list",
    "pending_decisions_list",
})


async def send_pending_interactions(ws: Any, session_id: str = "") -> None:
    items = await REGISTRY.list_pending(session_id)
    try:
        await ws.send(json.dumps({"type": "pending_interactions_list", "interactions": items}))
    except Exception:
        pass


async def handle_interaction_message(
    *,
    mtype: str,
    msg: dict,
    ws: Any,
    sessions: dict[str, Any],
    session_backend: SessionBackend,
    broadcast_json: BroadcastJson,
    msg_error: Callable[[str], dict],
) -> bool:
    if mtype in LIST_TYPES:
        await send_pending_interactions(ws, str(msg.get("session_id") or ""))
        return True

    if mtype not in RESPONSE_TYPES:
        return False

    response = dict(msg)
    if response.get("type") != "user_input_response":
        response["response_type"] = response.get("type")
        response["type"] = "user_input_response"

    item = await REGISTRY.resolve(response, broadcast_json=broadcast_json)
    if item is None:
        try:
            await ws.send(json.dumps(msg_error(f"Unknown user input request: {msg.get('request_id', '')}")))
        except Exception:
            pass
        return True

    session = sessions.get(item.session_id)
    if not session:
        return True
    backend = session_backend(session)
    handler = getattr(backend, "handle_user_input_response", None)
    if handler is not None:
        await handler(session, item, response)
    return True
