"""WebSocket handling for structured user-input interactions."""
from __future__ import annotations

import json
from typing import Any, Awaitable, Callable

from interactions import REGISTRY, normalize_questions

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

# Client can inject a user_input_request to broadcast to all clients (used for testing/tools)
INJECT_TYPES = frozenset({
    "user_input_request",
    "request_user_input",
    "choice_request",
    "multi_choice_request",
    "form_request",
    "confirmation_request",
    "question_request",
    "questions_request",
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

    if mtype in INJECT_TYPES:
        session_id = str(msg.get("session_id") or "")
        raw_questions = msg.get("questions") or msg.get("choices") or msg.get("options")
        if isinstance(raw_questions, list):
            command: Any = {"questions": raw_questions}
        elif isinstance(raw_questions, dict):
            command = raw_questions
        else:
            command = msg
        questions = normalize_questions(command)
        if not questions:
            questions = normalize_questions(msg)
        await REGISTRY.create(
            session_id=session_id,
            source=str(msg.get("source") or "client"),
            kind=str(msg.get("kind") or "choice"),
            questions=questions,
            header=str(msg.get("header") or ""),
            broadcast_json=broadcast_json,
        )
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
