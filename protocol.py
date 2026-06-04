"""Inbound WebSocket protocol validation."""
from __future__ import annotations

from typing import Literal, NotRequired, TypedDict


class PingMsg(TypedDict):
    type: Literal["ping"]


class MessageMsg(TypedDict):
    type: Literal["message"]
    session_id: str
    content: NotRequired[str]
    images: NotRequired[list]
    files: NotRequired[list]


class NewSessionMsg(TypedDict):
    type: Literal["new_session"]
    session_id: str
    name: str
    cwd: NotRequired[str]
    resume_claude_id: NotRequired[str]
    backend: NotRequired[str]
    model: NotRequired[str]
    sandbox: NotRequired[str]
    image_dir: NotRequired[str]


class StopMsg(TypedDict):
    type: Literal["stop"]
    session_id: str


class CloseSessionMsg(TypedDict):
    type: Literal["close_session"]
    session_id: str


class RenameSessionMsg(TypedDict):
    type: Literal["rename_session"]
    session_id: str
    name: str


class ClearSessionMsg(TypedDict):
    type: Literal["clear_session"]
    session_id: str


class GetUsageMsg(TypedDict):
    type: Literal["get_usage"]


class GetResumableSessionsMsg(TypedDict):
    type: Literal["get_resumable_sessions"]


class ShellCreateMsg(TypedDict):
    type: Literal["shell_create"]
    cwd: NotRequired[str]


class ShellInputMsg(TypedDict):
    type: Literal["shell_input"]
    shell_id: str
    data: str


class ShellCloseMsg(TypedDict):
    type: Literal["shell_close"]
    shell_id: str


class GetTasksMsg(TypedDict):
    type: Literal["get_tasks"]


class KillTaskMsg(TypedDict):
    type: Literal["kill_task"]
    id: str


class GetProcessesMsg(TypedDict):
    type: Literal["get_processes"]


class KillProcessMsg(TypedDict):
    type: Literal["kill_process"]
    pid: int
    force: NotRequired[bool]


class FcmTokenMsg(TypedDict):
    type: Literal["fcm_token"]
    token: str


class RequestSessionsListMsg(TypedDict):
    type: Literal["request_sessions_list"]


class BrowseDirMsg(TypedDict):
    type: Literal["browse_dir"]
    path: NotRequired[str]

class OpenFileMsg(TypedDict):
    type: Literal["open_file"]
    path: str


class HelloMsg(TypedDict):
    type: Literal["hello"]
    device_id: NotRequired[str]
    device_name: NotRequired[str]
    auth_token: NotRequired[str]


class SetSessionMetaMsg(TypedDict):
    type: Literal["set_session_meta"]
    session_id: str
    pinned: NotRequired[bool]
    hidden: NotRequired[bool]


class SwitchSessionConfigMsg(TypedDict):
    type: Literal["switch_session_config"]
    session_id: str
    backend: NotRequired[str]
    model: NotRequired[str]
    effort: NotRequired[str]
    sandbox: NotRequired[str]
    image_dir: NotRequired[str]


class PermissionResponseMsg(TypedDict):
    type: Literal["permission_response"]
    request_id: str
    decision: str


class RequestStatusMsg(TypedDict):
    type: Literal["request_status"]
    session_id: NotRequired[str]


class UserInputResponseMsg(TypedDict):
    type: Literal["user_input_response"]
    request_id: str
    answers: NotRequired[dict]


_INBOUND_REQUIRED: dict[str, list[tuple[str, type]]] = {
    "new_session": [("session_id", str), ("name", str)],
    "message": [("session_id", str)],
    "stop": [("session_id", str)],
    "close_session": [("session_id", str)],
    "rename_session": [("session_id", str), ("name", str)],
    "clear_session": [("session_id", str)],
    "shell_input": [("shell_id", str), ("data", str)],
    "shell_close": [("shell_id", str)],
    "kill_task": [("id", str)],
    "kill_process": [("pid", int)],
    "browse_dir": [],
    "open_file": [("path", str)],
    "request_history": [("session_id", str)],
    "set_effort": [("session_id", str), ("effort", str)],
    "set_session_meta": [("session_id", str)],
    "switch_session_config": [("session_id", str)],
    "permission_response": [("request_id", str), ("decision", str)],
    "user_input_response": [("request_id", str)],
    "request_user_input_response": [("request_id", str)],
    "choice_response": [("request_id", str)],
    "multi_choice_response": [("request_id", str)],
    "form_response": [("request_id", str)],
    "confirmation_response": [("request_id", str)],
    "question_response": [("request_id", str)],
    "questions_response": [("request_id", str)],
    "pending_decision_result": [("request_id", str)],
    "user_input_request": [],
    "request_user_input": [],
    "choice_request": [],
    "multi_choice_request": [],
    "form_request": [],
    "confirmation_request": [],
    "question_request": [],
    "questions_request": [],
    "request_status": [],
    "pending_interactions_list": [],
    "pending_decisions_list": [],
    "claim_bridge": [],
    "unclaim_bridge": [],
    "get_agent_tree": [("session_id", str)],
    "fork_session": [("session_id", str)],
    "get_git_diff": [("session_id", str)],
    "feed_push": [("title", str), ("html", str)],  # content_type optional
    "feed_fetch": [("feed_id", str)],
    "feed_mark_read": [("feed_id", str)],
    "feed_delete": [("feed_id", str)],
    "start_instance": [("name", str)],
    "stop_instance": [("name", str)],
    "upsert_instance": [("name", str), ("port", int), ("root_dir", str)],
    "delete_instance": [("name", str)],
}


_KNOWN_MSG_TYPES: frozenset[str] = frozenset({
    "ping", "message", "new_session", "stop", "close_session",
    "rename_session", "clear_session", "get_usage", "get_resumable_sessions",
    "shell_create", "shell_input", "shell_close", "get_tasks", "kill_task",
    "get_processes", "kill_process",
    "fcm_token", "tunnel_url_ack", "request_sessions_list", "browse_dir", "open_file", "request_history",
    "set_effort", "hello", "set_session_meta", "switch_session_config",
    "permission_response",
    "user_input_response", "request_user_input_response", "choice_response",
    "multi_choice_response", "form_response", "confirmation_response",
    "question_response", "questions_response", "pending_decision_result",
    "user_input_request", "request_user_input", "choice_request",
    "multi_choice_request", "form_request", "confirmation_request",
    "question_request", "questions_request",
    "pending_interactions_list", "pending_decisions_list",
    "request_status",
    "claim_bridge", "unclaim_bridge",
    "restart_bridge",
    "push_file", "file_push_ack", "get_inbox",
    "get_all_sessions",
    "request_search", "request_search_health", "request_session_list",
    "request_search_context",
    "webrtc_offer", "webrtc_answer", "webrtc_ice",
    "get_agent_tree",
    "fork_session",
    "get_git_diff",
    "feed_push",
    "feed_list_request",
    "feed_fetch",
    "feed_mark_read",
    "feed_delete",
    "list_instances", "start_instance", "stop_instance", "upsert_instance", "delete_instance",
})


def validate_client_msg(msg: object) -> str | None:
    """Return an error description, or None if the message is valid."""
    if not isinstance(msg, dict):
        return "message must be a JSON object"
    mtype = msg.get("type")
    if not isinstance(mtype, str):
        return "missing or non-string 'type' field"
    if mtype not in _KNOWN_MSG_TYPES:
        return f"unknown message type '{mtype}'"
    for field_name, expected_type in _INBOUND_REQUIRED.get(mtype, []):
        val = msg.get(field_name)
        if val is None:
            return f"'{mtype}' missing required field '{field_name}'"
        if not isinstance(val, expected_type):
            return f"'{mtype}.{field_name}' must be {expected_type.__name__}, got {type(val).__name__}"
    return None
