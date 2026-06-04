"""WebSocket message routing entrypoint."""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Coroutine

from handlers.feed_ops import (
    handle_feed_push,
    handle_feed_list_request,
    handle_feed_fetch,
    handle_feed_mark_read,
    handle_feed_delete,
)
from handlers.fork_ops import handle_fork_message
from handlers.instance_ops import handle_instance_msg
from prompt_routes import handle_prompt_message
from route_utils import safe_send_json as _safe_send_json
from session_routes import handle_session_message
import client_manager



_RECENT_HEAVY_REQUESTS: dict[tuple, float] = {}


def _throttle(key: tuple, interval: float) -> bool:
    now = time.time()
    last = _RECENT_HEAVY_REQUESTS.get(key, 0.0)
    if now - last < interval:
        return True
    _RECENT_HEAVY_REQUESTS[key] = now
    if len(_RECENT_HEAVY_REQUESTS) > 2048:
        cutoff = now - 120.0
        for old_key, ts in list(_RECENT_HEAVY_REQUESTS.items()):
            if ts < cutoff:
                _RECENT_HEAVY_REQUESTS.pop(old_key, None)
    return False


def _spawn_client_task(ctx: "RouterContext", client: Any, name: str, coro: Coroutine[Any, Any, Any]) -> Any:
    try:
        return ctx.spawn_task(name, coro, owner=getattr(client, "client_id", ""))
    except TypeError:
        return ctx.spawn_task(name, coro)


class _CurrentClientWs:
    def __init__(self, ws: Any, client: Any):
        self._ws = ws
        self._client = client
        self._bridge_ws = ws
        self._bridge_client = client
        self._enforce_current = ws in client_manager.CLIENTS

    async def send(self, payload: str) -> Any:
        if self._enforce_current and not client_manager.is_current(self._ws, self._client):
            raise asyncio.CancelledError("stale client websocket")
        ok = await client_manager.send_text(self._ws, payload, self._client)
        if not ok:
            raise asyncio.CancelledError("client websocket unavailable")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._ws, name)


def _current_ws(ws: Any, client: Any) -> _CurrentClientWs:
    return _CurrentClientWs(ws, client)

def _is_current_if_registered(ws: Any, client: Any) -> bool:
    if ws not in client_manager.CLIENTS:
        return True
    return client_manager.is_current(ws, client)



SendAllSessions = Callable[[Any], Awaitable[None]]
BroadcastJson = Callable[[dict], Awaitable[int]]
BuildSessionsList = Callable[[], dict]
PersistSessionMeta = Callable[[], None]
SpawnTask = Callable[..., Any]
SendUnreadSnapshot = Callable[[Any, Any], Awaitable[None]]
SendUnreadForClientSession = Callable[[Any, Any, Any], Awaitable[None]]
MarkRead = Callable[[str, str, int], None]
SendSessionHistoryResponse = Callable[..., Awaitable[None]]
HistoryRuntimePayload = Callable[[Any], dict | None]
EmitResumeProgress = Callable[[Any, str, int | None, str | None], Awaitable[None]]
SessionBackend = Callable[[Any], Any]


@dataclass
class RouterContext:
    sessions: dict[str, Any]
    build_sessions_list: BuildSessionsList
    broadcast_json: BroadcastJson
    persist_session_meta: PersistSessionMeta
    send_all_sessions: SendAllSessions
    spawn_task: SpawnTask
    handle_push_file: Callable[[Any, str, str], Coroutine[Any, Any, None]]
    handle_file_push_ack: Callable[[str, str], Coroutine[Any, Any, None]]
    msg_pong: Callable[[], dict]
    msg_session_history: Callable[..., dict]
    send_unread_snapshot: SendUnreadSnapshot
    send_unread_for_client_session: SendUnreadForClientSession
    mark_read: MarkRead
    persist_read_cursors: Callable[[], None]
    send_session_history_response: SendSessionHistoryResponse
    history_runtime_payload: HistoryRuntimePayload
    emit_resume_progress: EmitResumeProgress
    close_duplicate_device_clients: Callable[[Any, str], Awaitable[int]]
    log_warning: Callable[..., None]
    log_debug: Callable[..., None]
    sessions_lock: Any
    max_sessions: int
    default_cwd: str
    normalize_backend_name: Callable[[str | None], str]
    session_cls: Callable[..., Any]
    queued_command_cls: Callable[..., Any]
    msg_session_created: Callable[..., dict]
    msg_error: Callable[..., dict]
    msg_session_renamed: Callable[[str, str], dict]
    session_backend: SessionBackend
    send_event: Callable[[Any, dict], Awaitable[None]]
    evt_session_warning: Callable[[str], dict]
    evt_error: Callable[..., dict]
    persist_session: Callable[[Any], None]
    read_cursors: dict[str, Any]
    remove_saved_session: Callable[[str], None]
    invalidate_sessions_cache: Callable[[], None]
    preload_sessions_cache: Callable[[dict[str, Any]], Coroutine[Any, Any, Any]]
    backends: dict[str, Any]
    load_session_history_for_transfer: Callable[[Any, int], Awaitable[list[dict]]]
    build_handoff_prompt: Callable[[list[dict]], str]
    run_session_queue: Callable[[Any], Coroutine[Any, Any, None]]
    search_enabled: bool
    get_search_worker: Callable[[], Any]
    strip_turn_aborted_notice: Callable[[str], str]
    log_prompt_lifecycle: Callable[..., None]
    root_dir: str = ""
    data_dir: str = ""
    instance_name: str = ""
    pairing: dict = None  # type: ignore[assignment]
    stop_session_drain: Callable[[Any], Awaitable[None]] | None = None

    def __post_init__(self) -> None:
        if self.pairing is None:
            self.pairing = {}


async def handle_low_coupling_message(
    *,
    mtype: str,
    msg: dict,
    ws: Any,
    client: Any,
    ctx: RouterContext,
) -> bool:
    """Route validated WebSocket messages to the smallest owning handler."""
    if mtype == "ping":
        client.last_seen = time.time()
        await _safe_send_json(ws, ctx.msg_pong())
        return True

    if mtype == "request_sessions_list":
        await _safe_send_json(ws, ctx.build_sessions_list())
        return True

    if mtype == "hello":
        device_id = str(msg.get("device_id", "")).strip()
        if device_id:
            client.device_id = device_id[:128]
            await ctx.close_duplicate_device_clients(ws, client.device_id)
        device_name = str(msg.get("device_name", "")).strip()
        if device_name:
            client.device_name = device_name[:128]
        client.last_seen = time.time()
        _paired_token = str(ctx.pairing.get("paired_token") or "").strip()
        _provided_token = str(msg.get("auth_token") or "").strip()
        await _safe_send_json(ws, {
            "type": "hello_ack",
            "client_id": client.client_id,
            "device_id": client.device_id,
            "device_name": client.device_name,
            "is_locked": bool(_paired_token),
            "locked_to_me": bool(_paired_token) and _paired_token == _provided_token,
            "instance_name": ctx.instance_name,
            "root_dir": ctx.root_dir,
            "data_dir": ctx.data_dir,
        })
        _spawn_client_task(
            ctx,
            client,
            f"unread-snapshot:hello:{client.client_id}",
            ctx.send_unread_snapshot(_current_ws(ws, client), client),
        )
        return True

    if await handle_session_message(mtype=mtype, msg=msg, ws=ws, client=client, ctx=ctx):
        return True

    if await handle_fork_message(mtype=mtype, msg=msg, ws=ws, client=client, ctx=ctx):
        return True

    if await handle_instance_msg(mtype=mtype, msg=msg, ws=ws, client=client, ctx=ctx):
        return True

    if mtype == "request_history":
        sid = msg["session_id"]
        session = ctx.sessions.get(sid)
        if session and ctx.root_dir:
            from utils.path_jail import is_inside_jail
            if not session.cwd or not is_inside_jail(os.path.realpath(session.cwd), ctx.root_dir):
                session = None
        if not session:
            await _safe_send_json(
                ws,
                ctx.msg_session_history(
                    sid,
                    [],
                    source_count=0,
                    has_more_before=False,
                    runtime=None,
                ),
            )
            return True
        ctx.mark_read(sid, client.device_id, session.message_seq)
        ctx.persist_read_cursors()

        # Capture message args before the async task runs (msg dict may be reused)
        _limit = msg.get("limit")
        _known_last_id = str(msg.get("known_last_source_message_id") or "")
        _mode = str(msg.get("mode") or "auto")
        _before_id = str(msg.get("before_source_message_id") or "")

        # Send unread counts immediately (before the async task) so the client's
        # badge updates right away regardless of how long history loading takes.
        await ctx.send_unread_for_client_session(ws, client, session)

        # Fast-path: if the client's cursor matches our known latest, there is
        # nothing new to return.  Skip all JSONL work and reply with an empty
        # history_delta immediately.
        if (
            _known_last_id
            and not _before_id
            and session.latest_source_line
            and _known_last_id == session.latest_source_line
        ):
            async def _fast_delta() -> None:
                if not _is_current_if_registered(ws, client):
                    return
                await _safe_send_json(_current_ws(ws, client), {
                    "type": "history_delta",
                    "session_id": sid,
                    "after_source_message_id": _known_last_id,
                    "messages": [],
                    "source_count": 0,
                })
            _spawn_client_task(ctx, client, f"request-history-fast:{sid}", _fast_delta())
            return True

        async def _load_and_send() -> None:
            if not _is_current_if_registered(ws, client):
                return
            guarded_ws = _current_ws(ws, client)
            if session.resume_id:
                await ctx.emit_resume_progress(session, "resume_started", 5, "Resume started")
                await ctx.emit_resume_progress(session, "resume_loading_history", 35, "Loading history")
                try:
                    await ctx.send_session_history_response(
                        guarded_ws,
                        session,
                        limit=_limit,
                        known_last_source_message_id=_known_last_id,
                        mode=_mode,
                        before_source_message_id=_before_id,
                    )
                except Exception as exc:
                    ctx.log_warning("history response error sid=%s: %s", sid, exc)
                    await _safe_send_json(
                        guarded_ws,
                        ctx.msg_session_history(
                            sid,
                            [],
                            source_count=0,
                            has_more_before=False,
                            runtime=ctx.history_runtime_payload(session),
                        ),
                    )
                # Lazy spawn: do NOT pre-warm here. Claude/Codex spawn on first user message.
                await ctx.emit_resume_progress(session, "resume_ready", 100, "Resume ready")
            else:
                await _safe_send_json(
                    guarded_ws,
                    ctx.msg_session_history(
                        session.session_id,
                        [],
                        source_count=0,
                        has_more_before=False,
                        runtime=ctx.history_runtime_payload(session),
                    ),
                )

        # Spawn as a concurrent task so the WebSocket handler immediately proceeds
        # to the next message instead of waiting for the JSONL parse to finish.
        # Multiple request_history messages (e.g. 40+ on reconnect) now run in
        # parallel via the thread pool executor rather than serially.
        throttle_key = ("request_history", client.device_id, sid, _mode, _before_id or _known_last_id)
        if _throttle(throttle_key, 1.0):
            return True
        _spawn_client_task(ctx, client, f"request-history:{sid}", _load_and_send())
        return True

    if mtype == "set_session_meta":
        sid = msg["session_id"]
        session = ctx.sessions.get(sid)
        if not session:
            return True
        if "hidden" in msg:
            session.hidden = bool(msg["hidden"])
        ctx.persist_session_meta()
        await ctx.broadcast_json({
            "type": "session_meta_updated",
            "session_id": sid,
            "hidden": session.hidden,
        })
        return True

    if await handle_prompt_message(mtype=mtype, msg=msg, ws=ws, client=client, ctx=ctx):
        return True

    if mtype == "push_file":
        path = msg.get("path", "")
        ctx.spawn_task(
            f"push-file:{client.device_id}",
            ctx.handle_push_file(_current_ws(ws, client), path, client.device_id),
        )
        return True

    if mtype == "file_push_ack":
        file_id = msg.get("file_id", "")
        ctx.spawn_task(
            f"file-push-ack:{file_id}:{client.device_id}",
            ctx.handle_file_push_ack(file_id, client.device_id),
        )
        return True

    if mtype == "get_all_sessions":
        ctx.spawn_task(
            f"send-all-sessions:{client.client_id}",
            ctx.send_all_sessions(_current_ws(ws, client)),
        )
        return True

    if mtype == "feed_push":
        ctx.spawn_task(
            f"feed-push:{client.device_id}",
            handle_feed_push(
                _current_ws(ws, client),
                title=msg["title"],
                html=msg["html"],
                source=str(msg.get("source") or "pipeline"),
                url=str(msg.get("url") or ""),
                client_dedup_key=str(msg.get("client_dedup_key") or ""),
                content_type=str(msg.get("content_type") or "html"),
            ),
        )
        return True

    if mtype == "feed_list_request":
        if _throttle(("feed_list", client.device_id), 3.0):
            return True
        _spawn_client_task(
            ctx,
            client,
            f"feed-list:{client.device_id}",
            handle_feed_list_request(_current_ws(ws, client)),
        )
        return True

    if mtype == "feed_fetch":
        ctx.spawn_task(
            f"feed-fetch:{msg['feed_id']}",
            handle_feed_fetch(_current_ws(ws, client), feed_id=msg["feed_id"]),
        )
        return True

    if mtype == "feed_mark_read":
        ctx.spawn_task(
            f"feed-mark-read:{msg['feed_id']}",
            handle_feed_mark_read(_current_ws(ws, client), feed_id=msg["feed_id"]),
        )
        return True

    if mtype == "feed_delete":
        ctx.spawn_task(
            f"feed-delete:{msg['feed_id']}",
            handle_feed_delete(_current_ws(ws, client), feed_id=msg["feed_id"]),
        )
        return True

    return False
