"""WebSocket message routing entrypoint."""
from __future__ import annotations

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
from prompt_routes import handle_prompt_message
from route_utils import safe_send_json as _safe_send_json
from session_routes import handle_session_message


SendAllSessions = Callable[[Any], Awaitable[None]]
BroadcastJson = Callable[[dict], Awaitable[int]]
BuildSessionsList = Callable[[], dict]
PersistSessionMeta = Callable[[], None]
SpawnTask = Callable[[str, Coroutine[Any, Any, Any]], Any]
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
        ctx.spawn_task(
            f"unread-snapshot:hello:{client.client_id}",
            ctx.send_unread_snapshot(ws, client),
        )
        return True

    if await handle_session_message(mtype=mtype, msg=msg, ws=ws, client=client, ctx=ctx):
        return True

    if await handle_fork_message(mtype=mtype, msg=msg, ws=ws, client=client, ctx=ctx):
        return True

    if mtype == "request_history":
        sid = msg["session_id"]
        session = ctx.sessions.get(sid)
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
        await ctx.send_unread_for_client_session(ws, client, session)
        if session.resume_id:
            await ctx.emit_resume_progress(session, "resume_started", 5, "Resume started")
            await ctx.emit_resume_progress(session, "resume_loading_history", 35, "Loading history")
            try:
                await ctx.send_session_history_response(
                    ws,
                    session,
                    limit=msg.get("limit"),
                    known_last_source_message_id=str(msg.get("known_last_source_message_id") or ""),
                    mode=str(msg.get("mode") or "auto"),
                    before_source_message_id=str(msg.get("before_source_message_id") or ""),
                )
            except Exception as exc:
                ctx.log_warning("history response error sid=%s: %s", sid, exc)
                await _safe_send_json(
                    ws,
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
                ws,
                ctx.msg_session_history(
                    session.session_id,
                    [],
                    source_count=0,
                    has_more_before=False,
                    runtime=ctx.history_runtime_payload(session),
                ),
            )
        return True

    if mtype == "set_session_meta":
        sid = msg["session_id"]
        session = ctx.sessions.get(sid)
        if not session:
            return True
        if "pinned" in msg:
            session.pinned = bool(msg["pinned"])
        if "hidden" in msg:
            session.hidden = bool(msg["hidden"])
        ctx.persist_session_meta()
        await ctx.broadcast_json({
            "type": "session_meta_updated",
            "session_id": sid,
            "pinned": session.pinned,
            "hidden": session.hidden,
        })
        await ctx.broadcast_json(ctx.build_sessions_list())
        return True

    if await handle_prompt_message(mtype=mtype, msg=msg, ws=ws, client=client, ctx=ctx):
        return True

    if mtype == "push_file":
        path = msg.get("path", "")
        ctx.spawn_task(
            f"push-file:{client.device_id}",
            ctx.handle_push_file(ws, path, client.device_id),
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
            ctx.send_all_sessions(ws),
        )
        return True

    if mtype == "feed_push":
        ctx.spawn_task(
            f"feed-push:{client.device_id}",
            handle_feed_push(
                ws,
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
        ctx.spawn_task(
            f"feed-list:{client.device_id}",
            handle_feed_list_request(ws),
        )
        return True

    if mtype == "feed_fetch":
        ctx.spawn_task(
            f"feed-fetch:{msg['feed_id']}",
            handle_feed_fetch(ws, feed_id=msg["feed_id"]),
        )
        return True

    if mtype == "feed_mark_read":
        ctx.spawn_task(
            f"feed-mark-read:{msg['feed_id']}",
            handle_feed_mark_read(ws, feed_id=msg["feed_id"]),
        )
        return True

    if mtype == "feed_delete":
        ctx.spawn_task(
            f"feed-delete:{msg['feed_id']}",
            handle_feed_delete(ws, feed_id=msg["feed_id"]),
        )
        return True

    return False
