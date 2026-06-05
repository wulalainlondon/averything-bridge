"""Transport-neutral bridge command dispatch."""
from __future__ import annotations

from dataclasses import dataclass

from control_commands import handle_control_command


@dataclass(frozen=True)
class CommandDispatchContext:
    ws: object
    client: object
    system_ctx: dict
    runtime_ctx: dict
    file_ctx: dict
    router_ctx: object
    handler_func: object
    log: object
    perf: object
    perf_counter: object
    time_now: object
    sessions: object
    session_backend: object
    broadcast_json: object
    msg_error: object
    handle_interaction_message: object
    dispatch_search_message: object
    handle_system_msg: object
    handle_runtime_msg: object
    handle_file_msg: object
    webrtc_message_types: object
    handle_webrtc_message: object
    handle_low_coupling_message: object
    client_manager: object
    pairing: dict
    instance_id: str
    instance_name: str
    root_dir: str
    data_dir: str
    lan_ip: str
    get_generation: object
    get_current_tunnel_url: object
    send_pending_interactions: object
    save_pairing: object
    clear_pairing: object
    pending_file_push_items: object
    mark_tunnel_url_delivered: object


def _record_perf(ctx: CommandDispatchContext, mtype: str, op_started: float) -> None:
    ctx.perf.record(mtype, (ctx.perf_counter() - op_started) * 1000.0, ctx.log)


async def _handle_webrtc_signaling(
    ctx: CommandDispatchContext,
    mtype: str,
    msg: dict,
    *,
    op_started: float,
) -> bool:
    ws = ctx.ws
    handler_func = ctx.handler_func

    if mtype not in ctx.webrtc_message_types:
        return False

    async def _on_channel_ready(adapter):
        try:
            await handler_func(adapter)
        except Exception:
            ctx.log.exception("[webrtc] handler raised on adapter")

    if await ctx.handle_webrtc_message(mtype, msg, ws, _on_channel_ready):
        _record_perf(ctx, mtype, op_started)
        return True
    return False


async def _dispatch_route_chain(
    ctx: CommandDispatchContext,
    mtype: str,
    msg: dict,
    *,
    op_started: float,
) -> bool:
    ws = ctx.ws
    client = ctx.client
    system_ctx = ctx.system_ctx
    runtime_ctx = ctx.runtime_ctx
    file_ctx = ctx.file_ctx
    router_ctx = ctx.router_ctx

    if await ctx.handle_interaction_message(
        mtype=mtype,
        msg=msg,
        ws=ws,
        sessions=ctx.sessions,
        session_backend=ctx.session_backend,
        broadcast_json=ctx.broadcast_json,
        msg_error=ctx.msg_error,
    ):
        _record_perf(ctx, mtype, op_started)
        return True

    if await ctx.dispatch_search_message(ws, msg):
        _record_perf(ctx, mtype, op_started)
        return True

    if await ctx.handle_system_msg(mtype, msg, ws, system_ctx):
        _record_perf(ctx, mtype, op_started)
        return True
    if await ctx.handle_runtime_msg(mtype, msg, ws, runtime_ctx):
        _record_perf(ctx, mtype, op_started)
        return True
    if await ctx.handle_file_msg(mtype, msg, ws, file_ctx):
        _record_perf(ctx, mtype, op_started)
        return True

    if await _handle_webrtc_signaling(ctx, mtype, msg, op_started=op_started):
        return True

    if await ctx.handle_low_coupling_message(
        mtype=mtype,
        msg=msg,
        ws=ws,
        client=client,
        ctx=router_ctx,
    ):
        _record_perf(ctx, mtype, op_started)
        return True

    return False


async def dispatch_bridge_command(ctx: CommandDispatchContext, command, *, op_started: float) -> None:
    runtime_ctx = ctx.runtime_ctx
    msg = command.payload
    mtype = command.type
    runtime_ctx["client"] = ctx.client

    if await handle_control_command(
        ctx,
        mtype,
        msg,
        op_started=op_started,
        record_perf=_record_perf,
    ):
        return

    if await _dispatch_route_chain(ctx, mtype, msg, op_started=op_started):
        return

    ctx.log.debug("No direct handler matched for type=%s", mtype)
    _record_perf(ctx, mtype, op_started)
