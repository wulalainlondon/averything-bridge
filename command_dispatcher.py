"""Transport-neutral bridge command dispatch."""
from __future__ import annotations

from dataclasses import dataclass

from control_commands import handle_control_command


@dataclass(frozen=True)
class CommandDispatchContext:
    bv: object
    ws: object
    client: object
    system_ctx: dict
    runtime_ctx: dict
    file_ctx: dict
    router_ctx: object
    handler_func: object


def _record_perf(ctx: CommandDispatchContext, mtype: str, op_started: float) -> None:
    bv = ctx.bv
    bv._PERF.record(mtype, (bv.time.perf_counter() - op_started) * 1000.0, bv.log)


async def _handle_webrtc_signaling(
    ctx: CommandDispatchContext,
    mtype: str,
    msg: dict,
    *,
    op_started: float,
) -> bool:
    bv = ctx.bv
    ws = ctx.ws
    handler_func = ctx.handler_func

    if mtype not in bv.WEBRTC_MESSAGE_TYPES:
        return False

    async def _on_channel_ready(adapter):
        try:
            await handler_func(adapter)
        except Exception:
            bv.log.exception("[webrtc] handler raised on adapter")

    if await bv.handle_webrtc_message(mtype, msg, ws, _on_channel_ready):
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
    bv = ctx.bv
    ws = ctx.ws
    client = ctx.client
    system_ctx = ctx.system_ctx
    runtime_ctx = ctx.runtime_ctx
    file_ctx = ctx.file_ctx
    router_ctx = ctx.router_ctx

    if await bv.handle_interaction_message(
        mtype=mtype,
        msg=msg,
        ws=ws,
        sessions=bv._SESSIONS,
        session_backend=bv._session_backend,
        broadcast_json=bv._broadcast_json,
        msg_error=bv._msg_error,
    ):
        _record_perf(ctx, mtype, op_started)
        return True

    if await bv._dispatch_ws_message(ws, msg):
        _record_perf(ctx, mtype, op_started)
        return True

    if await bv.handle_system_msg(mtype, msg, ws, system_ctx):
        _record_perf(ctx, mtype, op_started)
        return True
    if await bv.handle_runtime_msg(mtype, msg, ws, runtime_ctx):
        _record_perf(ctx, mtype, op_started)
        return True
    if await bv.handle_file_msg(mtype, msg, ws, file_ctx):
        _record_perf(ctx, mtype, op_started)
        return True

    if await _handle_webrtc_signaling(ctx, mtype, msg, op_started=op_started):
        return True

    if await bv.handle_low_coupling_message(
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

    ctx.bv.log.debug("No direct handler matched for type=%s", mtype)
    _record_perf(ctx, mtype, op_started)
