"""Transport-neutral bridge command dispatch."""
from __future__ import annotations

from dataclasses import dataclass


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


async def _handle_control_command(
    ctx: CommandDispatchContext,
    mtype: str,
    msg: dict,
    *,
    op_started: float,
) -> bool:
    bv = ctx.bv
    ws = ctx.ws
    client = ctx.client

    if mtype == "hello":
        if isinstance(msg.get("device_id"), str) and msg.get("device_id", "").strip():
            client.device_id = msg["device_id"].strip()
            bv.client_manager.mark_latest(ws, client.device_id)
            await bv.client_manager.close_duplicate_device_clients(ws, client.device_id)
        if isinstance(msg.get("device_name"), str) and msg.get("device_name", "").strip():
            client.device_name = msg["device_name"].strip()
        paired_token = bv._PAIRING.get("paired_token", "").strip()
        provided_token = str(msg.get("auth_token") or "").strip()
        await bv.client_manager.send_json(ws, {
            "type": "hello_ack",
            "instance_id": bv._INSTANCE_ID,
            "gen": bv.get_generation(),
            "client_id": client.client_id,
            "device_id": client.device_id,
            "device_name": client.device_name,
            "is_locked": bool(paired_token),
            "locked_to_me": bool(paired_token) and paired_token == provided_token,
            "instance_name": bv._INSTANCE_NAME,
            "root_dir": bv._ROOT_DIR,
            "data_dir": bv._DATA_DIR,
            **({"lan_ip": bv._LAN_IP} if bv._LAN_IP else {}),
            **({"tunnel_url": bv.get_current_tunnel_url()} if bv.get_current_tunnel_url() else {}),
        }, client)
        await bv.send_pending_interactions(ws)
        _record_perf(ctx, mtype, op_started)
        return True

    if mtype == "claim_bridge":
        token = str(msg.get("auth_token") or "").strip()
        device_id = str(msg.get("device_id") or "").strip()
        if not token:
            await bv.client_manager.send_json(ws, bv._msg_error("auth_token required for claim_bridge"), client)
            _record_perf(ctx, mtype, op_started)
            return True
        existing = bv._PAIRING.get("paired_token", "").strip()
        if existing and existing != token:
            await bv.client_manager.send_json(ws, bv._msg_error("Bridge already claimed by another device"), client)
            _record_perf(ctx, mtype, op_started)
            return True
        # Synchronous block: asyncio single-thread guarantees atomicity.
        new_pairing = {"paired_token": token, "paired_device_id": device_id, "paired_at": int(bv.time.time())}
        bv._PAIRING.clear()
        bv._PAIRING.update(new_pairing)
        bv._save_pairing(bv._PAIRING)
        bv.log.info("Bridge claimed by device_id=%s", device_id)
        await bv.client_manager.send_json(ws, {"type": "claim_ack", "is_locked": True, "locked_to_me": True}, client)
        _record_perf(ctx, mtype, op_started)
        return True

    if mtype == "unclaim_bridge":
        token = str(msg.get("auth_token") or "").strip()
        paired = bv._PAIRING.get("paired_token", "").strip()
        if paired and paired != token:
            await bv.client_manager.send_json(ws, bv._msg_error("Unauthorized: token mismatch"), client)
            _record_perf(ctx, mtype, op_started)
            return True
        bv._PAIRING.clear()
        bv._clear_pairing()
        bv.log.info("Bridge unclaimed")
        await bv.client_manager.send_json(ws, {"type": "unclaim_ack", "is_locked": False}, client)
        _record_perf(ctx, mtype, op_started)
        return True

    if mtype == "get_inbox":
        inbox_conn = bv.client_manager.CLIENTS.get(ws)
        inbox_device_id = (inbox_conn.device_id if inbox_conn else "") or ""
        inbox_items = bv.pending_file_push_items(inbox_device_id, include_pushed_at=True)
        await bv.client_manager.send_json(ws, {"type": "inbox_list", "items": inbox_items}, client)
        _record_perf(ctx, mtype, op_started)
        return True

    if mtype == "tunnel_url_ack":
        bv.mark_tunnel_url_delivered()
        bv.log.info("tunnel_url_ack received — FCM retry cancelled")
        _record_perf(ctx, mtype, op_started)
        return True

    return False


async def dispatch_bridge_command(ctx: CommandDispatchContext, command, *, op_started: float) -> None:
    bv = ctx.bv
    ws = ctx.ws
    client = ctx.client
    system_ctx = ctx.system_ctx
    runtime_ctx = ctx.runtime_ctx
    file_ctx = ctx.file_ctx
    router_ctx = ctx.router_ctx
    handler_func = ctx.handler_func

    msg = command.payload
    mtype = command.type
    runtime_ctx["client"] = client

    if await _handle_control_command(ctx, mtype, msg, op_started=op_started):
        return

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
        return

    if await bv._dispatch_ws_message(ws, msg):
        _record_perf(ctx, mtype, op_started)
        return

    if await bv.handle_system_msg(mtype, msg, ws, system_ctx):
        _record_perf(ctx, mtype, op_started)
        return
    if await bv.handle_runtime_msg(mtype, msg, ws, runtime_ctx):
        _record_perf(ctx, mtype, op_started)
        return
    if await bv.handle_file_msg(mtype, msg, ws, file_ctx):
        _record_perf(ctx, mtype, op_started)
        return

    # WebRTC signaling: once the DataChannel opens we re-enter handler_func()
    # on the WebRTCChannel adapter so this same dispatch stack runs over P2P.
    if mtype in bv.WEBRTC_MESSAGE_TYPES:
        async def _on_channel_ready(adapter):
            try:
                await handler_func(adapter)
            except Exception:
                bv.log.exception("[webrtc] handler raised on adapter")
        if await bv.handle_webrtc_message(mtype, msg, ws, _on_channel_ready):
            _record_perf(ctx, mtype, op_started)
            return

    if await bv.handle_low_coupling_message(
        mtype=mtype,
        msg=msg,
        ws=ws,
        client=client,
        ctx=router_ctx,
    ):
        _record_perf(ctx, mtype, op_started)
        return

    bv.log.debug("No direct handler matched for type=%s", mtype)
    _record_perf(ctx, mtype, op_started)
