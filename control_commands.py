"""Control command handlers for bridge clients."""
from __future__ import annotations

from typing import Callable


async def handle_control_command(
    ctx,
    mtype: str,
    msg: dict,
    *,
    op_started: float,
    record_perf: Callable[[object, str, float], None],
) -> bool:
    ws = ctx.ws
    client = ctx.client

    if mtype == "hello":
        if isinstance(msg.get("device_id"), str) and msg.get("device_id", "").strip():
            client.device_id = msg["device_id"].strip()
            ctx.client_manager.mark_latest(ws, client.device_id)
            await ctx.client_manager.close_duplicate_device_clients(ws, client.device_id)
        if isinstance(msg.get("device_name"), str) and msg.get("device_name", "").strip():
            client.device_name = msg["device_name"].strip()
        paired_token = ctx.pairing.get("paired_token", "").strip()
        provided_token = str(msg.get("auth_token") or "").strip()
        await ctx.client_manager.send_json(ws, {
            "type": "hello_ack",
            "instance_id": ctx.instance_id,
            "gen": ctx.get_generation(),
            "client_id": client.client_id,
            "device_id": client.device_id,
            "device_name": client.device_name,
            "is_locked": bool(paired_token),
            "locked_to_me": bool(paired_token) and paired_token == provided_token,
            "instance_name": ctx.instance_name,
            "root_dir": ctx.root_dir,
            "data_dir": ctx.data_dir,
            **({"lan_ip": ctx.lan_ip} if ctx.lan_ip else {}),
            **({"tunnel_url": ctx.get_current_tunnel_url()} if ctx.get_current_tunnel_url() else {}),
        }, client)
        await ctx.send_pending_interactions(ws)
        record_perf(ctx, mtype, op_started)
        return True

    if mtype == "claim_bridge":
        token = str(msg.get("auth_token") or "").strip()
        device_id = str(msg.get("device_id") or "").strip()
        if not token:
            await ctx.client_manager.send_json(ws, ctx.msg_error("auth_token required for claim_bridge"), client)
            record_perf(ctx, mtype, op_started)
            return True
        existing = ctx.pairing.get("paired_token", "").strip()
        if existing and existing != token:
            await ctx.client_manager.send_json(ws, ctx.msg_error("Bridge already claimed by another device"), client)
            record_perf(ctx, mtype, op_started)
            return True
        # Synchronous block: asyncio single-thread guarantees atomicity.
        new_pairing = {"paired_token": token, "paired_device_id": device_id, "paired_at": int(ctx.time_now())}
        ctx.pairing.clear()
        ctx.pairing.update(new_pairing)
        ctx.save_pairing(ctx.pairing)
        ctx.log.info("Bridge claimed by device_id=%s", device_id)
        await ctx.client_manager.send_json(ws, {"type": "claim_ack", "is_locked": True, "locked_to_me": True}, client)
        record_perf(ctx, mtype, op_started)
        return True

    if mtype == "unclaim_bridge":
        token = str(msg.get("auth_token") or "").strip()
        paired = ctx.pairing.get("paired_token", "").strip()
        if paired and paired != token:
            await ctx.client_manager.send_json(ws, ctx.msg_error("Unauthorized: token mismatch"), client)
            record_perf(ctx, mtype, op_started)
            return True
        ctx.pairing.clear()
        ctx.clear_pairing()
        ctx.log.info("Bridge unclaimed")
        await ctx.client_manager.send_json(ws, {"type": "unclaim_ack", "is_locked": False}, client)
        record_perf(ctx, mtype, op_started)
        return True

    if mtype == "get_inbox":
        inbox_conn = ctx.client_manager.CLIENTS.get(ws)
        inbox_device_id = (inbox_conn.device_id if inbox_conn else "") or ""
        inbox_items = ctx.pending_file_push_items(inbox_device_id, include_pushed_at=True)
        await ctx.client_manager.send_json(ws, {"type": "inbox_list", "items": inbox_items}, client)
        record_perf(ctx, mtype, op_started)
        return True

    if mtype == "tunnel_url_ack":
        ctx.mark_tunnel_url_delivered()
        ctx.log.info("tunnel_url_ack received — FCM retry cancelled")
        record_perf(ctx, mtype, op_started)
        return True

    return False
