"""
WebSocket connection handler — the per-connection entry point.

Extracted verbatim from bridge_v2.py (zero behaviour change). All bridge_v2
module state and helpers are accessed through `bv.` so they resolve to the live
values that main()/_init_paths() assign at runtime; bridge_v2 re-exports
`handler` from here (bottom-of-file import) so `bridge_v2.handler` is unchanged.

`import bridge_v2` is deferred into the handler body (not module top-level):
when bridge_v2 runs as __main__ (`python bridge_v2.py`), a top-level import here
would re-import bridge_v2 under its real name and hit a partially-initialized
circular import. Deferring to call time avoids the cycle entirely.
"""

import inspect

from command_dispatcher import dispatch_bridge_command
from command_runtime import build_command_dispatch_context
from transport import BridgeTransport, transport_remote_address, transport_user_agent


async def handler(ws: BridgeTransport) -> None:
    import bridge_v2 as bv

    # Liveness probe short-circuit.  The supervisor's bridge_healthcheck.py
    # opens a WS every 3s, sends a control PING, then closes.  Without this
    # gate the handler would (a) register the probe as a client, (b) reassign
    # session.ws_ref on every existing session to this dying socket — causing
    # the next broadcast event to be sent to a closed socket and dropped from
    # the real app — and (c) serialize+send a 29KB sessions_list and replay
    # offline buffers into a socket that closes 12ms later.  Probes only need
    # the TCP/WS handshake to succeed and their control PING to be ponged
    # (websockets library handles control PING automatically); we just need to
    # keep the connection open until the probe closes it.
    ua = transport_user_agent(ws)
    if ua.startswith("bridge-healthcheck/"):
        try:
            async for _ in ws:
                pass  # discard any frames; probe normally sends none
        except Exception:
            pass
        return

    # Cancel pending auto-tunnel — client is back
    if bv._AUTO_TUNNEL_TASK and not bv._AUTO_TUNNEL_TASK.done():
        bv._AUTO_TUNNEL_TASK.cancel()
        bv._AUTO_TUNNEL_TASK = None

    # Proactively start tunnel so client gets the URL while still on WiFi,
    # avoiding FCM dependency on the next reconnect.
    # Skip when external tunnel mode is active (_TUNNEL_URL_FILE set): in that
    # case cloudflared_launcher.sh (managed by launchd) owns the tunnel lifecycle;
    # starting an in-process tunnel here would create a second trycloudflare URL.
    if (
        bv.os.environ.get("BRIDGE_AUTO_TUNNEL") == "1"
        and not bv._TUNNEL_URL_FILE
        and not bv._is_cloudflared_running()
    ):
        bv._spawn_task("cloudflared-start:on-connect", bv._start_cloudflared_tunnel(bv._BRIDGE_PORT))

    remote = transport_remote_address(ws)
    client = bv.ClientConn(
        client_id=f"c_{bv.uuid.uuid4().hex[:8]}",
        device_id=f"device_{bv.uuid.uuid4().hex[:8]}",
        device_name="Unknown device",
        ws=ws,
        connected_at=bv.time.time(),
        last_seen=bv.time.time(),
    )
    try:
        raw_first = await bv.asyncio.wait_for(ws.recv(), timeout=20)
        first_msg = bv.json.loads(str(raw_first))
    except bv.asyncio.TimeoutError:
        await bv.client_manager.send_json(ws, bv._msg_error("Handshake timeout: expected hello"))
        bv.client_manager.remove(ws)
        return
    except Exception:
        await bv.client_manager.send_json(ws, bv._msg_error("Handshake failed: invalid JSON"))
        bv.client_manager.remove(ws)
        return

    first_err = bv.validate_client_msg(first_msg)
    if first_err or first_msg.get("type") != "hello":
        await bv.client_manager.send_json(ws, bv._msg_error("Protocol error: first message must be hello"))
        bv.client_manager.remove(ws)
        return
    if not bv._is_auth_token_valid(first_msg):
        await bv.client_manager.send_json(ws, bv._msg_error("Unauthorized: invalid auth token"))
        bv.client_manager.remove(ws)
        return

    if isinstance(first_msg.get("device_id"), str) and first_msg.get("device_id", "").strip():
        client.device_id = first_msg["device_id"].strip()
    if isinstance(first_msg.get("device_name"), str) and first_msg.get("device_name", "").strip():
        client.device_name = first_msg["device_name"].strip()

    bv.client_manager.register(ws, client)
    await bv.client_manager.close_duplicate_device_clients(ws, client.device_id)
    bv.log.info("Client connected: %s (%s) device=%s", remote, client.client_id, client.device_id)
    bv._mark_client_activity()

    # Inject this ws into all existing sessions (reconnect scenario).
    # ws_ref must be set before hello_ack/sessions_list so live events dispatched
    # during the handshake go to this client instead of the offline buffer.
    for session in list(bv._SESSIONS.values()):
        session.ws_ref = ws

    try:
        _paired_token = bv._PAIRING.get("paired_token", "").strip()
        _provided_token = str(first_msg.get("auth_token") or "").strip()
        tunnel_url = bv.get_current_tunnel_url()
        bv.log.info("hello_ack → client=%s instance_id=%s tunnel=%s",
                 client.client_id, bv._INSTANCE_ID, bool(tunnel_url))
        ok = await bv.client_manager.send_json(ws, {
            "type": "hello_ack",
            "instance_id": bv._INSTANCE_ID,
            "gen": bv.get_generation(),
            "client_id": client.client_id,
            "device_id": client.device_id,
            "device_name": client.device_name,
            "is_locked": bool(_paired_token),
            "locked_to_me": bool(_paired_token) and _paired_token == _provided_token,
            "instance_name": bv._INSTANCE_NAME,
            "root_dir": bv._ROOT_DIR,
            "data_dir": bv._DATA_DIR,
            **({"lan_ip": bv._LAN_IP} if bv._LAN_IP else {}),
            **({"tunnel_url": tunnel_url} if tunnel_url else {}),
        }, client)
        if not ok:
            return

        async def _hydrate_reconnected_client() -> None:
            if not bv.client_manager.is_current(ws, client):
                return
            await bv.client_manager.send_json(ws, bv.build_sessions_list(), client)

            # Replay offline buffers AFTER sessions_list so the frontend has
            # hydrated its session state before it processes buffered events.
            await bv.replay_offline_buffers(ws, bv._SESSIONS.values())
            if not bv.client_manager.is_current(ws, client):
                return
            await bv._send_unread_snapshot_deferred(ws, client)
            device_id = client.device_id or ""
            for item in bv.pending_file_push_items(device_id):
                if not bv.client_manager.is_current(ws, client):
                    return
                payload = {"type": "file_push", **item}
                ok = await bv.client_manager.send_json(ws, payload, client)
                if not ok:
                    return

        try:
            spawn_sig = inspect.signature(bv._spawn_task)
            supports_owner = "owner" in spawn_sig.parameters
        except Exception:
            supports_owner = True
        if supports_owner:
            bv._spawn_task(
                f"client-hydrate:connect:{client.client_id}",
                _hydrate_reconnected_client(),
                owner=client.client_id,
            )
        else:
            bv._spawn_task(f"client-hydrate:connect:{client.client_id}", _hydrate_reconnected_client())
    except Exception:
        pass

    try:
        def _spawn_client_task(name, coro, **kwargs):
            kwargs.setdefault("owner", client.client_id)
            return bv._spawn_task(name, coro, **kwargs)

        dispatch_ctx = build_command_dispatch_context(
            bv=bv,
            ws=ws,
            client=client,
            handler_func=handler,
            spawn_client_task=_spawn_client_task,
        )
        async for raw in ws:
            op_started = bv.time.perf_counter()
            bv._mark_client_activity()
            raw_text = str(raw)
            raw_len = len(raw_text.encode("utf-8", errors="ignore"))

            command, validation_err = bv.parse_client_command(raw_text, raw_len=raw_len)
            if command is None:
                if validation_err == "invalid JSON":
                    bv.log.warning("Non-JSON from client: bytes=%d", raw_len)
                    continue
                bv.log.warning("Invalid client msg: %s | bytes=%d", validation_err, raw_len)
                await bv.client_manager.send_json(ws, bv._msg_error(f"Protocol error: {validation_err}"), client)
                continue

            msg = command.payload
            bv.log.debug("Received: %s", bv._summarize_client_msg(msg, raw_len))
            await dispatch_bridge_command(dispatch_ctx, command, op_started=op_started)

    except Exception as exc:
        name = type(exc).__name__
        if "ConnectionClosed" in name:
            bv.log.info("Client disconnected: %s (%s)", remote, exc)
        else:
            bv.log.exception("Unhandled error in handler: %s", exc)
    finally:
        bv._cancel_client_tasks(client.client_id)
        bv.client_manager.remove(ws)
        for session in list(bv._SESSIONS.values()):
            if session.ws_ref is ws:
                session.ws_ref = None
        for shell in list(bv._SHELL_SESSIONS.values()):
            if shell.ws_ref is ws:
                shell.ws_ref = None
        # Tear down any pending WebRTC PC anchored on this signaling socket
        # (the DC adapter, if promoted, has its own lifecycle).
        bv._webrtc_cleanup_for_ws(ws)
        bv.log.info("Client gone: %s (%s)", remote, client.client_id)

        if (
            bv.os.environ.get("BRIDGE_AUTO_TUNNEL") == "1"
            and not bv._TUNNEL_URL_FILE
            and not bv.client_manager.has_clients()
            and not bv._is_cloudflared_running()
        ):
            bv._AUTO_TUNNEL_TASK = bv._spawn_task("auto-tunnel-delayed", bv._auto_tunnel_after_delay(120))
